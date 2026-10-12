package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PubSub maps each topic to a Pub/Sub topic plus one shared subscription
// ("<topic>-hookgate") that all hookgate consumers of that topic compete on.
// Topics and subscriptions are created on first use. Set PUBSUB_EMULATOR_HOST
// to use the local emulator.
type PubSub struct {
	client *pubsub.Client

	mu         sync.Mutex
	publishers map[string]*pubsub.Publisher
	ready      map[string]bool // topics/subscriptions known to exist
}

func NewPubSub(ctx context.Context, project string) (*PubSub, error) {
	c, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("pubsub client: %w", err)
	}
	return &PubSub{client: c, publishers: map[string]*pubsub.Publisher{}, ready: map[string]bool{}}, nil
}

func (p *PubSub) Name() string { return "pubsub" }

func (p *PubSub) topicName(topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", p.client.Project(), topic)
}

func (p *PubSub) subName(topic string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s-hookgate", p.client.Project(), topic)
}

// ensure creates the topic and its subscription if missing. Creating the
// subscription up front means messages published before any consumer starts
// are retained rather than dropped.
func (p *PubSub) ensure(ctx context.Context, topic string) error {
	p.mu.Lock()
	ok := p.ready[topic]
	p.mu.Unlock()
	if ok {
		return nil
	}
	_, err := p.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: p.topicName(topic)})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsub create topic %s: %w", topic, err)
	}
	_, err = p.client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               p.subName(topic),
		Topic:              p.topicName(topic),
		AckDeadlineSeconds: 60,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsub create subscription %s: %w", topic, err)
	}
	p.mu.Lock()
	p.ready[topic] = true
	p.mu.Unlock()
	return nil
}

func (p *PubSub) publisher(topic string) *pubsub.Publisher {
	p.mu.Lock()
	defer p.mu.Unlock()
	pub, ok := p.publishers[topic]
	if !ok {
		pub = p.client.Publisher(p.topicName(topic))
		pub.PublishSettings.CountThreshold = 100
		pub.PublishSettings.DelayThreshold = 5 * time.Millisecond
		p.publishers[topic] = pub
	}
	return pub
}

func (p *PubSub) Publish(ctx context.Context, msgs []Message) error {
	results := make([]*pubsub.PublishResult, 0, len(msgs))
	for _, m := range msgs {
		if err := p.ensure(ctx, m.Topic); err != nil {
			return err
		}
		// The client batches these into a few RPCs; Get waits for the server ack.
		results = append(results, p.publisher(m.Topic).Publish(ctx, &pubsub.Message{
			Data:       m.Body,
			Attributes: map[string]string{idAttr: m.ID},
		}))
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			return fmt.Errorf("pubsub publish: %w", err)
		}
	}
	return nil
}

func (p *PubSub) Consume(ctx context.Context, topic string, concurrency int, h Handler) error {
	if err := p.ensure(ctx, topic); err != nil {
		return err
	}
	sub := p.client.Subscriber(p.subName(topic))
	sub.ReceiveSettings.MaxOutstandingMessages = max(concurrency, 1)
	sub.ReceiveSettings.NumGoroutines = 1
	err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		id := m.Attributes[idAttr]
		if id == "" {
			id = m.ID
		}
		if err := h(ctx, Message{ID: id, Topic: topic, Body: m.Data}); err != nil {
			m.Nack()
			return
		}
		m.Ack()
	})
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (p *PubSub) Close() error {
	p.mu.Lock()
	for _, pub := range p.publishers {
		pub.Stop()
	}
	p.mu.Unlock()
	return p.client.Close()
}
