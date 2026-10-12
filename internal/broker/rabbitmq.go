package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ publishes to durable quorum queues (one per topic, through the
// default exchange) with publisher confirms, and consumes with manual acks.
type RabbitMQ struct {
	url string

	mu       sync.Mutex // guards conn, pubCh and declared
	conn     *amqp.Connection
	pubCh    *amqp.Channel
	declared map[string]bool
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	r := &RabbitMQ{url: url, declared: map[string]bool{}}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.connLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RabbitMQ) Name() string { return "rabbitmq" }

// connLocked returns a live connection, redialling if the old one dropped.
func (r *RabbitMQ) connLocked() (*amqp.Connection, error) {
	if r.conn != nil && !r.conn.IsClosed() {
		return r.conn, nil
	}
	conn, err := amqp.DialConfig(r.url, amqp.Config{Heartbeat: 10 * time.Second, Properties: amqp.Table{"connection_name": "hookgate"}})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	r.conn, r.pubCh, r.declared = conn, nil, map[string]bool{}
	return conn, nil
}

func declare(ch *amqp.Channel, topic string) error {
	_, err := ch.QueueDeclare(topic, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	return err
}

func (r *RabbitMQ) Publish(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.publishLocked(ctx, msgs); err != nil {
		// Drop the channel so the next batch starts on a fresh one.
		if r.pubCh != nil {
			r.pubCh.Close()
			r.pubCh = nil
		}
		return err
	}
	return nil
}

func (r *RabbitMQ) publishLocked(ctx context.Context, msgs []Message) error {
	conn, err := r.connLocked()
	if err != nil {
		return err
	}
	if r.pubCh == nil || r.pubCh.IsClosed() {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return err
		}
		r.pubCh = ch
	}
	confirms := make([]*amqp.DeferredConfirmation, 0, len(msgs))
	for _, m := range msgs {
		if !r.declared[m.Topic] {
			if err := declare(r.pubCh, m.Topic); err != nil {
				return fmt.Errorf("rabbitmq declare %s: %w", m.Topic, err)
			}
			r.declared[m.Topic] = true
		}
		c, err := r.pubCh.PublishWithDeferredConfirmWithContext(ctx, "", m.Topic, false, false, amqp.Publishing{
			MessageId:    m.ID,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         m.Body,
		})
		if err != nil {
			return fmt.Errorf("rabbitmq publish: %w", err)
		}
		confirms = append(confirms, c)
	}
	// Publishes are pipelined; wait for the broker to confirm all of them.
	for _, c := range confirms {
		ok, err := c.WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("rabbitmq confirm: %w", err)
		}
		if !ok {
			return errors.New("rabbitmq: broker nacked a publish")
		}
	}
	return nil
}

func (r *RabbitMQ) Consume(ctx context.Context, topic string, concurrency int, h Handler) error {
	concurrency = max(concurrency, 1)
	r.mu.Lock()
	conn, err := r.connLocked()
	r.mu.Unlock()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer closeWithin(ch, 5*time.Second)
	if err := declare(ch, topic); err != nil {
		return err
	}
	if err := ch.Qos(concurrency*2, 0, false); err != nil {
		return err
	}
	tag := fmt.Sprintf("hookgate-%s-%d", topic, time.Now().UnixNano())
	deliveries, err := ch.Consume(topic, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			// Stop the server sending more, then hand back anything already
			// prefetched so another consumer picks it up immediately.
			ch.Cancel(tag, false)
			for d := range deliveries {
				d.Nack(false, true)
			}
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("rabbitmq: consumer channel closed")
			}
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				if err := h(ctx, Message{ID: d.MessageId, Topic: topic, Body: d.Body}); err != nil {
					d.Nack(false, true)
					return
				}
				d.Ack(false)
			})
		}
	}
}

// closeWithin closes ch without letting a wedged close (e.g. a connection
// that died mid-handshake) block shutdown.
func closeWithin(ch *amqp.Channel, d time.Duration) {
	done := make(chan struct{})
	go func() { ch.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
	}
}

func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
