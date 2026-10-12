package broker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSConfig struct {
	Region   string
	Endpoint string // e.g. http://localhost:9324 for ElasticMQ; empty for AWS
}

// SQS maps each topic to a standard queue, created on first use. It publishes
// with SendMessageBatch (10 per call) and consumes with long polling.
type SQS struct {
	client *sqs.Client

	mu   sync.Mutex
	urls map[string]string
}

const idAttr = "hookgate-id"

func NewSQS(ctx context.Context, c SQSConfig) (*SQS, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(c.Region)}
	if c.Endpoint != "" {
		// Local emulators accept any credentials; don't require real ones.
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	return &SQS{client: client, urls: map[string]string{}}, nil
}

func (s *SQS) Name() string { return "sqs" }

func (s *SQS) queueURL(ctx context.Context, topic string) (string, error) {
	s.mu.Lock()
	url, ok := s.urls[topic]
	s.mu.Unlock()
	if ok {
		return url, nil
	}
	// CreateQueue is idempotent for identical attributes and returns the URL.
	out, err := s.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(topic),
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	if err != nil {
		return "", fmt.Errorf("sqs create queue %s: %w", topic, err)
	}
	s.mu.Lock()
	s.urls[topic] = *out.QueueUrl
	s.mu.Unlock()
	return *out.QueueUrl, nil
}

func (s *SQS) Publish(ctx context.Context, msgs []Message) error {
	byTopic := map[string][]Message{}
	for _, m := range msgs {
		byTopic[m.Topic] = append(byTopic[m.Topic], m)
	}
	for topic, ms := range byTopic {
		url, err := s.queueURL(ctx, topic)
		if err != nil {
			return err
		}
		for chunk := range slices.Chunk(ms, 10) {
			entries := make([]types.SendMessageBatchRequestEntry, len(chunk))
			for i, m := range chunk {
				entries[i] = types.SendMessageBatchRequestEntry{
					Id:          aws.String(strconv.Itoa(i)),
					MessageBody: aws.String(string(m.Body)),
					MessageAttributes: map[string]types.MessageAttributeValue{
						idAttr: {DataType: aws.String("String"), StringValue: aws.String(m.ID)},
					},
				}
			}
			out, err := s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(url), Entries: entries})
			if err != nil {
				return fmt.Errorf("sqs send batch: %w", err)
			}
			if len(out.Failed) > 0 {
				f := out.Failed[0]
				return fmt.Errorf("sqs: %d of %d messages rejected (%s: %s)", len(out.Failed), len(chunk), aws.ToString(f.Code), aws.ToString(f.Message))
			}
		}
	}
	return nil
}

func (s *SQS) Consume(ctx context.Context, topic string, concurrency int, h Handler) error {
	concurrency = max(concurrency, 1)
	url, err := s.queueURL(ctx, topic)
	if err != nil {
		return err
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for ctx.Err() == nil {
		out, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(url),
			MaxNumberOfMessages:   int32(min(concurrency, 10)),
			WaitTimeSeconds:       10,
			MessageAttributeNames: []string{idAttr},
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("sqs receive: %w", err)
		}
		for _, m := range out.Messages {
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				id := aws.ToString(m.MessageId)
				if a, ok := m.MessageAttributes[idAttr]; ok {
					id = aws.ToString(a.StringValue)
				}
				// Settle with a context that outlives shutdown so in-flight
				// work is acked rather than redelivered.
				settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				if err := h(ctx, Message{ID: id, Topic: topic, Body: []byte(aws.ToString(m.Body))}); err != nil {
					// Make it visible again soon instead of after the full timeout.
					s.client.ChangeMessageVisibility(settle, &sqs.ChangeMessageVisibilityInput{
						QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 2,
					})
					return
				}
				s.client.DeleteMessage(settle, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
			})
		}
	}
	return ctx.Err()
}

func (s *SQS) Close() error { return nil }
