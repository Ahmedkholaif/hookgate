// Package broker is hookgate's transport: a small interface over RabbitMQ,
// Amazon SQS and Google Pub/Sub (plus an in-memory broker for tests).
//
// The broker only moves bytes. Retry scheduling lives in the database outbox
// and deduplication lives in the consumers, so every broker gets the same
// at-least-once semantics and none of hookgate's correctness depends on a
// broker-specific feature such as FIFO queues or exactly-once delivery.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Message is one unit on the wire. ID is stable across re-publishes of the
// same outbox row, so consumers can drop redeliveries.
type Message struct {
	ID    string
	Topic string
	Body  []byte
}

// Handler processes one message. Returning nil acks it; returning an error
// hands it back to the broker for redelivery.
type Handler func(ctx context.Context, m Message) error

type Broker interface {
	// Name is the broker kind: "rabbitmq", "sqs", "pubsub" or "memory".
	Name() string
	// Publish returns only after the broker has durably accepted every
	// message. On error, any subset may have been accepted; callers retry the
	// whole batch and rely on consumer-side dedup.
	Publish(ctx context.Context, msgs []Message) error
	// Consume delivers messages from topic to h, running up to concurrency
	// handlers at once, until ctx is cancelled or the connection fails.
	Consume(ctx context.Context, topic string, concurrency int, h Handler) error
	Close() error
}

// Envelope is the JSON body of every message. hookgate-nest produces and
// consumes the identical shape, so Go and NestJS workers can share queues.
type Envelope struct {
	ID       string          `json:"id"`       // broker message id: "ob-<outbox id>"
	Kind     string          `json:"kind"`     // "event" (inbound webhook) or "message" (outbound)
	RefID    string          `json:"ref_id"`   // events.id or messages.id
	Attempt  int             `json:"attempt"`  // processing attempt (events)
	Source   string          `json:"source"`   // webhook source
	Type     string          `json:"type"`     // event type
	Topic    string          `json:"topic"`    // destination topic
	EventID  string          `json:"event_id"` // originating event (same as RefID for events)
	Payload  json.RawMessage `json:"payload"`
	Producer string          `json:"producer"` // "go" or "nest"
}

func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("decode envelope: %w", err)
	}
	if e.ID == "" || e.RefID == "" || (e.Kind != "event" && e.Kind != "message") {
		return e, errors.New("decode envelope: missing id, ref_id or kind")
	}
	return e, nil
}

// Open connects to the broker named by BROKER (default "rabbitmq"), using the
// broker-specific environment variables documented in the README.
func Open(ctx context.Context) (Broker, error) {
	switch kind := strings.ToLower(getenv("BROKER", "rabbitmq")); kind {
	case "rabbitmq", "amqp":
		return NewRabbitMQ(getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
	case "sqs":
		return NewSQS(ctx, SQSConfig{
			Region:   getenv("AWS_REGION", "us-east-1"),
			Endpoint: os.Getenv("SQS_ENDPOINT"),
		})
	case "pubsub":
		return NewPubSub(ctx, getenv("PUBSUB_PROJECT", "hookgate-local"))
	case "memory":
		return NewMemory(), nil
	default:
		return nil, fmt.Errorf("unknown BROKER %q (want rabbitmq, sqs, pubsub or memory)", kind)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
