package broker

import (
	"context"
	"sync"
)

// Memory is an in-process broker for tests and single-binary demos. With
// Duplicate set it delivers every message twice, which lets tests prove that
// consumers are idempotent.
type Memory struct {
	Duplicate bool

	mu     sync.Mutex
	queues map[string]chan Message
}

func NewMemory() *Memory { return &Memory{queues: map[string]chan Message{}} }

func (m *Memory) Name() string { return "memory" }

func (m *Memory) queue(topic string) chan Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[topic]
	if !ok {
		q = make(chan Message, 100_000)
		m.queues[topic] = q
	}
	return q
}

func (m *Memory) Publish(ctx context.Context, msgs []Message) error {
	for _, msg := range msgs {
		q := m.queue(msg.Topic)
		for range 1 + boolInt(m.Duplicate) {
			select {
			case q <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (m *Memory) Consume(ctx context.Context, topic string, concurrency int, h Handler) error {
	q := m.queue(topic)
	var wg sync.WaitGroup
	for range max(concurrency, 1) {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-q:
					if err := h(ctx, msg); err != nil {
						q <- msg // nack: redeliver
					}
				}
			}
		})
	}
	wg.Wait()
	return ctx.Err()
}

func (m *Memory) Close() error { return nil }

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
