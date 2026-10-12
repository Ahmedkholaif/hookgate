package broker_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Ahmedkholaif/hookgate/internal/broker"
)

// TestConformance runs the same contract against every broker. The memory
// broker always runs; set BROKER_TEST=rabbitmq|sqs|pubsub (plus that broker's
// env, e.g. AMQP_URL) to run it against a real one.
func TestConformance(t *testing.T) {
	kinds := []string{"memory"}
	if k := os.Getenv("BROKER_TEST"); k != "" {
		kinds = append(kinds, k)
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("BROKER", kind)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			b, err := broker.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			t.Run("publish then consume every message", func(t *testing.T) { roundTrip(t, ctx, b) })
			t.Run("handler error redelivers", func(t *testing.T) { redelivery(t, ctx, b) })
		})
	}
}

func topic(t *testing.T) string {
	return fmt.Sprintf("conformance-%d", time.Now().UnixNano())
}

func roundTrip(t *testing.T, ctx context.Context, b broker.Broker) {
	tp := topic(t)
	const n = 25
	var msgs []broker.Message
	for i := range n {
		msgs = append(msgs, broker.Message{ID: fmt.Sprintf("ob-%d", i), Topic: tp, Body: fmt.Appendf(nil, `{"i":%d}`, i)})
	}
	// Publish before consuming: messages must be retained, not dropped.
	if err := b.Publish(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	seen := map[string]string{}
	done := make(chan struct{})
	go b.Consume(cctx, tp, 4, func(_ context.Context, m broker.Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen[m.ID] = string(m.Body)
		if len(seen) == n {
			close(done)
		}
		return nil
	})
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatalf("received %d of %d messages", len(seen), n)
	}
	if seen["ob-7"] != `{"i":7}` {
		t.Errorf("id/body mismatch: ob-7 => %q", seen["ob-7"])
	}
}

func redelivery(t *testing.T, ctx context.Context, b broker.Broker) {
	tp := topic(t)
	if err := b.Publish(ctx, []broker.Message{{ID: "ob-retry", Topic: tp, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	calls := 0
	done := make(chan struct{})
	go b.Consume(cctx, tp, 1, func(_ context.Context, m broker.Message) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return fmt.Errorf("transient")
		}
		if calls == 2 {
			close(done)
		}
		return nil
	})
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("nacked message was not redelivered")
	}
}
