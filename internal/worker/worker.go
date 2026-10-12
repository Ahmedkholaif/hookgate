// Package worker runs hookgate's background roles: the outbox relay, the
// event processor and the reference message consumer (sink).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Ahmedkholaif/hookgate/internal/backoff"
	"github.com/Ahmedkholaif/hookgate/internal/broker"
	"github.com/Ahmedkholaif/hookgate/internal/store"
)

// Actor prefixes every status_log entry this implementation writes, so the
// timeline shows which implementation (Go or NestJS) did each step.
const Producer = "go"

func actor(role string) string { return Producer + "/" + role }

// ------------------------------------------------------------------ relay

type RelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
}

// Relay moves outbox rows to the broker. Rows are claimed with SKIP LOCKED,
// published as a batch, and deleted only after the broker confirms.
type Relay struct {
	st  *store.Store
	b   broker.Broker
	cfg RelayConfig
	log *slog.Logger
}

func NewRelay(st *store.Store, b broker.Broker, cfg RelayConfig, log *slog.Logger) *Relay {
	return &Relay{st: st, b: b, cfg: cfg, log: log.With("role", "relay")}
}

func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			r.log.Error("relay tick", "err", err)
		}
		if n < r.cfg.BatchSize { // drained; otherwise go straight to the next batch
			sleep(ctx, r.cfg.PollInterval)
		}
	}
}

// Tick publishes one batch and returns how many rows it claimed.
func (r *Relay) Tick(ctx context.Context) (int, error) {
	items, err := r.st.ClaimOutbox(ctx, r.cfg.BatchSize, r.cfg.Lease)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	msgs := make([]broker.Message, len(items))
	ids := make([]int64, len(items))
	for i, it := range items {
		ids[i] = it.ID
		env := broker.Envelope{
			ID: fmt.Sprintf("ob-%d", it.ID), Kind: it.Kind, RefID: it.RefID, Attempt: it.Attempt,
			Source: it.Source, Type: it.Type, Topic: it.Topic, EventID: it.EventID, Payload: it.Payload, Producer: Producer,
		}
		body, _ := json.Marshal(env)
		msgs[i] = broker.Message{ID: env.ID, Topic: it.Topic, Body: body}
	}
	if err := r.b.Publish(ctx, msgs); err != nil {
		// Use a fresh context: on shutdown we still want the lease released.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return len(items), errors.Join(err, r.st.ReleaseOutbox(rctx, ids, err))
	}
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return len(items), r.st.MarkPublished(mctx, items, r.b.Name(), actor("relay"))
}

// ----------------------------------------------------------------- janitor

// Janitor prunes old status-log and inbox rows (see store.Prune). It runs
// alongside the relay; an advisory lock keeps it to one process at a time.
type Janitor struct {
	St        *store.Store
	Retention time.Duration
	Every     time.Duration
	Log       *slog.Logger
}

func (j *Janitor) Run(ctx context.Context) {
	for {
		if sleep(ctx, j.Every); ctx.Err() != nil {
			return
		}
		n, err := j.St.Prune(ctx, j.Retention)
		if err != nil && ctx.Err() == nil {
			j.Log.Error("prune", "err", err)
		} else if n > 0 {
			j.Log.Info("pruned old rows", "rows", n, "retention", j.Retention)
		}
	}
}

// -------------------------------------------------------------- processor

type ProcessorConfig struct {
	EventsTopic string
	Concurrency int
	MaxAttempts int
	RetryBase   time.Duration
	RetryMax    time.Duration
	// ChaosFailTypes makes events of these types fail, to demo retries and
	// dead-lettering without a real failing dependency.
	ChaosFailTypes []string
}

type Processor struct {
	st  *store.Store
	b   broker.Broker
	cfg ProcessorConfig
	log *slog.Logger
}

func NewProcessor(st *store.Store, b broker.Broker, cfg ProcessorConfig, log *slog.Logger) *Processor {
	return &Processor{st: st, b: b, cfg: cfg, log: log.With("role", "processor")}
}

func (p *Processor) Run(ctx context.Context) {
	consumeForever(ctx, p.b, p.cfg.EventsTopic, p.cfg.Concurrency, p.Handle, p.log)
}

// Handle processes one delivery. Processing failures are scheduled for retry
// through the outbox and the delivery is acked; only infrastructure errors
// (database unreachable) return an error and go back to the broker.
func (p *Processor) Handle(ctx context.Context, m broker.Message) error {
	env, err := broker.DecodeEnvelope(m.Body)
	if err != nil || env.Kind != "event" {
		p.log.Warn("dropping malformed message", "id", m.ID, "err", err)
		return nil
	}
	out, err := p.st.ProcessEvent(ctx, env.RefID, env.Attempt, store.ProcessConfig{
		EventsTopic: p.cfg.EventsTopic,
		MaxAttempts: p.cfg.MaxAttempts,
		RetryDelay: func(attempt int) time.Duration {
			return max(backoff.FullJitter(attempt, p.cfg.RetryBase, p.cfg.RetryMax, nil), 100*time.Millisecond)
		},
		Work: p.work,
	}, actor("processor"))
	if err != nil {
		return err
	}
	p.log.Debug("event handled", "event", env.RefID, "attempt", env.Attempt, "outcome", out)
	return nil
}

func (p *Processor) work(_ context.Context, ev store.EventRecord) error {
	for _, t := range p.cfg.ChaosFailTypes {
		if t != "" && t == ev.Type {
			return fmt.Errorf("chaos: %s is configured to fail", ev.Type)
		}
	}
	return nil
}

// ------------------------------------------------------------------- sink

// Sink is the reference downstream consumer for outbound topics. It shows the
// inbox pattern: each message is applied once however often it is delivered.
type Sink struct {
	st          *store.Store
	b           broker.Broker
	topics      []string
	concurrency int
	log         *slog.Logger
}

func NewSink(st *store.Store, b broker.Broker, topics []string, concurrency int, log *slog.Logger) *Sink {
	return &Sink{st: st, b: b, topics: topics, concurrency: concurrency, log: log.With("role", "sink")}
}

func (s *Sink) Run(ctx context.Context) {
	done := make(chan struct{})
	for _, t := range s.topics {
		go func() {
			defer func() { done <- struct{}{} }()
			consumeForever(ctx, s.b, t, s.concurrency, func(ctx context.Context, m broker.Message) error {
				return s.Handle(ctx, t, m)
			}, s.log.With("topic", t))
		}()
	}
	for range s.topics {
		<-done
	}
}

func (s *Sink) Handle(ctx context.Context, topic string, m broker.Message) error {
	env, err := broker.DecodeEnvelope(m.Body)
	if err != nil || env.Kind != "message" {
		s.log.Warn("dropping malformed message", "id", m.ID, "err", err)
		return nil
	}
	_, err = s.st.Deliver(ctx, "sink:"+topic, env.RefID, env.ID, actor("sink"))
	if errors.Is(err, store.ErrInvalid) {
		return nil
	}
	return err
}

// ---------------------------------------------------------------- helpers

// consumeForever keeps a consumer attached, reconnecting with backoff when
// the broker connection drops. Handlers run on a context that shutdown doesn't
// cancel, so in-flight work commits instead of being redelivered. A handler
// error is delayed slightly before the nack so a database outage doesn't
// become a hot redelivery loop.
func consumeForever(ctx context.Context, b broker.Broker, topic string, concurrency int, h broker.Handler, log *slog.Logger) {
	wrapped := func(ctx context.Context, m broker.Message) error {
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err := h(hctx, m)
		if err != nil {
			log.Error("handler failed; message returned to broker", "id", m.ID, "err", err)
			sleep(ctx, time.Second)
		}
		return err
	}
	for attempt := 1; ctx.Err() == nil; attempt++ {
		start := time.Now()
		err := b.Consume(ctx, topic, concurrency, wrapped)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			attempt = 1
		}
		d := backoff.FullJitter(attempt, 500*time.Millisecond, 30*time.Second, nil)
		log.Warn("consumer stopped; reconnecting", "topic", topic, "err", err, "in", d.Round(time.Millisecond))
		sleep(ctx, d)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// SplitList parses a comma-separated env value.
func SplitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
