package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ahmedkholaif/hookgate/internal/api"
	"github.com/Ahmedkholaif/hookgate/internal/broker"
	"github.com/Ahmedkholaif/hookgate/internal/signature"
	"github.com/Ahmedkholaif/hookgate/internal/store"
	"github.com/Ahmedkholaif/hookgate/internal/worker"
)

const eventsTopic = "hookgate-events"

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	st   *store.Store
	srv  *httptest.Server
	log  *slog.Logger
}

func setup(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE sources, routes, events, messages, outbox, inbox, status_log CASCADE`); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	srv := httptest.NewServer(api.New(st, api.Config{EventsTopic: eventsTopic, Actor: "go/api"}, log).Handler())
	t.Cleanup(srv.Close)
	return &env{t: t, pool: pool, st: st, srv: srv, log: log}
}

func (e *env) do(method, path string, body any, hdr map[string]string) (int, map[string]any) {
	e.t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		r = bytes.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		r = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, r)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) source(name string) string {
	e.t.Helper()
	code, out := e.do("POST", "/v1/sources", map[string]any{"name": name}, nil)
	if code != 201 {
		e.t.Fatalf("create source: %d %v", code, out)
	}
	return out["secret"].(string)
}

func (e *env) route(source, eventType, topic string) {
	e.t.Helper()
	if code, out := e.do("POST", "/v1/routes", map[string]any{"source": source, "event_type": eventType, "topic": topic}, nil); code != 201 {
		e.t.Fatalf("create route: %d %v", code, out)
	}
}

func (e *env) send(source, secret, id string, payload any) (int, map[string]any) {
	e.t.Helper()
	body, _ := json.Marshal(payload)
	hdr := map[string]string{signature.Header: signature.Sign(body, time.Now(), secret)}
	if id != "" {
		hdr["Webhook-Id"] = id
	}
	return e.do("POST", "/v1/ingest/"+source, body, hdr)
}

// runWorkers starts n copies of every role on a broker and returns a stop func.
func (e *env) runWorkers(b broker.Broker, n int, cfg worker.ProcessorConfig, sinkTopics []string) func() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	cfg.EventsTopic = eventsTopic
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryBase == 0 {
		cfg.RetryBase, cfg.RetryMax = 5*time.Millisecond, 20*time.Millisecond
	}
	cfg.Concurrency = 4
	for range n {
		r := worker.NewRelay(e.st, b, worker.RelayConfig{BatchSize: 50, PollInterval: 10 * time.Millisecond, Lease: 5 * time.Second}, e.log)
		p := worker.NewProcessor(e.st, b, cfg, e.log)
		s := worker.NewSink(e.st, b, sinkTopics, 4, e.log)
		wg.Go(func() { r.Run(ctx) })
		wg.Go(func() { p.Run(ctx) })
		wg.Go(func() { s.Run(ctx) })
	}
	return func() { cancel(); wg.Wait() }
}

func (e *env) waitFor(what string, cond func(store.Stats) bool) store.Stats {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, err := e.st.Stats(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s; stats=%+v", what, st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func TestIngestValidation(t *testing.T) {
	e := setup(t)
	secret := e.source("shop")

	if code, _ := e.send("nope", secret, "x", map[string]any{"type": "a"}); code != 404 {
		t.Errorf("unknown source: got %d, want 404", code)
	}
	if code, out := e.send("shop", "wrong-secret", "x", map[string]any{"type": "a"}); code != 401 {
		t.Errorf("bad signature: got %d %v, want 401", code, out)
	}
	stale := []byte(`{"type":"a"}`)
	code, _ := e.do("POST", "/v1/ingest/shop", stale, map[string]string{signature.Header: signature.Sign(stale, time.Now().Add(-time.Hour), secret)})
	if code != 401 {
		t.Errorf("replayed old signature: got %d, want 401", code)
	}
	notJSON := []byte(`[1,2]`)
	code, _ = e.do("POST", "/v1/ingest/shop", notJSON, map[string]string{signature.Header: signature.Sign(notJSON, time.Now(), secret)})
	if code != 400 {
		t.Errorf("non-object body: got %d, want 400", code)
	}
	if n := e.count(`SELECT count(*) FROM events`); n != 0 {
		t.Errorf("rejected webhooks stored %d events", n)
	}
}

func TestIngestDedup(t *testing.T) {
	e := setup(t)
	secret := e.source("shop")

	code, first := e.send("shop", secret, "evt_1", map[string]any{"type": "order.created", "n": 1})
	if code != 202 || first["duplicate"] != false {
		t.Fatalf("first delivery: %d %v", code, first)
	}
	// Same provider id, even with a different body (providers re-sign on retry).
	code, again := e.send("shop", secret, "evt_1", map[string]any{"type": "order.created", "n": 1, "retry": true})
	if code != 200 || again["duplicate"] != true || again["id"] != first["id"] {
		t.Fatalf("redelivery: %d %v", code, again)
	}
	// No id header: byte-identical bodies dedupe by hash.
	body := map[string]any{"type": "ping"}
	if code, _ := e.send("shop", secret, "", body); code != 202 {
		t.Fatalf("hash-keyed first: %d", code)
	}
	if code, out := e.send("shop", secret, "", body); code != 200 || out["duplicate"] != true {
		t.Fatalf("hash-keyed repeat: %d %v", code, out)
	}
	if n := e.count(`SELECT count(*) FROM events`); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
	if n := e.count(`SELECT count(*) FROM outbox`); n != 2 {
		t.Errorf("outbox rows = %d, want 2 (duplicates must not enqueue)", n)
	}
	if n := e.count(`SELECT duplicates FROM events WHERE dedup_key = 'id:evt_1'`); n != 1 {
		t.Errorf("duplicates counter = %d, want 1", n)
	}
}

// TestPipelineExactlyOnceEffects runs three copies of every worker on a broker
// that delivers every message twice, and checks each outbound message is
// created once and applied downstream once.
func TestPipelineExactlyOnceEffects(t *testing.T) {
	e := setup(t)
	secret := e.source("shop")
	e.route("shop", "order.*", "billing")
	e.route("shop", "order.created", "crm")
	e.route("shop", "*", "audit")

	const n = 60
	for i := range n {
		typ := "order.created"
		if i%3 == 0 {
			typ = "customer.updated"
		}
		if code, out := e.send("shop", secret, fmt.Sprintf("evt_%d", i), map[string]any{"type": typ, "i": i}); code != 202 {
			t.Fatalf("send %d: %d %v", i, code, out)
		}
	}
	// 20 customer.updated -> audit; 40 order.created -> billing, crm, audit.
	wantMessages := 20*1 + 40*3

	b := broker.NewMemory()
	b.Duplicate = true
	stop := e.runWorkers(b, 3, worker.ProcessorConfig{}, []string{"billing", "crm", "audit"})
	defer stop()

	st := e.waitFor("all messages delivered", func(s store.Stats) bool {
		return s.Events["processed"] == n && s.Messages["delivered"] == wantMessages && s.Outbox.Pending == 0
	})
	if got := e.count(`SELECT count(*) FROM messages`); got != wantMessages {
		t.Errorf("messages = %d, want %d", got, wantMessages)
	}
	if got := e.count(`SELECT count(*) FROM inbox`); got != wantMessages {
		t.Errorf("inbox rows = %d, want %d (each message applied once)", got, wantMessages)
	}
	if got := e.count(`SELECT count(*) FROM events WHERE attempts <> 1`); got != 0 {
		t.Errorf("%d events ran more than one attempt", got)
	}
	if st.DroppedDeliveries == 0 {
		t.Error("expected duplicate deliveries to be dropped and counted")
	}

	// Status tracking: one event's full story.
	var id string
	e.pool.QueryRow(context.Background(), `SELECT id::text FROM events WHERE dedup_key = 'id:evt_1'`).Scan(&id)
	code, ev := e.do("GET", "/v1/events/"+id, nil, nil)
	if code != 200 || ev["status"] != "processed" {
		t.Fatalf("get event: %d %v", code, ev)
	}
	if msgs := ev["messages"].([]any); len(msgs) != 3 {
		t.Errorf("event has %d messages, want 3", len(msgs))
	}
	statuses := timelineStatuses(ev)
	if statuses[0] != "received" || indexOf(statuses, "queued") < 0 || indexOf(statuses, "queued") > indexOf(statuses, "processed") {
		t.Errorf("timeline should read received → queued → processed, got %v", statuses)
	}
	msgID := ev["messages"].([]any)[0].(map[string]any)["id"].(string)
	code, msg := e.do("GET", "/v1/messages/"+msgID, nil, nil)
	if code != 200 || msg["status"] != "delivered" {
		t.Fatalf("get message: %d %v", code, msg)
	}
	if s := timelineStatuses(msg); s[0] != "pending" || !contains(s, "delivered") {
		t.Errorf("message timeline = %v", s)
	}
	if msgID != store.MessageID(id, msg["topic"].(string)) {
		t.Error("message id is not the deterministic UUIDv5 of (event, topic)")
	}
}

func TestRetryDeadLetterAndReplay(t *testing.T) {
	e := setup(t)
	secret := e.source("pay")
	e.route("pay", "*", "ledger")

	_, out := e.send("pay", secret, "evt_flaky", map[string]any{"type": "payment.flaky"})
	id := out["id"].(string)

	b := broker.NewMemory()
	stop := e.runWorkers(b, 1, worker.ProcessorConfig{MaxAttempts: 3, ChaosFailTypes: []string{"payment.flaky"}}, []string{"ledger"})
	e.waitFor("event dead", func(s store.Stats) bool { return s.Events["dead"] == 1 })
	stop()

	_, ev := e.do("GET", "/v1/events/"+id, nil, nil)
	if ev["attempts"] != float64(3) || ev["last_error"] == nil {
		t.Errorf("dead event = %v", ev)
	}
	if got := countOf(timelineStatuses(ev), "retrying"); got != 2 {
		t.Errorf("retrying entries = %d, want 2", got)
	}
	if code, _ := e.do("POST", "/v1/events/"+id+"/replay", nil, nil); code != 202 {
		t.Fatalf("replay: %d", code)
	}
	if code, _ := e.do("POST", "/v1/events/"+id+"/replay", nil, nil); code != 409 {
		t.Errorf("replaying a non-dead event: got %d, want 409", code)
	}

	// The dependency recovered: same pipeline without chaos.
	stop = e.runWorkers(b, 1, worker.ProcessorConfig{}, []string{"ledger"})
	defer stop()
	e.waitFor("replayed event delivered", func(s store.Stats) bool {
		return s.Events["processed"] == 1 && s.Messages["delivered"] == 1
	})
}

func TestRelayFailureKeepsOutbox(t *testing.T) {
	e := setup(t)
	secret := e.source("shop")
	e.send("shop", secret, "evt_1", map[string]any{"type": "a"})

	r := worker.NewRelay(e.st, failingBroker{broker.NewMemory()}, worker.RelayConfig{BatchSize: 10, PollInterval: time.Millisecond, Lease: time.Second}, e.log)
	if _, err := r.Tick(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}
	if got := e.count(`SELECT count(*) FROM outbox WHERE locked_until IS NULL AND available_at > now() AND last_error IS NOT NULL`); got != 1 {
		t.Errorf("outbox row should be released with a backoff and error, got %d", got)
	}
	if got := e.count(`SELECT count(*) FROM events WHERE status = 'received'`); got != 1 {
		t.Error("event must stay 'received' until the broker confirms")
	}
}

type failingBroker struct{ *broker.Memory }

func (failingBroker) Publish(context.Context, []broker.Message) error {
	return fmt.Errorf("broker unavailable")
}

func timelineStatuses(v map[string]any) []string {
	var out []string
	for _, x := range v["timeline"].([]any) {
		out = append(out, x.(map[string]any)["status"].(string))
	}
	return out
}

func contains(s []string, v string) bool { return countOf(s, v) > 0 }

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func countOf(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}
