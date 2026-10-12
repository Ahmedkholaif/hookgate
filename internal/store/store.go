// Package store is hookgate's PostgreSQL state: sources and routes, the event
// and message records with their status timelines, the transactional outbox
// and the consumer inbox.
package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid request")
	ErrConflict = errors.New("conflict")
)

// messageNamespace seeds outbound message ids: id = UUIDv5(ns, eventID+"/"+topic).
// hookgate-nest uses the same namespace, so both derive identical ids.
var messageNamespace = uuid.MustParse("4b1f6c2e-7d3a-5e9b-8c1d-2f6a9e0b3c71")

func MessageID(eventID, topic string) string {
	return uuid.NewSHA1(messageNamespace, []byte(eventID+"/"+topic)).String()
}

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// ---------------------------------------------------------------- sources

type Source struct {
	Name            string    `json:"name"`
	Secret          string    `json:"secret,omitempty"`
	SignatureHeader string    `json:"signature_header"`
	IDHeader        string    `json:"id_header"`
	TypeField       string    `json:"type_field"`
	CreatedAt       time.Time `json:"created_at"`
}

const sourceCols = `name, secret, signature_header, id_header, type_field, created_at`

func scanSource(r pgx.CollectableRow) (Source, error) {
	var s Source
	err := r.Scan(&s.Name, &s.Secret, &s.SignatureHeader, &s.IDHeader, &s.TypeField, &s.CreatedAt)
	return s, err
}

var sourceName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// CreateSource registers a webhook provider. Empty fields take defaults and
// an empty secret is generated.
func (s *Store) CreateSource(ctx context.Context, in Source) (Source, error) {
	if !sourceName.MatchString(in.Name) {
		return Source{}, errors.Join(ErrInvalid, errors.New("name must match ^[a-z0-9][a-z0-9_-]{0,62}$"))
	}
	if in.Secret == "" {
		b := make([]byte, 24)
		rand.Read(b)
		in.Secret = "whsec_" + base64.RawURLEncoding.EncodeToString(b)
	}
	rows, err := s.pool.Query(ctx, `
		INSERT INTO sources (name, secret, signature_header, id_header, type_field)
		VALUES ($1, $2, coalesce(nullif($3, ''), 'Hookgate-Signature'), coalesce(nullif($4, ''), 'Webhook-Id'), coalesce(nullif($5, ''), 'type'))
		RETURNING `+sourceCols, in.Name, in.Secret, in.SignatureHeader, in.IDHeader, in.TypeField)
	if err != nil {
		return Source{}, err
	}
	src, err := pgx.CollectExactlyOneRow(rows, scanSource)
	if isUniqueViolation(err) {
		return Source{}, errors.Join(ErrConflict, fmt.Errorf("source %q already exists", in.Name))
	}
	return src, err
}

func (s *Store) GetSource(ctx context.Context, name string) (Source, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+sourceCols+` FROM sources WHERE name = $1`, name)
	if err != nil {
		return Source{}, err
	}
	src, err := pgx.CollectExactlyOneRow(rows, scanSource)
	if errors.Is(err, pgx.ErrNoRows) {
		return Source{}, ErrNotFound
	}
	return src, err
}

func (s *Store) ListSources(ctx context.Context) ([]Source, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+sourceCols+` FROM sources ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanSource)
}

// ----------------------------------------------------------------- routes

type Route struct {
	ID        string    `json:"id"`
	Source    string    `json:"source"`
	EventType string    `json:"event_type"`
	Topic     string    `json:"topic"`
	CreatedAt time.Time `json:"created_at"`
}

const routeCols = `id::text, source, event_type, topic, created_at`

func scanRoute(r pgx.CollectableRow) (Route, error) {
	var rt Route
	err := r.Scan(&rt.ID, &rt.Source, &rt.EventType, &rt.Topic, &rt.CreatedAt)
	return rt, err
}

var topicName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,79}$`)

func (s *Store) CreateRoute(ctx context.Context, source, eventType, topic string) (Route, error) {
	if eventType == "" || len(eventType) > 200 {
		return Route{}, errors.Join(ErrInvalid, errors.New("event_type is required"))
	}
	if !topicName.MatchString(topic) {
		return Route{}, errors.Join(ErrInvalid, errors.New("topic must match ^[A-Za-z][A-Za-z0-9_-]{0,79}$ (valid on RabbitMQ, SQS and Pub/Sub)"))
	}
	rows, err := s.pool.Query(ctx, `INSERT INTO routes (source, event_type, topic) VALUES ($1, $2, $3) RETURNING `+routeCols, source, eventType, topic)
	if err != nil {
		return Route{}, err
	}
	rt, err := pgx.CollectExactlyOneRow(rows, scanRoute)
	var pgErr *pgconn.PgError
	switch {
	case isUniqueViolation(err):
		return Route{}, errors.Join(ErrConflict, errors.New("route already exists"))
	case errors.As(err, &pgErr) && pgErr.Code == "23503":
		return Route{}, errors.Join(ErrNotFound, fmt.Errorf("source %q does not exist", source))
	}
	return rt, err
}

func (s *Store) ListRoutes(ctx context.Context, source string) ([]Route, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+routeCols+` FROM routes WHERE $1 = '' OR source = $1 ORDER BY source, event_type, topic`, source)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRoute)
}

func (s *Store) DeleteRoute(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM routes WHERE id::text = $1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// ----------------------------------------------------------------- ingest

type IngestResult struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate"`
}

// Ingest records an inbound webhook and its outbox row in one transaction.
// A repeat of (source, dedupKey) creates nothing and returns the original.
func (s *Store) Ingest(ctx context.Context, source, dedupKey, eventType string, payload []byte, eventsTopic, actor string) (IngestResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback(ctx)

	var r IngestResult
	var inserted bool
	// xmax = 0 only for a freshly inserted row; on conflict the UPDATE bumps
	// the duplicate counter and returns the existing row.
	err = tx.QueryRow(ctx, `
		INSERT INTO events (source, dedup_key, type, payload) VALUES ($1, $2, $3, $4)
		ON CONFLICT (source, dedup_key) DO UPDATE SET duplicates = events.duplicates + 1, updated_at = now()
		RETURNING id::text, status, xmax = 0`, source, dedupKey, eventType, payload).Scan(&r.ID, &r.Status, &inserted)
	if err != nil {
		return IngestResult{}, err
	}
	if inserted {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (kind, ref_id, topic, attempt) VALUES ('event', $1, $2, 1)`, r.ID, eventsTopic); err != nil {
			return IngestResult{}, err
		}
		if err := logStatus(ctx, tx, r.ID, "received", "", actor); err != nil {
			return IngestResult{}, err
		}
	} else {
		r.Duplicate = true
		if err := logStatus(ctx, tx, r.ID, "duplicate_dropped", "webhook re-sent with the same id", actor); err != nil {
			return IngestResult{}, err
		}
	}
	return r, tx.Commit(ctx)
}

// ----------------------------------------------------------------- outbox

// OutboxItem is a claimed outbox row with everything needed to build its envelope.
type OutboxItem struct {
	ID        int64
	Kind      string
	RefID     string
	Topic     string
	Attempt   int
	Publishes int
	Source    string
	Type      string
	EventID   string
	Payload   json.RawMessage
	ClaimedAt time.Time // database time of the claim, before the publish
}

// ClaimOutbox leases up to limit ready rows. SKIP LOCKED lets any number of
// relays (Go or NestJS) share the outbox; a relay that dies mid-publish loses
// its lease and another relay re-publishes the same ids.
func (s *Store) ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]OutboxItem, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			UPDATE outbox SET locked_until = now() + $2 * interval '1 millisecond', publishes = publishes + 1
			WHERE id IN (
				SELECT id FROM outbox
				WHERE available_at <= now() AND (locked_until IS NULL OR locked_until < now())
				ORDER BY available_at, id LIMIT $1 FOR UPDATE SKIP LOCKED)
			RETURNING id, kind, ref_id, topic, attempt, publishes)
		SELECT c.id, c.kind, c.ref_id::text, c.topic, c.attempt, c.publishes,
		       coalesce(e.source, me.source, ''), coalesce(e.type, me.type, ''),
		       coalesce(e.id, m.event_id)::text, coalesce(e.payload, m.payload), clock_timestamp()
		FROM claimed c
		LEFT JOIN events e ON c.kind = 'event' AND e.id = c.ref_id
		LEFT JOIN messages m ON c.kind = 'message' AND m.id = c.ref_id
		LEFT JOIN events me ON me.id = m.event_id
		ORDER BY c.id`, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (OutboxItem, error) {
		var it OutboxItem
		var eventID *string
		err := r.Scan(&it.ID, &it.Kind, &it.RefID, &it.Topic, &it.Attempt, &it.Publishes, &it.Source, &it.Type, &eventID, &it.Payload, &it.ClaimedAt)
		if eventID != nil {
			it.EventID = *eventID
		}
		return it, err
	})
}

// MarkPublished deletes confirmed outbox rows and advances their records.
func (s *Store) MarkPublished(ctx context.Context, items []OutboxItem, broker, actor string) error {
	ids := make([]int64, 0, len(items))
	var events, messages []string
	attempts := map[string]int{}
	for _, it := range items {
		ids = append(ids, it.ID)
		if it.Kind == "event" {
			events = append(events, it.RefID)
			attempts[it.RefID] = it.Attempt
		} else {
			messages = append(messages, it.RefID)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
		return err
	}
	// Status only advances if the record hasn't moved on, but the publish is
	// always logged: a fast consumer may process or deliver it before this
	// transaction runs. The entry is stamped with the claim time (taken before
	// the publish), so the timeline still reads in causal order.
	if _, err := tx.Exec(ctx, `
		UPDATE events SET status = 'queued', updated_at = now()
		WHERE id = ANY($1::uuid[]) AND status IN ('received', 'retrying')`, events); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE messages SET status = 'published', published_at = now()
		WHERE id = ANY($1::uuid[]) AND status = 'pending'`, messages); err != nil {
		return err
	}
	for _, it := range items {
		status, detail := "published", "published to "+broker
		if it.Kind == "event" {
			status, detail = "queued", fmt.Sprintf("attempt %d published to %s", attempts[it.RefID], broker)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO status_log (ref_id, status, detail, actor, at) VALUES ($1, $2, $3, $4, $5)`,
			it.RefID, status, detail, actor, it.ClaimedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ReleaseOutbox returns rows whose publish failed, backing off up to a minute.
func (s *Store) ReleaseOutbox(ctx context.Context, ids []int64, cause error) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET locked_until = NULL, last_error = $2,
		       available_at = now() + least(power(2, publishes), 60) * interval '1 second'
		WHERE id = ANY($1)`, ids, truncate(cause.Error(), 500))
	return err
}

// ------------------------------------------------------------- processing

type Outcome string

const (
	Processed Outcome = "processed"
	Skipped   Outcome = "skipped" // duplicate or stale delivery
	Retrying  Outcome = "retrying"
	Dead      Outcome = "dead"
)

// EventRecord is what a processing step sees.
type EventRecord struct {
	ID       string
	Source   string
	Type     string
	Payload  json.RawMessage
	Attempts int
}

// ProcessConfig controls processing and retries.
type ProcessConfig struct {
	EventsTopic string
	MaxAttempts int
	RetryDelay  func(attempt int) time.Duration
	// Work runs inside the processing transaction before routing. An error
	// fails this attempt.
	Work func(ctx context.Context, ev EventRecord) error
}

// ProcessEvent handles one delivery of an inbound event, exactly once per
// attempt: the event row is locked and the delivery's attempt number must be
// the next one, so redeliveries and stale copies are dropped. Routing writes
// messages with deterministic ids plus their outbox rows in the same
// transaction as the status change.
func (s *Store) ProcessEvent(ctx context.Context, eventID string, attempt int, cfg ProcessConfig, actor string) (Outcome, error) {
	if uuid.Validate(eventID) != nil {
		return Skipped, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	ev, status, skip, err := lockForAttempt(ctx, tx, eventID, attempt)
	if err != nil {
		return "", err
	}
	if skip != "" {
		if status != "" {
			if err := logStatus(ctx, tx, eventID, "duplicate_dropped", skip, actor); err != nil {
				return "", err
			}
		}
		return Skipped, tx.Commit(ctx)
	}

	if cfg.Work != nil {
		if werr := cfg.Work(ctx, ev); werr != nil {
			tx.Rollback(ctx)
			return s.failEvent(ctx, eventID, attempt, werr, cfg, actor)
		}
	}

	topics, err := collectStrings(tx.Query(ctx, `
		SELECT DISTINCT topic FROM routes
		WHERE source = $1 AND (event_type = '*' OR event_type = $2
		      OR (right(event_type, 2) = '.*' AND starts_with($2, left(event_type, -1))))
		ORDER BY topic`, ev.Source, ev.Type))
	if err != nil {
		return "", err
	}
	created := 0
	for _, topic := range topics {
		id := MessageID(ev.ID, topic)
		body, _ := json.Marshal(map[string]any{"event_id": ev.ID, "source": ev.Source, "type": ev.Type, "data": ev.Payload})
		tag, err := tx.Exec(ctx, `INSERT INTO messages (id, event_id, topic, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
			id, ev.ID, topic, body)
		if err != nil {
			return "", err
		}
		if tag.RowsAffected() == 0 {
			continue // already created by an earlier attempt or replay
		}
		created++
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (kind, ref_id, topic) VALUES ('message', $1, $2)`, id, topic); err != nil {
			return "", err
		}
		if err := logStatus(ctx, tx, id, "pending", "routed to "+topic, actor); err != nil {
			return "", err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE events SET status = 'processed', attempts = $2, last_error = NULL, processed_at = now(), updated_at = now()
		WHERE id = $1`, eventID, attempt); err != nil {
		return "", err
	}
	detail := fmt.Sprintf("attempt %d: %d message(s) to %d route(s)", attempt, created, len(topics))
	if err := logStatus(ctx, tx, eventID, "processed", detail, actor); err != nil {
		return "", err
	}
	return Processed, tx.Commit(ctx)
}

// lockForAttempt locks the event and decides whether this delivery may run.
// A non-empty skip reason means drop it (status is empty if the event is gone).
func lockForAttempt(ctx context.Context, tx pgx.Tx, eventID string, attempt int) (ev EventRecord, status, skip string, err error) {
	err = tx.QueryRow(ctx, `SELECT id::text, source, type, payload, attempts, status FROM events WHERE id = $1 FOR UPDATE`, eventID).
		Scan(&ev.ID, &ev.Source, &ev.Type, &ev.Payload, &ev.Attempts, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, "", "event no longer exists", nil
	}
	if err != nil {
		return ev, "", "", err
	}
	switch {
	case status == "processed" || status == "dead":
		skip = fmt.Sprintf("redelivery of attempt %d dropped: event already %s", attempt, status)
	case attempt != ev.Attempts+1:
		skip = fmt.Sprintf("stale delivery of attempt %d dropped: %d attempt(s) already ran", attempt, ev.Attempts)
	}
	return ev, status, skip, nil
}

func (s *Store) failEvent(ctx context.Context, eventID string, attempt int, cause error, cfg ProcessConfig, actor string) (Outcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	_, _, skip, err := lockForAttempt(ctx, tx, eventID, attempt)
	if err != nil || skip != "" {
		return Skipped, err // another consumer settled this attempt meanwhile
	}
	msg := truncate(cause.Error(), 500)
	outcome := Retrying
	if attempt >= cfg.MaxAttempts {
		outcome = Dead
	}
	if _, err := tx.Exec(ctx, `UPDATE events SET status = $2, attempts = $3, last_error = $4, updated_at = now() WHERE id = $1`,
		eventID, string(outcome), attempt, msg); err != nil {
		return "", err
	}
	detail := fmt.Sprintf("attempt %d/%d failed: %s", attempt, cfg.MaxAttempts, msg)
	if outcome == Retrying {
		delay := cfg.RetryDelay(attempt)
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox (kind, ref_id, topic, attempt, available_at)
			VALUES ('event', $1, $2, $3, now() + $4 * interval '1 millisecond')`,
			eventID, cfg.EventsTopic, attempt+1, delay.Milliseconds()); err != nil {
			return "", err
		}
		detail += fmt.Sprintf("; retry in %s", delay.Round(time.Millisecond))
	}
	if err := logStatus(ctx, tx, eventID, string(outcome), detail, actor); err != nil {
		return "", err
	}
	return outcome, tx.Commit(ctx)
}

// Replay re-queues a dead event with a fresh set of attempts. Messages it
// already produced keep their ids, so replay never duplicates them.
func (s *Store) Replay(ctx context.Context, eventID, eventsTopic, actor string) error {
	if uuid.Validate(eventID) != nil {
		return ErrNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM events WHERE id = $1 FOR UPDATE`, eventID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "dead" {
		return errors.Join(ErrConflict, fmt.Errorf("only dead events can be replayed (status is %s)", status))
	}
	if _, err := tx.Exec(ctx, `UPDATE events SET status = 'retrying', attempts = 0, updated_at = now() WHERE id = $1`, eventID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (kind, ref_id, topic, attempt) VALUES ('event', $1, $2, 1)`, eventID, eventsTopic); err != nil {
		return err
	}
	if err := logStatus(ctx, tx, eventID, "retrying", "replayed manually", actor); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------- inbox

// Deliver is the reference downstream consumer: it records the message id in
// the inbox in the same transaction as its side effect (marking the message
// delivered), so a redelivered message is recognised and dropped.
func (s *Store) Deliver(ctx context.Context, consumer, messageID, brokerID, actor string) (duplicate bool, err error) {
	if uuid.Validate(messageID) != nil {
		return false, errors.Join(ErrInvalid, fmt.Errorf("message id %q is not a uuid", messageID))
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, consumer, messageID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		err = logStatus(ctx, tx, messageID, "duplicate_dropped", fmt.Sprintf("%s already consumed this message (broker id %s)", consumer, brokerID), actor)
		if err != nil {
			return true, err
		}
		return true, tx.Commit(ctx)
	}
	tag, err = tx.Exec(ctx, `UPDATE messages SET status = 'delivered', delivered_at = now() WHERE id = $1 AND status <> 'delivered'`, messageID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		if err := logStatus(ctx, tx, messageID, "delivered", "consumed by "+consumer, actor); err != nil {
			return false, err
		}
	}
	return false, tx.Commit(ctx)
}

// --------------------------------------------------------------- tracking

type StatusEntry struct {
	Status string    `json:"status"`
	Detail string    `json:"detail,omitempty"`
	Actor  string    `json:"actor"`
	At     time.Time `json:"at"`
}

type Message struct {
	ID          string          `json:"id"`
	EventID     string          `json:"event_id"`
	Topic       string          `json:"topic"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at"`
	DeliveredAt *time.Time      `json:"delivered_at"`
	Timeline    []StatusEntry   `json:"timeline,omitempty"`
}

type Event struct {
	ID          string          `json:"id"`
	Source      string          `json:"source"`
	DedupKey    string          `json:"dedup_key"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	Duplicates  int             `json:"duplicates"`
	LastError   *string         `json:"last_error"`
	ReceivedAt  time.Time       `json:"received_at"`
	ProcessedAt *time.Time      `json:"processed_at"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Messages    []Message       `json:"messages,omitempty"`
	Timeline    []StatusEntry   `json:"timeline,omitempty"`
}

const eventCols = `id::text, source, dedup_key, type, status, attempts, duplicates, last_error, received_at, processed_at`

func scanEvent(r pgx.CollectableRow) (Event, error) {
	var e Event
	err := r.Scan(&e.ID, &e.Source, &e.DedupKey, &e.Type, &e.Status, &e.Attempts, &e.Duplicates, &e.LastError, &e.ReceivedAt, &e.ProcessedAt)
	return e, err
}

const messageCols = `id::text, event_id::text, topic, status, created_at, published_at, delivered_at`

func scanMessage(r pgx.CollectableRow) (Message, error) {
	var m Message
	err := r.Scan(&m.ID, &m.EventID, &m.Topic, &m.Status, &m.CreatedAt, &m.PublishedAt, &m.DeliveredAt)
	return m, err
}

type EventFilter struct {
	Status, Source, Type string
	Limit                int
}

func (s *Store) ListEvents(ctx context.Context, f EventFilter) ([]Event, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT `+eventCols+` FROM events
		WHERE ($1 = '' OR status = $1) AND ($2 = '' OR source = $2) AND ($3 = '' OR type = $3)
		ORDER BY received_at DESC, id LIMIT $4`, f.Status, f.Source, f.Type, f.Limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanEvent)
}

// GetEvent returns an event with its payload, messages and full timeline.
func (s *Store) GetEvent(ctx context.Context, id string) (Event, error) {
	if uuid.Validate(id) != nil {
		return Event{}, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT `+eventCols+`, payload FROM events WHERE id = $1`, id)
	if err != nil {
		return Event{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.Source, &e.DedupKey, &e.Type, &e.Status, &e.Attempts, &e.Duplicates, &e.LastError, &e.ReceivedAt, &e.ProcessedAt, &e.Payload)
		return e, err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, err
	}
	rows, err = s.pool.Query(ctx, `SELECT `+messageCols+` FROM messages WHERE event_id = $1 ORDER BY topic`, id)
	if err != nil {
		return Event{}, err
	}
	if e.Messages, err = pgx.CollectRows(rows, scanMessage); err != nil {
		return Event{}, err
	}
	e.Timeline, err = s.timeline(ctx, id)
	return e, err
}

func (s *Store) GetMessage(ctx context.Context, id string) (Message, error) {
	if uuid.Validate(id) != nil {
		return Message{}, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT `+messageCols+`, payload FROM messages WHERE id = $1`, id)
	if err != nil {
		return Message{}, err
	}
	m, err := pgx.CollectExactlyOneRow(rows, func(r pgx.CollectableRow) (Message, error) {
		var m Message
		err := r.Scan(&m.ID, &m.EventID, &m.Topic, &m.Status, &m.CreatedAt, &m.PublishedAt, &m.DeliveredAt, &m.Payload)
		return m, err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrNotFound
	}
	if err != nil {
		return Message{}, err
	}
	m.Timeline, err = s.timeline(ctx, id)
	return m, err
}

func (s *Store) timeline(ctx context.Context, refID string) ([]StatusEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, coalesce(detail, ''), actor, at FROM status_log WHERE ref_id = $1 ORDER BY at, id`, refID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[StatusEntry])
}

type Stats struct {
	Events            map[string]int `json:"events"`
	Messages          map[string]int `json:"messages"`
	IngestDuplicates  int            `json:"ingest_duplicates"`
	DroppedDeliveries int            `json:"dropped_redeliveries"`
	Outbox            struct {
		Pending          int     `json:"pending"`
		Ready            int     `json:"ready"`
		OldestReadyAgeMS float64 `json:"oldest_ready_age_ms"`
	} `json:"outbox"`
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{
		Events:   map[string]int{"received": 0, "queued": 0, "retrying": 0, "processed": 0, "dead": 0},
		Messages: map[string]int{"pending": 0, "published": 0, "delivered": 0},
	}
	if err := countInto(ctx, s.pool, `SELECT status, count(*) FROM events GROUP BY status`, st.Events); err != nil {
		return st, err
	}
	if err := countInto(ctx, s.pool, `SELECT status, count(*) FROM messages GROUP BY status`, st.Messages); err != nil {
		return st, err
	}
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT coalesce(sum(duplicates), 0) FROM events),
		       (SELECT count(*) FROM status_log WHERE status = 'duplicate_dropped' AND actor NOT LIKE '%/api'),
		       count(*), count(*) FILTER (WHERE available_at <= now()),
		       coalesce(extract(epoch FROM now() - min(available_at) FILTER (WHERE available_at <= now())) * 1000, 0)::float8
		FROM outbox`).Scan(&st.IngestDuplicates, &st.DroppedDeliveries, &st.Outbox.Pending, &st.Outbox.Ready, &st.Outbox.OldestReadyAgeMS)
	return st, err
}

// ---------------------------------------------------------------- helpers

func logStatus(ctx context.Context, tx pgx.Tx, refID, status, detail, actor string) error {
	_, err := tx.Exec(ctx, `INSERT INTO status_log (ref_id, status, detail, actor) VALUES ($1, $2, nullif($3, ''), $4)`, refID, status, detail, actor)
	return err
}

func countInto(ctx context.Context, pool *pgxpool.Pool, sql string, into map[string]int) error {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return err
		}
		into[k] = n
	}
	return rows.Err()
}

func collectStrings(rows pgx.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// pruneLockID makes sure only one process prunes at a time.
const pruneLockID = 727_300_002

// Prune deletes status-log entries and inbox rows older than retention, in
// small batches so it never holds long locks. Events and messages themselves
// are kept. An inbox row only matters while its message could still be
// redelivered, which no broker keeps doing for days.
func (s *Store) Prune(ctx context.Context, retention time.Duration) (int64, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, pruneLockID).Scan(&locked); err != nil || !locked {
		return 0, err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, pruneLockID)
	var total int64
	for _, q := range []string{
		`DELETE FROM status_log WHERE id IN (SELECT id FROM status_log WHERE at < now() - $1 * interval '1 second' LIMIT 5000)`,
		`DELETE FROM inbox WHERE (consumer, message_id) IN (
			SELECT consumer, message_id FROM inbox WHERE processed_at < now() - $1 * interval '1 second' LIMIT 5000)`,
	} {
		for {
			tag, err := conn.Exec(ctx, q, retention.Seconds())
			if err != nil {
				return total, err
			}
			total += tag.RowsAffected()
			if tag.RowsAffected() < 5000 {
				break
			}
		}
	}
	return total, nil
}
