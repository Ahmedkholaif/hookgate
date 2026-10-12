-- hookgate schema. hookgate-nest (NestJS) ships a byte-identical copy of this
-- file under the same name, so either implementation can migrate a database
-- and run against one the other created.

-- A source is a webhook provider (Stripe, Shopify, hookline, ...). Each one
-- has its own signing secret and says where the provider puts its event id
-- and event type.
CREATE TABLE sources (
    name             text PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    secret           text NOT NULL,
    signature_header text NOT NULL DEFAULT 'Hookgate-Signature',
    id_header        text NOT NULL DEFAULT 'Webhook-Id',
    type_field       text NOT NULL DEFAULT 'type',
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- A route sends events of a type (exact, "prefix.*" or "*") from a source to
-- a broker topic.
CREATE TABLE routes (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source     text NOT NULL REFERENCES sources (name) ON DELETE CASCADE,
    event_type text NOT NULL CHECK (event_type <> ''),
    topic      text NOT NULL CHECK (topic ~ '^[A-Za-z][A-Za-z0-9_-]{0,79}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, event_type, topic)
);

-- Inbound webhooks. UNIQUE (source, dedup_key) is the ingest-side dedup: a
-- provider re-sending the same event id only bumps `duplicates`.
CREATE TABLE events (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source       text NOT NULL REFERENCES sources (name),
    dedup_key    text NOT NULL,
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    status       text NOT NULL DEFAULT 'received'
                 CHECK (status IN ('received', 'queued', 'retrying', 'processed', 'dead')),
    attempts     int NOT NULL DEFAULT 0,
    duplicates   int NOT NULL DEFAULT 0,
    last_error   text,
    received_at  timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    UNIQUE (source, dedup_key)
);
CREATE INDEX events_status_idx ON events (status, received_at DESC);
CREATE INDEX events_received_idx ON events (received_at DESC);

-- Outbound messages produced by processing an event. The id is a UUIDv5 of
-- (event id, topic), so processing the same event twice can never create a
-- second message.
CREATE TABLE messages (
    id           uuid PRIMARY KEY,
    event_id     uuid NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    topic        text NOT NULL,
    payload      jsonb NOT NULL,
    status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'published', 'delivered')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    delivered_at timestamptz
);
CREATE INDEX messages_event_idx ON messages (event_id);
CREATE INDEX messages_status_idx ON messages (status);

-- Transactional outbox: rows are written in the same transaction as the state
-- change that needs a broker publish, and deleted once the broker confirms.
-- `id` doubles as the broker message id, so a re-publish after a crash
-- carries the same id and consumers can drop it.
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind         text NOT NULL CHECK (kind IN ('event', 'message')),
    ref_id       uuid NOT NULL,
    topic        text NOT NULL,
    attempt      int NOT NULL DEFAULT 1,  -- processing attempt this publish starts (events)
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    publishes    int NOT NULL DEFAULT 0,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_ready_idx ON outbox (available_at, id);

-- Consumer-side idempotency (the inbox pattern): a consumer records each
-- message id in the same transaction as its side effects.
CREATE TABLE inbox (
    consumer     text NOT NULL,
    message_id   text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Status timeline for events and messages.
CREATE TABLE status_log (
    id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ref_id uuid NOT NULL,
    status text NOT NULL,
    detail text,
    actor  text NOT NULL,
    at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX status_log_ref_idx ON status_log (ref_id, id);
