# Operating hookgate

This guide covers deploying, tuning and running hookgate (Go) and [hookgate-nest](https://github.com/Ahmedkholaif/hookgate-nest). Both take the same environment variables. The one difference is the HTTP port: Go uses `ADDR` (default `:8080`) and NestJS uses `PORT` (default `3000`).

## Topology

Run the API and the workers as separate deployments from the same image:

| Deployment | `ROLES` | Scale on | Notes |
|---|---|---|---|
| api | `api` | Request rate / CPU | Stateless. Needs only Postgres, not the broker. |
| workers | `relay,processor,sink` | Queue depth, `/v1/stats` → `outbox.oldest_ready_age_ms` | Each process also runs the janitor, under a lock so only one prunes at a time. |

Any number of relays can share the outbox (`FOR UPDATE SKIP LOCKED`), and any number of processors can share a queue. Every process serves `GET /healthz` for liveness and readiness probes.

## Tuning

| Variable | Default | Guidance |
|---|---|---|
| `DB_MAX_CONNS` | `20` | Each in-flight handler holds one connection for its transaction. Keep it at or above `PROCESSOR_CONCURRENCY + SINK_CONCURRENCY` for workers. Check that *(replicas × DB_MAX_CONNS)* stays under Postgres `max_connections`, or put PgBouncer (transaction mode) in front. |
| `PROCESSOR_CONCURRENCY`, `SINK_CONCURRENCY` | `16` | Handlers in flight per process. This is also the broker prefetch (RabbitMQ `prefetch = 2×`, SQS batch ≤ 10, Pub/Sub `MaxOutstandingMessages`). |
| `RELAY_BATCH`, `RELAY_POLL_MS` | `100`, `200` | When a batch comes back full, the relay goes straight to the next one. The poll interval only adds latency when the outbox is idle. |
| `MAX_ATTEMPTS`, `RETRY_BASE_MS`, `RETRY_MAX_MS` | `5`, `1000`, `300000` | Full-jitter backoff. The worst-case time to dead-letter is roughly the sum of the capped delays. |
| `RETENTION_DAYS` | `14` | How long status timelines and consumer inbox rows are kept. `0` disables pruning. |

**Broker timeouts.** A handler is cut off after 30 seconds. SQS queues are created with a 60-second visibility timeout and Pub/Sub subscriptions with a 60-second ack deadline, so a slow handler can't be redelivered while it is still running. If you pre-create the queues yourself, keep those values above 30 seconds.

## Monitoring

`GET /v1/stats` returns everything worth alerting on:

| Field | Alert when |
|---|---|
| `outbox.oldest_ready_age_ms` | It stays above a few seconds: the relays can't keep up or the broker is down (the API keeps accepting webhooks meanwhile). |
| `events.dead`, `events.retrying` | They grow: processing is failing. Use `last_error` on the events to see why. |
| `messages.pending`, `messages.published` | They grow without `delivered` following: consumers are behind or down. |
| `ingest_duplicates`, `dropped_redeliveries` | These are informational. They show the dedup layers working. |

Logs are JSON (Go) or Nest's logger, one line per failure, with the message id.

## Runbook

- **Dead events.** Run `GET /v1/events?status=dead` and read `last_error` and the timeline. Fix the cause, then `POST /v1/events/{id}/replay`. Replays never duplicate messages the event already produced.
- **Broker outage.** Nothing is lost. Ingest keeps writing to Postgres and the outbox grows. Relays retry with backoff of up to 60 seconds per row and drain once the broker is back. Consumers reconnect on their own.
- **Postgres outage.** Ingest returns 500, so providers retry, which is what you want. Workers nack and the broker redelivers after a short delay.
- **Poison message.** Malformed envelopes are logged and acked, so they never block a queue.
- **Rolling deploys.** Migrations run on start under an advisory lock, and every migration is additive. On SIGTERM a process stops taking messages, hands prefetched ones back, and lets in-flight handlers commit. Give workers a grace period of at least 45 seconds.

## Security checklist

- [ ] Set `API_KEY`. Without it the admin API (`/v1/*`) is open, and the process logs a warning at startup. Ingest stays public; signatures protect it.
- [ ] Terminate TLS in front of the API (ingress or load balancer). Providers sign the body, but TLS keeps payloads private.
- [ ] Give each source its own signing secret (`POST /v1/sources` generates one). Rotate by creating a new source, or have the sender sign with both keys: `v1` signatures can be repeated in the header.
- [ ] The signature tolerance is 5 minutes. Keep server clocks in sync (NTP).
- [ ] Use a least-privilege database role. It needs DDL on first start for migrations, then DML only.

## Guarantees and limits

- **At-least-once to the broker, exactly-once effects inside hookgate.** Consumers you write yourself should dedupe on the message id. The reference sink shows the inbox pattern.
- **No global ordering.** It is the trade-off for horizontal consumers on every broker. Use the event's own timestamps or versions when order matters.
- **Webhook bodies are capped at 1 MiB.**
