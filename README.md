# hookgate

[![ci](https://github.com/Ahmedkholaif/hookgate/actions/workflows/ci.yml/badge.svg)](https://github.com/Ahmedkholaif/hookgate/actions/workflows/ci.yml)

**Webhook ingestion and fan-out in Go.** hookgate accepts webhooks from providers, verifies and dedupes them, hands them to a queue (**RabbitMQ, Amazon SQS or Google Pub/Sub**), processes them with retries and dead-lettering, and pushes the resulting messages onward over the same queues. Every message has its own status and a full timeline.

The goal is **exactly-once effects on an at-least-once pipeline.** Providers retry, brokers redeliver and workers crash mid-flight, and none of that produces a second message or a second side effect.

A NestJS twin, **[hookgate-nest](https://github.com/Ahmedkholaif/hookgate-nest)**, serves the same API on the same schema and wire format. CI runs Go and NestJS workers against the same database and queues, and they behave as one system.

```mermaid
flowchart LR
  P[Provider<br/>Stripe, Shopify, hookline…] -->|signed POST| A[api<br/>verify · dedup]
  A -->|one tx| DB[(Postgres<br/>events + outbox)]
  DB --> R[relay<br/>SKIP LOCKED]
  R -->|confirmed publish| Q{{RabbitMQ · SQS · Pub/Sub}}
  Q --> W[processor<br/>attempt fencing · routing]
  W -->|one tx| DB
  Q -->|billing · crm · audit …| C[consumers<br/>inbox dedup]
```

## How duplicates are stopped

| Where | Duplicate source | Mechanism |
|---|---|---|
| Ingest | The provider re-sends a webhook | `UNIQUE (source, dedup_key)`. The key is the provider's event id (`Webhook-Id`, `Hookline-Event-Id`, …), or a SHA-256 of the body if there is none. A repeat returns `200 {"duplicate": true}` and creates nothing. |
| Ingest → broker | Crash between "saved" and "published" | **Transactional outbox.** The event and its outbox row commit together, so nothing is lost. The relay deletes the row only after the broker confirms the publish. |
| Relay | A relay dies mid-batch and another re-publishes | Rows are leased with `FOR UPDATE SKIP LOCKED`. A re-publish carries the **same message id** (`ob-<outbox id>`). |
| Processor | Broker redelivery, or a stale copy of an old attempt | **Attempt fencing.** The event row is locked, and a delivery runs only if its attempt number is exactly `attempts + 1`. Anything else is dropped and logged as `duplicate_dropped`. |
| Fan-out | Reprocessing or a replay | Message ids are **UUIDv5(event id, topic)** and inserted with `ON CONFLICT DO NOTHING`, so the same event can never create the same message twice. |
| Consumers | Redelivery of an outbound message | **Inbox pattern.** `(consumer, message_id)` is recorded in the same transaction as the side effect. |

Retries never depend on broker features. A failed attempt schedules a new outbox row with full-jitter backoff (`available_at = now() + delay`). After `MAX_ATTEMPTS` the event is `dead` and can be replayed with `POST /v1/events/{id}/replay`. RabbitMQ, SQS and Pub/Sub therefore behave identically, and none of them needs FIFO or exactly-once delivery.

## Quick start (Docker Compose)

```bash
make up                 # Postgres + RabbitMQ + 1 api + 2 workers   (BROKER=sqs or BROKER=pubsub to switch)
make demo               # 50 signed webhooks, 10 provider retries, 1 event that always fails
```

```text
→ sending 50 webhooks (+10 provider retries, +1 that always fails)
  duplicates answered 200 (already received); flaky event: 202
→ timeline of event 45b89ba2-be1a-4387-9990-ee3148fa0531
  13:17:39.173  received            go/api
  13:17:39.211  queued              go/relay         attempt 1 published to rabbitmq
  13:17:39.221  processed           go/processor     attempt 1: 3 message(s) to 3 route(s)
  messages:
    audit: delivered
    billing: delivered
    crm: delivered
```

`make down` removes everything. The RabbitMQ management UI is at http://localhost:15672 (guest / guest).

## Kubernetes

The manifests in [`deploy/k8s`](deploy/k8s) use Kustomize. The base runs RabbitMQ, and `overlays/sqs` swaps in SQS. The API and the workers are separate Deployments from one image (`ROLES=api` vs `ROLES=relay,processor,sink`), so each scales on its own signal. Workers have an HPA and a grace period, so they drain in-flight messages on SIGTERM.

```bash
# minikube
make k8s-up                              # minikube start, build the image inside minikube, apply, wait for rollout
make k8s-demo                            # port-forward the API and run the demo
make k8s-up K8S=deploy/k8s/overlays/sqs  # same, on SQS
make k8s-down

# or by hand
minikube start --cpus=4 --memory=4g
minikube image build -t hookgate:dev .
kubectl apply -k deploy/k8s/base
kubectl -n hookgate rollout status deploy --timeout=300s
kubectl -n hookgate port-forward svc/hookgate-api 8080:8080
```

CI deploys both targets to a [kind](https://kind.sigs.k8s.io) cluster on every push and runs the smoke test through the Service (`make kind-up` does the same locally).

## API

| Method | Path | |
|---|---|---|
| `POST` | `/v1/ingest/{source}` | Webhook endpoint. Signature checked, `202` new, `200` duplicate, `401` bad or expired signature. |
| `POST` | `/v1/sources` | `{name, secret?, signature_header?, id_header?, type_field?}`. Returns the secret once. |
| `GET` | `/v1/sources`, `/v1/sources/{name}` | |
| `POST` | `/v1/routes` | `{source, event_type, topic}`. `event_type` is exact, `prefix.*` or `*`. |
| `GET` / `DELETE` | `/v1/routes`, `/v1/routes/{id}` | |
| `GET` | `/v1/events?status=&source=&type=&limit=` | Newest first. |
| `GET` | `/v1/events/{id}` | Payload, outbound messages and the full status timeline. |
| `POST` | `/v1/events/{id}/replay` | Re-queue a `dead` event. Messages it already produced are not duplicated. |
| `GET` | `/v1/messages/{id}` | An outbound message and its timeline (`pending → published → delivered`). |
| `GET` | `/v1/stats` | Counts by status, ingest duplicates, dropped redeliveries, and outbox depth and lag. |
| `GET` | `/healthz` | Served by every process, including workers. |

Signatures use `Hookgate-Signature: t=<unix>,v1=<hex HMAC-SHA256 of "t.body">` with a 5-minute tolerance. This is the scheme [hookline](https://github.com/Ahmedkholaif/hookline) signs with. A source can name a different header, so hookgate can ingest hookline deliveries directly: `{"name":"hookline","signature_header":"Hookline-Signature","id_header":"Hookline-Event-Id"}`.

Event statuses are `received → queued → processed`, or `retrying → … → dead`. Each timeline entry records which implementation did the step (`go/relay`, `nest/processor`, …).

## Configuration

| Variable | Default | |
|---|---|---|
| `ROLES` | `api,relay,processor,sink` | Any combination; split them across deployments to scale independently. |
| `DATABASE_URL` | `postgres://…/hookgate` | Migrations run on start, under an advisory lock. |
| `BROKER` | `rabbitmq` | `rabbitmq`, `sqs`, `pubsub` or `memory` |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ (quorum queues, publisher confirms). |
| `AWS_REGION`, `SQS_ENDPOINT` | `us-east-1`, empty | Empty endpoint means real AWS (default credential chain). |
| `PUBSUB_PROJECT`, `PUBSUB_EMULATOR_HOST` | `hookgate-local` | |
| `EVENTS_TOPIC` | `hookgate-events` | Queue carrying inbound events to the processors. |
| `SINK_TOPICS` | empty | Outbound topics the reference consumer reads. |
| `MAX_ATTEMPTS`, `RETRY_BASE_MS`, `RETRY_MAX_MS` | `5`, `1000`, `300000` | Processing retries (full jitter). |
| `PROCESSOR_CONCURRENCY`, `SINK_CONCURRENCY` | `16` | Handlers in flight per process (also the prefetch). |
| `RELAY_BATCH`, `RELAY_POLL_MS` | `100`, `200` | |
| `API_KEY` | empty | If set, `/v1/*` (except ingest) needs `Authorization: Bearer …`. |
| `CHAOS_FAIL_TYPES` | empty | Event types that always fail, to demo retries and dead-lettering. |

## Design notes

- **The API never talks to the broker.** Ingest is one Postgres transaction, so a broker outage doesn't fail the provider's request. The outbox absorbs it and drains when the broker returns (`/v1/stats` shows the lag).
- **Ordering is not guaranteed.** That is the trade-off for horizontal consumers on every broker. Handlers that need order should use the event's own timestamps or versions.
- **Graceful shutdown.** On SIGTERM, consumers stop taking messages, hand back anything prefetched, and let in-flight handlers commit before the process exits.
- **Every step is attributed.** Each timeline entry has an actor, so the interop test can check that Go relayed what NestJS processed, and the other way round.

## Tests

```bash
make test   # needs Postgres; see TEST_DATABASE_URL in the Makefile
```

- **End-to-end pipeline test.** 60 webhooks go through three copies of every worker on a broker that **delivers every message twice**. The test asserts that each of the 140 messages is created once and applied once, and that no event ran a second attempt.
- **Retry, dead-letter and replay**, plus **broker outage** (outbox rows are released with backoff and the event stays `received`).
- **Broker conformance suite.** Retention before a consumer exists, ids and bodies preserved, and redelivery on handler error. It runs against real RabbitMQ, ElasticMQ (SQS) and the Pub/Sub emulator in CI.
- **CI smoke test** on each broker, and on Kubernetes (kind, both overlays).

## Related

- [hookgate-nest](https://github.com/Ahmedkholaif/hookgate-nest): the same system in NestJS, interoperable with this one.
- [hookline](https://github.com/Ahmedkholaif/hookline): the outbound side, reliable webhook delivery. [hookscope](https://github.com/Ahmedkholaif/hookscope): a live webhook inspector.

MIT licensed.
