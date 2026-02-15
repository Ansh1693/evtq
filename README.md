# EvtQ

**EvtQ** (pronounced "event queue") is a local, self-hosted Amazon SQS clone for development, testing, and small-scale production deployments.

[![Go Version](https://img.shields.io/badge/go-1.22%2B-00ADD8.svg)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)

EvtQ provides durable queueing semantics on PostgreSQL with strict concurrency safety, visibility leases, FIFO ordering guarantees, long polling, batching, and trigger-driven consumers. It is designed to be operationally simple while preserving the message-delivery behaviors developers expect from SQS-like systems.

## Table of contents

- [Feature highlights](#feature-highlights)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API surface](#api-surface)
- [EvtQ vs AWS SQS](#evtq-vs-aws-sqs)
- [Plan ahead](#plan-ahead)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Feature highlights

- Standard queues: at-least-once delivery with best-effort ordering.
- FIFO queues: strict per-group ordering with deduplication support.
- Visibility timeout: lease-based claiming from 0s to 12h.
- Delay queues: queue-level and per-message delay up to 900s.
- Long polling: up to 20s wait to reduce empty receives.
- Batch APIs: send, receive, and delete up to 10 messages.
- Trigger system: webhook/Lambda-style dispatch with partial batch failure handling.
- Durable storage: PostgreSQL-backed with transactional claiming and cleanup workers.

## Quick start

### 1) Start EvtQ + PostgreSQL

```bash
docker compose up -d
```

Example `docker-compose.yml` is available in [Deployment](./docs/deployment.md#docker-compose-development).

### 2) Create a queue

```bash
curl -sS -X POST http://localhost:8080/queues \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "queue_type": "STANDARD",
    "visibility_timeout": 30,
    "message_retention": 345600
  }'
```

### 3) Send a message

```bash
curl -sS -X POST http://localhost:8080/queues/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "body": "{\"order_id\":\"ord_1001\",\"amount\":1299}",
    "attributes": {
      "tenant": {"type":"String","value":"acme"},
      "priority": {"type":"Number","value":"1"}
    }
  }'
```

### 4) Receive a message

```bash
curl -sS -X POST "http://localhost:8080/queues/orders/messages/receive?max_messages=1&wait_time_seconds=10"
```

### 5) Delete the message

```bash
curl -sS -X DELETE http://localhost:8080/queues/orders/messages/<receiptHandle>
```

## Configuration

EvtQ supports environment variables and a `configs/config.yaml` file.

Common environment variables:

- `DATABASE_URL` (default: `postgres://localhost:5432/evtq?sslmode=disable`)
- `LISTEN_ADDR` (default: `:8080`)
- `READ_TIMEOUT` (default: `30s`)
- `WRITE_TIMEOUT` (default: `30s`)
- `IDLE_TIMEOUT` (default: `60s`)
- `MAX_BODY_BYTES` (default: `1048576`)
- `TRIGGER_REFRESH_INTERVAL` (default: `10s`)
- `CLEANUP_INTERVAL` (default: `60s`)
- `DEDUP_CLEANUP_INTERVAL` (default: `60s`)
- `HARD_DELETE_INTERVAL` (default: `24h`)

See [docs/configuration.md](./docs/configuration.md) for complete settings and YAML schema.

## API surface

Queue management:

- `POST /queues`
- `GET /queues`
- `GET /queues/{name}`
- `PUT /queues/{name}`
- `DELETE /queues/{name}`

Message operations:

- `POST /queues/{name}/messages`
- `POST /queues/{name}/messages/batch`
- `POST /queues/{name}/messages/receive`
- `DELETE /queues/{name}/messages`
- `DELETE /queues/{name}/messages/{receiptHandle}`
- `DELETE /queues/{name}/messages/batch`
- `PUT /queues/{name}/messages/{receiptHandle}/visibility`

Triggers:

- `POST /queues/{name}/triggers`
- `GET /queues/{name}/triggers`
- `GET /queues/{name}/triggers/{id}`
- `PUT /queues/{name}/triggers/{id}`
- `DELETE /queues/{name}/triggers/{id}`
- `PUT /queues/{name}/triggers/{id}/enable`
- `GET /queues/{name}/triggers/{id}/metrics`

Metrics:

- `GET /queues/{name}/stats`

Full endpoint reference: [docs/api-reference.md](./docs/api-reference.md)

## EvtQ vs AWS SQS

EvtQ is intentionally SQS-like, but not a protocol-compatible drop-in service.

Supported:

- Standard and FIFO queue semantics
- Visibility timeout
- Delay queues and per-message delay
- Long polling
- Batch APIs

Different:

- REST endpoint model differs from AWS Query API / SDKs
- IAM/authn/authz are out of scope by default
- Multi-region replication and fully managed scaling are not included

Not supported:

- DLQ and redrive APIs
- Native AWS integrations (CloudWatch, EventBridge, KMS)
- Exact parity for every SQS edge behavior

Details: [docs/sqs-compatibility.md](./docs/sqs-compatibility.md)

## Plan ahead

Possible next extensions for EvtQ:

- Message attribute filtering on triggers (fan-out without consumer-side discard).
- Queue quotas and rate limits (send/receive throttling, in-flight caps).
- Standard-queue consumer dedup helper patterns for effectively-once workflows.
- Operational dashboard for queue depth, trigger lag, and failure rates.


## Documentation

- [Architecture](./docs/architecture.md)
- [Concepts](./docs/concepts.md)
- [API reference](./docs/api-reference.md)
- [Configuration](./docs/configuration.md)
- [Deployment](./docs/deployment.md)
- [Design decisions](./docs/design-decisions.md)
- [SQS compatibility](./docs/sqs-compatibility.md)
- [Triggers deep dive](./docs/triggers.md)
- [Testing](./docs/testing.md)

## Contributing

Contributions are welcome. For development workflow and test strategy:

1. Read [docs/testing.md](./docs/testing.md)
2. Open an issue describing behavior changes
3. Submit a focused pull request with tests and docs updates

Preferred PR characteristics:

- Small, reviewable diffs
- Backward-compatible API behavior (or explicit migration notes)
- Benchmarks or load-test evidence for concurrency-sensitive changes

## License

MIT. See `LICENSE`.
