# EvtQ Testing Guide

## Table of contents

- [Run test suite](#run-test-suite)
- [Test categories](#test-categories)
- [Key scenarios](#key-scenarios)
- [Test database setup](#test-database-setup)
- [Trigger testing locally](#trigger-testing-locally)
- [Benchmarking approach](#benchmarking-approach)

## Run test suite

From project root:

```bash
go test ./...
```

With race detector:

```bash
go test -race ./...
```

Verbose:

```bash
go test -v ./...
```

## Test categories

- **Unit tests**: validation, service-level behavior, mapper logic.
- **Integration tests**: PostgreSQL-backed receive/delete/visibility flows.
- **Concurrency tests**: parallel receives with lock contention.
- **Stress tests**: high message volume and trigger throughput.

## Key scenarios

### Queue/message core

- Create/update/delete queue constraints.
- Send/receive/delete lifecycle correctness.
- Visibility timeout reappearance.
- Long polling returns quickly on notify/new message.

### FIFO

- Per-group sequential ordering under concurrency.
- Cross-group parallel receive.
- Dedup behavior for repeated dedup IDs.

### Triggers

- Webhook success path deletes messages.
- Partial batch failures retain only listed items.
- Backoff activates after repeated failures.
- Trigger metrics correctness.

## Test database setup

Run PostgreSQL for tests:

```bash
docker run -d --name evtq-test-pg \
  -e POSTGRES_USER=evtq \
  -e POSTGRES_PASSWORD=evtq \
  -e POSTGRES_DB=evtq_test \
  -p 55432:5432 postgres:16
```

Set test DSN:

```bash
export DATABASE_URL="postgres://evtq:evtq@localhost:55432/evtq_test?sslmode=disable"
```

Apply migrations before integration tests.

## Trigger testing locally

Run mock webhook:

```bash
python3 -m http.server 9000
```

For realistic behavior, use a small mock server that:

- returns `200` for full success
- returns `{"batch_item_failures":[...]}` for partial failures
- intentionally returns `500` to validate retry/backoff

Then create trigger:

```bash
curl -sS -X POST http://localhost:8080/queues/orders/triggers \
  -H "Content-Type: application/json" \
  -d '{"target_type":"webhook","target_url":"http://localhost:9000/hook","batch_size":3}'
```

## Benchmarking approach

Use repeatable synthetic workloads:

- N producers, M consumers, fixed message size distributions.
- Separate tests for Standard and FIFO workloads.
- Measure:
  - send throughput
  - receive throughput
  - p95/p99 end-to-end latency
  - DB CPU/IO and lock waits
  - trigger dispatch success/failure rates

Sample benchmark command:

```bash
go test -run '^$' -bench . -benchmem ./...
```

For system-level benchmarking, run dedicated load client against a non-shared PostgreSQL instance and collect database metrics concurrently.

See also:

- [Architecture](./architecture.md)
- [Configuration](./configuration.md)
