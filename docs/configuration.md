# EvtQ Configuration

## Table of contents

- [Environment variables](#environment-variables)
- [configyaml](#configyaml)
- [Database configuration](#database-configuration)
- [Server configuration](#server-configuration)
- [Queue defaults](#queue-defaults)
- [Background workers](#background-workers)
- [Trigger settings](#trigger-settings)
- [Logging](#logging)

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://localhost:5432/evtq?sslmode=disable` | PostgreSQL DSN |
| `LISTEN_ADDR` | `:8080` | HTTP bind address |
| `READ_TIMEOUT` | `30s` | HTTP read timeout |
| `WRITE_TIMEOUT` | `30s` | HTTP write timeout |
| `IDLE_TIMEOUT` | `60s` | HTTP idle timeout |
| `MAX_BODY_BYTES` | `1048576` | Request body size limit |
| `PGX_MAX_CONNS` | `20` | Max DB pool connections |
| `PGX_MIN_CONNS` | `2` | Min DB pool connections |
| `CLEANUP_INTERVAL` | `60s` | Message expiry worker interval |
| `DEDUP_CLEANUP_INTERVAL` | `60s` | FIFO dedup cleanup interval |
| `HARD_DELETE_INTERVAL` | `24h` | Soft-delete compaction interval |
| `TRIGGER_REFRESH_INTERVAL` | `10s` | Trigger reconcile interval |
| `TRIGGER_WEBHOOK_TIMEOUT` | `30s` | Webhook dispatch timeout |
| `TRIGGER_FAILURE_THRESHOLD` | `5` | Consecutive failures before breaker behavior |
| `TRIGGER_MAX_BACKOFF` | `5m` | Max backoff for failing triggers |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |

## config.yaml

Example `configs/config.yaml`:

```yaml
server:
  listen_addr: ":8080"
  read_timeout: "30s"
  write_timeout: "30s"
  idle_timeout: "60s"
  max_body_bytes: 1048576

database:
  url: "postgres://localhost:5432/evtq?sslmode=disable"
  max_conns: 20
  min_conns: 2
  max_conn_lifetime: "30m"
  max_conn_idle_time: "5m"

queue_defaults:
  visibility_timeout: 30
  message_retention: 345600
  delay_seconds: 0

workers:
  cleanup_interval: "60s"
  dedup_cleanup_interval: "60s"
  hard_delete_interval: "24h"

triggers:
  refresh_interval: "10s"
  webhook_timeout: "30s"
  default_batch_size: 10
  default_max_concurrency: 1
  failure_threshold: 5
  max_backoff: "5m"
  max_pollers_per_trigger: 32

logging:
  level: "info"
  format: "json"
```

Precedence:

1. Environment variable
2. `config.yaml`
3. Built-in default

## Database configuration

Key recommendations:

- Use PostgreSQL 15+.
- Keep pool size proportional to CPU and expected concurrency.
- Enable `pgcrypto` extension for UUID generation.
- Prefer low network latency between EvtQ and PostgreSQL.

Baseline:

- `max_conns`: 20 for dev/small prod
- increase gradually with observed lock/wait metrics

## Server configuration

Server should fail fast on invalid payloads and not keep long stuck sockets.

- `read_timeout`: protects against slowloris behavior.
- `write_timeout`: bounds long responses.
- `max_body_bytes`: caps large message payload abuse.

## Queue defaults

| Setting | Default | Limits |
|---|---|---|
| visibility timeout | 30s | 0s..12h |
| message retention | 4 days | 60s..14 days |
| delay seconds | 0s | 0..900 |

## Background workers

- Cleanup worker: removes expired messages.
- Dedup cleanup worker: clears stale dedup records/window state.
- Hard-delete worker: compacts soft-deleted rows.

Tune intervals based on queue churn and PostgreSQL write budget.

## Trigger settings

Trigger-level values are per trigger object; global values define defaults and safety caps.

- `batch_size` 1..10
- `batch_window_seconds` 0..300
- `max_concurrency` >= 1
- `min_pollers`/`max_pollers` with min <= max
- circuit breaker uses consecutive failure threshold and exponential backoff

See [Triggers](./triggers.md) for runtime behavior.

## Logging

EvtQ supports structured logs:

- JSON logs for production parsing.
- Text logs for local development.
- Include request IDs and trigger IDs where possible.

Suggested production fields:

- `level`, `time`, `msg`
- `queue`, `trigger_id`, `target_type`
- `latency_ms`, `error`
