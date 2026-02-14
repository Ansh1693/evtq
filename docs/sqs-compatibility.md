# EvtQ SQS Compatibility

## Table of contents

- [Compatibility matrix](#compatibility-matrix)
- [Behavior differences](#behavior-differences)
- [API mapping](#api-mapping)
- [Migration guide](#migration-guide)
- [Known limitations](#known-limitations)

## Compatibility matrix

| Feature | AWS SQS | EvtQ status | Notes |
|---|---|---|---|
| Standard queues | Yes | Supported | At-least-once, best-effort ordering |
| FIFO queues | Yes | Supported | Per-group ordering and dedup |
| Visibility timeout | Yes | Supported | Queue-level + per-receive override |
| Delay queues | Yes | Supported | Queue + per-message delay |
| Long polling | Yes | Supported | Up to 20 seconds |
| Batch send/receive/delete | Yes | Supported | Max 10 entries |
| Message attributes | Yes | Supported | Typed key-value JSON model |
| DLQ redrive policy | Yes | Not Supported | DLQ and redrive are intentionally out of current EvtQ scope |
| Queue metrics | Yes | Supported | Approximate counts |
| Trigger invocation | No native SQS (separate integrations) | Supported (EvtQ feature) | Webhook/Lambda-style poller |
| IAM / authz | Yes | Not supported by default | Add via proxy/gateway |
| KMS encryption | Yes | Not supported | Use PG-level encryption strategy |
| CloudWatch integration | Yes | Not supported | Use custom metrics pipeline |
| Multi-AZ managed durability | Yes | Different | Depends on PostgreSQL deployment |

## Behavior differences

- EvtQ uses REST endpoints, not AWS Query API.
- EvtQ does not emulate AWS account/region model.
- Receipt-handle and lease semantics are SQS-like but API names differ.
- Trigger subsystem is built-in and queue-driven, unlike AWS's separate event source mapping service model.
- Operational scaling characteristics depend on PostgreSQL capacity.

## API mapping

| SQS action | EvtQ endpoint |
|---|---|
| `CreateQueue` | `POST /queues` |
| `ListQueues` | `GET /queues` |
| `GetQueueAttributes` | `GET /queues/{name}` |
| `SetQueueAttributes` | `PUT /queues/{name}` |
| `DeleteQueue` | `DELETE /queues/{name}` |
| `SendMessage` | `POST /queues/{name}/messages` |
| `SendMessageBatch` | `POST /queues/{name}/messages/batch` |
| `ReceiveMessage` | `POST /queues/{name}/messages/receive` |
| `DeleteMessage` | `DELETE /queues/{name}/messages/{receiptHandle}` |
| `DeleteMessageBatch` | `DELETE /queues/{name}/messages/batch` |
| `ChangeMessageVisibility` | `PUT /queues/{name}/messages/{receiptHandle}/visibility` |
| `PurgeQueue` | `DELETE /queues/{name}/messages` |

## Migration guide

If you currently use SQS and want local integration tests with EvtQ:

1. Replace AWS SDK queue actions with HTTP client calls to EvtQ endpoints.
2. Keep message contract unchanged (`body`, attributes, group/dedup IDs).
3. Map queue URL/name handling to `/{name}` paths.
4. Preserve retry/delete discipline exactly as in SQS consumer code.
5. Add compatibility adapter layer in tests if production code uses AWS SDK interfaces.

Minimal adapter strategy:

- define interface: `Send`, `Receive`, `Delete`, `ChangeVisibility`
- implement `SQSAdapter` and `EvtQAdapter`
- choose adapter by environment

## Known limitations

- No AWS IAM/authn by default.
- No protocol-level drop-in compatibility for all AWS SDK behavior.
- No managed HA; resilience depends on PostgreSQL topology.
- Feature parity is focused on queue semantics, not full AWS platform integration.

See [Design decisions](./design-decisions.md) for rationale.
