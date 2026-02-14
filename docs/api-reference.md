# EvtQ API Reference

## Table of contents

- [Conventions](#conventions)
- [Queue management](#queue-management)
- [Message operations](#message-operations)
- [Trigger operations](#trigger-operations)
- [Metrics](#metrics)

## Conventions

- Base URL: `http://localhost:8080`
- Content type: `application/json`
- UUID fields use RFC 4122 format.
- Common error shape:

```json
{ "error": "human readable error message" }
```

---

## Queue management

### POST `/queues`

Create a queue.

Request body:

```json
{
  "name": "orders.fifo",
  "queue_type": "FIFO",
  "content_based_dedup": true,
  "visibility_timeout": 30,
  "message_retention": 345600,
  "delay_seconds": 0
}
```

Schema:

- `name` string, required, unique.
- `queue_type` string, optional (`STANDARD` or `FIFO`, default `STANDARD`).
- `content_based_dedup` bool, FIFO only.
- `visibility_timeout` int, optional, 0..43200 (seconds), default 30.
- `message_retention` int, optional, 60..1209600 (seconds), default 345600.
- `delay_seconds` int, optional, 0..900, default 0.

Response `201`:

```json
{
  "id": "f7f4a6cf-38d4-4ea3-9cc5-c80e8e21f574",
  "name": "orders.fifo",
  "queue_type": "FIFO",
  "content_based_dedup": true,
  "visibility_timeout": 30,
  "message_retention": 345600,
  "delay_seconds": 0,
  "created_at": "2026-02-15T10:10:31Z",
  "updated_at": "2026-02-15T10:10:31Z"
}
```

Errors:

- `400` invalid config
- `409` queue already exists

```bash
curl -sS -X POST http://localhost:8080/queues \
  -H "Content-Type: application/json" \
  -d '{"name":"orders","queue_type":"STANDARD"}'
```

Notes:

- FIFO names should end with `.fifo`.

### GET `/queues`

List queues.

Query params: none.

Response `200`:

```json
{
  "queues": [
    { "name": "orders", "queue_type": "STANDARD" },
    { "name": "payments.fifo", "queue_type": "FIFO" }
  ]
}
```

```bash
curl -sS http://localhost:8080/queues
```

### GET `/queues/{name}`

Fetch queue attributes.

Response `200`:

```json
{
  "queue": {
    "id": "f7f4a6cf-38d4-4ea3-9cc5-c80e8e21f574",
    "name": "orders",
    "queue_type": "STANDARD",
    "visibility_timeout": 30,
    "message_retention": 345600,
    "delay_seconds": 0
  },
  "stats": {
    "available": 12,
    "in_flight": 3,
    "delayed": 0
  }
}
```

Errors: `404` queue not found.

```bash
curl -sS http://localhost:8080/queues/orders
```

### PUT `/queues/{name}`

Update mutable queue attributes.

Request body:

```json
{
  "visibility_timeout": 45,
  "message_retention": 604800,
  "delay_seconds": 5
}
```

Response `200`: updated queue object.

Errors:

- `400` invalid attribute values
- `404` queue not found

```bash
curl -sS -X PUT http://localhost:8080/queues/orders \
  -H "Content-Type: application/json" \
  -d '{"visibility_timeout":45}'
```

### DELETE `/queues/{name}`

Delete queue and associated messages.

Response: `204 No Content`

Errors: `404` queue not found.

```bash
curl -sS -X DELETE http://localhost:8080/queues/orders
```

---

## Message operations

### POST `/queues/{name}/messages`

Send one message.

Request body:

```json
{
  "body": "{\"order_id\":\"ord_9001\"}",
  "attributes": {
    "tenant": { "type": "String", "value": "acme" }
  },
  "delay_seconds": 0,
  "message_group_id": "group-A",
  "message_dedup_id": "dedup-001"
}
```

Schema:

- `body` string, required.
- `attributes` object, optional, up to 10 keys.
- `delay_seconds` int, optional, 0..900.
- FIFO only:
  - `message_group_id` string, required for FIFO queues.
  - `message_dedup_id` string, optional if content-based dedup is enabled.

Response `201`:

```json
{
  "id": "99676d70-bf65-49d8-ab8e-9f6fb9a2774a",
  "queue_id": "f7f4a6cf-38d4-4ea3-9cc5-c80e8e21f574",
  "body": "{\"order_id\":\"ord_9001\"}",
  "visible_at": "2026-02-15T10:15:31Z"
}
```

Errors: `400`, `404`.

```bash
curl -sS -X POST http://localhost:8080/queues/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"body":"hello"}'
```

### POST `/queues/{name}/messages/batch`

Send up to 10 messages.

Request:

```json
{
  "entries": [
    { "body": "m1" },
    { "body": "m2", "delay_seconds": 5 }
  ]
}
```

Constraints:

- `entries` required, 1..10.

Response `200`:

```json
{
  "results": [
    { "index": 0, "success": true, "id": "31dfbe5c-91d6-4036-a482-9ea3e6860f1c" },
    { "index": 1, "success": true, "id": "95af1f36-b614-49d7-b1af-f809d1921799" }
  ]
}
```

```bash
curl -sS -X POST http://localhost:8080/queues/orders/messages/batch \
  -H "Content-Type: application/json" \
  -d '{"entries":[{"body":"m1"},{"body":"m2"}]}'
```

### POST `/queues/{name}/messages/receive`

Receive and claim messages.

Query params:

- `max_messages` int, 1..10 (default 1)
- `wait_time_seconds` int, 0..20 (default 0)
- `visibility_timeout` int, 0..43200 (optional override)

Response `200`:

```json
{
  "messages": [
    {
      "id": "99676d70-bf65-49d8-ab8e-9f6fb9a2774a",
      "receipt_handle": "acdc9493-c16f-4d21-813f-fc6958f3fb98",
      "body": "hello",
      "receive_count": 1
    }
  ]
}
```

```bash
curl -sS -X POST \
  "http://localhost:8080/queues/orders/messages/receive?max_messages=5&wait_time_seconds=20"
```

Notes:

- New receipt handle is generated on each receive.

### DELETE `/queues/{name}/messages`

Purge all messages in queue.

Response `200`:

```json
{ "deleted": 42 }
```

```bash
curl -sS -X DELETE http://localhost:8080/queues/orders/messages
```

### DELETE `/queues/{name}/messages/{receiptHandle}`

Delete one in-flight message.

Response: `204 No Content`

Errors:

- `404` stale or unknown receipt handle

```bash
curl -sS -X DELETE \
  http://localhost:8080/queues/orders/messages/acdc9493-c16f-4d21-813f-fc6958f3fb98
```

### DELETE `/queues/{name}/messages/batch`

Delete up to 10 messages by receipt handle.

Request:

```json
{
  "entries": [
    { "receipt_handle": "acdc9493-c16f-4d21-813f-fc6958f3fb98" },
    { "receipt_handle": "941ec9b5-3439-4cc5-b0d5-81bf18dff6ad" }
  ]
}
```

Response `200`:

```json
{
  "results": [
    { "index": 0, "success": true },
    { "index": 1, "success": false, "error": "message not found or receipt handle is stale" }
  ]
}
```

```bash
curl -sS -X DELETE http://localhost:8080/queues/orders/messages/batch \
  -H "Content-Type: application/json" \
  -d '{"entries":[{"receipt_handle":"acdc9493-c16f-4d21-813f-fc6958f3fb98"}]}'
```

### PUT `/queues/{name}/messages/{receiptHandle}/visibility`

Change visibility timeout for one in-flight message.

Request:

```json
{ "visibility_timeout": 120 }
```

Constraints:

- `visibility_timeout` 0..43200.

Response: `204 No Content`

```bash
curl -sS -X PUT \
  http://localhost:8080/queues/orders/messages/acdc9493-c16f-4d21-813f-fc6958f3fb98/visibility \
  -H "Content-Type: application/json" \
  -d '{"visibility_timeout":120}'
```

## Trigger operations

### POST `/queues/{name}/triggers`

Create trigger.

Request:

```json
{
  "enabled": true,
  "target_type": "lambda",
  "target_url": "echo-fn",
  "batch_size": 10,
  "batch_window_seconds": 2,
  "max_concurrency": 4,
  "visibility_timeout_override": 60,
  "auto_scale": true,
  "min_pollers": 1,
  "max_pollers": 8,
  "failure_threshold": 5
}
```

Schema:

- `target_type` required (`webhook`, `lambda`, optional builds may support `grpc`/`local`).
- `target_url` required; for lambda this is function identifier in current implementation.
- `batch_size` 1..10.
- `batch_window_seconds` 0..300.
- `max_concurrency` >= 1.
- `visibility_timeout_override` optional 0..43200.
- `min_pollers`, `max_pollers` >= 1 and min <= max.

Response `201`: trigger object with IDs and timestamps.

```bash
curl -sS -X POST http://localhost:8080/queues/orders/triggers \
  -H "Content-Type: application/json" \
  -d '{"target_type":"webhook","target_url":"http://localhost:9000/hook","batch_size":5}'
```

### GET `/queues/{name}/triggers`

List triggers for queue.

Response `200`:

```json
{ "triggers": [ { "id": "b6e7...", "enabled": true, "target_type": "webhook" } ] }
```

```bash
curl -sS http://localhost:8080/queues/orders/triggers
```

### GET `/queues/{name}/triggers/{id}`

Get one trigger by ID.

Response `200`: trigger object.

Errors: `404`, `400` invalid UUID.

```bash
curl -sS http://localhost:8080/queues/orders/triggers/b6e7f76f-86df-4d7a-b252-2cff6e4d52ec
```

### PUT `/queues/{name}/triggers/{id}`

Update trigger.

Request: partial object; same fields as create.

Response `200`: updated trigger.

```bash
curl -sS -X PUT http://localhost:8080/queues/orders/triggers/b6e7f76f-86df-4d7a-b252-2cff6e4d52ec \
  -H "Content-Type: application/json" \
  -d '{"max_concurrency":8,"batch_window_seconds":1}'
```

### DELETE `/queues/{name}/triggers/{id}`

Delete trigger.

Response: `204 No Content`

```bash
curl -sS -X DELETE \
  http://localhost:8080/queues/orders/triggers/b6e7f76f-86df-4d7a-b252-2cff6e4d52ec
```

### PUT `/queues/{name}/triggers/{id}/enable`

Enable/disable trigger.

Request:

```json
{ "enabled": false }
```

Response `200`: trigger object with updated `enabled`.

```bash
curl -sS -X PUT http://localhost:8080/queues/orders/triggers/b6e7f76f-86df-4d7a-b252-2cff6e4d52ec/enable \
  -H "Content-Type: application/json" \
  -d '{"enabled":false}'
```

### GET `/queues/{name}/triggers/{id}/metrics`

Get trigger metrics.

Response `200`:

```json
{
  "trigger_id": "b6e7f76f-86df-4d7a-b252-2cff6e4d52ec",
  "invocations_total": 1200,
  "invocations_success": 1189,
  "invocations_failed": 11,
  "messages_processed_total": 10500,
  "messages_failed_total": 43,
  "average_invocation_duration_ms": 42,
  "iterator_age_ms": 0,
  "last_invocation_at": "2026-02-15T10:58:12Z"
}
```

```bash
curl -sS \
  http://localhost:8080/queues/orders/triggers/b6e7f76f-86df-4d7a-b252-2cff6e4d52ec/metrics
```

Notes:

- `batch_item_failures` from target are counted as partial failures.
- FIFO trigger dispatch preserves per-group ordering.

---

## Metrics

### GET `/queues/{name}/stats`

Get approximate queue counts.

Response `200`:

```json
{
  "queue": {
    "name": "orders",
    "queue_type": "STANDARD"
  },
  "stats": {
    "available": 100,
    "in_flight": 20,
    "delayed": 5
  }
}
```

```bash
curl -sS http://localhost:8080/queues/orders/stats
```

---

See also:

- [Concepts](./concepts.md)
- [Triggers](./triggers.md)
- [SQS compatibility](./sqs-compatibility.md)
