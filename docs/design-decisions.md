# EvtQ Design Decisions

## Table of contents

- [PostgreSQL over Redis/in-memory](#postgresql-over-redisin-memory)
- [`FOR UPDATE SKIP LOCKED` for claiming](#for-update-skip-locked-for-claiming)
- [Regenerating receipt handles](#regenerating-receipt-handles)
- [Visibility timeout as timestamp](#visibility-timeout-as-timestamp)
- [Trigger polling model](#trigger-polling-model)
- [FIFO receive query design](#fifo-receive-query-design)
- [Expiry worker over TTL index](#expiry-worker-over-ttl-index)
- [Tradeoffs and alternatives](#tradeoffs-and-alternatives)

## PostgreSQL over Redis/in-memory

EvtQ prioritizes durable semantics with operational simplicity:

- strong consistency and transaction support
- mature lock primitives for concurrent consumers
- broad ecosystem and operational familiarity

Redis/in-memory designs can be faster in absolute throughput, but require additional durability/ordering layers to match EvtQ guarantees.

## `FOR UPDATE SKIP LOCKED` for claiming

Claiming must be atomic under parallel consumers. `SKIP LOCKED` gives:

- non-blocking concurrent workers
- no duplicate claim of the same row in the same visibility window
- straightforward SQL that scales with consumer count

This avoids external distributed locks for standard queue receives.

## Regenerating receipt handles

Each receive issues a new `receipt_handle`:

- old handles become stale immediately
- delete/visibility operations are tied to current lease owner
- prevents accidental ack with outdated state

This mirrors SQS-like stale-handle behavior and avoids ambiguous acknowledgments.

## Visibility timeout as timestamp

EvtQ uses `visible_at` timestamp, not in-memory timers:

- timeout expiry is implicit in SQL predicate
- no per-message goroutines/timers
- crash-safe across restarts

This converts "lease expiration" into deterministic query logic.

## Trigger polling model

EvtQ triggers poll queue state rather than push-on-send:

- reuse existing receive path and semantics
- same failure/visibility behavior as manual consumers
- easy batching and backpressure control

Push-on-send can reduce latency but complicates retries, partial failures, and delivery ownership semantics.

## FIFO receive query design

FIFO query uses CTE stages:

1. determine free groups (no in-flight message)
2. pick lowest `sequence_number` per free group
3. claim atomically with lock checks

Advisory locks were considered and are useful for group-level exclusion under contention. Final query structure balances ordering guarantees with acceptable parallelism across groups.

See [Triggers](./triggers.md#fifo-ordering-guarantees-through-triggers).

## Expiry worker over TTL index

PostgreSQL does not have native TTL indexes equivalent to document stores. EvtQ uses periodic cleanup worker:

- predictable cleanup cadence
- controllable write pressure
- explicit operational visibility

Alternatives (trigger-based cleanup on reads/writes) increase hot-path cost and query complexity.

## Tradeoffs and alternatives

- **Chosen**: PostgreSQL single durable source.
  - **Tradeoff**: less horizontal scale than managed cloud queue services.
- **Chosen**: Poll-based triggers.
  - **Tradeoff**: slight extra latency vs immediate push.
- **Chosen**: strict FIFO per group.
  - **Tradeoff**: lower max throughput for same-group workloads.
- **Chosen**: explicit SQL semantics over ORM abstraction.
  - **Tradeoff**: more SQL maintenance, better control and predictability.
