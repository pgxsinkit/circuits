# 0022 — Subquery inner seeding still materializes the whole row vector

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/lifecycle.rs` (`create_subquery_three_phase`), `pg.rs` (`BackfillReader::collect`), `subquery.rs` (`finish_create`)
Reopen trigger: measured create-time RSS from a large/wide inner relation, or a consumer needs bounded subquery-seed peak memory.

Urgency: medium for engine creation memory. Whole-vector materialization is confirmed, but its practical peak and any OOM remain unmeasured.

## Evidence and scope

Source-confirmed; no OOM reproduction or quantified peak-memory test. Inner seed backfill uses the streamed reader but calls `collect()`, retaining `Vec<Row>` before installing membership contributors. The maintained inner set is necessary derived state; transport streaming does not remove the additional whole-row seed vector. Nested predicates may have several seed vectors staged at once. The outer backfill is streamed and should stay so.

Upstream [#17 first review](https://github.com/indexedlabs/electric-circuits/pull/17#pullrequestreview-5106691376) raised the same residual. It is not a completed upstream fix. First batch adds settled snapshots and cancellation-safe initialization; its serial admission limits concurrent initializers, not the size of one seed. Snapshot-settle bitmap and waiter caps are also separate. [0017](0017-pending-buffer-and-replay-resource-controls.md) covers live changes retained during seeding/replay.

## Acceptance and constraints

Measure retained row-vector bytes separately from the final membership state and pending changes. Explore a bounded chunk-to-contributor installation API rather than calling the transport streamed and claiming bounded RSS. Preserve one snapshot's gate/horizon, raw at-least-once ingest/highwater fencing, and cancellation-safe retractions after a partially installed seed. Capture every dependent table's sequenced-xid record before opening the snapshot, check its visibility, and roll back/release the pooled connection before waiting and retaking when necessary. Require a settled snapshot before publishing its seed. Postgres remains the system of record.

Test wide and nested inner relations, cancellation between chunks, partial circuit assertions, overlapping requests, schema drift and live membership flips during installation. A failed create must leave no nodes/refs, subscription, stream or circuit state. Do not introduce circuit structure scaling with subscriptions or keep a local row-table copy to avoid query-backs. Run both gates after implementation.
