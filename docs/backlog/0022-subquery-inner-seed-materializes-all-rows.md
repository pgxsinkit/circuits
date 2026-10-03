# 0022 — Subquery inner seeding still materializes the whole row vector

Status: dropped (fixed 2026-10-03)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/lifecycle.rs` (`create_subquery_three_phase`), `pg.rs` (`BackfillReader::collect`), `subquery.rs` (`finish_create`)
Reopen trigger: evidence that a subquery seed again retains full snapshot rows, or a consumer needs a heap/RSS bound beyond chunk staging.

Urgency: resolved for full-row snapshot staging; total engine peak and any OOM remain unmeasured.

## Evidence and scope

The former inner seed called `BackfillReader::collect()`, retaining `Vec<Row>` for each fresh node before installing membership contributors. The maintained inner set is necessary derived state; transport streaming did not remove that additional whole-row vector. Nested predicates staged several seed vectors at once. The outer backfill was already streamed.

Upstream [#17 first review](https://github.com/indexedlabs/electric-circuits/pull/17#pullrequestreview-5106691376) raised the same residual. It is not a completed upstream fix. First batch adds settled snapshots and cancellation-safe initialization; its serial admission limits concurrent initializers, not the size of one seed. Snapshot-settle bitmap and waiter caps are also separate. [0017](0017-pending-buffer-and-replay-resource-controls.md) covers live changes retained during seeding/replay.

## Acceptance and constraints

Measure retained row-vector bytes separately from the final membership state and pending changes. Explore a bounded chunk-to-contributor installation API rather than calling the transport streamed and claiming bounded RSS. Preserve one snapshot's gate/horizon, raw at-least-once ingest/highwater fencing, and cancellation-safe retractions after a partially installed seed. Capture every dependent table's sequenced-xid record before opening the snapshot, check its visibility, and roll back/release the pooled connection before waiting and retaking when necessary. Require a settled snapshot before publishing its seed. Postgres remains the system of record.

Test wide and nested inner relations, cancellation between chunks, partial circuit assertions, overlapping requests, schema drift and live membership flips during installation. A failed create must leave no nodes/refs, subscription, stream or circuit state. Do not introduce circuit structure scaling with subscriptions or keep a local row-table copy to avoid query-backs. Run both gates after implementation.

## Resolution and evidence

Phase B now consumes `next_chunk()` into `SubqueryRegistry::seed_chunk`: full rows are reduced to primary keys and projected values and dropped before the next cursor read. Fresh nodes keep `seed_buffer = Some` throughout chunk installation, so raw deltas and child re-derivations still queue. Phase C receives only `(signature, SnapshotGate)`, installs each gate, replays buffered deltas and releases deferred work. The settled snapshot scope still includes the inner table and every nested dependent table, and no registry lock spans Postgres I/O. Shutdown is checked between chunks.

Cancellation qualification exposed an additional defect: a circuit batch already queued by a dropped future could land after rollback scanned the last published contributor snapshot. The red regression retained one dead-node membership value after rollback. Rollback now awaits a FIFO circuit barrier before inspecting contributor state, and clears the removed signature from the host template index independently of submitted assertions. Admission remains owned until detached cleanup finishes. A retained-template test covers cancellation before enqueue, preserving the other bind's contributors and exact reference count.

The deterministic wide/nested Rust regression runs the former collect-loop staging algorithm on two sets of 2,048 rows, each with a 4,096-byte unused text payload, and compares it with 16-row chunks actually consumed by the new registry API. `HeapSize` estimates **17,170,480 bytes** for the former retained row vectors and **67,072 bytes** for the largest input chunk (256-fold difference). This is row-staging accounting, not RSS or an allocator peak; it excludes contributor state, primary-key dictionary/indexes, pending changes, driver buffering, and assertion batches. Both paths have 4,096 contributor rows; the test also verifies raw deletion replay and deferred child work remain pending until completion.

Focused evidence:

- `bun run engine:test seed_ -- --nocapture`: chunk staging, pending raw/deferred work and queued/pre-enqueue cancellation regressions passed.
- `bun run test:integration:harness packages/conformance/src/conformance-subquery-seed-chunks.test.ts` with PostgreSQL 18 binaries on `PATH`: three tests passed, using 4,096-byte unused payloads and a 16,384-byte chunk budget. They cover wide nested seeding, overlapping creates, live flips during the pending window, HTTP cancellation with fresh-node recreation, and schema-drift retirement/retry with exact node references.

The first full integration run passed 239 of 240 tests. The existing 250,000-row cancellation test's release-then-sleep timing assumed that seed installation happened after the outer backfill. With chunk installation preceding that backfill, the create correctly completed before the timer aborted it. The test now holds the outer table lock while aborting and waits for observable public-shape removal, node cleanup and stream deletion before recreation; its large seed, exact reference count and reusable named subscription remain covered. The final full-validation rerun passed after this correction.

The encoded-byte reader budget permits a single oversized row and one lookahead row. Contributor state and pending live/replay buffers remain separate concerns ([0017](0017-pending-buffer-and-replay-resource-controls.md)); membership query-backs still materialize candidate rows. No whole-engine memory bound, measured RSS reduction, or OOM remediation claim is made. Both required gates, `bun run validate` and `bun run validate:full`, passed; the final integration gate passed all 240 tests across 60 files, including the deterministic 250,000-row cancellation case. See the [resolution ledger](0016-indexedlabs-audit-resolution-ledger.md) for the combined batch record.
