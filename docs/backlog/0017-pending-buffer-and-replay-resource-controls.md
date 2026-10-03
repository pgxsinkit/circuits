# 0017 — Pending shapes and dormant replay need resource accounting and bounds

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/sequencer.rs` (`PendingShape`, replay), `engine/lifecycle.rs` (`resume_dormant`), memory introspection
Reopen trigger: measured pending-buffer growth, replay work or wake latency under a slow seed/storage peer, or an explicit decision to budget replay concurrency and memory.

Urgency: medium for the engine's ordinary read path. The source-confirmed growth needs measurement and a resource design; no local OOM was established.

## Evidence and urgency

Source-confirmed resource gap; no local OOM or production-shaped replay/RSS failure was reproduced. Pending shapes clone processed table envelopes into a vector while backfill/replay runs. Dormant replay scans until caught up, with no per-shape pending-buffer budget, scan work budget or coalesced concurrency control. Shape count and a prolonged seed/replay can multiply retained changes. This affects the native engine, unlike optional DS forks.

First batch `9779fc4` added finite HTTP deadlines and retryable incomplete-body failures. It did not bound pending memory or total replay work. Snapshot-settle record/waiter caps are separate bounds. [0022](0022-subquery-inner-seed-materializes-all-rows.md) now installs inner seed rows in chunks; that removes whole-row seed staging without bounding pending changes, replay work, or query-back candidate rows.

Upstream engine [#17](https://github.com/indexedlabs/electric-circuits/pull/17), merge [44d410ce344b168cb57d60e23dc476634726aae6](https://github.com/indexedlabs/electric-circuits/commit/44d410ce344b168cb57d60e23dc476634726aae6), supplies admission, coalesced scans, pending accounting and recreate outcomes. Its [final review reply](https://github.com/indexedlabs/electric-circuits/pull/17#issuecomment-5534855122) corrects earlier weak/unreproducible test claims. Upstream `reactivation_spans` was declared but never incremented; do not copy a dead metric.

## Design and acceptance

Start with measured memory/work accounting and public-path slow-backfill/slow-replay tests. Decide whether overflow backpressures, spills, or explicitly retires/recreates; an acknowledged batch must land or its shape must be retired through the durable catalog and close-then-delete path. Do not discard deltas while a shape remains registered, or invalidate a transaction because it is large. Keep `(lsn, seq)` exactly-once effect, transaction-end holds and snapshot gates.

If replay gets a fixed end, obtain it from the `BeginShape` acknowledgement where pending buffering starts. Upstream's earlier admission HEAD left a silent gap before buffering; the existing local replay-to-head path does not have that particular gap. Coalesce before taking scan permits, stop retired scans at page boundaries, and measure permit return/queued buffer growth rather than testing a semaphore helper alone.

Add a real growing-log replay with concurrent writes, several same-table wakes behind slow scans, cancellation/retirement, one huge transaction and repeated park/wake cycles. Record peak and post-cycle RSS, bytes scanned and delivery equality. Read caps require [#24](https://github.com/indexedlabs/electric-circuits/pull/24)'s append-budget floor and allowance for values larger than the page target; no engine byte cap exists today. Client retirement/refetch remains [0010](0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md). Complete both validation gates before calling this done.
