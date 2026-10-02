# 0024 — Aggregate stream creation precedes catalog identity: unproven crash lead

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/lifecycle.rs` (`create_aggregate_once`), catalog ID allocation/restore
Reopen trigger: a targeted crash/cancellation test shows an unrecorded aggregate stream being reused after restart with old bytes, or a deployed orphan-ID symptom matches this ordering.

Urgency: parked and unproven; do not treat this ordering lead as an urgent confirmed defect before reproducing an unsafe outcome.

## Observation, not a confirmed defect

The aggregate path allocates `sN` and ensures `shape/sN` before either circuit or generic aggregate path enqueues `Created`. The catalog restores the next ID past cataloged identities, not every allocation that only happened in memory. This is a window to investigate for a stream without durable identity. The audit did not reproduce stale rows, unsafe reminting or an acknowledged aggregate loss. Plain/subquery creation has different ordering; do not claim every create is vulnerable.

Upstream [#18 follow-ups](https://github.com/indexedlabs/electric-circuits/pull/18) names the older no-`Created`/reminted-stream hazard as `electriccircuits-task-vcl`, not a completed upstream fix. Second batch's private durable DS creation prevents append-visible partial storage creation; it does not change engine catalog identity allocation. Catalog restore already handles missing/closed **recorded** shape streams and incomplete `Dropped` retirement.

## Required diagnosis before a fix

Pause after successful PUT but before `Created`, crash the engine, inspect catalog and stream bytes, restart and create the same/different aggregate. Cover circuit COUNT and generic fold paths, partial seeds, cancellation versus process death, failed DELETE cleanup and catalogs containing only drops. Establish whether the reused stream is empty or carries a seed/live result and whether a subscriber was ever acknowledged.

Only if unsafe reuse is proved, choose durable allocation/create intent or earlier `Created` ordering with compensation. Never return before client-promised catalog durability, rewrite/reuse an acknowledged identity, or silently recreate a missing acknowledged stream. Keep failure cleanup cancellation-safe, classify schema/epoch races honestly, and run both gates. [0025](0025-failed-create-compensation-crash-recovery.md) is a different storage-level compensation lead.
