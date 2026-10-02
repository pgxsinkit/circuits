# 0023 — A restart resets the age used for dormant-shape TTL

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/catalog.rs` dormant restore, `retention.rs` (`ShapeLife`), lifecycle sweeps
Reopen trigger: an operator requires wall-clock dormancy TTL, or repeated restarts demonstrably retain abandoned shapes beyond an intended deadline.

Urgency: low. Current conservative retention extends identity lifetime; no production retention incident was reproduced.

## Evidence and current semantics

Source-confirmed behavior, not a reproduced production retention incident. Catalog dormant records persist resume position and gate; restore reconstructs dormant life using the new process's `Instant`. Seven days of dormancy therefore means measured uptime after restore, not an unchanged wall-clock deadline. Frequent restarts can extend retention. This conservative behavior preserves shapes; it is not silent data loss.

Upstream [#17 first review](https://github.com/indexedlabs/electric-circuits/pull/17#pullrequestreview-5106691376) explicitly left the same issue open. Neither audit batch changes it. Upstream's split lease/dormancy knobs do not imply a local lease mismatch: local advertisement and enforcement use the same idle timeout.

## If the trigger fires

Choose whether TTL is wall-clock retention or conservative process-local hygiene. For wall-clock behavior, persist `dormant_at` or an equivalent deadline with backward-compatible folding; define old-record defaults, clock adjustments and whether successful reactivation resets it. Do not age/evict reactivating shapes mid-replay or let a deleted segment race a durable checkpoint/shape pin.

Test repeated restart without reads, backward/forward clock changes, old catalogs, concurrent wake/sweep, durable retirement and change-log deletion floor. Preserve `GET /shapes/{id}` as a non-touch. Client refetch [0010](0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md) is a separate unresolved consumer behavior, and forced-exit replay [0011](0011-a-forced-exit-replays-the-checkpoint-window.md) is unrelated. No new behavior is chosen until the trigger fires.
