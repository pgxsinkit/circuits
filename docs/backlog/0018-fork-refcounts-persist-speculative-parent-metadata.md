# 0018 — Fork reference updates can persist speculative parent append metadata

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/store.rs` (`create`, parent release, `Meta::capture`, `write_meta_sync`), `handlers.rs` append publication
Reopen trigger: a consumer adopts DS forks, or a deterministic paused-append/crash test demonstrates a fork reference write persisting unacknowledged close/producer state.

Urgency: conditional medium for DS fork users; the ordinary engine path does not use forks, and the failure was not reproduced locally.

## Evidence and priority

Source-confirmed mechanism, not locally reproduced. Fork reservation/release changes `ref_count` and writes the parent's general metadata snapshot. Append handling can change speculative close, producer/dedupe or writer state before WAL staging/acknowledgement; `Meta::capture` can therefore persist that state as a side effect of a reference-only update. This is a durability concern for fork users. The ordinary Circuits engine does not use the DS fork API, so investigate on the trigger rather than extend the current batch.

Second batch prepares new streams privately and checks failed-create compensation. First-batch metadata/delete exclusion and queued soft-parent cleanup revalidation do not make parent reference writes field-specific. Fork graph recovery is separately [0019](0019-fork-graph-and-reference-recovery.md).

Source: [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410). Useful upstream tests: `fork_reservation_does_not_persist_an_inflight_append_snapshot`, `parent_refcount_release_does_not_persist_an_inflight_append_snapshot`, `parent_refcount_release_rolls_back_when_the_narrow_merge_fails`.

## Proposed boundary and acceptance

Merge only the reference field into previously committed metadata while holding `meta_lock`, checking the exact incarnation and preserving allowed soft-parent updates. Failure must restore or conservatively retain the pin; do not persist a speculative close/dedupe snapshot or permit deletion of a still-needed parent. Pause append before staging and before durable publication, cross reservation/release, inspect the disk image, then crash/reopen and assert committed bytes and producer state. Include narrow-merge/fsync failure and delete/rollback races. Extract the fix without upstream subscription/reaper architecture, and run both repository gates.
