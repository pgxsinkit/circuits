# 0030 — Checkpoint tail capture can cross a rejected append

Status: candidate (source review 2026-10-03; not locally reproduced)
Opened: 2026-10-03 · Area: `apps/durable-streams/src/wal/shard.rs` checkpoint tail capture, `handlers.rs` write/stage rollback, `wal/recovery.rs` tail reconciliation
Reopen trigger: a paused real append rejected before WAL staging crosses checkpoint tail capture and reopens with a logical frontier beyond its acknowledged bytes.

Urgency: investigate after 0029; potentially serious recovery/offset corruption if reproduced, with no executed local counterexample or known production incident.

## Source evidence and separation

While isolating [0029](0029-general-metadata-writers-capture-speculative-append-state.md), the implementer found that checkpoint captures `Shared.tail`, not just append metadata. `write_wire` advances that writer-facing tail before WAL staging. `stage_for_durability` registers the stream dirty before attempting `reserve_and_stage`; a stage failure then truncates the data file and restores the writer tail. Checkpoint captures the tail/file pair, performs its file barrier, then persists the tail map without excluding that write/stage/rollback window. A barrier proves the captured bytes reached storage at that moment; subsequent rollback can truncate those same bytes.

Recovery merges sidecar proof, checkpoint tail and replayed record ends by maximum. `reconcile_tail` truncates a longer file, but for a shorter file assumes replay already extended it, then publishes the logical frontier and appender count. A falsely high checkpoint tail with no corresponding WAL record could therefore publish an offset beyond physical bytes. This is a source-derived failure path, not an executed recovery result.

The 0029 checkpoint regression pauses after a known committed tail capture before starting the rejected append. Its exact acknowledged bytes survive reboot, establishing metadata contamination independently; it does not qualify this tail-proof path. General committed metadata views do not change checkpoint's `Shared.tail` capture.

## Next reproduction and acceptance

Use the same tiny-segment WAL harness and existing pre-stage hook. Commit a small baseline, pause an oversized POST after its bytes/tail mutation and dirty registration but before staging, and run the real checkpoint while paused. Inspect its persisted tail map, always release/join POST 500 and verify live rollback, then crash and fully reopen. Assert physical bytes, GET/HEAD offsets, appender position and a following successful append agree; repeat reopening. Compare the ordinary writer path with the post-tail-capture isolation fixture. Do not manufacture `Shared.tail` or the tail map to claim this race reproduced.

Do not blindly substitute `durable_tail`: WAL durability can advance before the corresponding async callback publishes that reader frontier. A checkpoint that recycles those records while recording a lagging reader tail could lose acknowledged bytes after the callback completes. Qualify a coherent accepted/staged tail boundary against the checkpoint LSN, or exclusion of synchronous tentative write/stage/rollback during capture; these are options, not an established design. Preserve registration-before-staging, durability-before-recycle, off-lock WAL waits, hard-retirement exclusion, compaction file/base consistency and progress on other streams. Test stage rejection, out-of-order publication, newly durable records, concurrent checkpoint/retirement and repeated crash recovery. Run both repository gates after implementation.
