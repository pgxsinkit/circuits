# 0026 — Qualify the log server's complete shutdown bound under stalled storage

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/main.rs` signal path, `engine_raw.rs` drain, `wal/walset.rs` committer join
Reopen trigger: a stalled request/fsync/committer makes actual process shutdown exceed its advertised grace, or deployments require a single enforceable shutdown deadline.

Urgency: parked and unproven; a request-drain bound alone does not establish either a whole-process guarantee or a reproduced shutdown failure.

## Observation and scope

Unproven local lead. The server already stops acceptance, shuts down Linux reactor SSE and bounds connection draining to 25 seconds. It subsequently stops/joins dedicated committers and shuts down telemetry. That request-drain timeout is not evidence that the entire process, a blocking fsync/final committer join, or every detached cleanup has the same total deadline. No local stuck-shutdown reproduction was run.

Upstream [4c3a6902f508ec6d8d2f55b884263df40817d7c3](https://github.com/indexedlabs/durable-streams-rust/commit/4c3a6902f508ec6d8d2f55b884263df40817d7c3) records a bounded-drain follow-up in its newly added reaper; the local server has no such reaper. It does not prove the same defect or supply a complete local shutdown patch. Engine HTTP deadlines from first batch do not bound the log server's filesystem operations.

## Acceptance before changing behavior

Measure actual process exit under idle/active long-poll, inline/reactor SSE, blocked append/group commit, checkpoint, create/delete compensation and telemetry shutdown. Inject a deterministic stall, record outstanding work and exit result; distinguish connection drain from final durable committer drain. Preserve acknowledged WAL and never acknowledge unflushed work because a timer fired.

If a total bound is required, use one deadline across phases with an explicit forced/incomplete outcome and recovery guarantee, not cancellation pretending success. Keep dedicated committers' final drain after admitted requests, inspect detached cleanup ownership, and test restart recovery of every acknowledged append/delete. Existing [0014](0014-log-server-lock-test-failed-once-under-load.md) is a second-server test timing observation, not this investigation. No new runtime work until the trigger; both gates required for a fix.
