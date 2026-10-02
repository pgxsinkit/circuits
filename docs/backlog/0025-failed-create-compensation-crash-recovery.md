# 0025 — Failed log-server create compensation needs crash-image qualification

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/store.rs` (`create`, `compensate_create`, `failed_creates`), recovery
Reopen trigger: an injected compensation/fsync failure plus restart resurrects a rejected child or loses a required parent pin, or a consumer requires persistent create-intent recovery.

Urgency: parked and unproven. Checked publication/compensation is already addressed; a remaining crash-recovery defect needs evidence.

## Fixed boundary and unproven remainder

Second batch prepares a new stream privately, persists its sidecar before registry publication and serializes same-path creators. Checked compensation removes child temp/metadata/data, fsyncs the directory, then releases/persists a parent reference; an uncertain rollback blocks that path in `failed_creates` for the process. Tests include a paused failing create invisible to append/competing PUT and compensation after sidecar rename. This resolves publication/checked-rollback ordering; it is not a persistent intent protocol.

The audit did not reproduce a local post-restart failed-create corruption. The in-memory blocked-path state does not itself survive restart, and arbitrary unlink/fsync/crash combinations warrant qualification, especially with forks. Do not label the current code defective solely because it has no durable create intent.

Source: upstream [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410); [4c3a6902f508ec6d8d2f55b884263df40817d7c3](https://github.com/indexedlabs/durable-streams-rust/commit/4c3a6902f508ec6d8d2f55b884263df40817d7c3)'s `REAPER-FU-2` documents incomplete durable compensation. Upstream is an investigation source, not a complete answer. Parent snapshot isolation and graph reconciliation remain [0018](0018-fork-refcounts-persist-speculative-parent-metadata.md)/[0019](0019-fork-graph-and-reference-recovery.md).

## Qualification requirements

Fault after child sidecar rename, each unlink, directory fsync and parent reference persistence, then crash/reopen twice. Inspect file artifacts, recovered graph and path ownership; confirm no successful POST could target an unpublished child. Distinguish deliberately conservative quarantined/blocked state from silently losing acknowledged bytes or reviving a rejected child. Preserve failed-cleanup evidence for repair and reserve identities if unresolved evidence requires it.

If a persisted create/rollback intent is necessary, specify durable stage transitions and idempotent recovery before implementing it. Do not expand into the entire upstream reaper/subscription feature. Storage shutdown is separately [0026](0026-log-server-shutdown-drain-qualification.md), and engine aggregate identity ordering is [0024](0024-aggregate-stream-before-catalog-identity.md). Run both gates for any implementation.
