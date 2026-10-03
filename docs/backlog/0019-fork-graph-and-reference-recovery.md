# 0019 — Fork reference counts need graph-aware crash recovery

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/store.rs` recovery, child deletion/parent release, fork reservation
Reopen trigger: DS forks become a supported consumer workload, or a child-unlink crash image demonstrates a phantom parent pin or reservation-versus-deletion failure.

Urgency: conditional medium for DS fork workloads, outside the engine's ordinary read path. Remaining crash reconciliation needs a deterministic failure image.

## Evidence and urgency

Source-confirmed candidate; the fork crash was not reproduced locally. Recovery trusts persisted `ref_count`. Child physical deletion can complete before asynchronous parent decrement/persistence, so a crash can leave a soft-deleted parent pinned without a surviving child. Phantom pins retain storage; an unsafe reconciliation that drops a real pin could destroy a child's readable prefix.

Source: [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410), especially `recovery_reconciles_a_soft_parents_stale_ref_after_child_unlink_crash`. Upstream [4c3a6902f508ec6d8d2f55b884263df40817d7c3](https://github.com/indexedlabs/durable-streams-rust/commit/4c3a6902f508ec6d8d2f55b884263df40817d7c3) documents incomplete compensation hardening; its code is not a complete filesystem-fault proof.

First batch protects quarantined parents/descendant WAL and revalidates queued soft-parent cleanup after rollback. Parent `begin_operation()` reservation already spans the durable reference update, and DELETE drains that reservation before deciding physical removal. Second batch preserves soft-parent tail evidence. These protections do not reconstruct the complete child-parent graph or supply persistent fork/create intents. [0018](0018-fork-refcounts-persist-speculative-parent-metadata.md) covers narrow parent metadata writes; [0025](0025-failed-create-compensation-crash-recovery.md) covers failed creation.

## Acceptance

The 2026-10-03 source review identified a deterministic fixture seam, not yet executed: extend `soft_parent_cleanup_waits_for_contended_metadata` in `store.rs`. Durably soft-delete a parent, hold its `meta_lock`, then delete the child. Copy the durable stream artifacts into an independent fixture before releasing the lock. The child files have been unlinked and directory-synced while the parent sidecar still records `soft_deleted=true, ref_count=1`. Reopen the copy, never the actively mutating source directory. Extend this to nested ancestors and repeated boots.

Graph completeness needs more than successful WAL recovery or the current quarantine ID set. A corrupt child may have no remaining WAL evidence yet still refer to a healthy parent; missing data/meta pairs, unresolved ancestry, cycles, duplicate identities or unreadable enumeration must retain conservative pins or refuse startup. Count only from a complete durable graph. Destructive cleanup belongs after successful WAL preflight/replay, before serving or starting committers, and before resetting WAL evidence. Prefer a checked startup finalization step with explicit WAL access; memory mode can finalize after its sidecar scan. Propagate failures and finish startup cleanup synchronously. The narrow writes in [0018](0018-fork-refcounts-persist-speculative-parent-metadata.md) do not close the child-unlink/crash window.

Reconcile counts from durable child-parent edges only when the graph is complete; quarantine/missing evidence requires conservative pins or fail-closed repair, never guessing zero. Preserve and test the existing parent operation reservation and DELETE drain across the durable reference update. Durably complete zero-reference soft-parent cleanup exactly once, including ancestor release.

Crash after child unlink but before parent decrement, after reference persistence, and during nested-fork cleanup. Test quarantine/missing child evidence, failed parent persistence, concurrent reservation/DELETE, and reopen without leaks or lost readable prefixes. Keep hard-retired WAL-tail forgetting distinct from soft-parent proof. The engine does not currently require forks; do not add a subscription manager or active reaper to solve the graph. Run both required gates on implementation.
