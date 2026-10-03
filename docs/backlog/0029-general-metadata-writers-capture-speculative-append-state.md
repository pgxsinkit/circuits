# 0029 — General metadata writers can capture speculative append state

Status: candidate (source review 2026-10-03; not locally reproduced)
Opened: 2026-10-03 · Area: `apps/durable-streams/src/store.rs` metadata sweep/capture, `wal/shard.rs` checkpoint, `handlers.rs` append
Reopen trigger: a deterministic paused-append test crosses a previously dirty metadata sweep/checkpoint and recovers close, producer or writer state from an append that failed before WAL staging.

Urgency: investigate next for ordinary log-server writes; conditional durability concern, with no executed local failure or production incident. This route does not require DS forks.

## Source evidence and scope

The review of [0018](0018-fork-refcounts-persist-speculative-parent-metadata.md) found an independent writer route. `handle_append_inner` changes `closed`, `closed_by`, producer state and `last_seq_header` before staging the WAL record. Stage failure restores the live state and truncates the attempted data bytes. `Meta::capture` reads those same shared fields. Its `meta_lock` serializes metadata writers and excludes hard deletion, but append mutations do not acquire it.

`Store::sweep_meta_once` checks the registry incarnation and dirty flag, then writes the general snapshot. An earlier memory-mode append or TTL read can already have queued that stream. WAL `checkpoint_blocking` drains dirty streams and may flush an earlier `meta_dirty` flag without acquiring the appender or draining admitted append operations. A previously completed producer/sequence/TTL append can therefore supply the dirty work while a later append has speculative state in memory. Sealing/offload writers in `tier.rs` also write general snapshots while later appends can run: their operation admission excludes deletion, not other admitted appends. Compaction holds the appender mutex, making its pre-stage overlap narrower. These are source observations, not a demonstrated crash image; qualify each writer before claiming complete coverage.

The documented lag in persisting a completed producer/access update is a separate contract: this candidate concerns persistence ahead of staging, including metadata from a rejected append. Narrow reference-only writes fix the fork-induced route in 0018; they do not establish a committed snapshot for general writers. Fork graph recovery remains [0019](0019-fork-graph-and-reference-recovery.md).

## Next investigation and acceptance

Use the existing WAL stage hook and a deliberately oversized record to pause a real close/producer/sequence POST after shared-state mutation, then force a staging failure after the hook. First arrange dirty work with an earlier successful write or TTL read. Cross the pause with the real metadata sweep or checkpoint, inspect the sidecar, release the append, assert HTTP 500 and live rollback, then crash/reopen. Keep all background owners quiescent before reopening the directory; compare committed bytes and every affected metadata field. Demonstrate the failure before choosing a fix, and test sweep and checkpoint separately.

A proposed sweep fixture uses the WAL harness with 256-byte segments and a TTL-60 stream. Commit producer epoch 1/sequence 0 with `Stream-Seq: 0000`, checkpoint the baseline and clear its dirty flag, then call `get_for_read` to queue a legitimate renewal. Pause a 512-byte POST carrying producer sequence 1, `Stream-Seq: 0001` and close. Call `sweep_meta_once` from a blocking worker, retain the parsed sidecar in test memory, then always release the hook and join the append before assertions or runtime teardown. Assert HTTP 500, live rollback, and reopen preserving the baseline producer/sequence/open state and bytes. The proposed command after adding that test is `bun run test:durable-streams e2e_general_meta_sweep_does_not_persist_failed_append -- --nocapture`; it has not been run.

Choose an explicit committed-state or writer-admission boundary. Do not solve it by blocking the metadata writer on an appender which itself waits for that writer, or by dropping dirty work on contention. Preserve close visibility, producer retry behavior, writer sequencing, TTL lag, tier manifest/compaction atomicity and lifecycle cancellation. Include failed staging and storage writes, concurrent successful appends, delete exclusion, and crash/reopen. Run both repository gates after implementation.
