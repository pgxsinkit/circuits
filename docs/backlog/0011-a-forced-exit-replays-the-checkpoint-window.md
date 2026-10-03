# 0011 — A forced exit replays the checkpoint window onto shape streams

Status: resolved for plain Postgres shapes (2026-10-03; other output scopes retained in 0034)
Opened: 2026-09-29 · Area: `apps/engine/src/engine/sequencer.rs` (the lazy checkpoint),
`apps/engine/src/shutdown.rs`, `packages/conformance/src/conformance-shutdown.test.ts`
Reopen trigger: fresh evidence that an acknowledged plain Postgres shape re-emits a covered source
effect after restart or an ambiguous retry, or loses an uncovered effect at the delivery boundary.

Resumed 2026-10-03 under the user's instruction to take the next most critical item across the whole backlog. Selected ahead of optional fork recovery because it affects the native production reader, not only DS fork workloads. The prior final-row convergence description below understated observable replay effects; the repair, executed qualification and limits are retained below.

## Current consumer exposure (2026-10-03)

Source review of the adjacent `pgxsinkit` checkout at `fc909af7f00085cfa4b76b870e4183463bfc3115` found that its native `StreamInbox` deduplicates only applied stream offsets. `envelopeToChange` removes the source LSN/sequence headers; its sync engine folds each committed batch in arrival order and applies unconditional row deletes/upserts. ADR-0056 deliberately makes an opaque per-stream DS offset the sole resume/dedup frontier, atomically committed with rows. Replayed output appended at a new stream offset passes that guard.

Inference from those sources: replay delivered in separate committed batches can temporarily regress a row or resurrect/delete it until later replay catches up. Final server-column convergence does not prove preservation of local-only columns: a genuine delete/recreate deliberately clears them, and replaying that lifecycle can repeat its effects. Native fold tests include a targeted recreation case and a 2,000-sequence oracle; suppressing every delete-before-upsert would break genuine row lifecycle semantics. No production incident or fresh executed downstream replay test is claimed by this source review.

Before this repair, emitted `seq` was absent, LSN was optional, and batch-generated deletions and backfills could lack it. Output still has no universal alternative source frontier across all shape modes. A blanket client LSN gate would change the established offset contract without covering all events. The harness subset's retained per-key LSN state also does not prove immunity to all replay; equal/missing LSNs pass and absent-key tombstones are pruned outside active page loads. Aggregates replace each received absolute value. These client APIs are distinct from pgxsinkit's native table reader.

The fix must keep processed/checkpoint publication behind landed output, preserve coupled change-log position/highwater and transaction holds, retain terminal stream-loss reconciliation, and preserve client opaque-offset behavior. The fresh regression plan uses a real engine/PG/log-server restart and intercepts only actual catalog Offset writes to retain an older durable checkpoint; no synthetic catalog records or envelopes stand in for production replay. Raw output and intermediate versions must be observed, not only the final fold.

## Fresh regression (2026-10-03)

The real-engine regression now holds only actual catalog `Offset` POSTs, observes a row at versions 1, 2 and 3, saves the consumer's shape-stream offset, kills the engine, and restarts over the same Postgres and WAL-backed log server. The durable catalog checkpoint is checked unchanged while the engine is down. Reads after the saved consumer offset returned versions **1, 2, 3** again: the observable consumer sequence is **3 → 1 → 2 → 3**. The final folded rows still matched Postgres. This semantic failure reproduced twice (9.78 s and 9.02 s test runs).

Earlier setup failures are separate evidence: the sandbox could not start Postgres, and the first corrected invocation timed out waiting for an intercepted checkpoint because a drain sentinel alone did not advance the tracked change log. The fixture now advances a real tracked marker row outside the shape predicate to drive the normal lazy checkpoint. Neither setup failure is counted as a reproduced defect.

Two source gaps explain the red: landed shape output can lead the catalog checkpoint, and plain restore installs a passthrough gate instead of preserving the original seed snapshot. The replayed baseline is the original INSERT replaying from the change log, not a new backfill. Any fix must suppress output already delivered while continuing to replay internal state and preserve the original snapshot's xid fence.

## Historical behavior before the repair

- The sequencer persists its position lazily: an `Offset` checkpoint at most every ~2 s of change,
  and only when a read returns. A shutdown that **completes** writes a final checkpoint, so a
  graceful restart replays nothing.
- An exit that cuts the shutdown short does not: a second signal during the grace period (exit 70),
  the grace period running out, a crash, a `SIGKILL`. The restart then resumes from the last lazy
  checkpoint and processes again every change after it, appending the same envelopes to the shape
  streams a second time.
- `shutdown.rs` says so: "A hard stop costs a replay (correct, but wasteful)". The `(lsn, seq)`
  highwater does not prevent it, because the highwater is restored from that same checkpoint.
- A replayed envelope is identical to the original. A reader that folds the stream converges to the
  same rows. A reader at the tail can see a key go back to an earlier value and forward again,
  within one replayed window.

## How it was found

`conformance-shutdown.test.ts` "a SECOND signal during the grace stops waiting and exits non-zero"
asserted that every key is on the stream exactly once after a forced exit. The sequencer stops at the
FIRST signal and sends its final checkpoint to the catalog writer at once, so the assertion held
whenever that append landed before the second signal ended the process. It passed in every local
run (24 of 24, idle and confined to one core) and in three runs on GitHub, and failed in the fourth
(run 36529448124), where the exit won the race: key `1` was on the stream twice.

Reproduced on purpose, 2026-09-29: a `SIGKILL` right after the first row was delivered, with no
`Offset` event yet in `meta/catalog`. After the restart and a second insert the shape stream held:

```
key 1  upsert  {"id":1,"label":"one","n":1}  txid 759  lsn 0/1BB9558
key 1  upsert  {"id":1,"label":"one","n":1}  txid 759  lsn 0/1BB9558
key 2  upsert  {"id":2,"label":"two","n":2}  txid 762  lsn 0/1BB96F8
```

At that point the test was weakened to assert nothing lost and identical repeated envelopes. That
was a historical qualification of the defect, not an acceptable native-reader delivery contract.
The new restart fixtures below require already delivered plain output to remain absent after restart.

## Delivery identity investigation (2026-10-03)

The earlier suggestion to attach producer headers is incomplete. The current WAL stores append bytes without producer or `Stream-Seq` identity; committed header state is promoted in memory after durability and persisted later in the metadata sidecar. WAL checkpoint/recycling can outlive that lagging identity. Existing sequence conflicts can also report a tentative append whose durability has not completed. A generic 409 is not proof that output landed.

A viable ordered delivery token must come from actual stable source provenance, not HTTP request ordinals, replay pages or chunk counters. Activation and dormant replay have different batch cuts from the live sequencer. A restored plain shape also needs its actual original `SnapshotGate`, durably recorded before create acknowledgment, including an empty seed. Aggregates intentionally reseed from a fresh snapshot and must retain that distinct fence; asynchronous subquery effects do not have a universal source-LSN ordering guarantee.

Two designs were considered: receipts embedded in output payloads (survive existing WAL recovery, but require retained-history scanning and an atomic append condition to fence late writes), or strengthening the log server's ordered sequence durability and exposing its committed frontier. The latter is implemented in [ADR-0012](../adr/0012-plain-shape-delivery-frontiers-survive-restart.md). A stale observed frontier causes refiltering of complete source-effect groups, not unconditional acceptance of a larger token. Legacy acknowledged shapes lack the original plain seed witness; the new format distinguishes named legacy-format refusal from an interrupted modern creation that was never acknowledged.

The registration boundary must also be durable, independently of the seed snapshot. A changes-only shape uses a passthrough snapshot gate, and an empty shape may have no output receipt at all. Both still need to exclude source transactions fully processed before their pending registration; the raw read position can include an unprocessed held transaction, so it cannot substitute for that boundary.

Two additional real-handler WAL regressions reproduced before production changes: after recovery, retrying the same `Stream-Seq` returned 204 and duplicated bytes; an overlapping batch carrying an outdated expected frontier returned 204 instead of refusing before mutation. The corrected fixtures failed semantically in 0.39 s. Initial fixture assumptions about a 200 success status were corrected to accept any 2xx before recording those reds.

Separate remaining scopes are retained in [0033](0033-producer-deduplication-identity-can-lag-durable-bytes.md) (producer-only crash identity, source-derived candidate) and [0034](0034-unsequenced-output-needs-separate-restart-qualification.md) (aggregate/subquery/library identity leads, parked pending a fresh semantic red). They are not silently included in the ordinary plain-output guarantee.

## Implemented repair and qualification (2026-10-03)

Plain Postgres output carries its actual `(lsn, seq)` source identity. The log server stores the
ordered token alongside its append bytes in a checksummed sequenced WAL record, captures accepted
tail/token pairs before checkpoint recycling, and transfers recovered proof durably before resetting
the WAL. HEAD and sequence-conflict responses expose committed coverage, never a tentative writer
reservation. Conditional admission compares the observed frontier under the append lock; a changed
frontier requires refiltering, and pending durability returns 503. This fences a late old-engine
request as well as an ambiguous retry. Complete key-change effects remain in one atomic append.

Creation persists the actual original SnapshotGate and immutable fully sequenced registration floor
in a Seeded catalog record before acknowledging the shape, including changes-only and empty seeds.
Restore, activation and dormant replay retain that coverage. Delivery filtering does not skip global
arrangements, aggregates, subquery weights, transaction holds or input highwater processing. Modern
proof records with missing or malformed required fields fail closed instead of silently disappearing
from the fold. Explicit-null registration floors are valid; a missing floor is not that proof.

The unchanged original real-Postgres regression passed after the repair (6.84 s), and the complete
seven-case shutdown file passed on the final source (51.08 s tests, 57.50 s total). Three companion
cases qualify equality routing, changes-only registration after uncheckpointed pre-create history,
and an empty seed with no output receipt, including later output and a second restart. Thirteen new
engine Rust tests cover conditional receipt filtering/lost responses, complete effect boundaries,
dormant duplicate input across raw pages, serialized seed coverage, legacy refusal and strict catalog
proof decoding. Twelve new log-server fixtures cover WAL recovery, lost acknowledgment, checkpoint
before callback promotion, compaction/recycling and second boot, rejected stage, pending close,
malformed unknown-stream records, conditional request validation and explicit memory capability.
The JSON empty-array hypothesis was refuted: that body is rejected before mutation, and the
qualification retains the rejection without widening production behavior.

Independent engine and log-server reviews found no remaining blockers. Both required gates passed
on the final source via `bun run validate:full` (which runs `validate` first): formatting, typecheck,
lint, 438 engine Rust unit tests plus 46 other engine cases, 225 log-server unit tests, eight CLI
cases, 82 TS unit tests, 250 engine integration cases across 61 files and 332 protocol cases. The
existing two Rust ignores and six protocol skips remain. No dependencies, tools or branches changed;
command output was captured directly.

The first full gate passed formatting, typechecking, lint, all Rust/TS unit suites and all 250
integration cases, then exposed a protocol compatibility regression: five simultaneous ordinary
`Stream-Seq` writers require successful writes or sequence conflicts, whereas pending durability
returned 503. A deterministic paused-callback fixture also reproduced that premature response.
Ordinary sequence conflicts now await committed metadata outside all locks and rerun full admission
before returning 409; conditional requests retain retryable 503. Notification registration precedes
the predicate check, and committed promotion and durable close wake waiters. Directly polled handler
futures establish that the waiter is seated before release. The twelve-case sequence qualification
passed in 0.95 s, including independent close wake/revalidation, cancellation and DELETE admission
release. Reporting a tentative reservation as a receipt or relaxing the protocol suite would conceal
the problem.

A later gate passed the Rust suites but hit the recurring CLI startup refusal in [0032](0032-cli-readiness-accepted-before-first-connection-refused.md)
before its first PUT. That separate lead now retains child/socket diagnostics and five passing
whole-file reruns, without claiming its cause fixed. The final full gate passed after both the
sequence correction and diagnostic amendment; those intermediate failures remain separate evidence.

Limits: these tests do not constitute a power-loss or throughput experiment, a fresh downstream
pgxsinkit lifecycle test, or a producer-only crash qualification. Each plain append adds a HEAD
request. WAL advertises durable coverage; memory mode advertises volatile coverage and retains no
log-server crash guarantee. Both binaries must support the new contract. Legacy plain catalog
records refuse boot, and opening the new sequenced WAL with an older binary is an unsupported
data-directory downgrade. Those boundaries are deliberate and documented, not migration coverage.
