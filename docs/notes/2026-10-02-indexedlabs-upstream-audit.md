# Indexed Labs upstream audit against Circuits

Audited 2026-10-02. The assessment below describes the baseline revisions in the scope table, not a decision to adopt upstream architecture. Implementation began later in the same session; see the execution update at the end for changes applied after the audit.

## Scope and method

| Repository | Audited revision |
|---|---|
| Local `pgxsinkit/circuits` | `4c48e8301c9cbd878279a45f0efb06124198e15e` |
| [`indexedlabs/electric-circuits`](https://github.com/indexedlabs/electric-circuits) | `9bd5b3e15d5b07033dd938d27ed834305b7c4bba` |
| [`indexedlabs/durable-streams-rust`](https://github.com/indexedlabs/durable-streams-rust) | `031fad9482f847774175fdc995b2fe8d44fea6df` |

The audit compared source and tests, not just commit messages. Separate implementers inspected engine commits, log-server commits, and all GitHub issues/PRs; the coordinator reviewed the newest engine fixes and checked the findings against local code. Read-only GitHub API snapshots and upstream clones are under `tmp/agents/upstream-audit-2026-10-02/`.

The engine's original fork point was `b784aaf`, but the actual latest shared ancestor is `474577a088b95c746bd9ab2c8e4b6552a72f151f`. Upstream has 181 reachable commits beyond that shared history (211 after the original fork point): 158 non-merge commits and 23 merges. Of these, 66 non-merge commits touch engine/API/client/protocol paths. Many earlier native-path fixes are our own shared history. All 16 log-server commits after its 13-commit extraction history were inventoried. Its extraction commit `1989021` has the exact tree `af3df4adb7023db36ba8aae8d8321deab1a5c43c` recorded in our provenance; histories were replayed under different hashes, so source comparison matters more than raw ancestry.

GitHub exposes 28 engine PRs (27 merged, one open) and one log-server PR (merged), with no standalone issues in either repository. All 29 bodies, nine issue comments, and seven review summaries were read; the engine's inline-review-comment inventory was empty. The engine's open PR #5 is managed-source work substantially superseded by merged #15. PRs #12 and #14 target a compatibility branch. Closed status was not treated as evidence that our version is fixed. The older Electric issue set is separately recorded in [the August triage](2026-08-21-upstream-issue-triage.md).

Confidence labels below distinguish **reproduced locally**, **confirmed in source**, and **candidate**. The TypeScript probes exercise actual local client code with supplied inputs/fake transport; they are not full Postgres reproductions. No production code or dependencies were changed.

## What should be brought across

### 1. Snapshot visibility and the page/live boundary — highest priority

Source: [engine PR #27](https://github.com/indexedlabs/electric-circuits/pull/27), [commit `d379511`](https://github.com/indexedlabs/electric-circuits/commit/d37951100fcfb681a3f0da260ff992681b60a164).

**Reproduced locally in the client; corresponding engine gap confirmed in source.** Three related problems remain:

- `packages/client/src/subset.ts:318` decides whether a live change is already in a page using only its commit LSN and the page/row LSN watermark. A transaction's commit record can precede the page's WAL position while the transaction remains invisible to the page snapshot. The client drops its later insert or update permanently.
- The feed's HEAD offset is captured before the page query (`subset.ts:422`). An invisible commit already delivered before that offset is in neither the page nor the subsequent feed. Full shapes have the analogous window: a transaction sequenced before `BeginShape` cannot enter its pending buffer, and a later backfill snapshot may still exclude it. See `apps/engine/src/engine/sequencer.rs:943` and `:1687`, and the unsettled snapshot opening in `apps/engine/src/pg.rs:1111`.
- Overlapping `loadMore()` calls start immediately against the same cursor (`subset.ts:594`). When snapshots have the same WAL LSN but different visibility, a late older response passes the `<` watermark check and replaces the newer row. The local probe observed row 3 change from `new` back to `old`.

The local `SnapshotGate` already solves the **buffered-after-registration** visibility boundary; it cannot recover changes sequenced before registration. This is a new gap beyond the existing xid-gate invariant.

Upstream's solution records sequenced transaction IDs before fan-out, settles dependent-table snapshots against that record, returns snapshot visibility plus a WAL insertion horizon with subset pages, merges client rows against those snapshots, and serializes `loadMore()`. The final commit also contains necessary follow-up fixes: capture the settle record before opening the snapshot, release pooled connections during waits, and retain per-table refusal fences when the bounded record overflows. Port the complete correctness contract and regression tests, adapting it to our native routes and schema-digest/fail-closed sequencer. Do not copy just the initial settle implementation.

**Related confirmed source defect:** `pg.rs:891–923` masks xid8 values to 32 bits and then uses ordinary `<`/`>=`, with no WAL insertion horizon. A gate taken before xid wrap can classify a later low-valued xid as already visible. PR #27 supplies modular xid comparison and a horizon guard; include its wraparound tests in the same work.

Local probe results:

```text
Page snapshot: 100:110:103,107; page LSN: 0/100
Live xid: 103; commit LSN: 0/FF
Absent-row insert: actual null; expected insert
Loaded-row update: actual null; expected update
Overlapping loadMore responses at the same LSN: new -> old
```

### 2. Cancellation-safe rollback and subquery initialization

Source: [engine PR #25](https://github.com/indexedlabs/electric-circuits/pull/25), [commit `1e15f5b`](https://github.com/indexedlabs/electric-circuits/commit/1e15f5bf283b59fd893167a79ac9b7b6bc91f204).

**Confirmed in source.** `CreateGuard::rollback` sets `armed = false` before awaiting asynchronous cleanup (`apps/engine/src/engine/lifecycle.rs:2010`). If the request is cancelled during that await, `Drop` does nothing. Cleanup can have removed the public shape while still waiting for registry/circuit retraction (`:2318–2343`), leaving partial initialization behind. Our existing cancellation tests cover dropping an armed creator, not cancelling this explicit error-cleanup path.

Also, a second create sharing a still-seeding inner node retries 100 times at 20 ms (`:1750–1768`), so an ordinary slow seed can make valid overlapping creates fail after approximately two seconds.

Bring across the detached cleanup ownership pattern and cancellation regressions. Upstream serializes initialization with an owned admission guard held through phase C or completed rollback, while leaving replication free to use the registry. That is a reasonable small implementation to assess; it trades concurrent unrelated initialization for predictable admission. The cleanup fix itself should not wait on a broader throughput redesign.

### 3. Log-server recovery must preserve quarantined streams and their WAL

Source: [log-server commit `5093702`](https://github.com/indexedlabs/durable-streams-rust/commit/5093702), nominally “add bounded expiration reaper.” This large commit also contains independent recovery fixes.

**Reproduced locally with fault injection.** The probe created a stream, received `204` for an 11-byte append, killed the server, truncated the unsynced data file, and corrupted its metadata sidecar while retaining its WAL. This deliberately models simultaneous data-file loss and metadata damage; it does not claim an ordinary restart spontaneously produces those faults.

On restart our server returned health `200` and stream `404`. It quarantined the sidecar for that boot, omitted the stream from WAL recovery, and reset the WAL. On the next restart it deleted both the quarantined metadata and data file as orphans.

The mechanism is in `apps/durable-streams/src/store.rs:635–704`: the quarantine list exists only for the current recovery pass. `wal/recovery.rs:65–96` only indexes streams recovered from usable sidecars; `main.rs:465–466` then recovers and resets the WAL. A quarantined identity is consequently treated like a deleted stream even though acknowledged data may depend on the retained WAL.

Extract persistent recognition of `.meta.corrupt`, reservation of quarantined stream IDs, and the pre-replay check that refuses WAL boot when quarantine may own retained WAL bytes or checkpoint-tail proof (or cannot be mapped to a known ID). Preserve the repair evidence and WAL across repeated failed starts. This does not require adopting subscriptions or the expiration reaper.

### 4. Log-server lifecycle fences and WAL metadata reclamation

Source: [log-server commit `5093702`](https://github.com/indexedlabs/durable-streams-rust/commit/5093702).

**Confirmed source gaps, with failed deletion and metadata retention reproduced locally.** The transferable changes are broader than TTL expiration:

- **False successful deletion.** Our hard-delete path ignores both `remove_file` results (`store.rs:1000–1001`). A real permission-fault probe made the scratch streams directory non-writable: DELETE returned `204`, HEAD returned `404`, and both files remained. After restoring permissions and restarting, GET returned `200` with the original `acked bytes`. Upstream propagates unlink errors and preserves a fenced identity for retry. This directly affects the durable deletion contract, including callers that close before deleting; closing does not remove files or make a false deletion acknowledgement correct.
- **Append/delete fencing.** Our delete path (`store.rs:942`) does not acquire a lifetime fence covering append through WAL acknowledgement and publication. An appender already holding the stream can outlive deletion. Upstream adds that fence and retirement state checks. Our engine's close-before-delete contract mitigates its normal shape retirement, but direct log-server DELETE and expiration remain separate paths to audit.
- **Create publication ordering.** Our `Store::create` inserts a stream into the live map before persisting its sidecar (`store.rs:1125–1154`). A concurrent append can observe it while creation is still fallible. The later rollback does not undo an append already acknowledged. Upstream persists before publishing and serializes competing creates.
- **Fork metadata isolation.** Updating a parent's fork reference currently writes its general live metadata while append state can be speculative (`store.rs:1130–1140`). Upstream persists the reference against committed metadata instead. This matters when using the log server's fork API; the Circuits engine does not use that API in its ordinary read path.
- **Metadata writers versus deletion.** Deletion does not take `meta_lock`; a queued sidecar flush can rename metadata after unlink. Upstream checks retirement inside the same metadata barrier and holds that barrier through deletion. This is a source-confirmed sidecar-leak/lifecycle gap, not proof that metadata alone restores a successfully unlinked data file.
- **WAL tail reclamation.** `wal/shard.rs:1009` cumulatively merges tail entries and never removes retired stream IDs. A local probe created, appended to, checkpointed, and deleted 12 streams; every DELETE returned `204`, no stream files remained, but all 12 tail entries survived repeated checkpoints. Port the retirement/checkpoint coordination, including prevention of an in-flight checkpoint reintroducing retired IDs. Shape and change-log churn make this relevant even without TTL streams.
- **Wake deleted readers.** A caught-up long-poll remained pending more than 500 ms after direct DELETE in the local probe. Upstream explicitly notifies retirement. Our engine already closes streams first, so this is a log-server protocol/lifecycle improvement rather than a demonstrated engine retirement failure.
- **Fork/TTL edge cases.** Local recovery trusts persisted fork counts (`store.rs:853`), and child deletion can precede the asynchronous parent decrement (`:1018`), allowing a crash to leave phantom references. Upstream reconciles the recovered child-parent graph and serializes fork reservation against deletion. Local TTL arithmetic also uses unchecked time addition (`store.rs:442`), and expiry lookup and request touch are separate operations. Upstream uses checked arithmetic and an atomic touch/expiry decision. These are source-confirmed candidates for fork/TTL users; neither fault was reproduced during this audit.

Extract these as separate changes with their race/crash tests. The complete upstream reaper commit changes roughly 10,000 lines and includes features outside our scope; a wholesale cherry-pick would hide the important fixes in unrelated machinery.

### 5. Startup ownership and bounded replay deserve focused follow-ups

**Second-engine startup — confirmed, already documented locally.** [Commit `8b63db5`](https://github.com/indexedlabs/electric-circuits/commit/8b63db5) delays stateful startup until epoch/slot ownership checks allow it. Locally, publication creation and replica-identity changes occur before catalog/epoch verification (`engine/mod.rs:1370–1384`), and `Verdict::Busy` still proceeds to restore (`:1422`). A second engine can therefore become another catalog/shape writer while waiting for the first engine's replication slot. [Our deployment guide](../deployment-postgres.md) already requires one replica and `Recreate` for exactly this reason. Adapt the basic wait-before-restore/startup-side-effects fix; upstream's much larger managed handoff system is not required. A startup busy check alone is not a general distributed writer-ownership guarantee.

**Reactivation and pending-buffer bounds — confirmed resource gaps, design adaptation required.** [PR #17](https://github.com/indexedlabs/electric-circuits/pull/17), [commit `44d410c`](https://github.com/indexedlabs/electric-circuits/commit/44d410c), adds replay admission/budgets, request deadlines, buffer accounting, and coalescing. Locally, every table change is cloned into each pending shape's unbounded vector (`engine/sequencer.rs:946–947`); slow backfill or dormant replay can accumulate them indefinitely. The HTTP client is built without explicit deadlines and reads successful bodies with `res.text()` (`ds.rs:368`, `:403`), so a stalled storage peer can prevent a replay or retry budget from progressing.

Useful pieces are operation-specific deadlines, replay concurrency/work accounting, and visible pending-buffer memory. Preserve our guarantees: large transactions are accepted, and an acknowledged shape's work is landed or its retirement completed. Do not import a budget-exhaustion discard policy merely because upstream uses it.

If adding a fixed replay end, capture it in the `BeginShape` acknowledgement, where pending buffering actually starts. An earlier admission-time HEAD leaves a gap between replay and buffering; upstream corrected that mistake during #17. Our existing replay-to-head implementation does not have that particular fixed-end gap.

[PR #24](https://github.com/indexedlabs/electric-circuits/pull/24), [commit `63d77d4`](https://github.com/indexedlabs/electric-circuits/commit/63d77d4184fce32289c12d676cae5a84b616030d), is an essential companion if response caps are introduced: a JSON page target is not a bound on one indivisible value, and the engine must be able to read what it appends. **Its existing 16 MiB cap crash-loop is not present locally**, because we have not introduced that engine-side cap.

**Missing replay segment — source-confirmed candidate, fault not reproduced.** Our reactivation failure tries to evict the now-unresumable shape (`lifecycle.rs:1193`), but a joining caller has already taken a provisional subscription and eviction skips positive refcounts (`:1439`). The failed join later releases its claim, leaving retirement to another attempt/sweep. #17 supplies terminal retirement and typed retry-versus-recreate outcomes. Test storage loss of the resume segment specifically; loss of the shape's own stream is a different branch already handled here.

**Small native API fix.** [`d6429a5`](https://github.com/indexedlabs/electric-circuits/commit/d6429a5106970458878fee75bc4736301526fe4b) classifies unknown tables/projection columns as bad requests. Locally `http.rs:299` forwards them to generic engine errors, and `http.rs:847` returns 500. This survives removal of the Electric adapter. Typed deterministic request errors plus native `/shapes` regressions are a useful bounded fix; it does not require upstream's OpenAPI or route aliases.

## Fixes already covered, and changes that need a product decision

| Upstream work | Assessment against this tree |
|---|---|
| Boot admission and Postgres-mode schema refusal, [#28](https://github.com/indexedlabs/electric-circuits/pull/28) | Already covered by `ensure_booted` (`engine/mod.rs:1130`), lifecycle guards, and `SchemaIsPostgres` (`:1591`). Upstream's separate “completed a read” shutdown guard is defense in depth here: our `reading` flag means permission to read, but no corresponding stale-cursor failure was demonstrated with our boot gates. Do not label the whole PR missing. |
| Catalog restore and vanished shape streams, [#18](https://github.com/indexedlabs/electric-circuits/pull/18) | Our ADR-0009 restore already retires definitively missing streams and fails/retries the whole restore safely. Preserve our held-sequencer/rollback design. |
| Idle replication-slot advancement, [#11](https://github.com/indexedlabs/electric-circuits/pull/11) / [#12](https://github.com/indexedlabs/electric-circuits/pull/12) | Already implemented locally with transaction-boundary restrictions and conformance coverage. |
| Avoid redundant pool rollbacks, [#8](https://github.com/indexedlabs/electric-circuits/pull/8) | Already implemented by local transaction tracking (`pg.rs:304` onward). |
| Durable purge, provider abstraction, native API work, #1–#4 | Substantial equivalent work already exists. Extract individual error-classification improvements only where useful; do not replace the native API with upstream's additional surfaces. |
| Sequencer processing failures (`8e93eda`), closed flip propagation (`7c3a797`), replication connection waits (`1e8e81f`) | Already addressed locally. Preserve ADR-0010's schema-aware parked sequencer rather than importing upstream's exit policy; the closed-channel cleanup and replication setup timeout also have local implementations. |
| Bounded log-server reads, `8dea433`; advertised read cap, `51d398f` | Local log-server read paging already exists. Capability advertisement is useful if adopting cross-service response-cap negotiation, not a missing paging fix. |
| WAL startup guards and explicit data directory, `4c2a180`, `2c1382a` | Substantial equivalents already exist locally. |
| Composite-key deletes, [#26](https://github.com/indexedlabs/electric-circuits/pull/26) | Fix is in the removed Electric adapter. Our native key encoding/projection must remain intact, but this is not a missing native delete fix. |
| Electric JSONB wire types, source transaction IDs, gateway bearer, compatibility packaging (#6, #7, #13, #14, part of #22) | Adapter/packaging-specific; the affected Electric surface was removed here. |
| Managed Postgres sources, source-table supervisor, runtime authority, blue/green handoff (#5, #10, #15, #19–#22) | Different deployment product. Consider only if we choose externally managed source setup or multiple source lifecycles; no blanket import. |
| External change-log consumers, [#16](https://github.com/indexedlabs/electric-circuits/pull/16) | New feature with durable consumer retention pins. Relevant only if an external consumer is deliberately added; the current engine is the change-log reader. |
| Private mTLS and optional TLS (#23; log-server `74cdfa4`) | Deployment capability, not a demonstrated bug in our current topology. |
| Reject RLS-enabled tracked tables, [`4b66aad`](https://github.com/indexedlabs/electric-circuits/commit/4b66aad) | Upstream makes RLS an unsupported configuration. Our tree does not impose that policy. Review the snapshot/replication role contract if needed; do not assume app-table RLS is invalid or that this blanket rejection is suitable for pgxsinkit. No deployed data leak was demonstrated. |
| Durable single/multi-stream subscriptions, log-server `abe7bbe`, `11137ca` | New log-server protocol surfaces. They are distinct from the engine's named shape subscriptions; not required to fix our existing paths. |
| Expiration reaper, log-server `5093702` | A bounded active reaper may be useful for standalone TTL workloads; our engine explicitly retires its own streams. Take the generic lifecycle/recovery fixes independently. |
| Memory diagnostics/OTLP, `e200573`, `9cc4b59` | Optional operational improvements. Pending-buffer accounting is the most directly useful part for our identified resource gap. |

## Relevant follow-ups that upstream has not finished

These are leads, not fixes available to copy:

- [PR #27's follow-ups](https://github.com/indexedlabs/electric-circuits/pull/27) identify an in-flight page overwriting a live update just outside the loaded window. The local merge also discards a never-present out-of-window upsert without retaining its version (`subset.ts:354`), while page merge only consults retained watermarks (`:624`). Add this case when fixing page/live merging. Its API-adapter follow-up also matters: a new snapshot-settle refusal must retain retryable status through our tRPC adapter, rather than becoming a generic 500.
- [PR #17's review](https://github.com/indexedlabs/electric-circuits/pull/17#pullrequestreview-5106691376) calls out whole inner-seed row materialization and dormant age resetting on restart. Local subquery seeding still collects rows (`lifecycle.rs:1789`); local restored dormant state uses process-local age. These are memory/retention design candidates, not evidence that upstream has completed either fix. The [final review reply](https://github.com/indexedlabs/electric-circuits/pull/17#issuecomment-5534855122) corrects earlier claims about supposedly failing regression tests. Validate production wiring locally when extracting #17; do not inherit its early test claims.
- [PR #18's follow-ups](https://github.com/indexedlabs/electric-circuits/pull/18) describe stream creation before durable catalog identity. Our row/subquery ordering differs, but aggregate stream creation still precedes enqueueing `Created` (`lifecycle.rs:657`, `:698`, `:797`). A separate crash/cancellation reproduction is needed before classifying this as a current defect. The same PR's sequential restore-preflight concern is already handled by our bounded concurrent HEAD checks.
- [PR #26's acknowledged follow-ups](https://github.com/indexedlabs/electric-circuits/pull/26#issuecomment-5824620947) concern the removed Electric adapter, including composite-key schema headers and delete encoding. They do not establish native-path defects here.
- [Log-server `4c3a690`](https://github.com/indexedlabs/durable-streams-rust/commit/4c3a690) records incomplete reaper hardening: its quarantine preflight still treats unreadable/malformed tails metadata as empty, create compensation needs stronger durability, and shutdown needs a bounded drain. Use a checked tails reader when adapting recovery; upstream's new implementation is not a complete answer under arbitrary filesystem faults.

These findings are distinct from existing backlog entries: client retirement resubscription ([0010](../backlog/0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md)), forced-exit checkpoint-window replay ([0011](../backlog/0011-a-forced-exit-replays-the-checkpoint-window.md)), and load-sensitive test timing ([0013](../backlog/0013-fail-closed-skip-test-times-out-under-load.md)). Upstream #28 does not promise exactly-once shape-stream delivery after a hard stop.

## Complete PR coverage

All engine numbers below refer to `indexedlabs/electric-circuits`, not the historical Electric tracker. “Covered” means the relevant behavior is already present, not that every feature in that PR was adopted.

| PR | State / disposition |
|---|---|
| Log server [#1](https://github.com/indexedlabs/durable-streams-rust/pull/1) | Merged; explicit non-loopback opt-in fixes an upstream-only WAL loopback restriction. Our CLI already allows the configured host. |
| Engine [#1](https://github.com/indexedlabs/electric-circuits/pull/1) | Merged; native API/PG18/durable purge substantially covered. |
| [#2](https://github.com/indexedlabs/electric-circuits/pull/2) | Merged; fake-storage lost-wakeup test fix covered by local release-generation test seams. |
| [#3](https://github.com/indexedlabs/electric-circuits/pull/3) | Merged; provider abstraction has a local equivalent; extra provider surface is conditional. |
| [#4](https://github.com/indexedlabs/electric-circuits/pull/4) | Merged; native/PG18 work partly covered; prototype and extended API surface differ. |
| [#5](https://github.com/indexedlabs/electric-circuits/pull/5) | Open; managed-source feature overlaps merged #15; no unique unmerged correctness fix identified. |
| [#6](https://github.com/indexedlabs/electric-circuits/pull/6) | Merged; Electric JSONB encoding, removed surface. |
| [#7](https://github.com/indexedlabs/electric-circuits/pull/7) | Merged; bundled Electric image topology, different from our separate images. Does not fix local log-server image backlog 0003. |
| [#8](https://github.com/indexedlabs/electric-circuits/pull/8) | Merged; pool rollback tracking covered. |
| [#9](https://github.com/indexedlabs/electric-circuits/pull/9) | Merged; CI concurrency precedent, not an established required local change. |
| [#10](https://github.com/indexedlabs/electric-circuits/pull/10) | Merged; managed writer handoff, optional deployment feature. |
| [#11](https://github.com/indexedlabs/electric-circuits/pull/11) | Merged; idle slot advancement covered. |
| [#12](https://github.com/indexedlabs/electric-circuits/pull/12) | Merged to `mighty/backport-require-tls`; duplicate of #11. |
| [#13](https://github.com/indexedlabs/electric-circuits/pull/13) | Merged; Electric txid headers, removed surface. |
| [#14](https://github.com/indexedlabs/electric-circuits/pull/14) | Merged to `mighty/backport-require-tls`; duplicate of #13. |
| [#15](https://github.com/indexedlabs/electric-circuits/pull/15) | Merged; managed Postgres setup, conditional. |
| [#16](https://github.com/indexedlabs/electric-circuits/pull/16) | Merged; external change-log reads and retention pins, conditional. |
| [#17](https://github.com/indexedlabs/electric-circuits/pull/17) | Merged; useful replay bounds/deadlines/accounting, with unresolved follow-ups above. |
| [#18](https://github.com/indexedlabs/electric-circuits/pull/18) | Merged; missing-stream restore covered; aggregate identity-ordering lead remains unverified. |
| [#19](https://github.com/indexedlabs/electric-circuits/pull/19) | Merged; partner runtime authority, absent product surface. |
| [#20](https://github.com/indexedlabs/electric-circuits/pull/20) | Merged; source-table supervisor, absent product surface. |
| [#21](https://github.com/indexedlabs/electric-circuits/pull/21) | Merged; source-scoped reset refusal applies to #20, not our operator epoch reset. |
| [#22](https://github.com/indexedlabs/electric-circuits/pull/22) | Merged; Mighty plugin ownership/gateway bearer, absent product surface. |
| [#23](https://github.com/indexedlabs/electric-circuits/pull/23) | Merged; relaxing upstream TLS admission, not a current local restriction. |
| [#24](https://github.com/indexedlabs/electric-circuits/pull/24) | Merged; required sizing lesson if we introduce read caps, exact current bug absent. |
| [#25](https://github.com/indexedlabs/electric-circuits/pull/25) | Merged; initialization and rollback lifetime fixes apply. |
| [#26](https://github.com/indexedlabs/electric-circuits/pull/26) | Merged; Electric composite-key deletes, removed surface. |
| [#27](https://github.com/indexedlabs/electric-circuits/pull/27) | Merged; snapshot visibility, xid wrap, page/live and concurrent-page fixes apply. |
| [#28](https://github.com/indexedlabs/electric-circuits/pull/28) | Merged; boot race independently covered; unread-checkpoint guard is defense in depth. |

## Suggested implementation order

1. Persistent quarantine and WAL recovery refusal, plus honest DELETE failure handling, with repeated-restart fault tests.
2. Explicit rollback cancellation, followed by predictable subquery initialization admission.
3. Snapshot/live-boundary correctness as one engine/protocol/client workstream, including xid wrap and overlapping `loadMore()` regressions.
4. Log-server create/delete/append fencing and retirement-aware WAL tail pruning, in independently reviewed changes.
5. Basic startup ownership guard, then bounded replay and pending-buffer accounting.

The first three address lost recoverability, incomplete cleanup, and silently stale results. Remaining deployment features can wait for an explicit need. None of these recommendations requires restoring Electric compatibility or making Circuits track either upstream again.

## Evidence and validation

Scratch evidence is under `tmp/agents/upstream-audit-2026-10-02/`:

- `engine-findings.md`, `engine-inventory.json`, `log-findings.md`, `issues-findings.md`: reviewed lane reports, including the full 211-row engine commit inventory and all 16 log-server commit dispositions.
- `subset-probe.ts` / `.json`: directly invokes local `mergeFeedDelta`; both invisible insert and update are dropped.
- `load-more-probe.ts` / `.json`: actual local `createSubset`, fake tRPC/empty live feed; reverses two page responses and observes `new -> old`.
- `quarantine-probe.ts` and `quarantine-probe-1790912176216/results.json`: isolated local log server, acknowledged append, injected file damage, two restarts.
- `retirement-probe-1790912261067/results.json`: normal create/append/delete lifecycle, persistent WAL tail entries and direct-delete long-poll behavior.
- `delete-error-probe.ts` and `delete-error-probe-1790912554121/results.json`: actual unlink permission failures produce a false successful DELETE and restart resurrection.

`bun run durable-streams:build` passed; probes used that fresh local binary. `bun run test:durable-streams` passed: 131 unit tests, four CLI durability-guard tests and three chunk-limit CLI tests; two tests were ignored. Localhost tests/probes required approved execution outside the sandbox after bind denial. All probe servers were stopped. These passing baseline tests do not cover the reproduced gaps. Upstream regression tests were inspected, not run.

These probes establish the stated mechanisms, not a clean conformance gate. The audit changes documentation only; engine conformance, the protocol conformance suite, `bun run validate`, and `bun run validate:full` were not run for this documentation change. `git diff --check` and the report's local-link check passed. Every eventual engine/log-server implementation must run both required repository gates.

## Execution update — first implementation batch

The user authorized implementation after the baseline audit. Separate implementers own log-server recovery/deletion, cancellation/admission, snapshot consistency across engine/API/client, and log-server HTTP deadlines; the coordinator reviews their changes and combines validation. No dependencies are changed.

The first batch addresses:

- Persistent quarantine, checked checkpoint-tail metadata, and refusal before WAL replay when unresolved identities own durability evidence.
- Honest DELETE failures, append/delete lifetime coordination, and exclusion of metadata writers from hard deletion.
- Detached explicit create **and join** rollback, with subquery initialization admission retained through installation or cleanup.
- Settled snapshots for dependent tables, modular xid ordering and the WAL insertion horizon, typed 503 propagation, serialized subset pagination, and preservation of live changes across in-flight pages. Review additionally found and covered deferred-update replay order when a map key is updated more than once.
- Finite connection, read and whole-request deadlines for log-server HTTP calls, with separate long-poll allowances and no response-size cap.

The audit's broader follow-ups remain separate work: durable create-before-publication, full fork metadata isolation/recovery and TTL edge cases, direct-delete reader notification, WAL tail reclamation, startup slot ownership, replay/pending-buffer resource controls, missing-segment retirement, and deterministic native bad-request classification. The initial DELETE changes cover the lifetime and metadata fences needed for honest deletion; they do not constitute the entire upstream reaper/lifecycle feature.

Validation of this batch is complete. Independent reviews covered recovery/deletion, cancellation/admission, HTTP deadlines, and settled snapshots; review findings were fixed and covered by regression tests. Focused tests passed for those paths, client merging, and eleven real-Postgres snapshot regressions.

Both required repository gates passed: `bun run validate` and `bun run validate:full` (with PostgreSQL 18's binaries on `PATH` and approved local process execution outside the sandbox). Formatting, typechecking, lint, both crates' Rust suites, and all 82 TypeScript unit tests passed. The full gate additionally passed all 225 engine integration tests across 56 files and 332 Durable Streams protocol tests; six protocol tests and two Rust tests remain skipped/ignored by their existing suites. Gate logs are under `tmp/agents/upstream-fixes-2026-10-02/` (`validate.log` and `validate-full.log`).
