# Backlog

The documented ledger of known defects, investigations, deferred designs and completed repairs,
with their evidence retained. Same rules as pgxsinkit's `docs/backlog`:

- One numbered file per item. Entries are **never deleted** — status flips instead, so a symptom
  someone trips over next year finds the prior investigation instead of restarting it.
- Every item carries a **Reopen trigger**: the concrete event or evidence that justifies picking it
  up. Until that fires, the item is settled — do not re-litigate it from scratch.
- `Status: resolved` (implemented/fixed, with validation and limits retained) · `in progress` (selected, with implementation or qualification underway) · `parked` (investigated, evidence recorded, waiting on the trigger) · `candidate`
  (improvement we would take, unscheduled) · `promoted → adr/00xx` (one-line pointer to the ADR that
  superseded it) · `dropped` (decided against without implementation; keep the why). Earlier entries used `dropped (fixed)` for completed work; those are now `resolved`.
- This directory is an engineering ledger, not user documentation.

## Items

Entry numbers are identifiers in creation order, not priority. The 2026-10-03 implementation sequence followed the upstream audit and related defects exposed by its fixes; it was not a comparison of every older entry against every newer one. Future selection should compare consumer impact, correctness, deployment exposure and executed evidence across the whole backlog before choosing another audit follow-up.

### Earlier entries: provisional triage (2026-10-04)

This assessment uses the entries' recorded evidence, with source spot checks for the image default, silent HTTP error conversion, client lifecycle and ignored dbsp settings. The 2026-10-04 refresh qualifies 0010's actual consumer exposure and resolves 0007's missing process lanes plus the 0013 fixture ordering defect exposed by full validation. It is not a fresh reproduction or closure audit of all fifteen items. Older recommendations and status may need updating before implementation; for example, 0001's separate pool-warning discussion predates the pool check-in fix recorded in 0016.

| Entries          | Assessment                                 | Reason / next decision                                                                                                                                                                                                         |
| ---------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 0011             | Resolved for plain Postgres shapes         | Durable delivery frontiers and original seed/registration coverage passed real restart, full engine and protocol qualification. Other output modes remain in 0034.                                                             |
| 0010             | Conditional medium client liveness         | The harness uses this client; production pgxsinkit's separate reader already recovers. Lease renewal rebinds changed handles; zero leases and false terminal responses still need executed qualification.                      |
| 0001             | Resolved                                   | Both native creation routes now emit bounded structured refusal diagnostics, including validation and extraction, with response semantics retained and full validation green. The earlier pool-warning work was already fixed. |
| 0005             | Medium deployment constraint               | New/re-created tables require restart; choose automatic discovery or explicit reload only when the migration/restart contract requires it.                                                                                     |
| 0007             | Resolved                                   | Real-binary tests cover both boot-fatal configurations and counts drift/restart/re-seed, including seed-covered TRUNCATE replay and subsequent live delivery; full validation passed.                                          |
| 0009             | Low to medium operational                  | Some misconfiguration retries indefinitely, but readiness and logs expose it. Error classification or retry-budget changes need a chosen contract.                                                                             |
| 0015             | Low operational; bounded cleanup candidate | Two accepted settings promise effects they do not have; current source still parses/logs them.                                                                                                                                 |
| 0002, 0006, 0008 | Low                                        | Harness noise, rare redundant resync and late configuration diagnostics respectively; recorded behavior does not show lost durable data.                                                                                       |
| 0003             | Deployment choice                          | Default image startup deliberately fails without an explicit durable data directory; current deployment arguments supply it. A default path needs an explicit persistence contract.                                            |
| 0004             | Fixed                                      | Retained historical test-race record.                                                                                                                                                                                          |
| 0013             | Resolved                                   | Controlled retirement-response scheduling reproduced an insufficient fixture barrier; held old pages now release only after the new schema is installed. Full validation passed.                                               |
| 0012, 0014       | Parked or trigger-dependent                | Dependency compatibility and the separate load-sensitive lock-test lead retain their recorded evidence and reopen conditions.                                                                                                  |

Plain Postgres delivery in 0011, creation-refusal diagnostics in 0001 and native refusal/recovery qualification in 0007 are resolved. Remaining medium items include deployment choices and conditional client recovery; refresh their current evidence and consumer exposure before selecting a repair. Optional 0019 fork recovery depends on fork workloads. Unsequenced-output leads in 0034 and producer-only identity in 0033 require fresh qualification; neither is a reproduced native defect. The harness-client issue should rise if a non-harness consumer needs retirement recovery or a client-level regression demonstrates stalled reads. Entry age alone is neither urgency nor authorization.

### Remaining correctness and recovery scope (2026-10-04)

Seven entries concern potential data correctness or crash-recovery risks: **0019, 0024, 0025,
0027, 0028, 0033 and 0034**. None currently records a reproduced local corruption failure.
This includes conservative fork-pin retention and defense-in-depth/optional-topology leads,
not seven established data-loss bugs; their individual triggers still govern qualification.

Three other entries record correctness or liveness limitations: **0005** (table discovery needs
restart), **0006** (redundant retirement/resync after TRUNCATE replay) and **0010** (client
terminal-read recovery gaps). They are not recorded as durable-data corruption. The fresh 0007
fixture observed 0006's premature replacement retirement; 0010's narrowed gaps remain
source-derived without a fresh client-level semantic red. **0007** qualifies existing safety
mechanisms rather than identifying another corruption defect.

### Upstream audit entries

The upstream-audit backlog resumed on 2026-10-03 with fixes for 0017, 0018, 0020, 0021, 0022, 0029, 0030 and 0031. Urgency describes
the remaining local surface; parked leads need evidence before they become defect repairs.

| Entries    | Urgency                      | Reason                                                                                                                                    |
| ---------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 0017       | Fixed                        | Shared pending memory/spill budget, replay admission and a fixed endpoint; total process RSS and disk capacity remain separate.           |
| 0018       | Fixed                        | Reference-only parent metadata writes, conservative failed-write pins and owned release retries.                                          |
| 0019       | Conditional medium           | Fork graph crash recovery remains separate; the engine's ordinary read path does not use DS forks.                                        |
| 0020, 0022 | Fixed                        | Checked TTL deadlines and atomic expiry/renewal; chunked subquery seeds and cancellation-safe contributor retraction.                     |
| 0021       | Fixed                        | Sticky DELETE observation ends inline and reactor SSE, including pending reads and backpressured sockets, without claiming close.         |
| 0023       | Low                          | Conservative dormant-age semantics; no production retention incident was reproduced.                                                      |
| 0024–0028  | Parked; no confirmed urgency | Crash/shutdown/ownership leads and checkpoint defense in depth require the recorded trigger and diagnosis.                                |
| 0029       | Fixed                        | Committed metadata capture, durable close retries and owned completion of cancelled staged POST/PUT bodies.                               |
| 0030       | Fixed                        | Accepted checkpoint tail capture and release recovery guards for false proofs and first/later WAL gaps.                                   |
| 0031       | Fixed                        | Coherent runtime cache/watch publication prevents stale callbacks from hiding newer bytes and durable EOF.                                |
| 0032       | Parked; unproved             | Two CLI startup connections failed after readiness; child/socket diagnostics added, five further full-file runs passed, cause unresolved. |
| 0016       | Historical record            | Resolution boundaries, validation and deferred product choices; no new implementation authorization.                                      |

- [0001 — A refused shape create is not logged](0001-refused-shape-create-not-logged.md) — resolved
- [0002 — The harness client retries a dead subscription's renewal at its floor cadence, logging a non-JSON body](0002-harness-client-renewal-retry-storm.md) — candidate
- [0003 — The log server image's default arguments do not start it](0003-log-server-image-default-arguments-do-not-start.md) — candidate
- [0004 — The boot-errors test raced the engine's first retry warning](0004-boot-errors-test-raced-the-first-retry-warning.md) — resolved
- [0005 — The reconciler does not pick up new or re-created tables without a restart](0005-reconciler-does-not-pick-up-new-tables.md) — candidate
- [0006 — A TRUNCATE replayed after a crash re-retires the table's shapes](0006-truncate-replay-window-re-retires-shapes.md) — candidate
- [0007 — Three refusal paths have no end-to-end lane](0007-three-refusal-paths-have-no-end-to-end-lane.md) — resolved
- [0008 — `CIRCUITS_DS_URL` is not validated at resolve](0008-ds-url-not-validated-and-prometheus-port-ignored.md) — candidate
- [0009 — A Postgres error with no SQLSTATE and no io source retries forever at boot](0009-no-sqlstate-postgres-error-retries-forever.md) — candidate
- [0010 — The harness client does not re-subscribe when its stream is retired](0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md) — candidate
- [0011 — A forced exit replays the checkpoint window onto shape streams](0011-a-forced-exit-replays-the-checkpoint-window.md) — resolved for plain Postgres shapes (other modes: 0034)
- [0012 — vitest is held at 4.x by the protocol suite](0012-vitest-held-at-4-by-the-protocol-suite.md) — parked
- [0013 — The change-log "skips the envelopes a migration outran" test times out under load](0013-fail-closed-skip-test-times-out-under-load.md) — resolved (fixture ordering)
- [0014 — The log server's second-server test failed once under heavy load](0014-log-server-lock-test-failed-once-under-load.md) — parked
- [0015 — Two dbsp settings are read, logged and do nothing](0015-two-dbsp-settings-are-read-and-do-nothing.md) — candidate
- [0016 — IndexedLabs upstream audit and resolution ledger](0016-indexedlabs-audit-resolution-ledger.md) — parked
- [0017 — Pending shapes and dormant replay need resource accounting and bounds](0017-pending-buffer-and-replay-resource-controls.md) — resolved
- [0018 — Fork reference updates can persist speculative parent append metadata](0018-fork-refcounts-persist-speculative-parent-metadata.md) — resolved
- [0019 — Fork reference counts need graph-aware crash recovery](0019-fork-graph-and-reference-recovery.md) — candidate
- [0020 — TTL deadline arithmetic and touch/expiry decisions need one safe boundary](0020-ttl-deadline-arithmetic-and-atomic-touch.md) — resolved
- [0021 — Direct DELETE terminal notification is not wired through SSE serving](0021-direct-delete-sse-terminal-notification.md) — resolved
- [0022 — Subquery inner seeding still materializes the whole row vector](0022-subquery-inner-seed-materializes-all-rows.md) — resolved
- [0023 — A restart resets the age used for dormant-shape TTL](0023-dormancy-age-restarts-at-boot.md) — parked
- [0024 — Aggregate stream creation precedes catalog identity: unproven crash lead](0024-aggregate-stream-before-catalog-identity.md) — parked
- [0025 — Failed log-server create compensation needs crash-image qualification](0025-failed-create-compensation-crash-recovery.md) — parked
- [0026 — Qualify the log server's complete shutdown bound under stalled storage](0026-log-server-shutdown-drain-qualification.md) — parked
- [0027 — Gate shutdown checkpointing on an actually completed read](0027-checkpoint-only-after-a-completed-read.md) — parked
- [0028 — A slot-busy startup guard is not distributed writer ownership](0028-simultaneous-cold-start-writer-coordination.md) — parked
- [0029 — General metadata writers can capture speculative append state](0029-general-metadata-writers-capture-speculative-append-state.md) — resolved
- [0030 — Checkpoint tail capture can cross a rejected append](0030-checkpoint-tail-can-cross-a-rejected-append.md) — resolved
- [0031 — Tail watch publication can regress after close](0031-tail-watch-publication-can-regress-after-close.md) — resolved
- [0032 — CLI readiness accepted before the first PUT connection was refused](0032-cli-readiness-accepted-before-first-connection-refused.md) — parked
- [0033 — Producer deduplication identity can lag durable bytes](0033-producer-deduplication-identity-can-lag-durable-bytes.md) — candidate
- [0034 — Unsequenced output needs separate restart qualification](0034-unsequenced-output-needs-separate-restart-qualification.md) — parked

## Not carried over

Four issues of `pgxsinkit/electric-circuits` were open when this repository was made and are not
items here. #4 (`subset()` with limit 0), #5 (`subset()` and NULL sort keys) and #14 (the walsender
connect timeout) were already fixed in the code. #18 is a defect of the compatibility adapter, which
has since been removed (ADR-0011).
