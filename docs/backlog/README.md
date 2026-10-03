# Backlog

The documented ledger for work we deliberately are **not** doing now: parked investigations (with
their evidence), improvement candidates, and escape-hatched designs. Same rules as pgxsinkit's
`docs/backlog`:

- One numbered file per item. Entries are **never deleted** — status flips instead, so a symptom
  someone trips over next year finds the prior investigation instead of restarting it.
- Every item carries a **Reopen trigger**: the concrete event or evidence that justifies picking it
  up. Until that fires, the item is settled — do not re-litigate it from scratch.
- `Status: parked` (investigated, evidence recorded, waiting on the trigger) · `candidate`
  (improvement we would take, unscheduled) · `promoted → adr/00xx` (one-line pointer to the ADR that
  superseded it) · `dropped` (decided against; keep the why).
- This directory is an engineering ledger, not user documentation.

## Items

The upstream-audit backlog resumed on 2026-10-03 with fixes for 0017, 0018, 0020, 0022, 0029 and 0030. Urgency describes
the remaining local surface; parked leads need evidence before they become defect repairs.

| Entries    | Urgency                      | Reason                                                                                                                            |
| ---------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 0017       | Fixed                        | Shared pending memory/spill budget, replay admission and a fixed endpoint; total process RSS and disk capacity remain separate.   |
| 0018       | Fixed                        | Reference-only parent metadata writes, conservative failed-write pins and owned release retries.                                  |
| 0019       | Conditional medium           | Fork graph crash recovery remains separate; the engine's ordinary read path does not use DS forks.                                |
| 0020, 0022 | Fixed                        | Checked TTL deadlines and atomic expiry/renewal; chunked subquery seeds and cancellation-safe contributor retraction.             |
| 0021, 0023 | Low                          | SSE terminal notification and conservative dormant-age semantics; no SSE failure or production retention incident was reproduced. |
| 0024–0028  | Parked; no confirmed urgency | Crash/shutdown/ownership leads and checkpoint defense in depth require the recorded trigger and diagnosis.                        |
| 0029       | Fixed                        | Committed metadata capture, durable close retries and owned completion of cancelled staged POST/PUT bodies.                       |
| 0030       | Fixed                        | Accepted checkpoint tail capture and release recovery guards for false proofs and first/later WAL gaps.                           |
| 0031       | Conditional; unproved        | Delayed watch publication may affect inline SSE; source-derived, not locally reproduced.                                          |
| 0016       | Historical record            | Resolution boundaries, validation and deferred product choices; no new implementation authorization.                              |

- [0001 — A refused shape create is not logged](0001-refused-shape-create-not-logged.md) — candidate
- [0002 — The harness client retries a dead subscription's renewal at its floor cadence, logging a non-JSON body](0002-harness-client-renewal-retry-storm.md) — candidate
- [0003 — The log server image's default arguments do not start it](0003-log-server-image-default-arguments-do-not-start.md) — candidate
- [0004 — The boot-errors test raced the engine's first retry warning](0004-boot-errors-test-raced-the-first-retry-warning.md) — dropped (fixed)
- [0005 — The reconciler does not pick up new or re-created tables without a restart](0005-reconciler-does-not-pick-up-new-tables.md) — candidate
- [0006 — A TRUNCATE replayed after a crash re-retires the table's shapes](0006-truncate-replay-window-re-retires-shapes.md) — candidate
- [0007 — Three refusal paths have no end-to-end lane](0007-three-refusal-paths-have-no-end-to-end-lane.md) — candidate
- [0008 — `CIRCUITS_DS_URL` is not validated at resolve](0008-ds-url-not-validated-and-prometheus-port-ignored.md) — candidate
- [0009 — A Postgres error with no SQLSTATE and no io source retries forever at boot](0009-no-sqlstate-postgres-error-retries-forever.md) — candidate
- [0010 — The harness client does not re-subscribe when its stream is retired](0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md) — candidate
- [0011 — A forced exit replays the checkpoint window onto shape streams](0011-a-forced-exit-replays-the-checkpoint-window.md) — candidate
- [0012 — vitest is held at 4.x by the protocol suite](0012-vitest-held-at-4-by-the-protocol-suite.md) — parked
- [0013 — The change-log "skips the envelopes a migration outran" test times out under load](0013-fail-closed-skip-test-times-out-under-load.md) — candidate
- [0014 — The log server's second-server test failed once under heavy load](0014-log-server-lock-test-failed-once-under-load.md) — parked
- [0015 — Two dbsp settings are read, logged and do nothing](0015-two-dbsp-settings-are-read-and-do-nothing.md) — candidate
- [0016 — IndexedLabs upstream audit and resolution ledger](0016-indexedlabs-audit-resolution-ledger.md) — parked
- [0017 — Pending shapes and dormant replay need resource accounting and bounds](0017-pending-buffer-and-replay-resource-controls.md) — dropped (fixed)
- [0018 — Fork reference updates can persist speculative parent append metadata](0018-fork-refcounts-persist-speculative-parent-metadata.md) — dropped (fixed)
- [0019 — Fork reference counts need graph-aware crash recovery](0019-fork-graph-and-reference-recovery.md) — candidate
- [0020 — TTL deadline arithmetic and touch/expiry decisions need one safe boundary](0020-ttl-deadline-arithmetic-and-atomic-touch.md) — dropped (fixed)
- [0021 — Direct DELETE terminal notification is not wired through SSE serving](0021-direct-delete-sse-terminal-notification.md) — candidate
- [0022 — Subquery inner seeding still materializes the whole row vector](0022-subquery-inner-seed-materializes-all-rows.md) — dropped (fixed)
- [0023 — A restart resets the age used for dormant-shape TTL](0023-dormancy-age-restarts-at-boot.md) — parked
- [0024 — Aggregate stream creation precedes catalog identity: unproven crash lead](0024-aggregate-stream-before-catalog-identity.md) — parked
- [0025 — Failed log-server create compensation needs crash-image qualification](0025-failed-create-compensation-crash-recovery.md) — parked
- [0026 — Qualify the log server's complete shutdown bound under stalled storage](0026-log-server-shutdown-drain-qualification.md) — parked
- [0027 — Gate shutdown checkpointing on an actually completed read](0027-checkpoint-only-after-a-completed-read.md) — parked
- [0028 — A slot-busy startup guard is not distributed writer ownership](0028-simultaneous-cold-start-writer-coordination.md) — parked
- [0029 — General metadata writers can capture speculative append state](0029-general-metadata-writers-capture-speculative-append-state.md) — dropped (fixed)
- [0030 — Checkpoint tail capture can cross a rejected append](0030-checkpoint-tail-can-cross-a-rejected-append.md) — dropped (fixed)
- [0031 — Tail watch publication can regress after close](0031-tail-watch-publication-can-regress-after-close.md) — candidate

## Not carried over

Four issues of `pgxsinkit/electric-circuits` were open when this repository was made and are not
items here. #4 (`subset()` with limit 0), #5 (`subset()` and NULL sort keys) and #14 (the walsender
connect timeout) were already fixed in the code. #18 is a defect of the compatibility adapter, which
has since been removed (ADR-0011).
