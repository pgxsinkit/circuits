# 0007 — Three refusal paths have no end-to-end lane

Status: resolved (2026-10-04)
Opened: 2026-08-22 · Area: `packages/conformance/src/conformance-refusal-paths.test.ts`, `apps/engine/src/pg.rs` (`inspect_publication`, `check_wal_level`), `apps/engine/src/engine/drift.rs` (`circuit_needs_rebuild`)
Reopen trigger: any change to one of the three refusal paths, or to the exit codes they use.

Carried over on 2026-09-29 from issue #13 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## Historical gap

At recording, three refusal paths were unit-tested but had no end-to-end lane:

1. **Circuit-tier drift exit (75).** `circuit_needs_rebuild` (ADR-0005) is unit-tested; no lane boots with `CIRCUITS_DBSP_COUNTS`, triggers drift on a circuit-served table, and asserts exit 75 → restart → re-seed → recover.
2. **Publication column-list refusal.** `pg::inspect_publication` refuses a publication with a column list (boot-fatal, exit 78); the harness always derives `<slot>_pub` itself, so the refusal is never exercised. `prattrs` is PG15+; only a hand-made publication reaches it.
3. **`wal_level` ≠ `logical` refusal.** `pg::check_wal_level` is called explicitly at connect and the classifier is unit-tested; the harness cluster is always `logical`, and changing `wal_level` needs a Postgres restart, so a conformance case needs a second throwaway cluster.

## Original fix direction

One conformance file per item, each booting the binary through the harness: (1) a `DBSP_COUNTS` lane with `ADD COLUMN` on the circuit table; (2) a hand-made publication with a column list → assert exit 78 and the named message; (3) an `initdb` with `wal_level = replica` → assert exit 78.

## Original deferral

Test debt only; the mechanisms themselves are covered. Surfaced in the slice-2b and slice-6 reviews (`docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).

## Current qualification (2026-10-04)

Selected after 0011 and 0001: these paths govern fail-closed boot and recovery for the native
engine, while 0005 requires a deployment contract and 0010's direct terminal-read recovery is
conditional on the harness client's use. A fresh source/test search found no process lane for
the three cases above. This is validation debt, not a newly reproduced production defect.
The implementation exercises the real binary and Postgres configuration, including restart,
re-seeding and subsequent live delivery for circuit drift. Executed results and limits follow.

## Implemented qualification

One integration file adds three real-process tests; it is automatically collected by the
integration project and changes no production code or dependency configuration.

- **Counts recovery:** proves actual seeded counts placement and its aggregate consumer through
  `/graph`, then verifies live counts before drift. `ADD COLUMN` plus an insert exits 75 with
  the named cause. The triggering xid is absent from the change log before exit and present
  after restart against the same database, slot and WAL-backed log server. The replacement
  aggregate is re-seeded to 3 and advances live to 4; the new column is readable, the original
  aggregate is retired, and an unrelated shape retains its stream and receives a subsequent
  update. A TRUNCATE companion independently reaches the unconditional replay path, observes
  the exact xid plus the seed-covered “Not restarting again” warning, then re-seeds to 1 and
  advances live to 2 without another exit.
- **Publication column list:** creates the matching publication and verifies `prattrs` before
  boot. A brief catalog lock holds setup while `/ready` and native shape creation both refuse
  with 503; releasing it leads to exit 78 naming the publication/table and whole-row requirement.
  The process never reports resolved listening.
- **Non-logical WAL:** starts an isolated ephemeral cluster, verifies `SHOW wal_level = replica`,
  and requires exit 78 naming the setting and required Postgres restart, without resolved listening.

Fixture cleanup reaps every engine before deleting its resources, attempts independent teardown
steps despite failures and retains the original assertion/setup failure alongside cleanup errors.
A replica cluster whose stop fails with a remaining PID file keeps its directory for inspection.

The first focused run passed ADD COLUMN recovery and both fatal refusals, then failed when a
replacement aggregate created before TRUNCATE replay was retired. This confirms the already
recorded [0006](0006-truncate-replay-window-re-retires-shapes.md) behavior, not a failed xid restart
guard. Waiting for actual replay before creating that replacement isolates this qualification.
The corrected file passed all three tests, including after cleanup error-flow changes; typecheck
passed. The first full gate stopped at `no-unsafe-finally` on two cleanup throws; those were
replaced with explicit error aggregation after cleanup, without lint suppressions. Coordinator
inspection and independent review found no remaining blocker.

Full-suite qualification then exposed two independent fixture-ordering assumptions. The first
run passed all three new cases but reopened 0013 at its stale-schema counter; its controlled
diagnosis and repair are recorded separately. With that repair, the next run passed all 0013
cases and 253 integration cases overall, but the counts case's immediate post-boot aggregate
creation received the expected retryable 503 while the replayed Relation was still resolving
the table. Global readiness does not promise that replay resolution has finished.

The existing exact ADD COLUMN xid replay wait now precedes replacement creation, as the TRUNCATE
barrier already did. That xid was absent before exit 75, and committed output follows inline
Relation handling, so its appearance supplies the missing phase boundary. Strict 200 creation
assertions remain; no general create retries or longer deadlines were added. Independent review
accepted the boundary and the corrected file passed 3/3 (10.90 seconds total). This proves the
chosen replay finished, not that arbitrary later concurrent drift cannot refuse a create.

Final `bun run validate:full` passed on that source: format, typecheck, lint, 440 engine Rust
unit tests plus 51 other engine cases, 225 log-server unit tests, eight CLI cases, 82 TS unit
tests, all 254 integration cases across 62 files and 332 protocol cases. The existing two Rust
ignores and six protocol skips remain. Both this item and the bounded 0013 fixture repair are
resolved; no production behavior or dependency/tool configuration changed.

## Limits

These are process/configuration and normal restart qualifications, not power-loss or filesystem
fault tests. ADD COLUMN matches the fresh boot schema, so it alone does not re-execute the drift
gate; the separate TRUNCATE replay proves the seed-covered branch. Its replacement is deliberately
created after replay: concurrent creation during replay remains 0006. Circuit configuration is
fixed across these boots; no dynamic table discovery, circuit-layout migration or throughput
guarantee is added. The replica cluster disables fsync for fixture startup and makes no durability
claim. The two rejected configurations never reach resolved admission; these tests do not promise
a stable duration of their pre-exit HTTP availability.
