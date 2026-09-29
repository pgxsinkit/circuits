# 0007 — Three refusal paths have no end-to-end lane

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `packages/conformance/src/` (no file yet), `apps/engine/src/pg.rs` (`inspect_publication`, `check_wal_level`), `apps/engine/src/engine/drift.rs` (`circuit_needs_rebuild`)
Reopen trigger: any change to one of the three refusal paths, or to the exit codes they use.

Carried over on 2026-09-29 from issue #13 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## The fact
Three refusal paths are unit-tested but have no end-to-end lane:

1. **Circuit-tier drift exit (75).** `circuit_needs_rebuild` (ADR-0005) is unit-tested; no lane boots with `CIRCUITS_DBSP_COUNTS`, triggers drift on a circuit-served table, and asserts exit 75 → restart → re-seed → recover.
2. **Publication column-list refusal.** `pg::inspect_publication` refuses a publication with a column list (boot-fatal, exit 78); the harness always derives `<slot>_pub` itself, so the refusal is never exercised. `prattrs` is PG15+; only a hand-made publication reaches it.
3. **`wal_level` ≠ `logical` refusal.** `pg::check_wal_level` is called explicitly at connect and the classifier is unit-tested; the harness cluster is always `logical`, and changing `wal_level` needs a Postgres restart, so a conformance case needs a second throwaway cluster.

## Fix direction
One conformance file per item, each booting the binary through the harness: (1) a `DBSP_COUNTS` lane with `ADD COLUMN` on the circuit table; (2) a hand-made publication with a column list → assert exit 78 and the named message; (3) an `initdb` with `wal_level = replica` → assert exit 78.

## Not now because
Test debt only; the mechanisms themselves are covered. Surfaced in the slice-2b and slice-6 reviews (`docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).
