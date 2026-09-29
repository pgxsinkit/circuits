# 0005 — The reconciler does not pick up new or re-created tables without a restart

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `apps/engine/src/pg.rs` (`setup_postgres`), the reconciler, `apps/engine/src/pgoutput.rs`
Reopen trigger: a deployment that cannot afford the restart a migration needs, or a re-created table found unsynced in production.

Carried over on 2026-09-29 from issue #11 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## The fact

A table that starts matching `CIRCUITS_PG_TABLES` after boot is not synced until the engine restarts. Two ways to get there: a migration **adds** a table the selector covers (`*`, `schema.*`, or a name listed ahead of time), or a table is **dropped and re-created** under the same name (new relid). Today the selector is resolved once, in `setup_postgres` (introspect → `REPLICA IDENTITY FULL` → register schema → dbsp input), and the 60 s reconciler (ADR-0005, `apps/engine/src/engine/drift.rs`) checks **drift of known tables only**.

## Consequence

The operational rule is "migration adds a table ⇒ restart the engine" (documented in `docs/deployment-postgres.md` "Adding a table" and `apps/engine/README.md`). A dropped table is handled loudly — its dependents are retired (stream closed, then deleted) and it is untracked — but the re-created table is simply not noticed. Under Kubernetes with `replicas: 1` a restart is a short unavailability, so this is a footgun, not a defect; it is the one item in the post-stabilisation list worth promoting to a feature.

## Fix direction

Let the reconciler re-resolve the selector on each tick: a newly matching table (or a known name with a new relid / no fingerprint) takes the boot path minus the slot — introspect, ensure identity, register the schema, spawn the input — and the decoder accepts a `Relation` for it mid-stream. A re-created table is treated as new (its old dependents are already retired). The minimal explicit variant is a `POST /tables/reload` that runs the same code on demand. Either way the table set should be observable on `GET /tables`.

## Not now because

The rule is documented and cheap to follow; nothing silently goes wrong. Surfaced while closing the stabilisation slices (ADR-0002/0005; `docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).
