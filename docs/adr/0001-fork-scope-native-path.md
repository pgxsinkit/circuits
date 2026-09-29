# Fork scope: the native path is the product surface; the Electric adapter is kept for upstream only

Status: superseded by [ADR-0011](0011-one-repository-no-longer-a-fork.md) (2026-09-29). Accepted 2026-08-21.

This repository is the `pgxsinkit/electric-circuits` fork of `electric-sql/electric-circuits`. Our
consumer reaches the engine only through the native control plane (`POST /shapes` with the predicate
AST) and reads shape streams from durable-streams directly; it never calls `GET /v1/shape`. Fork work
is therefore scoped to the native path — ingest, catalog, sequencer, lifecycle, the native HTTP
surface, and the streams contract. The Electric compatibility adapter (`electric.rs`, `where_sql.rs`)
is not developed on the fork: a defect there is fixed only when a native-path change fixes it as a
side effect (table-identity resolution replacing the schema-prefix stripping, for instance) and is
otherwise reported upstream.

Commits stay upstream-shaped — small, self-contained, referencing the upstream issue they address — so
any of them can be offered as a PR, but no fork work waits on an upstream merge; the fork publishes its
own engine images from `main`.

## Considered options

- Track upstream's whole surface, compat adapter included: rejected — it doubles the conformance
  burden for a path we never execute, and upstream's own `electric-conformance/` suite already covers it.
- Diverge freely with fork-only conventions: rejected — it forecloses contributing back for no gain.
