# AGENTS.md

Circuits is the server side of pgxsinkit's read path. App writes go to **Postgres**; the Rust
**engine** turns logical-replication changes into incrementally maintained, fully de-duplicated
**live shapes**. The Rust **log server** holds and serves their durable streams. They share one
Cargo workspace and are tested and released together, but run as separate processes.

Use the vocabulary in [CONTEXT.md](CONTEXT.md). The only supported surface is the **native path**:
the engine's control plane (`POST /shapes`, predicate AST) and reads from the log server.
Electric compatibility is not a goal; its adapter was removed in
[ADR-0011](docs/adr/0011-one-repository-no-longer-a-fork.md).

## Required reading

**Before changing the engine, log server, client or protocol, or designing circuit pipelines,
you MUST read [docs/engineering-invariants.md](docs/engineering-invariants.md).** It contains the
full pipeline design rules, reliability invariants, edge cases and testing guidance. The summary
below does not replace that normative reference. Keep it and the relevant architecture docs aligned
with code changes.

Before designing, also read the relevant existing docs:

- [README.md](README.md): the system in one page; [CONTEXT.md](CONTEXT.md): the glossary.
- [docs/adr/](docs/adr/): decisions; [docs/backlog/](docs/backlog/): known defects and debts.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): engine ingest, fencing, sharing, reliability and client layer.
- [docs/ivm-engine-internals.md](docs/ivm-engine-internals.md): execution strategies, cost model and
  circuit/routing/fallback serving tiers.
- [docs/live-queries-guide.md](docs/live-queries-guide.md): integrator guide;
  [docs/deployment-postgres.md](docs/deployment-postgres.md): source-of-record setup.
- [apps/durable-streams/ARCHITECTURE.md](apps/durable-streams/ARCHITECTURE.md): log-server design;
  `WAL_TUNING.md`, `CRASH_SIM_FINDINGS.md` and adjacent notes record measurements and their rationale.
- Each package's `README.md`: surface, commands and environment knobs.

## Layout

- `apps/engine`: Rust engine. In `src/engine/`, `sequencer.rs` orders by LSN;
  `lifecycle.rs` handles creation/sharing/retention; `circuit_serving.rs`, `executors.rs` and
  `planning.rs` handle serving and placement; `catalog.rs`, `drift.rs`, `epoch.rs` and
  `retirement.rs` handle durable lifecycle; `membership.rs` handles flips/query-backs;
  `output.rs` is the envelope codec; `introspection.rs` exposes state; `mod.rs` is the handle.
  Other key modules: `arrangements.rs` (circuit/counts/seeding), `subquery.rs` (shared inner sets),
  `replication.rs` + `pgoutput.rs` (ingest), `pg.rs` (backfill/`SnapshotGate`), `sql.rs`
  (predicate SQL), `ds.rs` (reliable streams client).
- `apps/durable-streams`: log-server crate `durable-streams`, binary `durable-streams-server`.
  `store.rs` is storage, `wal/` group commit, `handlers.rs` protocol, and `http1.rs`,
  `engine_raw.rs`, `sse_reactor.rs` serving. `conformance/` is the protocol suite.
- `apps/api`: tRPC API (`router.ts`) over the engine and log server (`core.ts`); harness entry point.
- `packages/protocol`: shared types and change-event envelope (`types.ts`, `envelope.ts`).
- `packages/client`: harness `shape()`, `subset()`, `aggregate()` client; `subset.ts` holds
  watermarks/tombstones. Lifecycles are tracked; `close()` releases once with retry.
- `packages/conformance`: engine-vs-oracle integration, replication, fuzz, NULLs, concurrency and sharing.
- `packages/oracle`: reference implementation. `packages/ds-rust`: starts this workspace's log
  server with a fresh directory and port per test.
- `container/`: engine and log-server image builds.

## Build and verification

Use **bun**, **podman** and **mise**, never npm/pnpm/yarn or Docker. Use package scripts.
`rust-toolchain.toml` pins Rust: stable rustc 1.97.0–1.98.1 crashes compiling dbsp. Cargo scripts
use `scripts/with-toolchain.sh` (`mise exec` when available, bare cargo in CI). If invoking cargo
directly from an unactivated shell, use `mise exec -- cargo …`.

Scripts are check-default; only `format:write` and `lint:fix` mutate files.

```bash
bun run validate                         # pre-commit: format + typecheck + lint + unit tests
bun run validate:full                    # shared CI lanes, five release storage configurations and candidate-image test
bun run format                           # Bun format check + cargo fmt --check
bun run format:write                     # apply both formatters
bun run lint                             # type-aware oxlint + clippy -D warnings for both crates
bun run lint:fix                          # oxlint --fix
bun run typecheck                        # whole TS workspace and tests; no PG or engine required
bun run test                             # both crates' Rust tests + vitest unit; no PG
bun run test:integration                 # engine conformance harness + log-server protocol suite
bun run build                            # engine + log server
```

**A task touching the engine or log server is not done until `bun run validate` and
`bun run validate:full` have run green.** Run them, or report exactly which ran and why the rest
could not. Every log-server change needs both integration suites: protocol conformance alone
cannot establish engine compatibility. Typechecking is mandatory; Vitest strips types without
checking them.

Integration boots ephemeral PostgreSQL 19 (`/usr/lib/postgresql/19/bin` on Debian/Ubuntu).
Full qualification also requires Linux amd64, `taskset` and Podman. The always-on circuit is exercised on every run;
`CIRCUITS_DBSP_INDEXES`/`_COUNTS` select serving tiers, not whether the circuit exists.

`vitest.config.ts` assigns every file to `unit` (explicit allowlist) or `integration` (everything
else). A new test is integration unless added to the unit list. Unit setup only builds the log
server; integration setup builds both binaries once, and `packages/ds-rust` starts the workspace
binary, never an unrelated binary on `PATH`. `DS_RUST_BIN` is an explicit override.
`DS_MEMORY=1` removes durability on Linux; macOS falls back to WAL.

`bun install` installs `.githooks/` via `prepare`; pre-commit runs `validate`. There is **no local
pre-push hook**. CI runs the shared validation lanes and tests the candidate image IDs before publishing them.
Focused test/build commands and retention-test knobs are in
[the engineering reference](docs/engineering-invariants.md#build--test).

## Critical invariants

Read the full reference before changing these paths. These reminders identify the contracts;
their implementation details and exceptions remain in that reference.

- **Postgres is the record of truth.** No hot-path table copy; circuit arrangements are derived,
  rebuildable state. Backfills stream bounded chunks; ingest spills large transactions rather than
  refusing, purging or retiring shapes because of their size.
- **Pipelines are few and fixed; shapes are many and dynamic.** Circuit structure never scales
  with subscriptions or parameter combinations. Cohort-keyed outputs fan out outside the circuit;
  union requires partitioning cohorts. The circuit is an optimization, never a correctness dependency.
- **Visibility uses xid snapshots, not WAL LSNs.** Seeded structures use `SnapshotGate`, modular
  xid comparison and settled-snapshot reads; capture dependent tables' sequenced-xid records before
  opening the snapshot. LSN alone loses or duplicates boundary rows.
- **At-least-once ingest requires exactly-once effects.** Preserve `(lsn, seq)` de-duplication and
  checkpoint both position and highwater whenever either changes. Plain Postgres delivery has a
  separate durable seed/registration frontier and committed receipts; it never replaces the input frontier.
- **Transactions are indivisible.** Only the final envelope has `headers.last = true`; hold
  incomplete trailing runs and pin checkpoints/deletion floors. Ingest acknowledgement and drain
  sentinels follow the final landed chunk. Flush one source transaction's output before processing the next.
- **Live output lands or the shape retires before advancing.** Use reliable appends, reconcile
  terminal storage answers against registration and `HEAD`, and retry transient activation/replay
  failures before retiring an acknowledged shape.
- **Lifecycle promises are durable before acknowledgement.** Catalog events never drop and carry
  stable enqueue-time `eid`s. Client-promised creates/claims/releases/purges wait for durability;
  re-check generations, degradation, registration and stream identity after the wait.
- **Creation and cleanup survive cancellation.** Roll back records, shares, edges, refcounts and
  streams atomically. Owned cleanup retains admission/claim guards until completion; never abandon
  rollback when the caller disconnects.
- **Retirement is durable intent then durable completion.** Record `Dropped`, close then delete
  through the retirement helper, record `Retired` only after storage accepts deletion, retry and
  restore pending intents. Never re-mint any catalogued shape id, including dropped ids.
- **Schema changes fail closed for affected dependents.** Drift, `TRUNCATE` and replica-identity
  regression retire the table's dependents; generation/resolve guards reject racing creates.
  Unresolved tables stay unavailable until re-introspection succeeds; never serve stale schema.
- **The replication slot belongs to a catalog epoch.** Verify before every connection. Slot loss,
  wrong plugin or cluster change retires/reset-binds or fails closed; never silently recreate a slot
  while retaining shapes. An unreadable catalog is fatal.
- **The change log is segmented.** Use `LogPosition { segment, offset }`; never append to a closed
  segment. Skip control envelopes by type, cross only closed-and-drained boundaries, and delete old
  segments only beyond the durable checkpoint and every shape pin.
- **Subscriptions are named, idempotent leases.** Equal shapes share an id/stream and a set of
  claims. Repeated named create renews, release is a no-op; guard provisional claims. The final
  release leaves the shape for retention, rather than deleting it. Clients still release what they create.
- **Subquery membership emits absolute per-pk state.** Deferred flips cannot safely emit deltas;
  NULL flips re-derive negated `IN` dependents. Aggregates follow SQL NULL semantics.
- **The wire preserves values and keys.** Match backfill and replication text exactly; use shared
  injective composite-key helpers; serialize integers beyond 2^53 as decimal strings. Ordered subsets
  require matching SQL `COLLATE "C"` and client code-point comparison, with the documented type limits.
- **Subset pages and live shapes differ.** Ranges/order/limit are subset-only. Read raw envelopes
  for deltas, retain delete tombstones across page/live seams, and capture changes-only feed offsets
  before page snapshots. Reuse facet feeds rather than recreating subscriptions for UI filter changes.
- **Shutdown drains without retiring shapes or advancing the slot.** Readiness fails first,
  ingestor/sequencer reach safe points, checkpoint and catalog drain, streams stay untouched.
  Blocking work joins shutdown cancellation and registers drain parties where needed.

## Git policy

Work on `develop`. Keep only `main` and `develop` as local branches; do not create task-specific
branches. Do not commit or push unless explicitly asked. History is one line: rebase, never merge.
At handoff, report changed files, validation run and suggested next commands.
