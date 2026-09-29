# Circuits

The server side of [pgxsinkit](https://github.com/pgxsinkit/pgxsinkit)'s read path: two programs
that turn changes in Postgres into streams a client can sync from.

- **The engine** (`apps/engine`) ingests Postgres logical replication and keeps every registered
  query result, a **shape**, up to date incrementally.
- **The log server** (`apps/durable-streams`) stores those results as durable streams and serves
  them over the [Durable Streams protocol](https://github.com/pgxsinkit/durable-streams/blob/a172acc389351cb3db6deb5cd60e3dec11e7ff39/PROTOCOL.md).

They are built, tested and released together, and run as separate processes.

```
  app ──SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                         │  logical replication
                         ▼
                      ENGINE ──appends──▶ LOG SERVER   changes        (the one ordered change log)
                         │                    │
                         │◀──reads it back────┘
                         │  Z-set deltas → shared circuits (membership, aggregation, routing)
                         ▼
                      LOG SERVER   shape/<id>          (one stream per distinct shape)
                         │  read / long-poll
                         ▼
                      CLIENTS
```

Postgres owns durability and transactions. The log server is the log between every layer. The
engine is a restartable consumer in the middle: it holds routing metadata and the shared inner sets
of subqueries, and no copy of any table. A client that is already reading a stream keeps reading it
while the engine restarts.

## Shapes

A shape is a query whose result is maintained as the database changes:

```sql
SELECT * FROM issues
WHERE status = 'todo' AND priority >= 3
```

A reader first receives the rows that match, then a feed of `upsert` (a row entered the result, or
changed while inside it) and `delete` (a row left it). Predicates cover comparisons, `LIKE`, null
tests, `AND`/`OR`/`NOT`, and `col [NOT] IN (SELECT … WHERE …)` subqueries, recursively. Identical
shapes share one maintained pipeline and one stream.

The engine is built on [DBSP](https://docs.rs/dbsp): a change is a delta of weighted rows, and each
operator turns a delta of its input into a delta of its output. Keeping a result current never
re-runs the query, so the cost follows the size of the change and not the size of the table.

- What can be expressed: [docs/live-queries-guide.md](docs/live-queries-guide.md)
- How a shape registers onto its circuit: [docs/how-queries-become-live.md](docs/how-queries-become-live.md)
- The design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- The log server's design: [apps/durable-streams/ARCHITECTURE.md](apps/durable-streams/ARCHITECTURE.md)
- Running against Postgres: [docs/deployment-postgres.md](docs/deployment-postgres.md)

## Images

| Image | Program |
|---|---|
| `ghcr.io/pgxsinkit/circuits/engine` | the engine |
| `ghcr.io/pgxsinkit/circuits/durable-streams` | the log server |

Both images of a release are built from one commit and carry the same tags, so the pair is pinned
with one value: a version (`1.2.3`), or a commit (`sha-0123abc`). Building them locally is in
[container/README.md](container/README.md).

## Working on it

The toolchain is pinned: Rust by `rust-toolchain.toml`, bun and node by `mise.toml`. The test
harness boots its own ephemeral Postgres, so it needs PostgreSQL 18's `initdb` and `pg_ctl` on
`PATH` (on Debian and Ubuntu they are in `/usr/lib/postgresql/18/bin`).

```bash
mise install
bun install

bun run engine:test                        # the engine's Rust tests
bun run test:durable-streams               # the log server's Rust tests
bun run typecheck
bun run test                               # every TypeScript suite, engine conformance included
bun run test:durable-streams:conformance   # the Durable Streams protocol suite, against the log server
bun run test:fuzz                          # random predicates against the oracle
```

The conformance invariant, asserted through the real stack: for any shape and any stream of
operations, the set a client materialises equals what `SELECT … WHERE <predicate>` returns from a
Postgres that received the same operations.

## Layout

| Path | Language | What it is |
|---|---|---|
| `apps/engine` | Rust | replication ingest, shape maintenance, the control plane |
| `apps/durable-streams` | Rust | the log server, and its protocol conformance run |
| `apps/api` | TypeScript | the API the test harness drives the engine through |
| `packages/protocol` | TypeScript | the shared contract: schema, predicate and envelope types |
| `packages/client` | TypeScript | the harness client |
| `packages/oracle`, `packages/conformance` | TypeScript | the reference implementation and the conformance suite |
| `packages/ds-rust` | TypeScript | starts the log server for a test |
| `container/` | | the two image builds |
| `electric-conformance/` | Elixir | tests for the compatibility adapter; removed with it (ADR-0011) |

Decisions are in [docs/adr/](docs/adr/), the glossary in [CONTEXT.md](CONTEXT.md), and where the
code came from in [PROVENANCE.md](PROVENANCE.md). Guidance for agents working here is in
[AGENTS.md](AGENTS.md).

## Licence

The engine and the test harness are dual-licensed under [MIT](LICENSE-MIT) or
[Apache 2.0](LICENSE-APACHE), at your option. The log server is licensed under
[Apache 2.0](apps/durable-streams/LICENSE); see its [NOTICE](apps/durable-streams/NOTICE).
