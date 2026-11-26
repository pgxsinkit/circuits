# @circuits/conformance

The end-to-end conformance suite. The invariant it asserts, through the **real** stack (Postgres →
replication → engine → durable-streams → client), against the
[oracle](../oracle/README.md):

> For any shape and any op stream, the client-materialized set equals the oracle's
> `SELECT … WHERE <predicate>`.

## What the harness boots (`src/harness.ts`)

Vitest builds the engine and log server once. Each integration file gets an ephemeral
PostgreSQL 19 cluster with `wal_level=logical`. Slot creation can wait for an open transaction
in another database on the same cluster. A separate cluster per file lets the idle-WAL test
hold a write open while other files boot their engines. Each harness starts these components:

- a per-test Postgres database (the system of record _and_, via `createPgOracle`, the truth),
- a `DurableStreamTestServer` (the log),
- the Rust engine as a child process in Postgres mode (spawned from `target/debug/`, discovered
  via its `ENGINE_LISTENING` stdout line),
- the tRPC API (`createApiServer`) and a real `@circuits/client`.

Comparison (`src/compare.ts`) is set equality over declared columns, keyed by stringified pk.

## Test areas

| Files                                               | What they pin down                                                                       |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `conformance.test.ts`, `-postgres`, `-backfill`     | core invariant: live replication, batched mutations, backfill ↔ live fencing             |
| `conformance-concurrency.test.ts`                   | concurrent writers                                                                       |
| `conformance-nulls.test.ts`                         | NULL three-valued logic (`NOT (col = x)` over NULL, `IS [NOT] NULL`)                     |
| `conformance-subquery*.test.ts`                     | `IN (SELECT …)`: scenarios, cross-table matrix, nested subqueries, inner-node sharing    |
| `conformance-sharing`, `-shape-sharing`             | identical shapes/feeds collapse to one stream, refcounted create/drop                    |
| `conformance-subset-positioning.test.ts`            | subset LSN positioning: a change in the page/feed overlap window is counted exactly once |
| `conformance-expressiveness`, `-transitions`        | full predicate grammar, rows entering/leaving shapes                                     |
| `conformance-fuzz`, `-fuzz-wide`, `-counterexample` | random-predicate fuzz vs the oracle + pinned counterexample replays                      |
| `harness-mechanics.test.ts`                         | the harness itself (boot, teardown, engine discovery)                                    |

## Running

Requires PostgreSQL 19 server binaries (`initdb` and `pg_ctl`). The suite boots its own clusters.
From the repo root:

```bash
bun run test:conformance     # the whole suite
bun run test:fuzz            # the random-predicate fuzz test
bun run loop [N]             # run the fuzz test repeatedly until failure (default 50 iterations)

# a fuzz failure prints `FAILED seed=<n>`; replay it exactly:
SEED=<n> bun run test:fuzz
```

Fuzz tunables: `FUZZ_SEEDS` (scenarios per run, default 5), `FUZZ_SHAPES`, `FUZZ_OPS`, `SEED`
(base seed). `src/simulator.ts` generates the random schemas/predicates/op streams.

This suite tests the **extended** API. Consistency model:
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
