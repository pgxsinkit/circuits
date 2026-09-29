# @circuits/oracle

The reference implementation that [conformance](../conformance/README.md) compares the real system
against: an actual Postgres that receives the same change events as Circuits and answers
`SELECT * FROM <table> WHERE <predicate>` for any shape. The conformance invariant is that the
client-materialized shape set equals this oracle's result set for the same op stream.

The oracle is built entirely from the [`@circuits/protocol`](../protocol/README.md) compilers —
`tableDDL` creates the tables, `changeEventToDML` applies each change (upsert/delete by pk), and
`shapeSelectSql` compiles the shape's predicate AST to a parameterized `WHERE`. Postgres itself is
the semantics: NULL three-valued logic, `IN (SELECT …)`, ordering — nothing is re-implemented.

## The interface

```ts
interface Oracle {
  applyChange(table: string, ev: ChangeEvent): Promise<void>;
  queryShape(shape: ShapeDef): Promise<Row[]>;
  reset(): Promise<void>; // TRUNCATE every table, keep the schema
  close(): Promise<void>;
}
```

- **`createPgOracle(schema, connectionString)`** — the oracle, on a Postgres connection. In the
  conformance harness the _same_ database is both the write source (changes flow
  source → logical replication → engine) and the comparison truth.
- **`createPgTables(connectionString, schema)`** — creates the schema's tables with
  `REPLICA IDENTITY FULL` (so logical decoding carries the full old row). Run before starting the
  engine.

## Usage

```ts
import { createPgOracle, createPgTables } from "@circuits/oracle";

await createPgTables(connectionString, schema);
const oracle = await createPgOracle(schema, connectionString);
await oracle.applyChange("todos", { op: "insert", pk: 1, row: { id: 1, title: "x", done: false } });
const truth = await oracle.queryShape({ table: "todos", where: { col: "done", op: "eq", value: false } });
```

See [packages/conformance](../conformance/README.md) for the harness that wires an oracle against
the live engine, and the root [README](../../README.md) for the overall architecture.
