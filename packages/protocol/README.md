# @circuits/protocol

The shared contract of [Circuits](../../README.md): the JSON types and compilers that the TS
API/oracle/client and the Rust engine (which mirrors them with serde) all agree on. Zero runtime
dependencies. Four modules, re-exported from the package root:

| Module | Contents |
|---|---|
| `types.ts` | `Schema`/`TableDef`/`ColumnType` (`int` `text` `bool` `float`), `Row`/`Value`, the **Predicate AST**, `ShapeDef`/`ShapeHandle` (incl. the handle's `subscription` + `leaseSeconds` — ADR-0008), `SubsetDef`/`SubsetResult`, `AggregateDef`/`AggFn`, `ChangeEvent`, stream-path helpers |
| `predicate.ts` | reference **evaluator** (`evaluate`) + `validatePredicate` |
| `sql.ts` | predicate → SQL compiler (`predicateToSql`), DDL (`tableDDL`), DML (`changeEventToDML`), `shapeSelectSql` |
| `envelope.ts` | `StreamEnvelope` — the State-Protocol change envelope on every table/shape durable stream (`type`, `key`, `value`, `headers.operation/txid/lsn/seq`) |

## Scalar values on the wire

`Value` is `number | string | boolean | null`, and the shape of an **`int`** cell is deliberately two
of those. Postgres `bigint` reaches `2^63-1` while every JavaScript JSON parser decodes numbers as
doubles, so the engine emits a JSON **number** while `|v| <= 2^53 - 1` and an exact decimal **string**
beyond it — a number that large would be silently rounded, and the engine does not hand back a wrong
value that looks right. The rule holds wherever a row value is serialised: shape-stream envelopes,
`query()` pages, subset pages, and MIN/MAX over an int column (the aggregate `SUM` encoding it
generalises is older). `String(v)` is always the exact decimal; `BigInt(v)` is the arithmetic form.
Consumers that assumed `typeof row.<intcol> === 'number'` must widen.

The envelope **`key`** is the stringified primary key. A composite key is its components escaped
(`\` -> `\\`, U+001F -> `\x1f`) and joined with U+001F, so distinct key tuples can never spell the
same key (`docs/ARCHITECTURE.md` §2). This package's `toTableEnvelope` covers the single-column case
only: `TableDef.primaryKey` is one column name, and the native write API takes one `pk` value.

## The Predicate AST

A restricted boolean expression over one table's columns, plus single-column subqueries as the one
cross-table form. Each node is one of:

```ts
{ col: 'priority', op: 'gte', value: 3 }              // leaf: eq neq lt lte gt gte
{ col: 'assignee', isNull: true }                     // col IS [NOT] NULL
{ and: [p1, p2] }  { or: [p1, p2] }  { not: p }       // boolean combinators
{ col: 'project_id',                                  // col [NOT] IN (SELECT project FROM table WHERE …)
  in: { table: 'project_members', project: 'project_id',
        where: { col: 'user_id', op: 'eq', value: 42 } },
  negated: false }                                    // inner `where` may nest subqueries
```

Type guards (`isLeaf`, `isIsNull`, `isAnd`, `isOr`, `isNot`, `isInSubquery`) discriminate nodes.

## Evaluator: SQL three-valued logic

`evaluate(pred, row)` answers "does this row satisfy the predicate" exactly as Postgres `WHERE`
does: comparisons with a NULL operand are UNKNOWN, AND/OR follow the SQL truth tables,
`NOT UNKNOWN = UNKNOWN`, and a row is included **iff the predicate is TRUE**. `isNull` is the one
two-valued leaf (TRUE/FALSE by the cell's null-ness, never UNKNOWN), so it composes soundly under
`not`. Subquery nodes deliberately **throw** — the row evaluator has no inner set; subquery shapes
are evaluated via SQL (the oracle) and via the engine's shared inner-set nodes.

`validatePredicate(pred, tableDef, schema?)` checks column existence and literal/column type
compatibility (`schema` is required for subqueries — the inner `where` is validated against the
inner table).

## SQL compilers

```ts
predicateToSql(pred)          // -> { text: '"priority" >= $1', params: [3] }  (parameterized)
tableDDL('todos', def)        // -> CREATE TABLE "todos" (…, PRIMARY KEY ("id"))
changeEventToDML('todos', def, ev)  // insert/update -> upsert by pk (partial update -> plain UPDATE
                                    // of provided columns); delete -> DELETE by pk
shapeSelectSql('todos', where)      // -> SELECT * FROM "todos" WHERE <pred>
```

These compilers are what [`@circuits/oracle`](../oracle/README.md) is built from — the same
predicate JSON drives the engine, the oracle's `SELECT`, and the client, which is what makes the
[conformance invariant](../conformance/README.md) checkable end-to-end.
