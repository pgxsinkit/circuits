# @circuits/client

The browser/Node client for the extended Circuits API: a typed tRPC client over
[`@circuits/api`](../../apps/api/README.md) plus `@durable-streams/state` for materializing
shape streams into live TanStack DB collections.

```ts
import { createClient } from "@circuits/client";

const client = createClient({
  apiUrl, // the tRPC API server
  schema, // Schema from @circuits/protocol
  dsBaseUrl, // optional: durable-streams base override (e.g. '/ds' behind a dev proxy)
  liveMode, // true (SSE, default) | 'sse' | 'long-poll'
});
```

**Either table spelling works, whichever one keyed your schema.** `schema.name` is canonical and a
bare name is shorthand for `public.<name>` (ADR-0002), so `shape({ table: 'public.issues' })` and
`shape({ table: 'issues' })` resolve to the same entry of a `Schema` keyed `issues` _or_
`public.issues`. The keys of your `Schema` are local config — the engine never sees them — so the
client resolves the caller's spelling against them rather than requiring the two to match
(`lookupTableDef` / `resolveTableDef` / `canonicalTableIndex` / `tableSpellings` are exported if you
keep your own map). A returned `handle.table` is always the canonical form the engine answered with,
and only `public` has a bare shorthand: `billing.issues` never resolves to `issues`. The same rule
runs through `client.tables`: one entry per table, reachable under **both** spellings — with a
schema keyed `issues`, `client.tables.issues` and `client.tables['public.issues']` are the same
helper.

**A canonical collision is refused at construction.** `createClient` canonicalises the schema keys
once, so a `Schema` carrying both `issues` and `public.issues` **throws** — they are one Postgres
table, and two entries would make column/primary-key validation depend on which alias a call
happened to use. Identical definitions are refused as well: the rule is one entry per table, not
"duplicates are fine when they agree".

## `shape(def)` — materialized, live

Registers a shape (backfilled + maintained server-side) and materializes its stream into a
TanStack DB collection. Identical definitions from any number of clients share **one** maintained
stream, ref-counted.

```ts
const shape = await client.shape({
  table: "issues",
  where: { col: "status", op: "eq", value: "open" }, // Predicate AST (see packages/protocol)
  columns: ["id", "title", "status"], // optional projection; pk always included
});
shape.currentRows(); // Row[]
shape.collection; // TanStack DB collection (usable with useLiveQuery)
const unsub = shape.subscribe((changes) => {
  /* live change batches */
});
await shape.awaitTxId(txid); // resolve once the write bearing txid is materialized
await shape.close();
```

## `subset(def)` / `query(def)` — ordered pages, shared live tail

`query()` is one-shot: the engine runs a single `SELECT … ORDER BY … LIMIT/OFFSET` against
Postgres and returns `{ rows, lsn }` — nothing is stored server-side. `subset()` builds on it:
first page + a **changes-only** live tail on the base predicate, merged client-side by per-pk LSN
watermarks (a stale page can never resurrect a deleted row).

```ts
const page = await client.subset({
  table: "issues",
  orderBy: { col: "created", desc: true }, // pk appended as tiebreaker
  limit: 50,
  where,
});
page.collection; // live collection of the loaded window
await page.loadMore(50); // next keyset page; resolves to rows added (0 when exhausted)
page.hasMore();
await page.close();
```

Paging rules worth knowing: `offset` positions the **first** page only (later pages move a keyset
cursor past the boundary row) and its window is closed at the bottom too — a live change that moves
a row below the first loaded row does not pull it into the page the offset skipped past; NULL sort
keys page correctly in both directions, following
Postgres's `ORDER BY` defaults — ascending puts NULLs last, descending first; `hasMore()` turns
false once a page comes back **shorter than requested**, so exhausting a set takes one final
`loadMore()` that returns 0; and `limit: 0` is ended from the start (a zero-size page can never be
short, and never moves the cursor).

**Text ordering in a subset is CODE-POINT order, not your database's collation.** Membership in the
loaded window is decided here, in the client, from the values it received — it cannot reproduce an
arbitrary Postgres collation. So the page query orders text columns `COLLATE "C"` (and the keyset
cursor's range comparisons match), and the client compares code points rather than JavaScript's
UTF-16 code units — `'\u{1F600}' < '\uE000'` is `true` under `<` and false by code point, which is
enough to pull a row into a window Postgres put outside it. If your ordering has to follow a locale
collation, sort a materialized `shape()` yourself instead. Only ordering comparisons are collated;
equality is unaffected. Two caveats: the guarantee applies to columns the engine **introspected**
from PostgreSQL (a schema pushed with `defineSchema` has no PostgreSQL type to check, so it keeps the
database's default ordering), and on a non-`C` database an ordered subset over a large table wants an
expression index — `CREATE INDEX … ON t ((col COLLATE "C"))` — since `COLLATE "C"` cannot use the
column's default-collation btree index.

**An `int` cell can be a decimal string.** Postgres `bigint` outruns a JSON number, so a value
outside `±(2^53 - 1)` arrives as an exact string rather than a rounded number (the same rule as
`AggregateValue` below). `String(v)` is always the exact decimal, `BigInt(v)` the arithmetic form; a
subset's comparator already treats an `int` column numerically whichever form it arrives in.

## `aggregate(def)` — live scalar

```ts
const agg = await client.aggregate({ table: 'issues', fn: 'count', where })
agg.value()                     // AggregateValue (null before first value / empty avg-min-max)
agg.count()                     // matching-row count, available for every fn
agg.subscribe((v) => { … })
await agg.close()
```

`fn` is `'count' | 'sum' | 'avg' | 'min' | 'max'`; `col` is required for all but `count`.

`AggregateValue` is `number | string | boolean | null` — usually a number, but MIN/MAX carry the
column's own value (a text column yields a string) and an **integer SUM outside the `2^53` range a
JSON number round-trips arrives as a decimal string**, because the engine will not hand back a
silently rounded total (`BigInt(v)` it). Float SUM/AVG are always numbers, and so is a `numeric`
column's — exactness is for integer columns.

## Writes and lifecycle

Writes go to Postgres with ordinary SQL (the engine ingests via replication). In library mode
(no Postgres), use `client.write(...)` or the schema-derived helpers
`client.tables.<t>.insert/update/delete(row, txid?)`.

**Every materialization holds a named subscription, and renews it.** `shape()`, `subset()` and
`aggregate()` each mint a `subscription` id (a uuid) and send it with the create; the engine records
the claim under that name and returns it on the handle together with `leaseSeconds`
(ADR-0008). Two things follow:

- **`close()` is one-shot and idempotent on the wire.** It releases exactly this materialization's
  claim, by id, with retry — and because a release names the claim, a retry after a lost response
  releases nothing a second time. (Before this, a retried delete could take another subscriber's
  reference on a shared shape.) `client.close()` tears down everything still open.
- **The subscription is a lease, so the client renews it.** Native reads go straight to
  durable-streams, where the engine cannot see them, so an un-renewed claim is released after
  `leaseSeconds` (the engine's `CIRCUITS_SHAPE_IDLE_SECS`) and the shape follows its
  retention lifecycle. Each open materialization renews on a third of that window automatically; a
  caller whose timers do not run (a suspended tab, a test that controls time) can say it explicitly
  with `shape.renew()` / `subset.renew()` / `aggregate.renew()` — the same create with the same id,
  which the engine treats as "still here", never as a second subscriber. If a lease does lapse, the
  next create simply re-subscribes (possibly to a fresh shape — the ordinary re-subscribe contract).
- **A renewal may hand back a REPLACEMENT, and the materialization rebinds itself to it.** A lease
  that lapses long enough for retention to evict the shape means the next renewal creates a new one:
  the handle it returns is authoritative, and `shape()`, `subset()` and `aggregate()` each move onto
  it rather than keep reading a stream that is gone. `handle` is updated in place and `close()`
  releases the claim on the shape it actually ended up holding, so a caller that kept a reference to
  the handle stays correct.

  What that costs each one is different, because a replacement stream carries no history:
  - `shape()` re-preloads the whole new stream. Its `collection` object is REPLACED, so read it
    through `mat.collection` / `mat.currentRows()` rather than caching it; every listener registered
    with `mat.subscribe()` is re-subscribed to the new collection **with initial state**, i.e. it
    receives the full contents again as inserts, not just the delta since the last change it saw.
  - `aggregate()` seeds the full value from the new stream, so `value()`/`count()` simply continue.
  - `subset()` re-runs its page query and installs the result as the loaded window in a single
    commit. The replacement feed is changes-only from its own creation, so anything that changed
    while the subscription was lapsed and its feed evicted exists in no feed at all — only re-reading
    the page recovers it. Pages beyond the first that `loadMore()` had loaded are NOT restored: the
    window is back to the initial `limit`, and `hasMore()` describes that window again.

Design context: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
