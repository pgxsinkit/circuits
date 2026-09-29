// Circuits client: a thin wrapper over a typed tRPC client plus stream-db
// (`@durable-streams/state/db`) for materializing a shape into a live TanStack DB collection.

import type { AppRouter } from "@circuits/api";
import type {
  AggregateDef,
  Op,
  Row,
  Schema,
  ShapeDef,
  StreamEnvelope,
  SubsetDef,
  SubsetResult,
  TableDef,
  Value,
} from "@circuits/protocol";
import { canonicalTable } from "@circuits/protocol";
import { stream } from "@durable-streams/client";
import { createStateSchema, createStreamDB } from "@durable-streams/state/db";
import { createTRPCClient, httpBatchLink } from "@trpc/client";
import { z } from "zod";

import {
  createSubset,
  deleteShapeWithRetry,
  newSubscriptionId,
  startLeaseRenewal,
  type SubsetSubscription,
} from "./subset.js";
import { canonicalTableIndex, resolveTableDef, tableSpellings } from "./tables.js";

export type { SubsetSubscription } from "./subset.js";
// Table-spelling resolution (ADR-0002): exported so an app that keeps its own schema map can key it
// the same way the client does.
export { canonicalTableIndex, lookupTableDef, resolveTableDef, tableSpellings } from "./tables.js";
// LSN-positioning primitives (also unit-tested in subset.test.ts) — exported so integration tests can
// exercise the real merge logic against the live engine.
export { lsnToU64, mergeFeedDelta, type SubsetView, type MergeAction } from "./subset.js";

export interface ShapeHandle {
  shapeId: string;
  table: string;
  streamPath: string;
  streamUrl: string;
  /** This materialization's subscription id (ADR-0008) — see `@circuits/protocol`. */
  subscription?: string;
  /** Seconds a subscription may go unrenewed before the engine releases it (`0` = never). */
  leaseSeconds?: number;
}

export interface ShapeMaterialization {
  handle: ShapeHandle;
  /** The underlying TanStack DB collection (usable with @tanstack/react-db's useLiveQuery). */
  collection: unknown;
  /** Current materialized rows (declared columns + virtual props). */
  currentRows(): Row[];
  /** Resolve once an event bearing `txid` has been consumed (append-then-read determinism). */
  awaitTxId(txid: string, timeoutMs?: number): Promise<void>;
  /** Subscribe to live change batches; returns an unsubscribe fn. */
  subscribe(cb: (changes: Array<{ type: string; key: unknown; value?: unknown }>) => void): () => void;
  /**
   * Renew this materialization's subscription lease (ADR-0008): the same create, with the same
   * subscription id, which the engine treats as "still here" rather than as a second subscriber.
   *
   * The client already renews on the server's own cadence (`handle.leaseSeconds`) for as long as the
   * materialization is open — this is for a caller whose timers do not run (a suspended tab, a test
   * that controls time) and wants to say it explicitly. After `close()` it is a **no-op**: a closed
   * materialization must not resurrect the subscription it just released.
   */
  renew(): Promise<void>;
  close(): Promise<void>;
}

/** Per-table ingestion helpers derived from the schema (pk read from the row's pk column). */
export interface TableApi {
  insert(row: Row, txid?: string): Promise<{ txid: string }>;
  update(row: Row, txid?: string): Promise<{ txid: string }>;
  delete(pk: Value, txid?: string): Promise<{ txid: string }>;
}

/**
 * An aggregate's current value. Usually a number, but not always: MIN/MAX carry the column's own
 * value (so a text column yields a string), and an integer SUM whose exact total is outside the
 * `2^53` range a JSON number round-trips arrives as a **decimal string** — the engine will not
 * hand back a silently rounded number (`docs/ARCHITECTURE.md` §2). `BigInt(v)` it when you need
 * arithmetic on that scale.
 */
export type AggregateValue = number | string | boolean | null;

/** A live scalar aggregation (COUNT/SUM/AVG/MIN/MAX) maintained by the engine. */
export interface AggregateSubscription {
  /** Current aggregate value (null before the first value, or empty avg/min/max). */
  value(): AggregateValue;
  /** Count of rows matching the predicate (available for every aggregation). */
  count(): number;
  subscribe(cb: (value: AggregateValue) => void): () => void;
  /** Renew this subscription's lease — see `ShapeMaterialization.renew`. */
  renew(): Promise<void>;
  close(): Promise<void>;
}

export interface CircuitsClient {
  defineSchema(schema: Schema): Promise<unknown>;
  write(input: { table: string; op: Op; pk: Value; row?: Row; txid?: string }): Promise<{ txid: string }>;
  /** Schema-derived typed ingestion API, one entry per table. */
  tables: Record<string, TableApi>;
  /** Register a **materialized, live** shape (backfilled + maintained as a durable stream). */
  shape(def: ShapeDef): Promise<ShapeMaterialization>;
  /**
   * Run a one-shot **subset query** — the non-materialized counterpart to {@link shape}. Returns the
   * page rows + the Postgres snapshot LSN directly, with no stream and no server-side state. Page by
   * moving a keyset cursor in `where` (preferred) or bumping `offset`; keep it live by following the
   * table's tail and re-checking view membership rather than materializing a per-page shape.
   */
  query(def: SubsetDef): Promise<SubsetResult>;
  /**
   * Open a **live subset**: query-back the first page, then follow the table's tail to keep the loaded
   * window current (paging via {@link SubsetSubscription.loadMore}). Non-materialized — the engine
   * never stores the page; a change is matched against one base predicate, never fanned across ranges.
   */
  subset(def: SubsetDef): Promise<SubsetSubscription>;
  /** Open a live scalar **aggregation** over a filtered set (Circuits extension). */
  aggregate(def: AggregateDef): Promise<AggregateSubscription>;
  close(): Promise<void>;
}

function zodRowSchema(def: TableDef, cols?: string[]): z.ZodType {
  // When the shape projects a column subset, validate only those columns (+ pk) — the projected rows
  // genuinely omit the rest, so requiring them would reject every row. The pk is always present.
  const names = cols ? Array.from(new Set([def.primaryKey, ...cols])) : Object.keys(def.columns);
  const shape: Record<string, z.ZodTypeAny> = {};
  for (const col of names) {
    const c = def.columns[col];
    if (!c) continue;
    // pk is validated as its declared type here, then the dispatcher stringifies it on the row.
    // `int` accepts `number | string`: a PostgreSQL bigint outside the 2^53 range a JSON number
    // round-trips arrives as an exact decimal STRING rather than a silently rounded number
    // (`docs/ARCHITECTURE.md` §2) — `BigInt(v)` it when you need arithmetic at that scale.
    const base =
      c.type === "bool"
        ? z.boolean()
        : c.type === "text"
          ? z.string()
          : c.type === "int"
            ? z.union([z.number(), z.string()])
            : z.number();
    // Non-pk columns are nullable (the pk is never null); allow null cells to materialize.
    shape[col] = col === def.primaryKey ? base : base.nullable();
  }
  // be permissive about extra/loose fields the stream layer may add
  return z.object(shape).loose();
}

export function createClient(opts: {
  apiUrl: string;
  schema: Schema;
  /** Override the durable-streams base URL for shape reads (e.g. '/ds' behind a dev proxy). */
  dsBaseUrl?: string;
  /** Live mode passed to stream-db. 'long-poll' is the most proxy-friendly. Default true (SSE). */
  liveMode?: boolean | "sse" | "long-poll";
}): CircuitsClient {
  const trpc = createTRPCClient<AppRouter>({ links: [httpBatchLink({ url: opts.apiUrl })] });
  // Everything the client opens (shape materializations, subset subscriptions AND aggregate
  // subscriptions) so `close()` can tear them all down — otherwise a live stream leaks and blocks
  // shutdown. `track` wraps each close with a one-shot guard and prunes the entry on completion:
  // the engine DELETE decrements a shared refcount per call, so every subscription must be closed
  // exactly once (a double close would steal another subscriber's reference on a shared shape).
  const open: { close: () => Promise<void> }[] = [];
  function track<T extends { close(): Promise<void> }>(item: T): T {
    const inner = item.close.bind(item);
    let closing: Promise<void> | undefined;
    item.close = () => {
      closing ??= inner().finally(() => {
        const i = open.indexOf(item);
        if (i >= 0) open.splice(i, 1);
      });
      return closing;
    };
    open.push(item);
    return item;
  }

  const write = (input: { table: string; op: Op; pk: Value; row?: Row; txid?: string }) =>
    trpc.ingest.write.mutate(input);

  // Derive a typed ingestion helper per table from the schema. The schema is canonicalised ONCE
  // here (ADR-0002), which is also where a canonical conflict — the same table under two spellings —
  // is refused: constructing a client whose validation depends on which alias a call used is not a
  // state worth entering. Each table is then exposed under every spelling it answers to (`items`
  // and `public.items` are one entry reachable by two names, never two entries).
  const tables: Record<string, TableApi> = {};
  for (const [table, tdef] of canonicalTableIndex(opts.schema)) {
    const pkCol = tdef.primaryKey;
    const api: TableApi = {
      insert: (row, txid) => write({ table, op: "insert", pk: row[pkCol] ?? null, row, txid }),
      update: (row, txid) => write({ table, op: "update", pk: row[pkCol] ?? null, row, txid }),
      delete: (pk, txid) => write({ table, op: "delete", pk, txid }),
    };
    for (const spelling of tableSpellings(table)) tables[spelling] = api;
  }

  return {
    defineSchema: (schema) => trpc.schema.define.mutate({ schema }),

    write,
    tables,

    async shape(def) {
      // Either spelling: `items` and `public.items` name the same table, whichever one keyed the
      // local schema (see `./tables.ts`).
      const tableDef = resolveTableDef(opts.schema, def.table);

      // One subscription id per materialization (ADR-0008). It makes this create idempotent — the
      // same id is a renewal, never a second subscriber — so a retry after an ambiguous failure
      // costs nothing, and it names exactly what `close()` releases.
      const subscription = newSubscriptionId();
      const requestHandle = () =>
        trpc.shapes.create.mutate({
          table: def.table,
          where: def.where as never,
          columns: def.columns,
          subscription,
        }) as Promise<ShapeHandle>;
      const handle = await requestHandle();
      let claimedHandle = handle;

      // The envelope `type` on a shape stream is the table's CANONICAL `schema.name` (ADR-0002),
      // whatever spelling the caller used — the collection must be registered under that or nothing
      // materializes. `opts.schema.tables` stays keyed by the caller's own spelling: it is
      // client-side config, not the wire.
      const table = canonicalTable(def.table);
      const state = createStateSchema({
        [table]: { schema: zodRowSchema(tableDef, def.columns), type: table, primaryKey: tableDef.primaryKey },
      });
      const openDb = async (next: ShapeHandle) => {
        const streamUrl = opts.dsBaseUrl ? `${opts.dsBaseUrl.replace(/\/$/, "")}/${next.streamPath}` : next.streamUrl;
        const nextDb = createStreamDB({
          streamOptions: { url: streamUrl, contentType: "application/json" },
          state,
          live: opts.liveMode ?? true,
        });
        await nextDb.preload();
        return nextDb;
      };
      // `state` registers exactly one collection, keyed by the canonical table name, so this always
      // resolves; the lookup is optional only because `collections` is index-signature typed.
      const collectionOf = (streamDb: Awaited<ReturnType<typeof openDb>>) => {
        const c = streamDb.collections[table];
        if (!c) throw new Error(`stream DB has no collection for table ${table}`);
        return c;
      };
      let db = await openDb(handle);
      let collection = collectionOf(db);
      type Listener = {
        cb: (changes: Array<{ type: string; key: unknown; value?: unknown }>) => void;
        unsubscribe: () => void;
      };
      const listeners = new Set<Listener>();

      const renew = async () => {
        const next = await requestHandle();
        claimedHandle = next;
        if (next.shapeId === handle.shapeId && next.streamPath === handle.streamPath) return;

        // A lease can lapse long enough for retention to evict the old shape. The same
        // subscription then creates a replacement and the returned handle is authoritative: bind
        // the new stream before publishing it, so callers never observe a half-swapped materialization.
        const nextDb = await openDb(next);
        const nextCollection = collectionOf(nextDb);
        const previousDb = db;
        db = nextDb;
        collection = nextCollection;
        Object.assign(handle, next);
        for (const listener of listeners) {
          listener.unsubscribe();
          const sub = collection.subscribeChanges(listener.cb as never, { includeInitialState: true });
          listener.unsubscribe = () => sub.unsubscribe();
        }
        await previousDb.close?.();
      };

      // Renew for as long as the materialization is open: the engine cannot see reads that go
      // straight to durable-streams, so the renewal IS the liveness signal (ADR-0008).
      const lease = startLeaseRenewal(handle.leaseSeconds, renew);

      const mat: ShapeMaterialization = {
        handle,
        get collection() {
          return collection;
        },
        currentRows: () => collection.toArray as Row[],
        awaitTxId: (txid, timeoutMs) => db.utils.awaitTxId(txid, timeoutMs),
        subscribe: (cb) => {
          const sub = collection.subscribeChanges(cb as never, { includeInitialState: false });
          const listener: Listener = { cb, unsubscribe: () => sub.unsubscribe() };
          listeners.add(listener);
          return () => {
            listener.unsubscribe();
            listeners.delete(listener);
          };
        },
        renew: () => lease.renew(),
        close: async () => {
          // Stop AND drain the lease keeper first: a renewal still in flight is a create, and a
          // create landing after the release would re-take the claim this close just gave up
          // (see `startLeaseRenewal`). A `renew()` after this is a no-op.
          await lease.stop();
          await db.close?.();
          // Release OUR subscription: shapes are shared server-side, so every shape() must release
          // exactly the claim it took — by id, which is also what makes the retry inside
          // `deleteShapeWithRetry` safe.
          await deleteShapeWithRetry(trpc, claimedHandle.shapeId, subscription);
        },
      };
      return track(mat);
    },

    async query(def) {
      const result = await trpc.subset.query.query({
        table: def.table,
        where: def.where as never,
        columns: def.columns,
        orderBy: def.orderBy,
        limit: def.limit,
        offset: def.offset,
      });
      return result as SubsetResult;
    },

    async subset(def) {
      const sub = await createSubset(
        {
          trpc,
          schema: opts.schema,
          liveMode: opts.liveMode === true ? "long-poll" : (opts.liveMode ?? "long-poll"),
          resolveStreamUrl: (handle) =>
            opts.dsBaseUrl ? `${opts.dsBaseUrl.replace(/\/$/, "")}/${handle.streamPath}` : handle.streamUrl,
        },
        def,
      );
      return track(sub);
    },

    async aggregate(def) {
      const subscription = newSubscriptionId();
      const requestHandle = () =>
        trpc.aggregate.create.mutate({
          table: def.table,
          where: def.where as never,
          fn: def.fn,
          col: def.col,
          subscription,
        }) as Promise<ShapeHandle>;
      const handle = await requestHandle();
      let boundHandle = handle;
      let claimedHandle = handle;
      let current: AggregateValue = null;
      let n = 0;
      const subs = new Set<(v: AggregateValue) => void>();
      let readerGeneration = 0;
      let reader: AbortController | undefined;
      const startReader = (next: ShapeHandle) => {
        reader?.abort();
        const ac = new AbortController();
        reader = ac;
        const generation = ++readerGeneration;
        const url = opts.dsBaseUrl ? `${opts.dsBaseUrl.replace(/\/$/, "")}/${next.streamPath}` : next.streamUrl;
        // The engine streams the running aggregate as `{ value, n }` envelopes (keyed "agg"); keep the latest.
        void (async () => {
          try {
            const resp = await stream<StreamEnvelope>({
              url,
              offset: "-1",
              live: opts.liveMode === true ? "long-poll" : (opts.liveMode ?? "long-poll"),
              json: true,
              signal: ac.signal,
            });
            for await (const env of resp.jsonStream()) {
              // A retired reader can still yield a buffered batch after abort. Only the currently
              // bound generation may publish aggregate state.
              if (ac.signal.aborted || generation !== readerGeneration) break;
              const v = env.value as { value?: AggregateValue; n?: number } | undefined;
              if (v && "value" in v) {
                current = v.value ?? null;
                n = v.n ?? 0;
                for (const cb of subs) cb(current);
              }
            }
          } catch (e) {
            // Retirement and explicit close abort the old fetch; only a live reader reports errors.
            if (!ac.signal.aborted && generation === readerGeneration) console.error("aggregate stream error", e);
          }
        })();
      };
      startReader(handle);
      const renew = async () => {
        const next = await requestHandle();
        claimedHandle = next;
        if (next.shapeId === boundHandle.shapeId && next.streamPath === boundHandle.streamPath) return;
        boundHandle = next;
        startReader(next);
      };
      const lease = startLeaseRenewal(handle.leaseSeconds, renew);
      const sub: AggregateSubscription = {
        value: () => current,
        count: () => n,
        subscribe: (cb) => {
          subs.add(cb);
          return () => {
            subs.delete(cb);
          };
        },
        renew: () => lease.renew(),
        close: async () => {
          await lease.stop(); // drain an in-flight renewal before releasing — see `shape()` above
          reader?.abort();
          await deleteShapeWithRetry(trpc, claimedHandle.shapeId, subscription);
        },
      };
      return track(sub);
    },

    async close() {
      // Iterate a copy: each close() prunes itself from `open`, and anything the caller already
      // closed is gone — so teardown is exactly-once per subscription.
      for (const m of [...open]) await m.close();
    },
  };
}
