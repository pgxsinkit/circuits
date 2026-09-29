// Subset queries — the non-materialized counterpart to a shape. Rows come from one-shot Postgres
// query-backs (`subset.query`); the loaded page is kept live by following a changes-only tail feed
// (`subset.live`) and re-checking each delta's membership in the loaded window *client-side*. The
// engine never holds per-page/per-range state, so a change is matched against one predicate (the base
// filter) and never fans out across ranges. This is our extension of Electric's static Subset: same
// non-materialized query-back, plus a single-range live tail.

import type { AppRouter } from "@circuits/api";
import { canonicalTable } from "@circuits/protocol";
import type {
  ColumnType,
  Predicate,
  Row,
  Schema,
  StreamEnvelope,
  SubsetDef,
  SubsetResult,
  Value,
} from "@circuits/protocol";
import { stream } from "@durable-streams/client";
import { createCollection, type ChangeMessageOrDeleteKeyMessage, type Collection } from "@tanstack/db";
import type { createTRPCClient } from "@trpc/client";

import { resolveTableDef } from "./tables.js";

type Trpc = ReturnType<typeof createTRPCClient<AppRouter>>;

export interface SubsetSubscription<T extends Row = Row> {
  /** Live collection: the query-back rows within the loaded window, kept current by the live tail. */
  collection: Collection<T, string>;
  /** Fetch the next page from Postgres and append it; resolves to the rows added (0 once exhausted). */
  loadMore(pageSize?: number): Promise<number>;
  /** False once a page returned fewer rows than requested (the set is fully loaded). */
  hasMore(): boolean;
  /**
   * Renew this subscription's lease (ADR-0008). The client already renews on the server's cadence
   * while the subscription is open; this is here for a caller that suspends its own timers (a
   * backgrounded tab, a test) and wants to say "still here" explicitly.
   */
  renew(): Promise<void>;
  /** Tear down the live feed (drops the server-side changes-only feed) and stop following the tail. */
  close(): Promise<void>;
}

export interface SubsetDeps {
  trpc: Trpc;
  schema: Schema;
  /** Resolve a feed handle to a readable stream URL (honoring any dev-proxy base). */
  resolveStreamUrl(handle: { streamPath: string; streamUrl: string }): string;
  liveMode?: boolean | "sse" | "long-poll";
}

/**
 * Compare two strings by Unicode **code point**, which is the order the engine's subset page arrives
 * in (`ORDER BY <col> COLLATE "C"`) and the order the engine's own predicate evaluation uses.
 *
 * JavaScript's `<` compares UTF-16 **code units**, which is a different order: a supplementary
 * character (U+1F600 → surrogates D83D DE00) sorts before the private-use area (U+E000) under `<`,
 * and after it by code point — so a live row could be admitted to, or dropped from, a loaded window
 * PostgreSQL puts on the other side of the boundary. Iterating with `for…of` yields code points.
 */
export function cmpCodePoints(a: string, b: string): number {
  if (a === b) return 0;
  const ai = a[Symbol.iterator]();
  const bi = b[Symbol.iterator]();
  for (;;) {
    const x = ai.next();
    const y = bi.next();
    if (x.done) return y.done ? 0 : -1;
    if (y.done) return 1;
    const ax = x.value.codePointAt(0) as number;
    const bx = y.value.codePointAt(0) as number;
    if (ax !== bx) return ax < bx ? -1 : 1;
  }
}

/**
 * Compare two cell values of a column of type `ty`. NULL sorts **last**, which is PostgreSQL's
 * default for an ascending `ORDER BY`; `makeCmp` multiplies by the order's direction, so a
 * descending order gets NULLS FIRST — Postgres's other default. The engine's page query uses
 * `ORDER BY <col> COLLATE "C" <dir>` for text, so this is the same order the page arrives in — see
 * `packages/client/README.md` and `docs/ARCHITECTURE.md` §7.
 *
 * The column TYPE decides the comparison, never the runtime shape of the value: an `int` cell is
 * `number | string` on the wire (a bigint beyond 2^53 arrives as an exact decimal string), so
 * comparing "whatever looks numeric" numerically would order a **text** column's `'10'` before
 * `'9'`, which PostgreSQL does not.
 *
 * A cell the caller projected away is `undefined` rather than `null`; it sorts with the NULLs
 * (`== null`), which is what the engine does with a column that is not in the row.
 */
function cmpVal(a: Value | undefined, b: Value | undefined, ty?: ColumnType): number {
  if (a === b) return 0;
  if (a == null) return b == null ? 0 : 1;
  if (b == null) return -1;
  if (ty === "int") {
    const ai = BigInt(a as number | string);
    const bi = BigInt(b as number | string);
    return ai < bi ? -1 : ai > bi ? 1 : 0;
  }
  if (typeof a === "number" && typeof b === "number") return a - b;
  return cmpCodePoints(String(a), String(b));
}

/**
 * Row comparator matching the engine's `ORDER BY <col> <dir>, <pk> <dir>` (pk tiebreaker, same
 * dir). `types` resolves a column's declared type so each comparison uses the column's own order
 * (see [`cmpVal`]); an unknown column falls back to the value-shape rules.
 */
export function makeCmp(
  pk: string,
  orderBy?: { col: string; desc?: boolean },
  types?: (col: string) => ColumnType | undefined,
): (a: Row, b: Row) => number {
  const dir = orderBy?.desc ? -1 : 1;
  const col = orderBy?.col;
  const colType = col ? types?.(col) : undefined;
  const pkType = types?.(pk);
  return (a, b) => {
    if (col) {
      const d = cmpVal(a[col], b[col], colType);
      if (d !== 0) return dir * d;
    }
    return dir * cmpVal(a[pk], b[pk], pkType);
  };
}

/**
 * Keyset predicate for rows strictly *after* `b` in the order — the cursor for the next page.
 *
 * NULL sort keys need their own arms, because no comparison is ever TRUE against or about a NULL
 * (three-valued logic), so a plain `col > b.col` cursor both fails to *reach* the NULL block and
 * fails to *leave* it — paging would stop at the first NULL-keyed row for ever. The NULL block's
 * position is the ORDER BY default this cursor has to agree with: ascending puts it last,
 * descending puts it first.
 */
function cursorPredicate(pk: string, orderBy: { col: string; desc?: boolean } | undefined, b: Row): Predicate {
  const pkOp = orderBy?.desc ? "lt" : "gt";
  // The pk is force-projected into every page (see `cols`), so a boundary row without it means the
  // page and the schema disagree about the table's key — there is no cursor to build from it.
  const bpk = b[pk];
  if (bpk === undefined) throw new Error(`subset cursor row is missing its primary key column ${pk}`);
  if (!orderBy?.col) return { col: pk, op: pkOp, value: bpk };
  const col = orderBy.col;
  const colOp = orderBy.desc ? "lt" : "gt";
  const nullBlockAfter: Predicate = {
    and: [
      { col, isNull: true },
      { col: pk, op: pkOp, value: bpk },
    ],
  };
  if (b[col] == null) {
    // Inside the NULL block. Ascending (NULLS LAST): only later NULLs remain. Descending (NULLS
    // FIRST): later NULLs, and then the whole non-NULL body of the table.
    return orderBy.desc ? { or: [nullBlockAfter, { col, isNull: false }] } : nullBlockAfter;
  }
  const after: Predicate = {
    or: [
      { col, op: colOp, value: b[col] },
      {
        and: [
          { col, op: "eq", value: b[col] },
          { col: pk, op: pkOp, value: bpk },
        ],
      },
    ],
  };
  // Ascending: the NULL block still lies ahead of a non-NULL boundary and must be paged into.
  // Descending: it sorted before the boundary, so it is already behind us.
  return orderBy.desc ? after : { or: [after, { col, isNull: true }] };
}

function andPredicate(base: Predicate | undefined, cursor: Predicate): Predicate {
  return base ? { and: [base, cursor] } : cursor;
}

/** Does this error mean the shape/feed is already gone? Then a delete has nothing left to do. */
function isNotFoundError(e: unknown): boolean {
  const data = (e as { data?: { code?: string; httpStatus?: number } } | null)?.data;
  if (data?.code === "NOT_FOUND" || data?.httpStatus === 404) return true;
  return /not[ _-]?found|404/i.test(e instanceof Error ? e.message : String(e));
}

/**
 * Release a server-side subscription, retrying transient failures.
 *
 * The retry is only safe because the release is **identified**: it names this materialization's own
 * `subscription` id (ADR-0008), and releasing an id the shape no longer holds is a no-op. Before
 * that, a success response lost in transit turned one `close()` into two decrements and stole
 * another subscriber's claim on a shared shape. Omitting the id falls back to the engine's legacy
 * anonymous decrement, which is NOT retry-safe — nothing in this package does.
 *
 * A swallowed release still matters (it pins the shape until the lease lapses), so failures retry
 * with backoff and warn once. "Not found" counts as success (the shape was already evicted).
 */
export async function deleteShapeWithRetry(trpc: Trpc, id: string, subscription?: string): Promise<void> {
  const attempts = 5;
  for (let i = 0; i < attempts; i++) {
    try {
      await trpc.shapes.delete.mutate({ id, ...(subscription ? { subscription } : {}) });
      return;
    } catch (e) {
      if (isNotFoundError(e)) return;
      if (i === attempts - 1) {
        console.warn(`client: failed to delete shape ${id} after ${attempts} attempts:`, e);
        return;
      }
      await new Promise((r) => setTimeout(r, 200 * 2 ** i));
    }
  }
}

/** A fresh subscription id for one materialization (ADR-0008). */
export function newSubscriptionId(): string {
  return globalThis.crypto.randomUUID();
}

/** What a materialization holds to keep its subscription alive; see [`startLeaseRenewal`]. */
export interface LeaseKeeper {
  /** Renew now. A no-op once [`stop`](LeaseKeeper.stop) has been called. */
  renew(): Promise<void>;
  /** Stop renewing, and wait for a renewal already in flight to settle. */
  stop(): Promise<void>;
}

/**
 * Keep one subscription's lease alive until it is closed.
 *
 * A native subscriber reads its shape straight from durable-streams, so the engine cannot see the
 * reads: an un-renewed subscription is indistinguishable from a client that vanished, and after
 * `leaseSeconds` the engine releases it (ADR-0008). Renewing is the same create with the same id —
 * idempotent, so a missed or duplicated renewal costs nothing.
 *
 * The cadence is a third of the server's own window (so two consecutive failures still leave a
 * chance), clamped to a sane range, and read from the handle rather than assumed: the window is the
 * server's to set. `leaseSeconds === 0` means dormancy is off and leases never lapse, so no timer
 * runs — an explicit `renew()` still works, because the caller asked for it.
 *
 * **`stop()` is what makes `close()` safe.** A renewal is a create, so one still in flight when the
 * `DELETE` goes out can land *after* it and re-take the very claim the close just released — a
 * subscription nothing will ever release again, pinning the shape until its lease lapses. So a close
 * stops the keeper first and awaits the in-flight renewal, and a `renew()` after that is a no-op
 * rather than a resurrection (closing is one-shot in this client; a renewal racing it must lose).
 */
export function startLeaseRenewal(leaseSeconds: number | undefined, renew: () => Promise<unknown>): LeaseKeeper {
  let stopped = false;
  // Serialize every attempt accepted before stop(). A single mutable "current promise" loses an
  // older request when renewals overlap, allowing that request to land after close() has released
  // the subscription. The caught tail keeps later attempts running after a transient failure while
  // each caller still receives its own attempt's rejection.
  let tail: Promise<unknown> = Promise.resolve();
  const once = (): Promise<void> => {
    if (stopped) return Promise.resolve();
    const attempt = tail.then(() => renew());
    tail = attempt.catch(() => {});
    return attempt.then(() => undefined);
  };
  const timer =
    leaseSeconds && leaseSeconds > 0
      ? setInterval(
          () => {
            void once().catch((e) => {
              // A failed renewal is not fatal: the next tick tries again, and if the lease does
              // lapse the materialization's next read simply finds a fresh shape (ADR-0007).
              console.warn("client: subscription renewal failed:", e);
            });
          },
          Math.min(Math.max((leaseSeconds * 1000) / 3, 250), 5 * 60_000),
        )
      : undefined;
  // Node keeps the process alive for a pending interval; a lease keeper must never do that.
  (timer as unknown as { unref?: () => void } | undefined)?.unref?.();
  return {
    renew: once,
    stop: async () => {
      stopped = true;
      if (timer) clearInterval(timer);
      await tail;
    },
  };
}

/**
 * Parse a Postgres LSN (`"HI/LO"` hex) into a comparable bigint — mirrors the engine's
 * `pg::lsn_to_u64` (`(hi << 32) | lo`). `null`/empty/malformed → null (library/no-Postgres mode, or
 * an unparseable header): callers treat a null LSN as "apply fresh". Never throws — a throw here
 * would propagate out of the feed's async iterator and silently kill the live tail.
 */
export function lsnToU64(lsn: string | undefined | null): bigint | null {
  if (!lsn) return null;
  const slash = lsn.indexOf("/");
  if (slash < 0) return null;
  const hi = Number.parseInt(lsn.slice(0, slash), 16);
  const lo = Number.parseInt(lsn.slice(slash + 1), 16);
  if (Number.isNaN(hi) || Number.isNaN(lo)) return null;
  return (BigInt(hi) << 32n) | BigInt(lo);
}

/** The loaded subset window's membership + per-row LSN watermark (the merge state). */
export interface SubsetView {
  snapshotLsn: bigint;
  present: Set<string>;
  applied: Map<string, bigint>;
  /** Is the row within the currently-loaded keyset window? */
  inView: (row: Row) => boolean;
}

/** A collection write to emit, or `null` to drop the delta. */
export type MergeAction = { type: "insert" | "update"; value: Row } | { type: "delete"; key: string } | null;

/**
 * Decide how one live-feed delta updates the loaded subset view, applying **LSN positioning** +
 * **last-writer-wins**. Mutates `view.present`/`view.applied`. Returns the write to emit, or `null`
 * to drop the delta because it is: already reflected in the page (commit LSN < the row's watermark /
 * the snapshot floor), stale w.r.t. a newer page/delta, or out of the loaded window. Exported so the
 * no-double-count invariant can be unit-tested without the full stack.
 */
export function mergeFeedDelta(view: SubsetView, env: StreamEnvelope): MergeAction {
  const key = env.key;
  const deltaLsn = lsnToU64(env.headers.lsn);
  // A null LSN (library/no-Postgres mode) always applies — the old idempotent-by-pk behaviour.
  const fresh = (): boolean => {
    if (deltaLsn === null) return true;
    const w = view.applied.get(key);
    return w === undefined ? deltaLsn >= view.snapshotLsn : deltaLsn >= w;
  };
  if (env.headers.operation === "delete") {
    if (!fresh()) return null;
    const wasPresent = view.present.has(key);
    view.present.delete(key);
    // Keep a tombstone watermark instead of clearing it: absence from `present` + watermark w means
    // "deleted at ≥ w", so an in-flight loadMore page snapshotted before the delete (pageLsn < w)
    // is skipped by the loadMore guard rather than resurrecting the row. Recorded even for a
    // never-seen pk — otherwise a stale page could insert a ghost row the feed already deleted.
    if (deltaLsn !== null) view.applied.set(key, deltaLsn);
    else view.applied.delete(key);
    return wasPresent ? { type: "delete", key } : null;
  }
  const value = env.value;
  if (!value || !fresh()) return null;
  if (view.inView(value)) {
    const type = view.present.has(key) ? "update" : "insert";
    view.present.add(key);
    if (deltaLsn !== null) view.applied.set(key, deltaLsn);
    return { type, value };
  }
  if (view.present.has(key)) {
    // Moved out of the loaded window (e.g. its sort key dropped below the boundary). Same tombstone
    // treatment as a delete: a stale in-flight page must not re-insert the pre-move version.
    view.present.delete(key);
    if (deltaLsn !== null) view.applied.set(key, deltaLsn);
    else view.applied.delete(key);
    return { type: "delete", key };
  }
  return null;
}

/**
 * Manual-write handles captured from the collection's sync callback (used by load-more + the feed).
 * The write message shape is TanStack DB's own — an insert/update carries the row, a delete carries
 * only the key — so the captured handles keep their exact signature instead of a looser restatement.
 */
interface SyncCtl {
  begin: (options?: { immediate?: boolean }) => void;
  write: (m: ChangeMessageOrDeleteKeyMessage<Row, string>) => void;
  commit: () => void;
}

export async function createSubset<T extends Row = Row>(
  deps: SubsetDeps,
  def: SubsetDef,
): Promise<SubsetSubscription<T>> {
  // Either spelling — `items` or `public.items` — whichever one keyed the local schema.
  const tableDef = resolveTableDef(deps.schema, def.table);
  // Feed envelopes spell the table canonically (`schema.name`, ADR-0002) whatever spelling the
  // caller used, so the envelope filter below must compare against the canonical form.
  const feedType = canonicalTable(def.table);
  const pk = tableDef.primaryKey;
  const cmp = makeCmp(pk, def.orderBy, (col) => tableDef.columns[col]?.type);
  const limit = def.limit ?? 100;
  // The order column + pk must be present on every row so membership/cursoring can be evaluated, even
  // when the caller projects a narrower column set.
  const cols = def.columns
    ? Array.from(new Set([pk, ...(def.orderBy ? [def.orderBy.col] : []), ...def.columns]))
    : undefined;

  // 1. Open the live tail FIRST so it captures every change from ~now. The feed may be SHARED with other
  //    subscriptions on the same predicate (the engine ref-counts identical changes-only feeds).
  // This subscription's own id: the feed may be SHARED with other subscriptions on the same
  // predicate, and the id is what lets this one be renewed and released without touching theirs.
  const subscription = newSubscriptionId();
  const requestFeed = () =>
    deps.trpc.subset.live.mutate({ table: def.table, where: def.where as never, columns: cols, subscription });
  const feed = await requestFeed();
  let boundFeed = feed;
  let claimedFeed = feed;
  let feedUrl = deps.resolveStreamUrl(feed);
  let lease: LeaseKeeper | undefined;

  let tail: AbortController | undefined;
  let tailGeneration = 0;
  let closed = false;

  // The feed above is the only server-side state we hold; if any of the remaining setup (offset
  // capture, page query-back, preload) throws, delete it before rethrowing — nobody else will.
  try {
    // 1b+2. One page load: capture the feed's tail offset BEFORE the page snapshot, then query the
    //     page back straight from Postgres (no stream, no materialization).
    //
    //     The offset comes first because reading the live tail from there (rather than the stream
    //     origin) means a joiner to a SHARED, long-lived feed does not replay the whole backlog — it
    //     starts at "≈now". Everything at/before this offset committed before the page's snapshot LSN
    //     and is already in the page; the `< snapshotLsn` drop covers the small [thisOffset,
    //     snapshot] overlap. Falls back to the stream origin if HEAD is unavailable.
    //
    //     `offset` applies to THIS page only: it is where the caller's window starts, and every
    //     later page is reached by moving the keyset cursor past the boundary — re-applying the
    //     offset there would skip that many rows again.
    //
    //     Factored because a renewal that hands back a REPLACEMENT feed has to run exactly this
    //     again, in exactly this order (see `renew` below).
    const loadPage = async (url: string): Promise<{ offset: string; page: SubsetResult }> => {
      let offset = "-1";
      try {
        const head = await fetch(url, { method: "HEAD" });
        offset = head.headers.get("stream-next-offset") ?? "-1";
      } catch {
        /* proxy/env without HEAD support → read from origin; correctness unaffected (only backlog). */
      }
      const page = (await deps.trpc.subset.query.query({
        table: def.table,
        where: def.where as never,
        columns: cols,
        orderBy: def.orderBy,
        limit,
        offset: def.offset,
      })) as SubsetResult;
      return { offset, page };
    };
    const { offset: feedOffset, page: first } = await loadPage(feedUrl);

    // Every field below is derived from the CURRENT page by `seedPage`, which is why they are `let`:
    // a replacement feed re-runs the page load and re-derives the whole window from it.
    //
    // `boundary` = the last (lowest-in-order) loaded row; the loaded window is everything sorting <= it.
    let boundary: Row | null = null;
    // ...and, WITH AN OFFSET, everything sorting >= the first loaded row. Without this lower bound
    // the window is open at the bottom, so a live delta that moves a row into the region the offset
    // deliberately skipped would insert it — the subset would silently grow past the page the
    // caller asked for. A row sorting strictly before the first loaded row is out of the window
    // under keyset semantics, and under OFFSET semantics it would SHIFT the window, which no live
    // delta may do; "not in view" is the right answer either way. (An empty first page leaves no
    // lower bound to take — nothing was loaded to anchor it to.)
    let lower: Row | null = null;
    // A page shorter than the page size means the set is fully loaded. A page size of ZERO is never
    // short, and can never move the cursor either, so it ends the subset outright rather than
    // promising a next page that could never arrive.
    let ended = false;
    const present = new Set<string>();
    // LSN positioning: `view.snapshotLsn` is the CURRENT page's read point in the engine's replication
    // timeline. `applied` is a per-present-row watermark — the snapshot LSN the row's current value was
    // read at (page or loadMore), bumped to a feed delta's LSN when applied. A feed delta is accepted
    // only if its commit LSN is at/after the relevant watermark, so deltas already reflected in the page
    // (commit LSN < snapshotLsn) are dropped — exactly-once after the snapshot, no double-count.
    const applied = new Map<string, bigint>();
    const inView = (row: Row): boolean => {
      // The lower bound holds even once `ended`: "fully loaded" means the pages ran out, not that
      // the rows below the offset joined the window.
      if (lower != null && cmp(row, lower) < 0) return false;
      return ended || boundary == null || cmp(row, boundary) <= 0;
    };

    let ctl: SyncCtl | null = null;
    let loadsInFlight = 0;

    const view: SubsetView = { snapshotLsn: 0n, present, applied, inView };
    // Bumped by every `seedPage`. A `loadMore` page that was requested against the PREVIOUS window
    // is meaningless once the window has been re-derived from a fresh page, so it is dropped rather
    // than merged into rows it no longer describes.
    let windowGeneration = 0;

    /**
     * Install one query-back page as the entire loaded window. The collection writes happen in a
     * SINGLE commit — rows the page no longer contains are deleted alongside the inserts/updates —
     * so a `subscribeChanges` consumer never observes an empty intermediate collection.
     *
     * The initial load and a replacement feed's reload are the same operation; on the first call
     * `present` is empty, so it degenerates to the plain seed it has always been.
     */
    const seedPage = (w: SyncCtl, page: SubsetResult): void => {
      const rows = page.rows;
      const pageLsn = lsnToU64(page.lsn) ?? 0n;
      const keys = new Set(rows.map((r) => String(r[pk])));
      w.begin();
      for (const k of present) if (!keys.has(k)) w.write({ type: "delete", key: k });
      for (const r of rows) w.write({ type: present.has(String(r[pk])) ? "update" : "insert", value: r });
      w.commit();
      present.clear();
      applied.clear();
      for (const k of keys) {
        present.add(k);
        applied.set(k, pageLsn);
      }
      boundary = rows.length ? rows[rows.length - 1]! : null;
      lower = def.offset && rows.length ? rows[0]! : null;
      ended = limit === 0 || rows.length < limit;
      view.snapshotLsn = pageLsn;
      windowGeneration += 1;
    };
    const applyEnvelope = (env: StreamEnvelope): void => {
      if (!ctl || env.type !== feedType) return;
      const action = mergeFeedDelta(view, env);
      if (!action) return;
      ctl.begin();
      ctl.write(action);
      ctl.commit();
    };

    const startTail = (url: string, offset: string): void => {
      tail?.abort();
      const ac = new AbortController();
      tail = ac;
      const generation = ++tailGeneration;
      void (async () => {
        try {
          const resp = await stream<StreamEnvelope>({
            url,
            offset,
            live: deps.liveMode ?? "long-poll",
            json: true,
            signal: ac.signal,
          });
          for await (const env of resp.jsonStream()) {
            if (ac.signal.aborted || generation !== tailGeneration) break;
            applyEnvelope(env);
          }
        } catch (e) {
          if (!ac.signal.aborted && !closed && generation === tailGeneration) console.error("subset feed error", e);
        }
      })();
    };

    const renew = async () => {
      const next = await requestFeed();
      claimedFeed = next;
      if (next.shapeId === boundFeed.shapeId && next.streamPath === boundFeed.streamPath) return;
      boundFeed = next;
      feedUrl = deps.resolveStreamUrl(next);
      // A replacement feed is CHANGES-ONLY from its own creation, so every insert/update/delete that
      // happened while this subscription was lapsed and its old feed evicted — the gap — is in no
      // feed at all: not the dead one, not the new one. Reading the replacement from its origin
      // would therefore keep the pre-gap page forever. The page is re-read instead, exactly as the
      // initial load reads it, and installed as the window; the tail then starts from the offset
      // captured BEFORE that snapshot, so the [offset, snapshot] overlap is dropped by LSN
      // positioning rather than double-counted (same capture order, same reason).
      tail?.abort();
      const { offset, page } = await loadPage(feedUrl);
      if (closed || !ctl) return;
      seedPage(ctl, page);
      startTail(feedUrl, offset);
    };

    // Built as a plain wire-`Row` collection — that is what the feed and the pages actually carry —
    // and re-stated as the caller's `T` once, at the return below.
    const collection = createCollection<Row>({
      id: `subset:${def.table}:${feed.shapeId}`,
      getKey: (r) => String(r[pk]),
      sync: {
        sync: (params) => {
          ctl = params;
          // Seed the query-back page.
          seedPage(params, first);
          params.markReady();
          // 3. Follow the raw live tail from the offset captured before the snapshot, applying each change
          //    filtered by membership + LSN positioning (deltas already in the page are dropped).
          startTail(feedUrl, feedOffset);
          return () => tail?.abort();
        },
      },
    });
    await collection.preload();
    // Started only now: a renewal that lands a replacement feed has to reseed through the sync
    // handles, and those exist only once the collection has run its sync callback.
    lease = startLeaseRenewal(feed.leaseSeconds, renew);

    return {
      // The one place the caller's `T` is applied. `T` is an unverified claim about the wire row
      // shape (nothing validates it), and the collection is genuinely built over `Row`, so this is
      // a re-badge, not a conversion — hence the trip through `unknown`. Keys really are strings:
      // `getKey` stringifies the pk.
      collection: collection as unknown as Collection<T, string>,
      hasMore: () => !ended,

      async loadMore(pageSize = limit) {
        // `pageSize <= 0` asks for nothing: answer 0 without a round-trip, and without concluding
        // the set is exhausted (a zero-row answer to a zero-row request proves nothing about it).
        if (closed || ended || pageSize <= 0 || !boundary || !ctl) return 0;
        const where = andPredicate(def.where, cursorPredicate(pk, def.orderBy, boundary));
        const generation = windowGeneration;
        loadsInFlight++;
        try {
          const page = (await deps.trpc.subset.query.query({
            table: def.table,
            where: where as never,
            columns: cols,
            orderBy: def.orderBy,
            limit: pageSize,
          })) as SubsetResult;
          // A replacement feed re-derived the whole window from a fresh page while this one was in
          // flight: it describes a window that no longer exists, and merging it would resurrect rows
          // the new page does not contain. Report nothing loaded, and leave `ended` to the new page.
          if (generation !== windowGeneration) return 0;
          if (page.rows.length) {
            // This page is a fresh Postgres snapshot at `pageLsn`; its rows are the authoritative state as
            // of that LSN. Don't let a stale page regress a row already advanced past `pageLsn` by the live
            // feed (the loadMore-vs-feed race), and set each row's watermark so older feed deltas drop.
            // Tombstoned rows (watermark without membership) are skipped the same way — a page older than
            // the delete must not resurrect the row.
            const pageLsn = lsnToU64(page.lsn) ?? view.snapshotLsn;
            ctl.begin();
            for (const r of page.rows) {
              const k = String(r[pk]);
              const w = applied.get(k);
              if (w !== undefined && pageLsn < w) continue;
              ctl.write({ type: present.has(k) ? "update" : "insert", value: r });
              present.add(k);
              applied.set(k, pageLsn);
            }
            ctl.commit();
            boundary = page.rows[page.rows.length - 1]!;
          }
          if (page.rows.length < pageSize) ended = true;
          return page.rows.length;
        } finally {
          loadsInFlight--;
          // Tombstone watermarks only exist to guard in-flight loadMore pages; once none are in
          // flight, prune them so delete churn doesn't grow `applied` unboundedly.
          if (loadsInFlight === 0) {
            for (const k of applied.keys()) if (!present.has(k)) applied.delete(k);
          }
        }
      },

      async renew() {
        await lease!.renew();
      },

      async close() {
        // Still one-shot, and now idempotent on the wire too: the DELETE names THIS subscription,
        // so neither a double close nor a retried one can touch another subscriber's claim. The
        // lease keeper is stopped AND drained first — a renewal still in flight would otherwise
        // land after the release and re-take the claim (see `startLeaseRenewal`).
        if (closed) return;
        closed = true;
        await lease!.stop();
        tail?.abort();
        await deleteShapeWithRetry(deps.trpc, claimedFeed.shapeId, subscription);
      },
    };
  } catch (e) {
    closed = true;
    await lease?.stop();
    tail?.abort();
    await deleteShapeWithRetry(deps.trpc, claimedFeed.shapeId, subscription);
    throw e;
  }
}
