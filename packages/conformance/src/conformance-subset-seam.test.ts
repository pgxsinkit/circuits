// A committed transaction that is not yet visible to new snapshots, against the subset
// snapshot/live-tail seam and the full-shape backfill/live seam.
//
// PostgreSQL flushes a commit record (and a logical walsender may decode it) BEFORE the backend
// leaves the ProcArray, and only leaving the ProcArray makes a transaction visible to snapshots
// taken afterwards. This file holds a transaction T in exactly that window, on stock PostgreSQL 18:
// the cluster names a synchronous standby that never connects (`phantom_standby`), T commits with
// `synchronous_commit = on` and blocks in `SyncRepWaitForLSN` after its commit record is flushed
// and clog-marked, and `pg_cancel_backend` later ends the wait ("already committed locally") so T
// becomes visible. Every other session uses `synchronous_commit = local`.
//
// The cluster is owned by this file (initdb into a temp directory, private socket directory and
// port) so `ALTER SYSTEM`-grade settings never touch the shared fixture or any other cluster.
//
// Each test drives the PUBLIC surfaces only: the TypeScript client's `subset()` (feed, HEAD, query,
// tail, merge — the real `createSubset`) or the native `POST /shapes`, SQL against the system of
// record, and raw durable-stream reads for mechanism evidence. The client's own HEAD and page
// requests are observed (never altered) through a `fetch` wrapper, which is also where a test
// places T relative to them. The verdict is the client/stream state after a causal fence (a later
// row on the same feed the client must have applied) compared with SQL.
import { execFileSync } from "node:child_process";
import { appendFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { createServer, request, type IncomingMessage, type ServerResponse } from "node:http";
import { join, resolve } from "node:path";

import { lsnToU64, type SubsetSubscription } from "@circuits/client";
import type { Row, Schema, StreamEnvelope } from "@circuits/protocol";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: { items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" } },
};
const def = { table: "items", orderBy: { col: "id" }, limit: 100 } as const;
const FENCE_ID = 99;
const tools = { initdb: "initdb", pgCtl: "pg_ctl" };
let dir: string;
let previousUrl: string | undefined;
let h: Harness | undefined;
let sub: SubsetSubscription | undefined;
let proxy: ChangeLogHold | undefined;
let held: { client: pg.Client; commit: Promise<unknown>; pid: number; xid: string } | undefined;

beforeAll(() => {
  previousUrl = process.env.CIRCUITS_TEST_PG_URL;
  const scratch = resolve("tmp/agents/snapshot-fixes");
  mkdirSync(scratch, { recursive: true });
  dir = mkdtempSync(join(scratch, "subset-seam-pg-"));
  execFileSync(tools.initdb, ["-D", join(dir, "data"), "-U", "postgres", "--auth=trust", "--no-sync"], {
    stdio: "ignore",
  });
  appendFileSync(
    join(dir, "data/postgresql.conf"),
    `\nwal_level=logical\nmax_replication_slots=10\nmax_wal_senders=10\nsynchronous_commit=local\n` +
      `synchronous_standby_names='phantom_standby'\nlisten_addresses='127.0.0.1'\nunix_socket_directories=''\n`,
  );
  for (let attempt = 0; attempt < 8; attempt++) {
    const port = 59000 + Math.floor(Math.random() * 5000);
    try {
      execFileSync(
        tools.pgCtl,
        ["-D", join(dir, "data"), "-l", join(dir, "postgres.log"), "-o", `-p ${port}`, "-w", "start"],
        { stdio: "ignore" },
      );
      process.env.CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${port}/postgres`;
      return;
    } catch (error) {
      if (attempt === 7) throw error;
    }
  }
});
afterAll(() => {
  globalThis.fetch = realFetch;
  if (previousUrl === undefined) delete process.env.CIRCUITS_TEST_PG_URL;
  else process.env.CIRCUITS_TEST_PG_URL = previousUrl;
  if (dir) {
    try {
      execFileSync(tools.pgCtl, ["-D", join(dir, "data"), "-m", "immediate", "-w", "stop"], { stdio: "ignore" });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});
afterEach(async () => {
  steps = {};
  holdPage = undefined;
  await release();
  await sub?.close().catch(() => {});
  sub = undefined;
  await h?.shutdown();
  h = undefined;
  proxy = undefined;
});

// ---- observing the real client's requests --------------------------------------------------------

type Page = { rows: Row[]; lsn: string; [extra: string]: unknown };
/** Hooks run inside `createSubset`, around its own feed HEAD. Each fires once. */
let steps: { beforeHead?: () => Promise<void>; afterHead?: () => Promise<void> } = {};
/** What the client's single page load saw: the feed URL it HEADed, the offset, and the page. */
let seen: { feedUrl?: string; offset?: string; page?: Page } = {};
let holdPage: ((res: Response) => Promise<void>) | undefined;
const realFetch = globalThis.fetch;
globalThis.fetch = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  const method = init?.method ?? (input instanceof Request ? input.method : "GET");
  // Only the client's HEAD of ITS feed (the stream `subset.live` answered with) is a hook point;
  // the harness HEADs the change log too.
  const feedHead = method === "HEAD" && seen.feedUrl !== undefined && url.split("?")[0] === seen.feedUrl;
  if (feedHead && steps.beforeHead) {
    const step = steps.beforeHead;
    steps.beforeHead = undefined;
    await step();
  }
  const res = await realFetch(input, init);
  if (url.includes("/subset.live") && seen.feedUrl === undefined) {
    seen.feedUrl = trpcData<{ streamUrl: string }>(await res.clone().json())?.streamUrl;
  }
  if (feedHead && seen.offset === undefined) {
    seen.offset = res.headers.get("stream-next-offset") ?? undefined;
    if (steps.afterHead) {
      const step = steps.afterHead;
      steps.afterHead = undefined;
      await step();
    }
  }
  if (url.includes("/subset.query") && seen.page === undefined) seen.page = trpcData<Page>(await res.clone().json());
  if (url.includes("/subset.query") && holdPage) {
    const hook = holdPage;
    holdPage = undefined;
    await hook(res);
  }
  return res;
};
function trpcData<T>(body: unknown): T | undefined {
  const one = (Array.isArray(body) ? body[0] : body) as { result?: { data?: T } } | undefined;
  return one?.result?.data;
}

// ---- the invisible committed transaction ----------------------------------------------------------

async function sql(text: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pg.Client({ connectionString: h!.pgUrl });
  await c.connect();
  try {
    return (await c.query(text, params)).rows as Row[];
  } finally {
    await c.end();
  }
}

/**
 * Commit `statement` as T and return once T's commit record is flushed and T is parked in
 * `SyncRepWaitForLSN` — committed, still in the ProcArray, invisible to new snapshots. A later
 * local commit then moves every new snapshot's xmax past T, so T appears in their `xip`.
 */
async function holdCommit(statement: string): Promise<string> {
  const client = new pg.Client({ connectionString: h!.pgUrl });
  await client.connect();
  await client.query("BEGIN; SET LOCAL synchronous_commit = on");
  const info = (await client.query("SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid")).rows[0];
  await client.query(statement);
  held = { client, commit: client.query("COMMIT").catch((e: Error) => e), pid: info.pid, xid: info.xid };
  const deadline = Date.now() + 10000;
  // A named PostgreSQL wait event orders the test, never elapsed time.
  for (;;) {
    const [state] = await sql("SELECT wait_event FROM pg_stat_activity WHERE pid = $1", [info.pid]);
    if (state?.wait_event === "SyncRep") break;
    if (Date.now() > deadline) throw new Error(`T ${info.xid} did not reach SyncRep`);
    await new Promise((r) => setTimeout(r, 10));
  }
  await sql("SELECT pg_current_xact_id()"); // a later completed xid: xmax moves past T
  return info.xid;
}

/** T is committed but invisible: still waiting in SyncRep, and in the xip of a fresh snapshot. */
async function expectInvisible(xid: string): Promise<void> {
  const [probe] = await sql(
    `SELECT a.wait_event, pg_current_snapshot()::text AS snapshot, pg_xact_status($1::xid8) AS status
       FROM pg_stat_activity a WHERE a.pid = $2`,
    [xid, held!.pid],
  );
  expect(probe?.wait_event, "T is parked after its commit record was flushed").toBe("SyncRep");
  expect(String(probe!.snapshot).split(":")[2]!.split(","), "T is in a fresh snapshot xip").toContain(xid);
  console.log("T invisible", { xid, ...probe });
}

/** End T's SyncRep wait. PostgreSQL reports it committed locally, and T becomes visible. */
async function release(): Promise<void> {
  if (!held) return;
  const { client, commit, pid } = held;
  held = undefined;
  try {
    await sql("SELECT pg_cancel_backend($1)", [pid]);
    await commit;
  } finally {
    await client.end();
  }
}

/**
 * Resolve once `pending` has settled, or once the engine reports a snapshot waiting for a committed
 * transaction to become visible — whichever comes first. The caller then releases T. Without such a
 * wait the request finishes with T invisible; with one it cannot finish until T is released.
 */
async function untilSettledOrWaiting(pending: Promise<unknown>): Promise<void> {
  let settled = false;
  void pending.then(
    () => (settled = true),
    () => (settled = true),
  );
  const deadline = Date.now() + 20000;
  while (!settled) {
    const status = (await (await realFetch(`${h!.engineUrl}/replication/lsn`)).json()) as { visibilityWaits?: number };
    if ((status.visibilityWaits ?? 0) > 0) return;
    if (Date.now() > deadline) throw new Error("request neither finished nor reported a visibility wait");
    await new Promise((r) => setTimeout(r, 10));
  }
}

// ---- evidence helpers ----------------------------------------------------------------------------

async function read(url: string, offset = "-1"): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = [];
  for (let i = 0; i < 100; i++) {
    const res = await realFetch(`${url}?offset=${encodeURIComponent(offset)}`);
    if (res.status === 204) return out;
    expect(res.ok).toBe(true);
    const text = (await res.text()).trim();
    if (text) out.push(...(JSON.parse(text) as StreamEnvelope[]));
    offset = res.headers.get("stream-next-offset")!;
    if (res.headers.has("stream-up-to-date") || !text) return out;
  }
  throw new Error("stream exceeded 100 catch-up pages");
}
const envOf = (envs: StreamEnvelope[], xid: string) => envs.find((e) => e.headers.txid === xid);
const lsn = (value: string | undefined) => lsnToU64(value)!;
const rowsOf = (rows: Iterable<Row>) =>
  [...rows].map((r) => ({ id: Number(r.id), n: Number(r.n) })).sort((a, b) => a.id - b.id);
const truth = async () => rowsOf(await sql("SELECT id, n FROM items ORDER BY id"));

/**
 * Causal fence for the client: commit a later row, wait until the subset has applied it, and only
 * then compare. The fence row follows T on the same feed, so the client has judged T by then.
 */
async function fencedSubsetRows(): Promise<Array<{ id: number; n: number }>> {
  await sql("INSERT INTO items VALUES ($1, 0)", [FENCE_ID]);
  await drainEngine(h!);
  const deadline = Date.now() + 20000;
  while (!(sub!.collection.toArray as unknown as Row[]).some((r) => Number(r.id) === FENCE_ID)) {
    if (Date.now() > deadline) throw new Error("the subset never applied the fence row");
    await new Promise((r) => setTimeout(r, 20));
  }
  return rowsOf(sub!.collection.toArray as unknown as Row[]);
}

// ---- a durable-streams hold on the engine's change-log appends ----------------------------------

interface ChangeLogHold {
  url: string;
  hold(on: boolean): void;
  /** Resolves once an append carrying `xid` has reached the hold: decoded, on no feed yet. */
  arrived(xid: string): Promise<void>;
  close(): Promise<void>;
}

async function startChangeLogHold(upstreamUrl: string): Promise<ChangeLogHold> {
  const upstream = new URL(upstreamUrl);
  const changes = "/changes/";
  let holding = false;
  const queue: Array<() => void> = [];
  const bodies: string[] = [];
  const waiters: Array<() => void> = [];
  const forward = (incoming: IncomingMessage, body: Buffer, outgoing: ServerResponse) => {
    const forwarded = request(
      new URL(incoming.url ?? "/", upstream),
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(outgoing);
      },
    );
    forwarded.on("error", (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502, { "content-type": "text/plain" });
      outgoing.end(String(error));
    });
    forwarded.end(body);
  };
  const server = createServer((incoming, outgoing) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (c: Buffer) => chunks.push(c));
    incoming.on("end", () => {
      const body = Buffer.concat(chunks);
      const path = new URL(incoming.url ?? "/", upstream).pathname;
      if (holding && incoming.method === "POST" && path.startsWith(changes)) {
        bodies.push(body.toString());
        waiters.splice(0).forEach((w) => w());
        queue.push(() => forward(incoming, body, outgoing));
        return;
      }
      forward(incoming, body, outgoing);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("change-log hold did not bind TCP");
  return {
    url: `http://127.0.0.1:${address.port}`,
    hold(on) {
      holding = on;
      if (!on) queue.splice(0).forEach((go) => go());
    },
    async arrived(xid) {
      const deadline = Date.now() + 10000;
      while (!bodies.some((b) => b.includes(`"txid":"${xid}"`))) {
        if (Date.now() > deadline) throw new Error(`no change-log append for T ${xid} reached the hold`);
        await new Promise<void>((r) => {
          waiters.push(r);
          setTimeout(r, 50);
        });
      }
    },
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

async function boot(withHold = false): Promise<void> {
  h = await bootHarness(schema, {
    // The hold forwards everything unless told to hold, so the stack behaves exactly as without it.
    ...(withHold ? { wrapEngineDs: async (upstream: string) => (proxy = await startChangeLogHold(upstream)) } : {}),
  });
  seen = {};
  await drainEngine(h);
}

// ---- the contracts -------------------------------------------------------------------------------

describe("a committed transaction invisible to the snapshot (subset + full-shape seams)", () => {
  it("an out-of-window live update survives a page response captured before that update", async () => {
    await boot();
    await sql("INSERT INTO items VALUES (1, 1), (2, 2), (3, 3)");
    await drainEngine(h!);
    sub = await h!.client.subset({ table: "items", orderBy: { col: "id" }, limit: 1 });
    let releasePage!: () => void;
    let captured!: () => void;
    const capture = new Promise<void>((resolve) => {
      captured = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      releasePage = resolve;
    });
    holdPage = async (res) => {
      expect(trpcData<Page>(await res.clone().json())?.rows.map((r) => r.n)).toEqual([2, 3]);
      captured();
      await gate;
    };
    const loading = sub.loadMore(2);
    try {
      await capture;
      // The feed's id3 update is outside the old boundary. A later update of loaded
      // id1 is a causal fence proving the real client has consumed both envelopes.
      await sql("UPDATE items SET n = 30 WHERE id = 3; UPDATE items SET n = 10 WHERE id = 1");
      await drainEngine(h!);
      await expect.poll(() => (sub!.collection.toArray as unknown as Row[]).find((r) => r.id === 1)?.n).toBe(10);
      releasePage();
      await loading;
      expect(rowsOf(sub.collection.toArray as unknown as Row[])).toEqual(await truth());
    } finally {
      releasePage();
      await loading.catch(() => {});
    }
  });

  it("A: an insert decoded after the page snapshot still reaches the subset (commit LSN < snapshot LSN)", async () => {
    await boot(true);
    let xid = "";
    steps.afterHead = async () => {
      // T commits after the client's HEAD; its change-log append is held, so it is decoded but on
      // no feed while the page snapshot is taken, and lands on the feed after the HEAD offset.
      proxy!.hold(true);
      xid = await holdCommit("INSERT INTO items VALUES (1, 10)");
      await proxy!.arrived(xid);
      await expectInvisible(xid);
    };
    const creating = h!.client.subset(def);
    await untilSettledOrWaiting(creating);
    proxy!.hold(false);
    await release();
    sub = await creating;
    await drainEngine(h!);

    const env = envOf(await read(seen.feedUrl!, seen.offset!), xid);
    expect(env, "T's envelope is on the feed after the client's HEAD offset").toBeDefined();
    expect(lsn(env!.headers.lsn) < lsn(seen.page!.lsn), "T.lsn < the page snapshot LSN").toBe(true);
    console.log("A", { xid, offset: seen.offset, commitLsn: env!.headers.lsn, page: seen.page });

    expect(await fencedSubsetRows()).toEqual(await truth());
  });

  it("watermark: an update to a loaded row, decoded after the page snapshot, still reaches the subset", async () => {
    await boot(true);
    await sql("INSERT INTO items VALUES (1, 0)");
    await drainEngine(h!);
    let xid = "";
    steps.afterHead = async () => {
      proxy!.hold(true);
      xid = await holdCommit("UPDATE items SET n = 10 WHERE id = 1");
      await proxy!.arrived(xid);
      await expectInvisible(xid);
    };
    const creating = h!.client.subset(def);
    await untilSettledOrWaiting(creating);
    proxy!.hold(false);
    await release();
    sub = await creating;
    await drainEngine(h!);

    // The row IS in the page (at its pre-T value), so the per-row watermark decides T's fate.
    expect(seen.page!.rows.map((r) => ({ id: Number(r.id), n: Number(r.n) }))).toEqual([{ id: 1, n: 0 }]);
    const env = envOf(await read(seen.feedUrl!, seen.offset!), xid);
    expect(env, "T's envelope is on the feed after the client's HEAD offset").toBeDefined();
    expect(lsn(env!.headers.lsn) < lsn(seen.page!.lsn), "T.lsn < the page snapshot LSN").toBe(true);

    expect(await fencedSubsetRows()).toEqual(await truth());
  });

  it("B: a commit on the feed before the HEAD offset, invisible to the page snapshot, still reaches the subset", async () => {
    await boot();
    let xid = "";
    steps.beforeHead = async () => {
      // The feed is registered. T is decoded and appended to it, then the client HEADs past it,
      // while T stays invisible for the page snapshot that follows.
      xid = await holdCommit("INSERT INTO items VALUES (1, 10)");
      await drainEngine(h!);
      await expectInvisible(xid);
    };
    const creating = h!.client.subset(def);
    await untilSettledOrWaiting(creating);
    await release();
    sub = await creating;
    await drainEngine(h!);

    const whole = await read(seen.feedUrl!);
    expect(envOf(whole, xid), "T's envelope is on the subset feed").toBeDefined();
    expect(envOf(await read(seen.feedUrl!, seen.offset!), xid), "...before the client's HEAD offset").toBeUndefined();
    expect(lsn(envOf(whole, xid)!.headers.lsn) < lsn(seen.page!.lsn), "T.lsn < the page snapshot LSN").toBe(true);
    console.log("B", { xid, offset: seen.offset, page: seen.page });

    expect(await fencedSubsetRows()).toEqual(await truth());
  });

  it("B pre-registration: a commit sequenced before the subset feed exists, invisible to the page snapshot, still reaches the subset", async () => {
    await boot();
    // A diagnostic feed with a different predicate (so it is not shared with the subset's feed)
    // proves T was sequenced before the subset registered its own feed.
    const diagnostic = (await (
      await realFetch(`${h!.engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table: "items", changesOnly: true, where: { col: "id", op: "gte", value: 0 } }),
      })
    ).json()) as { streamUrl: string };
    const xid = await holdCommit("INSERT INTO items VALUES (1, 10)");
    await drainEngine(h!);
    await expectInvisible(xid);
    expect(
      envOf(await read(diagnostic.streamUrl), xid),
      "T was sequenced before the subset feed existed",
    ).toBeDefined();

    const creating = h!.client.subset(def);
    await untilSettledOrWaiting(creating);
    await release();
    sub = await creating;
    await drainEngine(h!);

    expect(envOf(await read(seen.feedUrl!), xid), "T is on no part of the subset feed").toBeUndefined();
    console.log("B pre-registration", { xid, page: seen.page });

    expect(await fencedSubsetRows()).toEqual(await truth());
  });

  it("B full shape: a commit sequenced before BeginShape, invisible to the backfill snapshot, still reaches the shape", async () => {
    await boot();
    const diagnostic = (await (
      await realFetch(`${h!.engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table: "items", changesOnly: true }),
      })
    ).json()) as { streamUrl: string };
    const xid = await holdCommit("INSERT INTO items VALUES (1, 10)");
    await drainEngine(h!);
    await expectInvisible(xid);
    expect(envOf(await read(diagnostic.streamUrl), xid), "T was sequenced before the shape was created").toBeDefined();

    const creating = realFetch(`${h!.engineUrl}/shapes`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ table: "items" }),
    });
    await untilSettledOrWaiting(creating);
    await release();
    const res = await creating;
    expect(res.ok).toBe(true);
    const shape = (await res.json()) as { streamUrl: string };
    await sql("INSERT INTO items VALUES ($1, 0)", [FENCE_ID]);
    await drainEngine(h!);

    const folded = new Map<string, Row>();
    for (const e of await read(shape.streamUrl)) {
      if (e.headers.operation === "delete") folded.delete(e.key);
      else if (e.value) folded.set(e.key, e.value);
    }
    expect(folded.has(String(FENCE_ID)), "the shape applied the fence row").toBe(true);
    expect(rowsOf(folded.values())).toEqual(await truth());
  });
});
