// Settling snapshots against what the engine has already fanned out — the failure behaviour.
//
// `conformance-subset-seam.test.ts` proves the settle makes a committed-but-invisible transaction T
// reach the subset and the full shape. This file proves the settle cannot turn into an outage:
//
//  * a shape create whose snapshot cannot settle answers a retryable 503 with `Retry-After` (never a
//    500), and a query on the held table fails fast with the same, within the settle budget;
//  * a held transaction on one table never delays reads of another table;
//  * past the waiter cap, excess requests are refused at once, and the admitted one still succeeds
//    once T becomes visible;
//  * a long-open transaction the engine never sequenced (pinning `xmin`) never blocks settling of a
//    table the settle record still covers, even once the record has overflowed its bound;
//  * at its bound the record gives up its oldest transactions, and fences the tables it gave them up
//    on: a snapshot of such a table either provably includes them (its xmin is past the fence) or
//    answers a retryable 503 — never a silent success that is missing a transaction.
//
// T is held exactly as in the seam test: the cluster names a synchronous standby that never connects
// (`phantom_standby`), T commits with `synchronous_commit = on` and parks in `SyncRepWaitForLSN` after
// its commit record is flushed (committed, decodable, invisible to new snapshots), and
// `pg_cancel_backend` ends the wait. The cluster is owned by this file.
import { execFileSync } from "node:child_process";
import { appendFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";

import type { Row, Schema, StreamEnvelope } from "@circuits/protocol";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
    other: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
  },
};
const tools = { initdb: "initdb", pgCtl: "pg_ctl" };
let dir: string;
let previousUrl: string | undefined;
let h: Harness | undefined;
let held: { client: pg.Client; commit: Promise<unknown>; pid: number; xid: string } | undefined;
let pin: pg.Client | undefined;

beforeAll(() => {
  previousUrl = process.env.CIRCUITS_TEST_PG_URL;
  const scratch = resolve("tmp/agents/snapshot-fixes");
  mkdirSync(scratch, { recursive: true });
  dir = mkdtempSync(join(scratch, "settle-admission-pg-"));
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
  await release();
  if (pin) {
    await pin.query("ROLLBACK").catch(() => {});
    await pin.end().catch(() => {});
    pin = undefined;
  }
  await h?.shutdown();
  h = undefined;
});

async function boot(engineEnv: Record<string, string>, tables: Schema = schema): Promise<void> {
  h = await bootHarness(tables, { engineEnv });
  await drainEngine(h);
}

async function sql(text: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pg.Client({ connectionString: h!.pgUrl });
  await c.connect();
  try {
    return (await c.query(text, params)).rows as Row[];
  } finally {
    await c.end();
  }
}

/** Commit `statement` as T and return once T is parked in SyncRep: committed, invisible. */
async function holdCommit(statement: string): Promise<string> {
  const client = new pg.Client({ connectionString: h!.pgUrl });
  await client.connect();
  await client.query("BEGIN; SET LOCAL synchronous_commit = on");
  const info = (await client.query("SELECT pg_backend_pid() AS pid, pg_current_xact_id()::text AS xid")).rows[0];
  await client.query(statement);
  held = { client, commit: client.query("COMMIT").catch((e: Error) => e), pid: info.pid, xid: info.xid };
  const deadline = Date.now() + 10000;
  for (;;) {
    const [state] = await sql("SELECT wait_event FROM pg_stat_activity WHERE pid = $1", [info.pid]);
    if (state?.wait_event === "SyncRep") break;
    if (Date.now() > deadline) throw new Error(`T ${info.xid} did not reach SyncRep`);
    await new Promise((r) => setTimeout(r, 10));
  }
  await sql("SELECT pg_current_xact_id()"); // a later completed xid: xmax moves past T, T is in xip
  return info.xid;
}

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

type Answer = { status: number; ms: number; retryAfter: string | null; body: unknown };
async function post(path: string, body: unknown): Promise<Answer> {
  const t0 = performance.now();
  const res = await fetch(`${h!.engineUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json: unknown = text;
  try {
    json = JSON.parse(text);
  } catch {
    /* not JSON */
  }
  return { status: res.status, ms: performance.now() - t0, retryAfter: res.headers.get("retry-after"), body: json };
}
const query = (table: string) => post("/query", { table, orderBy: { col: "id" }, limit: 100 });

/** Resend a request while it answers the retryable 503 (honouring its `Retry-After` contract). */
async function untilAccepted(send: () => Promise<Answer>, what: string): Promise<Answer> {
  const deadline = Date.now() + 20000;
  for (;;) {
    const answer = await send();
    if (answer.status !== 503) return answer;
    expect(answer.retryAfter).toBe("1");
    if (Date.now() > deadline)
      throw new Error(`${what} still answered 503 at the deadline: ${JSON.stringify(answer.body)}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** A shape stream's rows, folded, after a causal fence row on the same table has been applied. */
async function shapeRows(streamUrl: string, fenceId: number): Promise<Array<{ id: number; n: number }>> {
  await sql("INSERT INTO items VALUES ($1, 0)", [fenceId]);
  await drainEngine(h!);
  const folded = new Map<string, Row>();
  let offset = "-1";
  for (let page = 0; ; page++) {
    if (page === 100) throw new Error("shape stream exceeded 100 catch-up pages");
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`);
    if (res.status === 204) break;
    expect(res.ok).toBe(true);
    const text = (await res.text()).trim();
    for (const e of (text ? JSON.parse(text) : []) as StreamEnvelope[]) {
      if (e.headers.operation === "delete") folded.delete(e.key);
      else if (e.value) folded.set(e.key, e.value);
    }
    offset = res.headers.get("stream-next-offset")!;
    if (res.headers.has("stream-up-to-date") || !text) break;
  }
  expect(folded.has(String(fenceId)), "the shape applied the fence row").toBe(true);
  return rowsOf(folded.values());
}
const rowsOf = (rows: Iterable<Row>) =>
  [...rows].map((r) => ({ id: Number(r.id), n: Number(r.n) })).sort((a, b) => a.id - b.id);
const truth = async () => rowsOf(await sql("SELECT id, n FROM items ORDER BY id"));
interface SettleStats {
  timeouts: number;
  rejections: number;
  xidsDropped: number;
  sequencedXids: number;
}

async function replicationStatus(): Promise<{ visibilityWaits?: number; settle?: SettleStats }> {
  return (await (await fetch(`${h!.engineUrl}/replication/lsn`)).json()) as {
    visibilityWaits?: number;
    settle?: SettleStats;
  };
}
async function settleStats(): Promise<SettleStats> {
  const stats = (await replicationStatus()).settle;
  if (!stats) throw new Error("engine omitted snapshot settle metrics");
  return stats;
}

describe("settling snapshots never turns into an outage", () => {
  it("a shape create whose snapshot cannot settle answers 503 with Retry-After, and a held-table query fails fast", async () => {
    await boot({ CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: "600" });
    await holdCommit("INSERT INTO items VALUES (1, 10)");
    await drainEngine(h!); // T is sequenced (on the change log) while still invisible

    const create = await post("/shapes", { table: "items" });
    console.log("create on the held table", create);
    expect(create.status, JSON.stringify(create.body)).toBe(503);
    expect(create.retryAfter).toBe("1");
    expect(String((create.body as { error?: string }).error)).toContain("not settled");

    const q = await query("items");
    expect(q.status).toBe(503);
    expect(q.retryAfter).toBe("1");
    expect(q.ms, "bounded by the settle budget, not a gateway timeout").toBeLessThan(5000);

    // The native refusal stays retryable across the actual API/tRPC HTTP adapter.
    await expect(h!.client.query({ table: "items", limit: 100 })).rejects.toMatchObject({
      data: { code: "SERVICE_UNAVAILABLE", httpStatus: 503, retryAfter: "1" },
    });

    // Once T is visible the same requests succeed, and nothing was retired meanwhile.
    await release();
    const again = await post("/shapes", { table: "items" });
    expect(again.status).toBe(200);
    expect((await query("items")).status).toBe(200);
    expect((await settleStats()).timeouts).toBeGreaterThanOrEqual(2);
  });

  it("a transaction held on one table never delays reads, or shape creates, on another", async () => {
    await boot({ CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: "3000", CIRCUITS_PG_POOL_SIZE: "1" });
    await sql("INSERT INTO other VALUES (1, 1)");
    await holdCommit("INSERT INTO items VALUES (1, 10)");
    await drainEngine(h!);

    const held = query("items"); // waits the full budget, holding no pooled connection
    const others = await Promise.all(Array.from({ length: 10 }, () => query("other")));
    const create = await post("/shapes", { table: "other" });
    expect(others.map((a) => a.status)).toEqual(Array(10).fill(200));
    expect(Math.max(...others.map((a) => a.ms)), "unrelated reads are not queued behind the held one").toBeLessThan(
      1500,
    );
    expect(create.status).toBe(200);
    expect((await held).status).toBe(503);
  });

  it("a subquery page settles against a sequenced invisible commit on its inner table", async () => {
    await boot({ CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: "600" });
    await sql("INSERT INTO items VALUES (1, 1)");
    await holdCommit("INSERT INTO other VALUES (1, 1)");
    await drainEngine(h!);
    const where = { col: "id", in: { table: "other", project: "id" } };
    const refused = await post("/query", { table: "items", where, limit: 100 });
    expect(refused.status).toBe(503);
    expect(refused.retryAfter).toBe("1");
    const refusedShape = await post("/shapes", { table: "items", where });
    expect(refusedShape.status).toBe(503);
    expect(refusedShape.retryAfter).toBe("1");
    await release();
    const accepted = await post("/query", { table: "items", where, limit: 100 });
    expect(accepted.status).toBe(200);
    expect((accepted.body as { rows: Row[] }).rows).toEqual([{ id: 1, n: 1 }]);
    const full = await post("/shapes", { table: "items", where });
    expect(full.status).toBe(200);
    const read = await fetch(`${(full.body as { streamUrl: string }).streamUrl}?offset=-1`);
    const envelopes = (await read.json()) as StreamEnvelope[];
    expect(envelopes.filter((e) => e.headers.operation === "upsert").map((e) => e.value)).toEqual([{ id: 1, n: 1 }]);
  });

  it("past the waiter cap a request is refused at once; the admitted one succeeds when T becomes visible", async () => {
    await boot({ CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: "8000", CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS: "1" });
    await holdCommit("INSERT INTO items VALUES (1, 10)");
    await drainEngine(h!);
    const rejectedBefore = (await settleStats()).rejections ?? 0;

    const first = query("items");
    // Wait until it is admitted and waiting (a named engine state, not elapsed time).
    const deadline = Date.now() + 5000;
    while (((await replicationStatus()).visibilityWaits ?? 0) < 1) {
      if (Date.now() > deadline) throw new Error("the first query never started waiting");
      await new Promise((r) => setTimeout(r, 10));
    }
    const excess = await Promise.all(Array.from({ length: 5 }, () => query("items")));
    expect(excess.map((a) => a.status)).toEqual(Array(5).fill(503));
    expect(excess.every((a) => a.retryAfter === "1")).toBe(true);
    expect(Math.max(...excess.map((a) => a.ms)), "refused at once, not after the budget").toBeLessThan(1000);
    expect((await settleStats()).rejections - rejectedBefore).toBe(5);

    await release();
    const admitted = await first;
    expect(admitted.status).toBe(200);
    expect((admitted.body as { rows: Row[] }).rows.map((r) => Number(r.id))).toEqual([1]);
  });

  it("at its bound the record fences what it gave up: T is in the shape once visible, or the create answers 503", async () => {
    // A poller that effectively never ticks on its own and the smallest bound the engine accepts
    // (2^20 xids = 1024 chunks of 1024): the record only grows, so it must hit the bound. It is
    // bounded in chunks, one per table per 1024-xid window, so FILLERS tables written once per
    // window for WINDOWS windows overflow it without committing a million transactions.
    const FILLERS = 64;
    const WINDOWS = 17; // 64 × 17 = 1088 chunks > 1024
    const wide: Schema = { tables: { ...schema.tables } };
    for (let f = 0; f < FILLERS; f++) wide.tables[`f${f}`] = schema.tables.items!;
    await boot({ CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS: "1048576", CIRCUITS_SNAPSHOT_SETTLE_POLL_MS: "600000" }, wide);
    pin = new pg.Client({ connectionString: h!.pgUrl });
    await pin.connect();
    await pin.query("BEGIN");
    await pin.query("INSERT INTO other VALUES (1000, 0)"); // holds an xid (and xmin) until it ends
    const [{ x: pinXid }] = await pin.query("SELECT pg_current_xact_id()::text AS x").then((r) => r.rows);

    // T, held and sequenced, is the OLDEST entry on `items`; then every filler table is written
    // once in each of WINDOWS later 1024-xid windows (the xids in between are burnt by empty
    // transactions, which the engine never sequences).
    await holdCommit("INSERT INTO items VALUES (1, 10)");
    const writer = new pg.Client({ connectionString: h!.pgUrl });
    await writer.connect();
    await writer.query("SET synchronous_commit = off");
    const burn = "BEGIN; SELECT pg_current_xact_id(); COMMIT;".repeat(1024);
    for (let w = 0; w < WINDOWS; w++) {
      await writer.query(burn);
      const inserts = Array.from({ length: FILLERS }, (_, f) => `INSERT INTO f${f} VALUES (${w}, 0);`).join(" ");
      await writer.query(`BEGIN; ${inserts} COMMIT;`);
    }
    await writer.end();
    await drainEngine(h!, 60000);

    const stats = await settleStats();
    console.log("settle after overflow", { pinXid, ...stats, waitMs: undefined });
    expect(stats.xidsDropped, "the bound was hit and counted").toBeGreaterThan(0);
    expect(stats.sequencedXids).toBeLessThanOrEqual(1024 * 1024);
    const [snap] = await sql("SELECT pg_current_snapshot()::text AS s");
    expect(String(snap!.s).startsWith(`${pinXid}:`), "xmin is pinned by the open writer").toBe(true);

    // A shape create on T's table while T is still held. T was given up at the bound, and xmin is
    // pinned below it, so no snapshot can be proved to include it: the create either answers the
    // retryable 503, or — accepted — its shape must hold T once T is visible. A success whose
    // shape is missing T is the lost change.
    const whileHeld = await post("/shapes", { table: "items" });
    console.log("create on the fenced table while T is held", whileHeld.status, whileHeld.body);
    await release();
    if (whileHeld.status === 200) {
      const shape = whileHeld.body as { streamUrl: string };
      expect(await shapeRows(shape.streamUrl, 98), "the accepted shape holds T").toEqual(await truth());
    } else {
      expect(whileHeld.status).toBe(503);
      expect(whileHeld.retryAfter).toBe("1");
    }
    // T is visible now; with xmin still pinned below the fence a query either includes T or is refused.
    const afterRelease = await query("items");
    if (afterRelease.status === 200) {
      const ids = (afterRelease.body as { rows: Row[] }).rows.map((r) => Number(r.id));
      expect(ids, "a subset page on the fenced table includes T").toContain(1);
    } else {
      expect(afterRelease.status).toBe(503);
      expect(afterRelease.retryAfter).toBe("1");
    }

    // A table the record still covers keeps working, and the unsequenced pin never blocks it.
    const answers = await Promise.all(Array.from({ length: 20 }, () => query("other")));
    expect(answers.map((a) => a.status)).toEqual(Array(20).fill(200));
    expect(Math.max(...answers.map((a) => a.ms))).toBeLessThan(2000);
    expect((await post("/shapes", { table: "other" })).status).toBe(200);

    // Once the pin ends, xmin moves past the fence: T's table is served again, and a new shape holds T.
    await pin.query("ROLLBACK");
    await pin.end();
    pin = undefined;
    const created = await untilAccepted(
      () => post("/shapes", { table: "items", where: { col: "n", op: "gte", value: 0 } }),
      "create",
    );
    expect(created.status, JSON.stringify(created.body)).toBe(200);
    const rows = await shapeRows((created.body as { streamUrl: string }).streamUrl, 99);
    expect(rows.map((r) => r.id)).toContain(1);
    expect(rows).toEqual(await truth());
  });
});
