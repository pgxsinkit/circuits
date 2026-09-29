// A transaction far too large to hold in memory must still reach every shape, intact and in order
// (ADR-0003) — end to end against the live engine, real Postgres and the real durable-streams
// server.
//
// The engine is booted with byte-scale knobs (production defaults: a 128 MiB per-transaction memory
// cap, a 64 MiB append budget — see `apps/engine/README.md`), so a few thousand small rows in one
// transaction is, to this engine, the multi-gigabyte `UPDATE` the ADR is about: the ingestor's
// buffer spills to a temporary file and the commit is appended in many chunks.
//
// What is asserted:
//   1. a single large INSERT transaction converges — the folded shape stream equals Postgres;
//   2. the engine really did take the spill/chunk path (`/metrics`), rather than passing the test by
//      never crossing the cap;
//   3. a large UPDATE in one transaction (every row rewritten, old + new carried under REPLICA
//      IDENTITY FULL — the ADR's worst case) converges too;
//   4. a small transaction afterwards is still live: the hot path is not left in a chunked/spilled
//      mode, and nothing about transaction size retired the shape.

import type { Row, Schema, StreamEnvelope } from "@circuits/protocol";
import pgpkg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: "int" }, n: { type: "int" }, label: { type: "text" } },
      primaryKey: "id",
    },
  },
};

/** Rows per large transaction. Modest on purpose — the knobs, not the volume, force the spill. */
const ROWS = 4000;

/**
 * Byte-scale large-transaction knobs. 4 KiB of buffered changes is a handful of envelopes, and a
 * 16 KiB append budget is a few dozen — so `ROWS` rows in one transaction spill and chunk hard.
 */
const knobs = {
  CIRCUITS_TXN_MEMORY_BYTES: "4096",
  CIRCUITS_CHANGES_APPEND_BYTES: "16384",
};

let h: Harness;
beforeAll(async () => {
  h = await bootHarness(schema, { engineEnv: knobs });
}, 120000);
afterAll(async () => {
  await h?.shutdown();
});

async function pg(sql: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl });
  await c.connect();
  try {
    return (await c.query(sql, params)).rows as Row[];
  } finally {
    await c.end().catch(() => {});
  }
}

interface ShapeResp {
  shapeId: string;
  streamPath: string;
  streamUrl: string;
}

const createShape = (where: unknown) =>
  fetch(`${h.engineUrl}/shapes`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ table: "items", where }),
  }).then(async (res) => {
    if (!res.ok) throw new Error(`POST /shapes -> ${res.status} ${await res.text()}`);
    return (await res.json()) as ShapeResp;
  });

/** Fold a shape stream (raw durable-streams reads) into its current key -> row map. */
async function foldStream(streamUrl: string): Promise<Map<string, Row>> {
  const rows = new Map<string, Row>();
  let offset = "-1";
  for (let i = 0; i < 5000; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`);
    if (res.status === 204) break;
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`);
    const body = (await res.text()).trim();
    const envs: StreamEnvelope[] = body ? (JSON.parse(body) as StreamEnvelope[]) : [];
    for (const env of envs) {
      if (env.headers.operation === "delete") rows.delete(env.key);
      else if (env.value) rows.set(env.key, env.value as Row);
    }
    const next = res.headers.get("stream-next-offset");
    const upToDate = res.headers.get("stream-up-to-date") !== null;
    if (!next || next === offset) break;
    offset = next;
    if (upToDate) break;
  }
  return rows;
}

interface Counters {
  txn_spills_total: number;
  txn_spill_bytes: number;
  txn_chunked_appends_total: number;
}

async function counters(): Promise<Counters> {
  const res = await fetch(`${h.engineUrl}/metrics`);
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`);
  const body = (await res.json()) as { counters: Counters };
  return body.counters;
}

describe("conformance: a transaction too large for memory spills and is appended in chunks", () => {
  it("one huge INSERT transaction converges, having spilled and chunked", async () => {
    const shape = await createShape({ col: "n", op: "gte", value: 0 });

    // ONE transaction, `ROWS` inserts. `INSERT … SELECT generate_series` commits as a single
    // transaction, so the ingestor sees one Begin/Commit pair carrying every change.
    await pg(
      `INSERT INTO items (id, n, label)
       SELECT g, g, 'row-' || g || '-' || repeat('x', 40) FROM generate_series(1, $1) AS g`,
      [ROWS],
    );
    // A generous barrier: this one transaction becomes ~70 chunked appends plus the sequencer's
    // pass over every one of them.
    await drainEngine(h, 90000);

    const rows = await foldStream(shape.streamUrl);
    const oracle = await pg("SELECT id, n, label FROM items WHERE n >= 0 ORDER BY id");
    expect(oracle.length).toBe(ROWS);
    expect(rows.size).toBe(ROWS);
    // A sampled row must be byte-identical, not merely present.
    const sample = oracle[Math.floor(ROWS / 2)]!;
    expect(rows.get(String(sample.id))?.label).toBe(sample.label);
    expect(rows.get(String(sample.id))?.n).toBe(sample.n);
    expect(rows.has("1")).toBe(true);
    expect(rows.has(String(ROWS))).toBe(true);

    // ...and it really did take the ADR-0003 path.
    const c = await counters();
    expect(c.txn_spills_total).toBeGreaterThanOrEqual(1);
    expect(c.txn_chunked_appends_total).toBeGreaterThanOrEqual(2);
    expect(c.txn_spill_bytes).toBeGreaterThan(0);
  }, 180000);

  it("a huge UPDATE in one transaction (old + new per row) converges too", async () => {
    const shape = await createShape({ col: "n", op: "gte", value: 0 });
    const before = await counters();

    // REPLICA IDENTITY FULL means every one of these carries the whole prior row as well — the
    // memory profile the ADR is actually about.
    await pg(`UPDATE items SET n = n + 1000000, label = label || '-v2'`);
    await drainEngine(h, 90000);

    const rows = await foldStream(shape.streamUrl);
    const oracle = await pg("SELECT id, n, label FROM items WHERE n >= 0 ORDER BY id");
    expect(oracle.length).toBe(ROWS);
    expect(rows.size).toBe(ROWS);
    const sample = oracle[Math.floor(ROWS / 3)]!;
    expect(rows.get(String(sample.id))?.n).toBe(sample.n);
    expect(rows.get(String(sample.id))?.label).toBe(sample.label);
    expect(String(sample.label).endsWith("-v2")).toBe(true);

    const after = await counters();
    expect(after.txn_spills_total).toBeGreaterThan(before.txn_spills_total);
    expect(after.txn_chunked_appends_total).toBeGreaterThan(before.txn_chunked_appends_total);
  }, 180000);

  it("an ordinary small transaction afterwards is still live", async () => {
    const shape = await createShape({ col: "n", op: "gte", value: 0 });
    const before = await counters();

    await pg(`INSERT INTO items (id, n, label) VALUES ($1, 7, 'after-the-storm')`, [ROWS + 1]);
    await drainEngine(h);

    const rows = await foldStream(shape.streamUrl);
    expect(rows.get(String(ROWS + 1))?.label).toBe("after-the-storm");
    expect(rows.size).toBe(ROWS + 1);

    // The small transaction fit in one append and never touched the disk: the hot path is unchanged
    // by the large ones that preceded it.
    const after = await counters();
    expect(after.txn_spills_total).toBe(before.txn_spills_total);
    expect(after.txn_chunked_appends_total).toBe(before.txn_chunked_appends_total);
  }, 120000);
});
