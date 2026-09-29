// The change log is segmented, and a segment is deleted once nothing can resume inside it
// (ADR-0006) — end to end against the live engine, real Postgres and the real durable-streams
// server.
//
// Four properties, one per test:
//   1. rotation is invisible to a live shape: writes keep flowing across several rotations, the
//      sequencer's reported segment advances, and a segment it has passed with nothing pinning it
//      is deleted;
//   2. a dormant shape resumes ACROSS a rotation: it parks with its resume position in segment k,
//      changes happen (including a rotation), and reactivation replays through the pointer;
//   3. a dormant shape PINS its segment against deletion — until the retain window elapses, at
//      which point the shape is evicted and the segment goes;
//   4. a restart resumes on the right segment.
//
// The engine is booted with second-scale knobs (production defaults: 1 GiB / 1 day segments, a
// 7 day retain window — see `apps/engine/README.md`).

import type { Row, Schema, StreamEnvelope } from "@circuits/protocol";
import pgpkg from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { bootHarness, drainEngine, engineChangesOffset, engineChangesSegment, type Harness } from "./harness.js";

const schema: Schema = {
  tables: { items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" } },
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Second-scale segmentation + retention, so a test can watch a week of production behaviour. */
const knobs = {
  // Age-only rotation: a test cannot write a gigabyte, and every write here is a commit boundary.
  CIRCUITS_CHANGES_SEGMENT_BYTES: "0",
  CIRCUITS_CHANGES_SEGMENT_SECS: "2",
  CIRCUITS_CHANGES_RETAIN_SECS: "4",
  CIRCUITS_RETENTION_SWEEP_SECS: "1",
  CIRCUITS_SHAPE_IDLE_SECS: "1",
  // The dormancy TTL must not be what evicts in test 3 — the change log's retain window must be.
  CIRCUITS_SHAPE_DORMANT_TTL_SECS: "3600",
};

let h: Harness;
beforeEach(async () => {
  h = await bootHarness(schema, { engineEnv: knobs });
});
afterEach(async () => {
  await h.shutdown();
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

const createShape = (where: unknown, subscription?: string) =>
  fetch(`${h.engineUrl}/shapes`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ table: "items", where, ...(subscription ? { subscription } : {}) }),
  }).then(async (res) => {
    if (!res.ok) throw new Error(`POST /shapes -> ${res.status} ${await res.text()}`);
    return (await res.json()) as ShapeResp;
  });

/**
 * Keep a subscription's lease alive (ADR-0008) while a test holds its shape without reading it
 * through the engine.
 *
 * These tests run with a ONE SECOND idle window, and a native subscriber's reads (raw
 * durable-streams) are invisible to the engine — so a shape held for the ten-odd seconds a rotation
 * takes is, without renewals, a shape whose subscriber has said nothing for ten idle windows, and
 * the engine parks it. Renewing is the same create with the same subscription id. (The tests that
 * WANT dormancy release explicitly instead.)
 */
function keepAlive(where: unknown, subscription: string): () => void {
  const timer = setInterval(() => {
    void createShape(where, subscription).catch(() => {});
  }, 400);
  return () => clearInterval(timer);
}

const release = (id: string) => fetch(`${h.engineUrl}/shapes/${id}`, { method: "DELETE" });

async function shapeState(id: string): Promise<{ status: number; state?: string }> {
  const res = await fetch(`${h.engineUrl}/shapes/${encodeURIComponent(id)}`);
  if (!res.ok) return { status: res.status };
  return { status: res.status, state: ((await res.json()) as { state?: string }).state };
}

/** Fold a shape stream (raw durable-streams reads) into its current key -> row map. */
async function foldStream(streamUrl: string): Promise<Map<string, Row>> {
  const rows = new Map<string, Row>();
  let offset = "-1";
  for (let i = 0; i < 200; i++) {
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

async function segmentStatus(n: number): Promise<number> {
  return (await fetch(`${h.dsUrl}/changes/${n}`, { method: "HEAD" })).status;
}

async function waitFor(cond: () => Promise<boolean>, what: string, timeoutMs = 30000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(150);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** Write, then wait, enough times that the age-based policy has rotated at least `n` times. */
async function writeAcrossRotations(n: number, startId: number): Promise<number> {
  const from = await engineChangesSegment(h.engineUrl);
  let id = startId;
  await waitFor(async () => {
    await pg("INSERT INTO items (id, n) VALUES ($1, $2)", [id, id * 10]);
    id += 1;
    await sleep(300);
    const now = await engineChangesSegment(h.engineUrl);
    return now !== null && from !== null && now >= from + n;
  }, `${n} change-log rotation(s)`);
  return id;
}

describe("conformance: the change log rotates into segments and old segments are deleted", () => {
  it("a live shape keeps receiving every change across rotations, and passed segments are deleted", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);
    const where = { col: "n", op: "gte", value: 10 };
    const shape = await createShape(where, "live-across-rotations");
    const stopRenewing = keepAlive(where, "live-across-rotations");

    expect(await engineChangesSegment(h.engineUrl)).toBe(0);
    expect(await segmentStatus(0)).toBe(200);

    const nextId = await writeAcrossRotations(2, 2);

    // Every write landed on the shape, whichever segment carried it.
    await drainEngine(h);
    const rows = await foldStream(shape.streamUrl);
    const oracle = await pg("SELECT id FROM items WHERE n >= 10 ORDER BY id");
    expect([...rows.keys()].sort()).toEqual(oracle.map((r) => String(r.id)).sort());
    expect(oracle.length).toBeGreaterThan(2);

    // The SEQUENCER followed the rotation pointers, not just the ingestor.
    const pos = await engineChangesOffset(h.engineUrl);
    expect(pos).not.toBeNull();
    expect(pos!.segment).toBeGreaterThanOrEqual(2);

    // Nothing is dormant, so the sweeper deletes every segment the sequencer has passed.
    await waitFor(async () => (await segmentStatus(0)) === 404, "segment 0 to be deleted");
    expect(await segmentStatus(pos!.segment)).toBe(200); // never the current one

    // ...and the engine keeps working afterwards.
    await pg("INSERT INTO items (id, n) VALUES ($1, 999)", [nextId]);
    await drainEngine(h);
    await waitFor(async () => (await foldStream(shape.streamUrl)).has(String(nextId)), "a post-deletion write");
    stopRenewing();
  }, 90000);

  it("a dormant shape reactivates by replaying ACROSS a rotation pointer", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10), (2, 20), (3, 5)");
    await drainEngine(h);

    const where = { col: "n", op: "gte", value: 10 };
    const a = await createShape(where);
    await release(a.shapeId);
    await waitFor(async () => (await shapeState(a.shapeId)).state === "dormant", "the shape to go dormant");
    const parkedIn = await engineChangesSegment(h.engineUrl);
    expect(parkedIn).not.toBeNull();

    // Changes while dormant — enter, leave, delete — spread across at least one rotation, so the
    // replay MUST follow a pointer to see all of them.
    await pg("INSERT INTO items (id, n) VALUES (4, 40)");
    await writeAcrossRotations(1, 100);
    await pg("UPDATE items SET n = 1 WHERE id = 1");
    await pg("DELETE FROM items WHERE id = 2");
    await drainEngine(h);
    expect(await engineChangesSegment(h.engineUrl)).toBeGreaterThan(parkedIn!);

    // Any touch reactivates: the replay crosses the pointer and the folded stream matches Postgres.
    const b = await createShape(where);
    expect(b.shapeId).toBe(a.shapeId);
    const rows = await foldStream(b.streamUrl);
    const oracle = await pg("SELECT id, n FROM items WHERE n >= 10 ORDER BY id");
    expect([...rows.keys()].sort()).toEqual(oracle.map((r) => String(r.id)).sort());
    for (const r of oracle) expect(rows.get(String(r.id))?.n).toBe(r.n);
    expect(rows.has("1"), "the row that left while dormant is gone").toBe(false);
    expect(rows.has("2"), "the row deleted while dormant is gone").toBe(false);
  }, 90000);

  // The reader must step through EVERY closed segment, not jump to the open one: a resume in
  // segment k with k and k+1 both closed has to replay both, or the changes in k+1 are lost.
  it("a dormant shape resumes across TWO closed segments and loses nothing in the middle one", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);

    const where = { col: "n", op: "gte", value: 10 };
    const a = await createShape(where);
    await release(a.shapeId);
    await waitFor(async () => (await shapeState(a.shapeId)).state === "dormant", "the shape to go dormant");
    const parkedIn = await engineChangesSegment(h.engineUrl);

    // One write per segment across two rotations: `first` lands in the segment the shape parked in,
    // `middle` in the one after it, `last` in the open one. The middle segment is the one a
    // jump-to-the-open-segment bug would skip.
    await pg("INSERT INTO items (id, n) VALUES (901, 91)");
    await writeAcrossRotations(1, 910);
    await pg("INSERT INTO items (id, n) VALUES (902, 92)");
    await writeAcrossRotations(1, 920);
    await pg("INSERT INTO items (id, n) VALUES (903, 93)");
    await drainEngine(h);
    expect(await engineChangesSegment(h.engineUrl)).toBeGreaterThanOrEqual(parkedIn! + 2);

    const b = await createShape(where);
    expect(b.shapeId).toBe(a.shapeId);
    const rows = await foldStream(b.streamUrl);
    const oracle = await pg("SELECT id FROM items WHERE n >= 10 ORDER BY id");
    // Every row Postgres has, including the ones written in the segments between the resume point
    // and the open one.
    expect([...rows.keys()].sort()).toEqual(oracle.map((r) => String(r.id)).sort());
    for (const id of ["901", "902", "903"]) {
      expect(rows.has(id), `the change written in the segment carrying ${id} was replayed`).toBe(true);
    }
  }, 120000);

  it("a dormant shape pins its segment until the retain window elapses, then is evicted and it is deleted", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);

    const a = await createShape({ col: "n", op: "gte", value: 10 });
    await release(a.shapeId);
    await waitFor(async () => (await shapeState(a.shapeId)).state === "dormant", "the shape to go dormant");
    const pinned = await engineChangesSegment(h.engineUrl);
    expect(pinned).not.toBeNull();

    // Rotate past the pinned segment. The sequencer moves on, so nothing but this dormant shape
    // stands between the segment and the sweeper.
    await writeAcrossRotations(1, 200);
    await drainEngine(h);

    // Within the retain window (4 s) the pin holds, even though the sequencer is past it.
    await sleep(1500);
    expect(await segmentStatus(pinned!)).toBe(200);
    expect((await shapeState(a.shapeId)).state).toBe("dormant");

    // Past it, the shape is evicted (which is what unpins the segment) and the segment is deleted.
    await waitFor(async () => (await shapeState(a.shapeId)).status === 404, "the pinning shape to be evicted");
    await waitFor(async () => (await segmentStatus(pinned!)) === 404, "the unpinned segment to be deleted");
    // Eviction is terminal, so its stream went with it — an extended-API client recreates.
    expect((await fetch(a.streamUrl)).status).toBe(404);
  }, 90000);

  // The disk bound must hold for an engine nobody has ever created a shape on: the ingestor is
  // still appending, so segments must still be deleted. (In Postgres mode the sequencer is spawned
  // at boot whether or not any shape exists, so it checkpoints and its floor advances.)
  it("segments are deleted on an engine that has never had a shape", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);
    expect(await segmentStatus(0)).toBe(200);

    await writeAcrossRotations(2, 400);
    await drainEngine(h);

    await waitFor(async () => (await segmentStatus(0)) === 404, "segment 0 to be deleted with no shapes at all");
    const pos = await engineChangesOffset(h.engineUrl);
    expect(await segmentStatus(pos!.segment)).toBe(200);
  }, 90000);

  it("a restart after rotations resumes on the right segment", async () => {
    await pg("INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);
    const where = { col: "n", op: "gte", value: 10 };
    const shape = await createShape(where, "live-across-restart");
    // Renewals continue across the restart below: the restored claim comes back with its lease age,
    // so it must keep saying it is there on the far side too.
    const stopRenewing = keepAlive(where, "live-across-restart");

    const nextId = await writeAcrossRotations(2, 300);
    await drainEngine(h);
    const before = await engineChangesOffset(h.engineUrl);
    expect(before!.segment).toBeGreaterThanOrEqual(2);

    await h.restartEngine();

    // The restored sequencer picks up in the segment its checkpoint named — never back at 0 (whose
    // stream may not even exist any more) and never past the tail.
    const after = await engineChangesOffset(h.engineUrl);
    expect(after).not.toBeNull();
    expect(after!.segment).toBeGreaterThanOrEqual(before!.segment);

    // And the restored shape is live: a new write reaches its stream, and the barrier works.
    await pg("INSERT INTO items (id, n) VALUES ($1, 777)", [nextId]);
    await drainEngine(h);
    const rows = await foldStream(shape.streamUrl);
    expect(rows.has(String(nextId))).toBe(true);
    expect(rows.has("1")).toBe(true);
    stopRenewing();
  }, 120000);
});
