import type { Schema } from "@circuits/protocol";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { createShape, lockTable, pgQuery, streamKeys, tableLockWaiters, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    deep: {
      columns: { id: { type: "int" }, active: { type: "bool" }, payload: { type: "text" } },
      primaryKey: "id",
    },
    middle: {
      columns: { id: { type: "int" }, deep_id: { type: "int" }, payload: { type: "text" } },
      primaryKey: "id",
    },
    outer_rows: { columns: { id: { type: "int" }, middle_id: { type: "int" } }, primaryKey: "id" },
    other_rows: { columns: { id: { type: "int" }, middle_id: { type: "int" } }, primaryKey: "id" },
  },
};
const where = {
  col: "middle_id",
  in: {
    table: "middle",
    project: "id",
    where: {
      col: "deep_id",
      in: { table: "deep", project: "id", where: { col: "active", op: "eq", value: true } },
    },
  },
};

async function nodes(h: Harness): Promise<{ inner_table: string; distinct_values: number; refcount: number }[]> {
  const res = await fetch(`${h.engineUrl}/subqueries`);
  return ((await res.json()) as { nodes: { inner_table: string; distinct_values: number; refcount: number }[] }).nodes;
}

describe("native: wide nested membership seeds install in chunks", () => {
  let h: Harness;
  beforeEach(async () => {
    // Each 4096-byte unused payload forces many chunks despite the small retained projection.
    h = await bootHarness(schema, { engineEnv: { CIRCUITS_BACKFILL_APPEND_BYTES: "16384" } });
    await pgQuery(h, "INSERT INTO deep SELECT i, i % 2 = 1, repeat('x', 4096) FROM generate_series(1, 128) AS i");
    await pgQuery(h, "INSERT INTO middle SELECT i, i, repeat('y', 4096) FROM generate_series(1, 128) AS i");
    await pgQuery(h, "INSERT INTO outer_rows VALUES (1, 1), (2, 2), (3, 129)");
    await pgQuery(h, "INSERT INTO other_rows VALUES (10, 1), (20, 2), (30, 129)");
    await drainEngine(h);
  }, 60000);
  afterEach(async () => await h?.shutdown());

  it("keeps shared admission and replays a nested membership flip after the snapshot", async () => {
    const lock = await lockTable(h, "outer_rows");
    const first = createShape(h, { table: "outer_rows", where, subscription: "first" });
    let overlapping: ReturnType<typeof createShape> | undefined;
    try {
      await waitFor(
        async () => (await tableLockWaiters(h, "outer_rows")).length > 0,
        "outer backfill after both seeds",
      );
      const pending = await nodes(h);
      expect(pending).toHaveLength(2);
      expect(pending.map((node) => node.distinct_values)).toEqual([64, 64]);
      overlapping = createShape(h, { table: "other_rows", where, subscription: "overlapping" });
      // The outer snapshot predates this change. Both raw node deltas and child re-derivations
      // must survive the pending window, without applying against an incomplete parent seed.
      await pgQuery(h, "UPDATE deep SET active = NOT active WHERE id IN (1, 2)");
      await drainEngine(h);
    } finally {
      await lock.release();
    }
    const [a, b] = await Promise.all([first, overlapping!]);
    await drainEngine(h);
    expect(await streamKeys(a.streamUrl)).toEqual(["2"]);
    expect(await streamKeys(b.streamUrl)).toEqual(["20"]);
    expect((await nodes(h)).map((node) => node.refcount).sort((a, b) => a - b)).toEqual([1, 2]);
    await pgQuery(h, "UPDATE deep SET active = NOT active WHERE id IN (1, 2)");
    await drainEngine(h);
    expect(await streamKeys(a.streamUrl)).toEqual(["1"]);
    expect(await streamKeys(b.streamUrl)).toEqual(["10"]);
  }, 60000);

  it("retracts all chunked contributors when the client disconnects before outer activation", async () => {
    const lock = await lockTable(h, "outer_rows");
    const controller = new AbortController();
    const first = createShape(h, { table: "outer_rows", where, subscription: "retryable" }, controller.signal).then(
      () => "completed",
      () => "aborted",
    );
    try {
      await waitFor(
        async () => (await tableLockWaiters(h, "outer_rows")).length > 0,
        "outer snapshot after chunk assertions",
      );
      expect((await nodes(h)).map((node) => node.distinct_values)).toEqual([64, 64]);
      controller.abort();
      expect(await first).toBe("aborted");
      await waitFor(async () => (await nodes(h)).length === 0, "detached contributor cleanup");
      const graph = (await fetch(`${h.engineUrl}/graph`).then((res) => res.json())) as { shapes: unknown[] };
      expect(graph.shapes).toHaveLength(0);
    } finally {
      await lock.release();
    }
    const again = await createShape(h, { table: "outer_rows", where, subscription: "retryable" });
    await drainEngine(h);
    expect(await streamKeys(again.streamUrl)).toEqual(["1"]);
    expect((await nodes(h)).map((node) => node.refcount)).toEqual([1, 1]);
  }, 60000);

  it("retries a schema drift after chunk assertions with fresh nodes and one subscription", async () => {
    const lock = await lockTable(h, "outer_rows");
    const first = createShape(h, { table: "outer_rows", where, subscription: "drift" });
    let id: string | undefined;
    try {
      await waitFor(
        async () => (await tableLockWaiters(h, "outer_rows")).length > 0,
        "outer snapshot after chunk assertions",
      );
      const graph = (await fetch(`${h.engineUrl}/graph`).then((res) => res.json())) as { shapes: { id: string }[] };
      id = graph.shapes[0]!.id;
      await pgQuery(h, "ALTER TABLE deep ADD COLUMN extra int");
      // A Relation message lets the ingestor detect drift before the blocked create answers.
      await pgQuery(h, "UPDATE deep SET extra = 1 WHERE id = 1");
      await waitFor(
        async () => (await fetch(`${h.engineUrl}/shapes/${id}`)).status === 404,
        "drift retirement of pending create",
      );
    } finally {
      await lock.release();
    }
    const fresh = await first;
    expect(fresh.shapeId).not.toBe(id);
    await drainEngine(h);
    expect(await streamKeys(fresh.streamUrl)).toEqual(["1"]);
    expect((await nodes(h)).map((node) => node.refcount)).toEqual([1, 1]);
  }, 60000);
});
