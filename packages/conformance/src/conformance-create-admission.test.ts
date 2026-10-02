// Shared membership initialization can take longer than two seconds. A queued create must wait
// for that seed while replication and existing shapes keep advancing.
import type { Schema } from "@circuits/protocol";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  createShape,
  lockTable,
  pgQuery,
  type ShapeResp,
  sleep,
  streamKeys,
  tableLockWaiters,
  waitFor,
} from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: "int" }, active: { type: "bool" } }, primaryKey: "id" },
    child: { columns: { id: { type: "int" }, parent_id: { type: "int" } }, primaryKey: "id" },
    child2: { columns: { id: { type: "int" }, parent_id: { type: "int" } }, primaryKey: "id" },
  },
};
const where = {
  col: "parent_id",
  in: { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } },
};

describe("native: overlapping creates wait for shared membership initialization", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
  }, 60000);
  afterAll(async () => await h?.shutdown());

  it("survives a seed held beyond the former retry budget while an existing stream advances", async () => {
    await pgQuery(h, "INSERT INTO parent (id, active) VALUES (1, true), (2, false)");
    await drainEngine(h);
    const live = await createShape(h, { table: "child" });
    const lock = await lockTable(h, "parent");
    const creates: Promise<{ status: number; body: ShapeResp | { error: string } }>[] = [];
    const feed = (table: string, subscription: string) => {
      const result = fetch(`${h.engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table, where, changesOnly: true, subscription }),
      }).then(async (res) => ({ status: res.status, body: (await res.json()) as ShapeResp | { error: string } }));
      creates.push(result);
    };
    try {
      feed("child", "first");
      await waitFor(async () => (await tableLockWaiters(h, "parent")).length > 0, "the first membership seed to block");
      feed("child2", "overlapping");
      feed("child", "same-signature");
      await waitFor(async () => {
        const graph = (await fetch(`${h.engineUrl}/graph`).then((res) => res.json())) as {
          shapes: { table: string }[];
        };
        return graph.shapes.some((shape) => shape.table === "public.child2");
      }, "the overlapping create to register");
      // Exceed the previous 100 x 20ms conflict retry budget with the first seed still blocked.
      await sleep(3000);
      expect((await tableLockWaiters(h, "parent")).length).toBeGreaterThan(0);
      await pgQuery(h, "INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 2)");
      await waitFor(
        async () => (await streamKeys(live.streamUrl)).join(",") === "1,2",
        "the existing stream to advance",
      );
    } finally {
      await lock.release();
    }
    const results = await Promise.all(creates);
    expect(results.map(({ status, body }) => (status === 200 ? status : { status, body }))).toEqual([200, 200, 200]);
    const feeds = results.map(({ body }) => body as ShapeResp);
    expect(feeds[0]!.shapeId).toBe(feeds[2]!.shapeId);
    expect(feeds[1]!.shapeId).not.toBe(feeds[0]!.shapeId);
    await pgQuery(h, "INSERT INTO child (id, parent_id) VALUES (3, 1), (4, 2)");
    await pgQuery(h, "INSERT INTO child2 (id, parent_id) VALUES (3, 1), (4, 2)");
    await drainEngine(h);
    expect(await streamKeys(feeds[0]!.streamUrl)).toEqual(["1", "3"]);
    expect(await streamKeys(feeds[1]!.streamUrl)).toEqual(["3"]);
    const graph = (await fetch(`${h.engineUrl}/graph`).then((res) => res.json())) as { shapes: unknown[] };
    expect(graph.shapes).toHaveLength(3);
  }, 60000);
});
