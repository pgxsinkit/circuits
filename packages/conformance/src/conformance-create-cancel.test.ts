// A create whose HTTP client disconnects must not leave a shape the next identical create joins.
//
// `POST /shapes` registers the shape's share entry BEFORE its backfill runs, so later identical
// creates join it and wait on the creator's outcome. If the creator's request is dropped mid-backfill
// (client timeout, control-plane restart, connection reset), the handler future goes with it: the
// outcome is never published, and every later identical create joins the half-made shape and waits
// on a creator that no longer exists. The shape definition is then uncreatable until the engine
// restarts, and whatever the create was buffering keeps buffering.
//
// Both shape kinds a native consumer uses — a plain equality shape and a subquery shape — are covered.

import type { Schema } from "@circuits/protocol";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  createShape,
  lockTable,
  pgQuery,
  sleep,
  streamKeys,
  tableLockWaiters,
  waitFor,
  waitForLockWaiter,
} from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: "int" }, active: { type: "bool" } }, primaryKey: "id" },
    child: { columns: { id: { type: "int" }, parent_id: { type: "int" } }, primaryKey: "id" },
  },
};

const activeParents = { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } } as const;

describe("native: an aborted create does not poison later creates of the same shape", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "INSERT INTO parent (id, active) VALUES (1, true), (2, false)");
    await pgQuery(h, "INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)");
    await drainEngine(h);
  }, 60000);
  afterAll(async () => await h?.shutdown());

  /** Park `def`'s backfill on a table lock, drop the request mid-backfill, release, then create it again. */
  async function abortThenRecreate(def: unknown): Promise<string[]> {
    const lock = await lockTable(h, "child");
    const controller = new AbortController();
    const aborted = createShape(h, def, controller.signal).then(
      () => "completed" as const,
      () => "aborted" as const,
    );
    try {
      await waitForLockWaiter(h, "the create to block in its backfill");
      controller.abort();
      expect(await aborted).toBe("aborted");
      // Let the engine observe the closed connection before the backfill is allowed to proceed.
      await sleep(300);
    } finally {
      await lock.release();
    }
    const again = await createShape(h, def);
    await drainEngine(h);
    return streamKeys(again.streamUrl);
  }

  it("plain shape", async () => {
    const keys = await abortThenRecreate({ table: "child", where: { col: "parent_id", op: "eq", value: 1 } });
    expect(keys).toEqual(["1", "2", "3"]);
  }, 60000);

  it("subquery shape", async () => {
    const keys = await abortThenRecreate({ table: "child", where: { col: "parent_id", in: activeParents } });
    expect(keys).toEqual(["1", "2", "3"]);
  }, 60000);
});

describe("native: cancellation after chunking a large membership seed rolls back before activation", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "INSERT INTO parent (id, active) SELECT i, true FROM generate_series(1, 250000) AS i");
    await pgQuery(h, "INSERT INTO child (id, parent_id) VALUES (1, 1)");
    await drainEngine(h, 60000);
  }, 90000);
  afterAll(async () => await h?.shutdown());

  it("an identical create succeeds after the in-flight request is aborted", async () => {
    const def = { table: "child", where: { col: "parent_id", in: activeParents }, subscription: "large-seed-retry" };
    const lock = await lockTable(h, "child");
    const controller = new AbortController();
    const first = createShape(h, def, controller.signal).then(
      () => "completed" as const,
      () => "aborted" as const,
    );
    const nodes = async () => {
      const res = await fetch(`${h.engineUrl}/subqueries`);
      return ((await res.json()) as { nodes: { distinct_values: number; refcount: number }[] }).nodes;
    };
    const shapes = async () => {
      const res = await fetch(`${h.engineUrl}/graph`);
      return ((await res.json()) as { shapes: { id: string }[] }).shapes;
    };

    try {
      // All inner chunks have reached the circuit before the outer backfill requests its table
      // lock. Hold that lock through cancellation so completion cannot win a timer race.
      await waitFor(
        async () => (await tableLockWaiters(h, "child")).length > 0,
        "the outer backfill to block after the large seed chunks",
        60000,
      );
      expect(await nodes()).toEqual([expect.objectContaining({ distinct_values: 250000, refcount: 1 })]);
      const pendingShapes = await shapes();
      expect(pendingShapes).toHaveLength(1);
      const cancelledId = pendingShapes[0]!.id;
      expect((await fetch(`${h.dsUrl}/shape/${cancelledId}`, { method: "HEAD" })).status).toBe(200);
      controller.abort();
      expect(await first).toBe("aborted");
      // Public removal alone precedes contributor retraction and stream deletion. Wait for each
      // cleanup signal while the outer snapshot is still blocked, then reuse the subscription.
      await waitFor(async () => (await shapes()).length === 0, "the cancelled public shape to disappear", 60000);
      await waitFor(async () => (await nodes()).length === 0, "all large-seed contributors to retract", 60000);
      await waitFor(
        async () => (await fetch(`${h.dsUrl}/shape/${cancelledId}`, { method: "HEAD" })).status === 404,
        "the cancelled shape stream to be removed",
        60000,
      );
    } finally {
      await lock.release();
    }

    const again = await createShape(h, def);
    await drainEngine(h, 60000);
    expect(await streamKeys(again.streamUrl)).toEqual(["1"]);
    expect(again.subscription).toBe("large-seed-retry");
    expect(await nodes()).toEqual([expect.objectContaining({ distinct_values: 250000, refcount: 1 })]);
    expect(await shapes()).toHaveLength(1);
  }, 120000);
});
