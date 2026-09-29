// A deferred subquery flip whose Postgres query-back fails must not be dropped.
//
// An inner-table change reconciles the node set under the registry lock and hands the dependent
// shapes' query-backs to the flip propagator. If that query-back errors, the node already says the
// value has moved, so nothing will ever re-derive those outer rows: a revocation never reaches the
// shape stream, and the only thing a native consumer can read — `GET /replication/lsn`'s
// `pendingFlips` — drains to 0 as if it had. A lost revocation is a row the subject keeps after their
// access was taken away.
//
// Driven entirely through public surfaces: the query-back is made to fail by parking it on an
// `ACCESS EXCLUSIVE` lock and terminating the blocked backend (`pg_terminate_backend`), which is what a
// connection reset, a failover, or a statement timeout looks like from the engine's side.

import type { Schema } from "@circuits/protocol";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createShape, lockTable, lockWaiters, pgQuery, streamKeys, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: "int" }, active: { type: "bool" } }, primaryKey: "id" },
    child: { columns: { id: { type: "int" }, parent_id: { type: "int" } }, primaryKey: "id" },
  },
};

const activeParents = { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } } as const;

describe("native: a flip whose query-back fails still lands", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
  }, 60000);
  afterAll(async () => await h?.shutdown());

  it("a revocation survives its query-back backend being terminated", async () => {
    await pgQuery(h, "INSERT INTO parent (id, active) VALUES (1, true), (2, false)");
    await pgQuery(h, "INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)");
    await drainEngine(h);

    const shape = await createShape(h, { table: "child", where: { col: "parent_id", in: activeParents } });
    await drainEngine(h);
    expect(await streamKeys(shape.streamUrl)).toEqual(["1", "2", "3"]);

    // Park the revocation's query-back on the outer table, then kill it.
    const lock = await lockTable(h, "child");
    try {
      await pgQuery(h, "UPDATE parent SET active = false WHERE id = 1");
      await waitFor(async () => (await lockWaiters(h)).length > 0, "the query-back to block on the table lock");
      const waiters = await lockWaiters(h);
      for (const pid of waiters) await pgQuery(h, "SELECT pg_terminate_backend($1)", [pid]);
    } finally {
      await lock.release();
    }
    // The fault must have actually fired — otherwise this test proves nothing.
    await waitFor(
      async () => /subquery flip propagation failed/.test(h.engineStderr()),
      "the engine to report the failed query-back",
      10000,
    );

    // The barrier the native consumer aligns on has to mean "every computed effect is on a stream".
    await drainEngine(h);
    expect(await streamKeys(shape.streamUrl)).toEqual([]);
  }, 60000);
});
