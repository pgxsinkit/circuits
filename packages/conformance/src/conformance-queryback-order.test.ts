import type { Schema } from "@circuits/protocol";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: "int" }, active: { type: "bool" } }, primaryKey: "id" },
    child: {
      columns: {
        id: { type: "int" },
        parent_id: { type: "int" },
        group_id: { type: "int" },
        payload: { type: "text" },
      },
      primaryKey: "id",
    },
    grandchild: { columns: { id: { type: "int" }, group_id: { type: "int" } }, primaryKey: "id" },
  },
};

const activeParents = { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } } as const;

// Every engine FROM is schema-qualified (ADR-0002), so the running query-back reads
// `from "public"."child" t` — the probe below matches that exact text.
async function candidateQueryRunning(h: Harness): Promise<boolean> {
  const rows = await pgQuery(
    h,
    `SELECT 1
       FROM pg_stat_activity
      WHERE datname = current_database()
        AND state = 'active'
        AND query ILIKE '%from "public"."child" t%'
        AND pid <> pg_backend_pid()
      LIMIT 1`,
  );
  return rows.length > 0;
}

describe("native: a query-back cannot overwrite a newer outer-row decision", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "INSERT INTO parent (id, active) VALUES (1, false), (2, false)");
    await pgQuery(
      h,
      `INSERT INTO child (id, parent_id, payload)
       SELECT i, 1, repeat('x', 256) FROM generate_series(1, 100000) AS i`,
    );
    await drainEngine(h, 60000);
  }, 90000);
  afterAll(async () => await h?.shutdown());

  it("a row moved after the query snapshot does not re-enter from that stale snapshot", async () => {
    const shape = await createShape(h, { table: "child", where: { col: "parent_id", in: activeParents } });
    await drainEngine(h);
    expect((await foldStream(shape.streamUrl)).size).toBe(0);

    // Activating parent 1 starts a query-back for all 100,000 candidate rows. Once Postgres is
    // visibly executing that SELECT, move row 1 to inactive parent 2 in a newer transaction.
    await pgQuery(h, "UPDATE parent SET active = true WHERE id = 1");
    await waitFor(() => candidateQueryRunning(h), "the membership query-back to establish its snapshot");
    await pgQuery(h, "UPDATE child SET parent_id = 2 WHERE id = 1");

    await drainEngine(h, 60000);
    const rows = await foldStream(shape.streamUrl);
    expect(rows.has("1")).toBe(false);
    expect(rows.size).toBe(99999);
  }, 90000);
});

describe("native: a nested query-back cannot overwrite a newer parent-node decision", () => {
  let h: Harness;
  beforeAll(async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "INSERT INTO parent (id, active) VALUES (1, false), (2, false)");
    await pgQuery(
      h,
      `INSERT INTO child (id, parent_id, group_id, payload)
       SELECT i, 1, CASE WHEN i = 1 THEN 1 ELSE 2 END, repeat('x', 256)
         FROM generate_series(1, 100000) AS i`,
    );
    await pgQuery(h, "INSERT INTO grandchild (id, group_id) VALUES (1, 1)");
    await drainEngine(h, 60000);
  }, 90000);
  afterAll(async () => await h?.shutdown());

  it("a child row moved after the parent-node snapshot does not re-enter through that stale snapshot", async () => {
    const activeChildGroups = {
      table: "child",
      project: "group_id",
      where: { col: "parent_id", in: activeParents },
    } as const;
    const shape = await createShape(h, {
      table: "grandchild",
      where: { col: "group_id", in: activeChildGroups },
    });
    await drainEngine(h);
    expect((await foldStream(shape.streamUrl)).size).toBe(0);

    // This query-back updates an intermediate membership node rather than emitting directly to a
    // shape. Its Postgres snapshot needs the same recency fence as the outer-shape query-back above.
    await pgQuery(h, "UPDATE parent SET active = true WHERE id = 1");
    await waitFor(() => candidateQueryRunning(h), "the nested membership query-back to establish its snapshot");
    await pgQuery(h, "UPDATE child SET parent_id = 2 WHERE id = 1");

    await drainEngine(h, 60000);
    const rows = await foldStream(shape.streamUrl);
    expect(rows.has("1")).toBe(false);
  }, 90000);
});
