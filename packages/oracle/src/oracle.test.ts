import type { Schema } from "@circuits/protocol";
import pgpkg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createPgOracle, createPgTables, type Oracle } from "./index.js";

// The oracle is a real Postgres, so its tests need one: the cluster the integration project's global
// setup boots. Each suite gets a database of its own and drops it afterwards.
function adminUrl(): string {
  const url = process.env.CIRCUITS_TEST_PG_URL;
  if (!url) throw new Error("CIRCUITS_TEST_PG_URL not set (vitest globalSetup should boot Postgres)");
  return url;
}

let databases = 0;

async function oracleOn(schema: Schema): Promise<{ oracle: Oracle; drop: () => Promise<void> }> {
  const name = `oracle_test_${process.pid}_${++databases}`;
  const admin = new pgpkg.Client({ connectionString: adminUrl() });
  await admin.connect();
  try {
    await admin.query(`CREATE DATABASE ${name}`);
  } finally {
    await admin.end();
  }
  const url = new URL(adminUrl());
  url.pathname = `/${name}`;
  await createPgTables(url.toString(), schema);
  const oracle = await createPgOracle(schema, url.toString());
  return {
    oracle,
    drop: async () => {
      await oracle.close();
      const again = new pgpkg.Client({ connectionString: adminUrl() });
      await again.connect();
      try {
        await again.query(`DROP DATABASE ${name}`);
      } finally {
        await again.end();
      }
    },
  };
}

// The ids are numbers: order them as numbers, not as the strings a bare `sort()` compares.
const byNumber = (a: unknown, b: unknown): number => Number(a) - Number(b);

const schema: Schema = {
  tables: {
    users: {
      columns: {
        id: { type: "int" },
        name: { type: "text" },
        age: { type: "int" },
        active: { type: "bool" },
        score: { type: "float" },
      },
      primaryKey: "id",
    },
  },
};

const familySchema: Schema = {
  tables: {
    parent: { columns: { id: { type: "int" }, active: { type: "bool" } }, primaryKey: "id" },
    child: { columns: { id: { type: "int" }, parent_id: { type: "int" } }, primaryKey: "id" },
  },
};

describe("oracle", () => {
  let oracle: Oracle;
  let drop: () => Promise<void>;
  beforeAll(async () => {
    ({ oracle, drop } = await oracleOn(schema));
  });
  afterAll(async () => {
    await drop();
  });

  it("upserts and filters rows", async () => {
    await oracle.applyChange("users", {
      op: "insert",
      pk: 1,
      row: { id: 1, name: "Alice", age: 30, active: true, score: 9.5 },
    });
    await oracle.applyChange("users", {
      op: "insert",
      pk: 2,
      row: { id: 2, name: "Bob", age: 17, active: false, score: 3.2 },
    });
    await oracle.applyChange("users", {
      op: "insert",
      pk: 3,
      row: { id: 3, name: "Carol", age: 40, active: true, score: 7.1 },
    });

    const active = await oracle.queryShape({ table: "users", where: { col: "active", op: "eq", value: true } });
    expect(active.map((r) => r.id).sort(byNumber)).toEqual([1, 3]);
    // types round-trip exactly
    expect(active.find((r) => r.id === 1)).toMatchObject({ name: "Alice", age: 30, active: true, score: 9.5 });
  });

  it("update (upsert) moves a row in and out of a shape", async () => {
    // Bob becomes active -> enters the shape.
    await oracle.applyChange("users", {
      op: "update",
      pk: 2,
      row: { id: 2, name: "Bob", age: 18, active: true, score: 3.2 },
    });
    let active = await oracle.queryShape({ table: "users", where: { col: "active", op: "eq", value: true } });
    expect(active.map((r) => r.id).sort(byNumber)).toEqual([1, 2, 3]);

    // Alice becomes inactive -> leaves the shape.
    await oracle.applyChange("users", {
      op: "update",
      pk: 1,
      row: { id: 1, name: "Alice", age: 30, active: false, score: 9.5 },
    });
    active = await oracle.queryShape({ table: "users", where: { col: "active", op: "eq", value: true } });
    expect(active.map((r) => r.id).sort(byNumber)).toEqual([2, 3]);
  });

  it("delete removes a row", async () => {
    await oracle.applyChange("users", { op: "delete", pk: 3 });
    const active = await oracle.queryShape({ table: "users", where: { col: "active", op: "eq", value: true } });
    expect(active.map((r) => r.id)).toEqual([2]);
  });

  it("comparison + boolean predicates match Postgres semantics", async () => {
    await oracle.reset();
    for (let i = 1; i <= 5; i++) {
      await oracle.applyChange("users", {
        op: "insert",
        pk: i,
        row: { id: i, name: `u${i}`, age: 10 * i, active: i % 2 === 0, score: i },
      });
    }
    const rows = await oracle.queryShape({
      table: "users",
      where: {
        and: [
          { col: "age", op: "gte", value: 20 },
          {
            or: [
              { col: "active", op: "eq", value: true },
              { col: "score", op: "gt", value: 4 },
            ],
          },
        ],
      },
    });
    // age>=20 -> ids 2,3,4,5 ; AND (active(2,4) OR score>4(5)) -> ids 2,4,5
    expect(rows.map((r) => r.id).sort(byNumber)).toEqual([2, 4, 5]);
  });
});

describe("oracle subqueries", () => {
  let oracle: Oracle;
  let drop: () => Promise<void>;
  beforeAll(async () => {
    ({ oracle, drop } = await oracleOn(familySchema));
    for (const [id, active] of [
      [1, true],
      [2, false],
      [3, true],
    ] as const) {
      await oracle.applyChange("parent", { op: "insert", pk: id, row: { id, active } });
    }
    // children round-robin across parents 1,2,3
    for (let id = 1; id <= 6; id++) {
      await oracle.applyChange("child", { op: "insert", pk: id, row: { id, parent_id: ((id - 1) % 3) + 1 } });
    }
  });
  afterAll(async () => {
    await drop();
  });

  it("answers an IN (SELECT … active) subquery shape", async () => {
    const rows = await oracle.queryShape({
      table: "child",
      where: {
        col: "parent_id",
        in: { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } },
      },
    });
    // active parents = 1,3 ; children of 1 = {1,4}, of 3 = {3,6}
    expect(rows.map((r) => r.id).sort((a, b) => Number(a) - Number(b))).toEqual([1, 3, 4, 6]);
  });

  it("answers a NOT IN subquery shape", async () => {
    const rows = await oracle.queryShape({
      table: "child",
      where: {
        col: "parent_id",
        negated: true,
        in: { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } },
      },
    });
    // inactive parent = 2 ; children of 2 = {2,5}
    expect(rows.map((r) => r.id).sort((a, b) => Number(a) - Number(b))).toEqual([2, 5]);
  });

  it("reflects a parent deactivation (move-out) in the subquery result", async () => {
    await oracle.applyChange("parent", { op: "update", pk: 1, row: { id: 1, active: false } });
    const rows = await oracle.queryShape({
      table: "child",
      where: {
        col: "parent_id",
        in: { table: "parent", project: "id", where: { col: "active", op: "eq", value: true } },
      },
    });
    // now only parent 3 active -> children {3,6}
    expect(rows.map((r) => r.id).sort((a, b) => Number(a) - Number(b))).toEqual([3, 6]);
  });
});
