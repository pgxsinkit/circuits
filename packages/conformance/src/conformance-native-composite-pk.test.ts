// PostgreSQL composite primary keys are supported by native introspection. Distinct key tuples must
// therefore remain distinct through backfill, routing, and durable-stream emission.

import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery } from "./engine-native.js";
import { bootHarness, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: {
      columns: { a: { type: "text" }, b: { type: "text" }, payload: { type: "text" } },
      // The public TS schema is singular, but PostgreSQL introspection below discovers both key
      // columns. This client-side declaration is used only to boot the black-box harness.
      primaryKey: "a",
    },
  },
};

let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

describe("native composite primary-key identity", () => {
  it("does not collapse distinct text key tuples containing the internal separator", async () => {
    h = await bootHarness(schema, {
      ddl: `
        CREATE TABLE items (a text NOT NULL, b text NOT NULL, payload text NOT NULL, PRIMARY KEY (a, b));
        ALTER TABLE items REPLICA IDENTITY FULL;
      `,
    });
    const sep = "\u001f";
    await pgQuery(h, "INSERT INTO items (a, b, payload) VALUES ($1, $2, $3), ($4, $5, $6)", [
      "x",
      `y${sep}z`,
      "first",
      `x${sep}y`,
      "z",
      "second",
    ]);

    const shape = await createShape(h, { table: "items" });
    const rows = await foldStream(shape.streamUrl);
    expect(rows.size, "two distinct PostgreSQL primary keys must emit two native rows").toBe(2);
    expect([...rows.values()].map((row) => row.payload).sort()).toEqual(["first", "second"]);
  });

  it("includes every composite primary-key column in a projected native shape", async () => {
    h = await bootHarness(schema, {
      ddl: `
        CREATE TABLE items (a text NOT NULL, b text NOT NULL, payload text NOT NULL, PRIMARY KEY (a, b));
        ALTER TABLE items REPLICA IDENTITY FULL;
      `,
    });
    await pgQuery(h, "INSERT INTO items (a, b, payload) VALUES ('same', 'one', 'first'), ('same', 'two', 'second')");

    const shape = await createShape(h, { table: "items", columns: ["payload"] });
    const rows = await foldStream(shape.streamUrl);
    expect(rows.size).toBe(2);
    expect(
      [...rows.values()]
        .map((row) => ({ a: row.a, b: row.b, payload: row.payload }))
        .sort((left, right) => String(left.payload).localeCompare(String(right.payload))),
      "a projected native row must retain the complete PostgreSQL primary key",
    ).toEqual([
      { a: "same", b: "one", payload: "first" },
      { a: "same", b: "two", payload: "second" },
    ]);
  });
});
