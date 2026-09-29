// PostgreSQL's supported scalar domain must survive the native query path without JSON precision
// loss. The published client is the external consumer here; no engine internals are involved.

import type { Row, Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { pgQuery } from "./engine-native.js";
import { bootHarness, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: "int" }, amount: { type: "int" } },
      primaryKey: "id",
    },
  },
};

let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

describe("native scalar fidelity", () => {
  it("returns an exact PostgreSQL bigint through the published one-shot query API", async () => {
    h = await bootHarness(schema, {
      ddl: `
        CREATE TABLE items (id integer PRIMARY KEY, amount bigint NOT NULL);
        ALTER TABLE items REPLICA IDENTITY FULL;
      `,
    });
    await pgQuery(h, "INSERT INTO items (id, amount) VALUES (1, 9007199254740993)");

    const page = await h.client.query({ table: "items" });
    const row = page.rows[0] as Row;
    expect(String(row.amount), "a supported bigint must not be rounded by JSON/JavaScript").toBe("9007199254740993");
  });
});
