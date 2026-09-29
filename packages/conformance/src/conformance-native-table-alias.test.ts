// ADR-0002 makes a bare table name ingress shorthand for public.<name>. The published native client
// must therefore accept either spelling independently of which spelling keyed its local Schema.

import { createClient } from "@circuits/client";
import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { bootHarness, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
  },
};

const qualifiedSchema: Schema = {
  tables: {
    "public.items": { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
  },
};

let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

describe("published native client table aliases", () => {
  it("accepts public.items when the local schema is keyed by its items shorthand", async () => {
    h = await bootHarness(schema);
    const shape = await h.client.shape({ table: "public.items" });
    expect(shape.handle.table).toBe("public.items");
    await shape.close();
  }, 90000);

  it("accepts items when the local schema is keyed by its canonical public.items name", async () => {
    h = await bootHarness(qualifiedSchema);
    const shape = await h.client.shape({ table: "items" });
    expect(shape.handle.table).toBe("public.items");
    await shape.close();
  }, 90000);

  it("exposes schema-derived table helpers under both equivalent public spellings", async () => {
    h = await bootHarness(schema);
    expect(h.client.tables.items).toBeDefined();
    expect(h.client.tables["public.items"]).toBeDefined();
  }, 90000);

  it("rejects conflicting local definitions for the same canonical table", () => {
    const conflicting: Schema = {
      tables: {
        items: { columns: { id: { type: "int" } }, primaryKey: "id" },
        "public.items": { columns: { id: { type: "text" } }, primaryKey: "id" },
      },
    };

    expect(() => createClient({ apiUrl: "http://127.0.0.1:1", schema: conflicting })).toThrow(/conflict|collision/i);
  });
});
