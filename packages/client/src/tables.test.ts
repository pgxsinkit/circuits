import type { Schema } from "@circuits/protocol";
import { describe, expect, it } from "vitest";

import { canonicalTableIndex, lookupTableDef, resolveTableDef, tableSpellings } from "./tables.js";

const items = { columns: { id: { type: "int" as const } }, primaryKey: "id" };

const bareKeyed: Schema = { tables: { items } };
const canonicallyKeyed: Schema = { tables: { "public.items": items } };
const otherSchema: Schema = { tables: { "billing.items": items } };

describe("local schema lookup accepts either spelling (ADR-0002)", () => {
  it("resolves a canonical reference against a bare-keyed schema", () => {
    expect(lookupTableDef(bareKeyed, "public.items")).toBe(items);
    expect(lookupTableDef(bareKeyed, "items")).toBe(items);
  });

  it("resolves a bare reference against a canonically keyed schema", () => {
    expect(lookupTableDef(canonicallyKeyed, "items")).toBe(items);
    expect(lookupTableDef(canonicallyKeyed, "public.items")).toBe(items);
  });

  it("never resolves across schemas: only `public` has a bare shorthand", () => {
    expect(lookupTableDef(otherSchema, "items")).toBeUndefined();
    expect(lookupTableDef(otherSchema, "public.items")).toBeUndefined();
    expect(lookupTableDef(otherSchema, "billing.items")).toBe(items);
    expect(lookupTableDef(bareKeyed, "billing.items")).toBeUndefined();
  });

  it("a schema carrying both spellings of one table is a conflict, not two entries", () => {
    // `items` and `public.items` are ONE Postgres table. Two entries would let client-side
    // validation (columns, primary key) depend on which spelling a call used.
    const bare = { columns: { id: { type: "int" as const } }, primaryKey: "id" };
    const qualified = { columns: { id: { type: "text" as const } }, primaryKey: "id" };
    const conflicting: Schema = { tables: { items: bare, "public.items": qualified } };
    expect(() => canonicalTableIndex(conflicting)).toThrow(/conflict/i);
    expect(() => lookupTableDef(conflicting, "items")).toThrow(/conflict/i);

    // Identical definitions are refused just the same: the rule is one entry per table, not
    // "duplicates are fine as long as they agree".
    const duplicate: Schema = { tables: { items: bare, "public.items": bare } };
    expect(() => canonicalTableIndex(duplicate)).toThrow(/conflict/i);
  });

  it("indexes by canonical name and lists the spellings each table answers to", () => {
    expect([...canonicalTableIndex(bareKeyed).keys()]).toEqual(["public.items"]);
    expect([...canonicalTableIndex(canonicallyKeyed).keys()]).toEqual(["public.items"]);
    expect(tableSpellings("public.items")).toEqual(["public.items", "items"]);
    expect(tableSpellings("billing.items")).toEqual(["billing.items"]);
  });

  it("a genuinely unknown table is a miss, and the throwing form names it", () => {
    expect(lookupTableDef(bareKeyed, "orders")).toBeUndefined();
    expect(() => resolveTableDef(bareKeyed, "orders")).toThrow(/unknown table "orders"/);
  });

  it("a malformed reference throws as malformed, not as unknown", () => {
    // Reading `a.b.c` as "unknown table" would send an operator looking for a missing table.
    expect(() => lookupTableDef(bareKeyed, "a.b.c")).toThrow(/separator/);
    expect(() => lookupTableDef(bareKeyed, "")).toThrow(/empty/);
  });
});
