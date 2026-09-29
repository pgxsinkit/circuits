// Resolving a caller's table spelling against the local Schema.
//
// ADR-0002 makes `schema.name` canonical and a bare name shorthand for `public.<name>` — at the API
// boundary. A client's `Schema` is not the API boundary: it is local config the app wrote, keyed by
// whatever spelling it chose, and the engine never sees those keys. So `shape({ table: 'public.items' })`
// against a schema keyed `items` is not a mistake to reject; both spellings name the same table, and
// the engine accepts either one on the wire.
//
// The corollary is the rule this module enforces: **canonical identity is the only identity**. A
// schema key is canonicalised once, and two keys that canonicalise to the same table are a
// CONFLICT, not two entries — `items` and `public.items` are one Postgres table, so letting each
// carry its own columns/primary key would make client-side validation depend on which spelling a
// call happened to use.

import type { Schema, TableDef } from "@circuits/protocol";
import { canonicalTable, PUBLIC_SCHEMA, parseTableRef } from "@circuits/protocol";

/**
 * The schema's tables keyed by their canonical `schema.name`, built once.
 *
 * Throws on two keys that name the same table — with conflicting definitions or with identical
 * ones. Duplicates are refused either way: there is nothing to gain from writing a table twice, and
 * accepting the identical case would make the rule "we compare definitions", which is a deep
 * equality the schema type does not promise.
 */
export function canonicalTableIndex(schema: Schema): Map<string, TableDef> {
  const byCanonical = new Map<string, TableDef>();
  const spelledAs = new Map<string, string>();
  for (const [key, def] of Object.entries(schema.tables)) {
    const canonical = canonicalTable(key);
    const previous = spelledAs.get(canonical);
    if (previous !== undefined) {
      throw new Error(
        `client: schema keys "${previous}" and "${key}" are the same table (${canonical}) — ` +
          `a canonical table-name conflict. Keep exactly one entry per table.`,
      );
    }
    spelledAs.set(canonical, key);
    byCanonical.set(canonical, def);
  }
  return byCanonical;
}

/**
 * Every public spelling a canonical table answers to: the canonical `schema.name` always, plus the
 * bare shorthand for a table in `public`. Used to expose one table's helpers under both names.
 */
export function tableSpellings(canonical: string): string[] {
  const ref = parseTableRef(canonical);
  return ref.schema === PUBLIC_SCHEMA ? [canonical, ref.name] : [canonical];
}

/**
 * The local schema entry for a table, whichever of its equivalent spellings the caller used and
 * whichever one keyed the schema: both sides are canonicalised, so `items` and `public.items` are
 * the same lookup. `undefined` when the schema genuinely has no such table.
 *
 * An invalid reference (empty, two dots, a quote) is NOT resolved to `undefined`: `parseTableRef`
 * throws, and a malformed table name should say so rather than read as "unknown table". A schema
 * carrying the same table under two spellings throws too — see [`canonicalTableIndex`].
 */
export function lookupTableDef(schema: Schema, table: string): TableDef | undefined {
  // Parse the REFERENCE first: a malformed argument is the caller's error and must be reported as
  // such, ahead of anything the schema might also be wrong about.
  const canonical = canonicalTable(table);
  return canonicalTableIndex(schema).get(canonical);
}

/** [`lookupTableDef`], but a miss is the error every caller would otherwise write itself. */
export function resolveTableDef(schema: Schema, table: string): TableDef {
  const def = lookupTableDef(schema, table);
  if (!def) throw new Error(`client: unknown table "${table}"`);
  return def;
}
