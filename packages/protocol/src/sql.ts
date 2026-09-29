import {
  type ChangeEvent,
  type ColumnType,
  isAnd,
  isInSubquery,
  isIsNull,
  isLeaf,
  isNot,
  isOr,
  type LeafOp,
  type Predicate,
  type TableDef,
  type Value,
} from "./types.js";

export interface SqlFragment {
  text: string;
  params: Value[];
}

/** Quote a SQL identifier (column/table name). */
function q(id: string): string {
  return `"${id.replace(/"/g, '""')}"`;
}

/** A schema-qualified table identity (ADR-0002); the canonical spelling is `schema.name`. */
export interface TableRef {
  schema: string;
  name: string;
}

/** The schema a bare table name resolves to. */
export const PUBLIC_SCHEMA = "public";

/**
 * The one parse rule for a textual table reference, mirroring the engine's `TableRef::parse`:
 * `schema.name` is taken as given, a bare `name` means `public.<name>`, and anything else (an empty
 * part, more than one dot, a quote) throws. There is no second rule anywhere in this package.
 */
export function parseTableRef(ref: string): TableRef {
  const s = ref.trim();
  if (s === "") throw new Error("empty table reference");
  const dot = s.indexOf(".");
  const [schema, name] = dot === -1 ? [PUBLIC_SCHEMA, s] : [s.slice(0, dot), s.slice(dot + 1)];
  for (const [what, part] of [
    ["schema", schema],
    ["name", name],
  ] as const) {
    if (part === "") throw new Error(`table reference "${ref}" has an empty ${what}`);
    if (part.includes("."))
      throw new Error(`table reference ${what} "${part}" contains a '.'; use exactly one schema.name separator`);
    if (part.includes('"'))
      throw new Error(`table reference ${what} "${part}" contains a '"'; quoted identifiers are not supported`);
  }
  return { schema, name };
}

/** The canonical `schema.name` spelling — what goes on the wire and into every map key. */
export function canonicalTable(ref: string | TableRef): string {
  const t = typeof ref === "string" ? parseTableRef(ref) : ref;
  return `${t.schema}.${t.name}`;
}

/** The SQL identifier form of a table reference: `"schema"."name"`. */
export function qualifiedIdent(ref: string | TableRef): string {
  const t = typeof ref === "string" ? parseTableRef(ref) : ref;
  return `${q(t.schema)}.${q(t.name)}`;
}

const OP_SQL: Record<LeafOp, string> = {
  eq: "=",
  neq: "<>",
  lt: "<",
  lte: "<=",
  gt: ">",
  gte: ">=",
};

const TYPE_SQL: Record<ColumnType, string> = {
  int: "INTEGER",
  float: "DOUBLE PRECISION",
  text: "TEXT",
  bool: "BOOLEAN",
};

/**
 * Compile a predicate to a parameterized SQL boolean expression.
 * `startIndex` is the first `$n` placeholder number to use (1-based).
 */
export function predicateToSql(pred: Predicate, startIndex = 1): SqlFragment {
  const params: Value[] = [];
  let next = startIndex;
  const text = build(pred);
  return { text, params };

  function ph(value: Value): string {
    params.push(value);
    return `$${next++}`;
  }

  function build(p: Predicate): string {
    if (isLeaf(p)) {
      return `${q(p.col)} ${OP_SQL[p.op]} ${ph(p.value)}`;
    }
    if (isIsNull(p)) {
      return `${q(p.col)} IS ${p.isNull ? "" : "NOT "}NULL`;
    }
    if (isAnd(p)) {
      if (p.and.length === 0) return "TRUE";
      return `(${p.and.map(build).join(" AND ")})`;
    }
    if (isOr(p)) {
      if (p.or.length === 0) return "FALSE";
      return `(${p.or.map(build).join(" OR ")})`;
    }
    if (isNot(p)) {
      return `(NOT ${build(p.not)})`;
    }
    if (isInSubquery(p)) {
      const op = p.negated ? "NOT IN" : "IN";
      const inner = p.in.where ? ` WHERE ${build(p.in.where)}` : "";
      return `${q(p.col)} ${op} (SELECT ${q(p.in.project)} FROM ${qualifiedIdent(p.in.table)}${inner})`;
    }
    throw new Error(`unknown predicate node: ${JSON.stringify(p)}`);
  }
}

/** `CREATE TABLE` DDL for one table. */
export function tableDDL(name: string, def: TableDef): string {
  const cols = Object.entries(def.columns).map(([col, c]) => `${q(col)} ${TYPE_SQL[c.type]}`);
  cols.push(`PRIMARY KEY (${q(def.primaryKey)})`);
  return `CREATE TABLE ${qualifiedIdent(name)} (\n  ${cols.join(",\n  ")}\n)`;
}

/**
 * Compile a change event to a parameterized DML statement.
 * insert/update -> upsert by pk; delete -> delete by pk.
 */
export function changeEventToDML(name: string, def: TableDef, ev: ChangeEvent): SqlFragment {
  const pk = def.primaryKey;
  if (ev.op === "delete") {
    return { text: `DELETE FROM ${qualifiedIdent(name)} WHERE ${q(pk)} = $1`, params: [ev.pk] };
  }
  if (!ev.row) throw new Error(`change event op="${ev.op}" requires a row`);
  const columns = Object.keys(def.columns);
  // A *partial* update (the row omits some columns — e.g. a projected list view that never synced
  // `description`) becomes a plain UPDATE of only the provided columns, so the omitted ones aren't
  // clobbered to NULL. A *full* row falls through to the upsert below, preserving Electric's semantics
  // that an "update" carrying an as-yet-unseen pk inserts the row (a partial upsert can't — Postgres
  // checks NOT NULL on the proposed insert tuple before conflict resolution).
  if (ev.op === "update" && !columns.every((c) => c in ev.row!)) {
    const cols = columns.filter((c) => c !== pk && c in ev.row!);
    if (cols.length === 0) return { text: `SELECT 1`, params: [] };
    const set = cols.map((c, i) => `${q(c)} = $${i + 1}`);
    const params: Value[] = cols.map((c) => ev.row![c] ?? null);
    params.push(ev.pk);
    return {
      text: `UPDATE ${qualifiedIdent(name)} SET ${set.join(", ")} WHERE ${q(pk)} = $${cols.length + 1}`,
      params,
    };
  }
  const params: Value[] = columns.map((c) => ev.row![c] ?? null);
  const placeholders = columns.map((_, i) => `$${i + 1}`);
  const updates = columns.filter((c) => c !== pk).map((c) => `${q(c)} = EXCLUDED.${q(c)}`);
  const colList = columns.map(q).join(", ");
  const text =
    updates.length === 0
      ? `INSERT INTO ${qualifiedIdent(name)} (${colList}) VALUES (${placeholders.join(", ")}) ` +
        `ON CONFLICT (${q(pk)}) DO NOTHING`
      : `INSERT INTO ${qualifiedIdent(name)} (${colList}) VALUES (${placeholders.join(", ")}) ` +
        `ON CONFLICT (${q(pk)}) DO UPDATE SET ${updates.join(", ")}`;
  return { text, params };
}

/** `SELECT * ... WHERE <pred>` for a shape, parameterized. */
export function shapeSelectSql(name: string, where?: Predicate): SqlFragment {
  if (!where) return { text: `SELECT * FROM ${qualifiedIdent(name)}`, params: [] };
  const frag = predicateToSql(where, 1);
  return { text: `SELECT * FROM ${qualifiedIdent(name)} WHERE ${frag.text}`, params: frag.params };
}
