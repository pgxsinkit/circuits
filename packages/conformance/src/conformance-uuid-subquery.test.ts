// Subquery shapes over uuid columns, driven through the native surface (`POST /shapes` + raw
// durable-streams reads). The engine coarsens uuid to `text` on introspection, so a predicate's uuid
// literal arrives as a string and the backfill must bind it as `$n::text::uuid` — binding it as-is
// is refused by the driver ("cannot convert String -> uuid"), and comparing `col::text = $n` works
// but keeps the inner select off the column's index. Asserted: correct rows for two literals
// (distinct literals are distinct shapes), a nested/depth-2 subquery that backfills a second table,
// the index-eligibility of the cast form, and a LIVE move-in whose query-back runs over uuid columns.

import type { Predicate, Schema } from '@circuits/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createShape, foldStream, waitFor } from './engine-native.js'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

// The engine coarsens uuid -> text on introspection, so the protocol schema declares these as text;
// the real Postgres columns are uuid (created by the ddl below).
const schema: Schema = {
  tables: {
    projects: { columns: { id: { type: 'text' }, owner_id: { type: 'text' } }, primaryKey: 'id' },
    issues: { columns: { id: { type: 'text' }, project_id: { type: 'text' } }, primaryKey: 'id' },
    comments: { columns: { id: { type: 'text' }, issue_id: { type: 'text' } }, primaryKey: 'id' },
  },
}

const ddl = `
  CREATE TABLE projects (id uuid PRIMARY KEY, owner_id uuid NOT NULL);
  CREATE INDEX projects_owner_idx ON projects (owner_id);
  ALTER TABLE projects REPLICA IDENTITY FULL;
  CREATE TABLE issues (id uuid PRIMARY KEY, project_id uuid NOT NULL REFERENCES projects(id));
  ALTER TABLE issues REPLICA IDENTITY FULL;
  CREATE TABLE comments (id uuid PRIMARY KEY, issue_id uuid NOT NULL REFERENCES issues(id));
  ALTER TABLE comments REPLICA IDENTITY FULL;
`

const uuid = () => crypto.randomUUID()

/** `project_id IN (SELECT id FROM projects WHERE owner_id = <owner>)` */
const issuesOfOwner = (owner: string): Predicate => ({
  col: 'project_id',
  in: { table: 'projects', project: 'id', where: { col: 'owner_id', op: 'eq', value: owner } },
})

/** `issue_id IN (SELECT id FROM issues WHERE project_id IN (SELECT id FROM projects WHERE owner_id = <owner>))` */
const commentsOfOwner = (owner: string): Predicate => ({
  col: 'issue_id',
  in: { table: 'issues', project: 'id', where: issuesOfOwner(owner) },
})

async function shapeKeys(h: Harness, table: string, where: Predicate): Promise<{ shapeId: string; keys: string[] }> {
  const shape = await createShape(h, { table, where })
  return { shapeId: shape.shapeId, keys: [...(await foldStream(shape.streamUrl)).keys()].sort() }
}

// owners u1,u2; projects p1,p3 -> u1, p2 -> u2; issues fan across projects; comments on issues.
const u1 = uuid()
const u2 = uuid()
const p1 = uuid()
const p2 = uuid()
const p3 = uuid()
const i1 = uuid()
const i2 = uuid()
const i3 = uuid()
const i4 = uuid()
const c1 = uuid()
const c2 = uuid()
const c3 = uuid()

describe('conformance: subquery shapes over uuid columns', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema, { ddl })
    for (const [id, owner_id] of [[p1, u1], [p2, u2], [p3, u1]] as const) {
      await applyOp(h, 'projects', { op: 'insert', pk: id, row: { id, owner_id } })
    }
    for (const [id, project_id] of [[i1, p1], [i2, p2], [i3, p3], [i4, p2]] as const) {
      await applyOp(h, 'issues', { op: 'insert', pk: id, row: { id, project_id } })
    }
    // comments c1,c2 on issues of u1's projects (i1,i3); c3 on i2 (u2's).
    for (const [id, issue_id] of [[c1, i1], [c2, i3], [c3, i2]] as const) {
      await applyOp(h, 'comments', { op: 'insert', pk: id, row: { id, issue_id } })
    }
    await drainEngine(h)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('backfills a subquery shape whose literal is a uuid', async () => {
    // owner u1 -> projects p1,p3 -> issues i1,i3.
    const { keys } = await shapeKeys(h, 'issues', issuesOfOwner(u1))
    expect(keys).toEqual([i1, i3].sort())
  })

  it('distinct literals are distinct shapes with distinct rows (no collision)', async () => {
    const a = await shapeKeys(h, 'issues', issuesOfOwner(u1))
    const b = await shapeKeys(h, 'issues', issuesOfOwner(u2))
    expect(b.shapeId).not.toBe(a.shapeId)
    expect(b.keys).toEqual([i2, i4].sort())
  })

  it('a nested/depth-2 subquery backfills a second table', async () => {
    // comments whose issue belongs to a project owned by u1 -> c1 (i1), c2 (i3).
    const { keys } = await shapeKeys(h, 'comments', commentsOfOwner(u1))
    expect(keys).toEqual([c1, c2].sort())
  })

  // The point of casting `$n::text::uuid` (vs `owner_id::text = $n`) is to keep the inner select
  // index-eligible. EXPLAIN the two forms with seqscan disabled: the cast form can use the owner_id
  // btree index; the text-cast form cannot (the `::text` expression doesn't match the index).
  it('the $n::text::uuid cast keeps the inner select on an index scan', async () => {
    const c = new pgpkg.Client({ connectionString: h.pgUrl })
    await c.connect()
    try {
      await c.query('SET enable_seqscan = off')
      const plan = async (sql: string) => {
        const r = await c.query(`EXPLAIN (FORMAT JSON) ${sql}`, [u1])
        return JSON.stringify(r.rows[0]['QUERY PLAN'])
      }
      const cast = await plan('SELECT id FROM projects WHERE owner_id = $1::text::uuid')
      const textCast = await plan('SELECT id FROM projects WHERE owner_id::text = $1')
      expect(cast).toContain('Index') // Index Scan / Bitmap Index Scan — uses projects_owner_idx
      expect(textCast).toContain('Seq Scan') // the ::text expression can't use the btree index
    } finally {
      await c.end()
    }
  })
})

// A LIVE inner change that re-derives outer membership via a Postgres query-back over uuid columns —
// the path that once failed SILENTLY (process_envelope: "backfill select comments: cannot convert
// String -> uuid"), dropping move-in rows while the shape stream stayed clean. Own harness so the
// mutation is isolated from the read-only tests above.
describe('conformance: LIVE subquery move-in over uuid columns', () => {
  let h: Harness
  const uOwner = uuid()
  const uOther = uuid()
  const pl = uuid()
  const il = uuid()
  const cl = uuid()

  beforeAll(async () => {
    h = await bootHarness(schema, { ddl })
    // Pre-shape: project pl owned by uOther (NOT uOwner), issue il on pl, comment cl on il.
    await applyOp(h, 'projects', { op: 'insert', pk: pl, row: { id: pl, owner_id: uOther } })
    await applyOp(h, 'issues', { op: 'insert', pk: il, row: { id: il, project_id: pl } })
    await applyOp(h, 'comments', { op: 'insert', pk: cl, row: { id: cl, issue_id: il } })
    await drainEngine(h)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('re-derives a uuid move-in on a live inner change (no silent drop, no engine error)', async () => {
    // The depth-2 comments shape for uOwner starts empty (pl is owned by uOther).
    const shape = await createShape(h, { table: 'comments', where: commentsOfOwner(uOwner) })
    expect([...(await foldStream(shape.streamUrl)).keys()]).toEqual([])

    const errBefore = h.engineStderr().length

    // LIVE inner change: pl's owner becomes uOwner -> pl enters -> il enters -> the engine re-derives
    // `comments WHERE issue_id = il` (a uuid query-back) -> cl must move in.
    await applyOp(h, 'projects', { op: 'update', pk: pl, row: { id: pl, owner_id: uOwner } })
    await drainEngine(h)

    // (a) the move-in row actually arrives on the shape stream.
    await waitFor(
      async () => (await foldStream(shape.streamUrl)).has(cl),
      'the comment to move in through the live uuid subquery re-derive',
      8000,
    )

    // (b) no engine-side process_envelope failure during the re-derive.
    const errNew = h.engineStderr().slice(errBefore)
    expect(errNew).not.toContain('process_envelope failed')
    expect(errNew).not.toContain('cannot convert')
  }, 60000)
})
