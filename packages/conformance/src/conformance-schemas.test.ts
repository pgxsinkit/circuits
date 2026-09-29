// Table identity is (schema, name) — ADR-0002. The same table NAME in two Postgres schemas is two
// tables, end to end: two introspections, two replication relations, two shapes, two spellings on the
// wire. Bare names remain accepted at ingress as `public.<name>` sugar, and resolve to the SAME
// identity as the explicit form (so `items` and `public.items` can never become two separately
// maintained shapes).
//
// Everything here is driven through the NATIVE surface (`POST /shapes` + raw durable-streams reads),
// the boundary a table name crosses.

import type { Schema, StreamEnvelope } from '@circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createShape, foldStream, pgQuery, waitFor } from './engine-native.js'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

/** The same table name, the same columns, in two different schemas. */
const columns = { id: { type: 'int' as const }, name: { type: 'text' as const } }
const schema: Schema = {
  tables: {
    'public.items': { columns, primaryKey: 'id' },
    'other.items': { columns, primaryKey: 'id' },
  },
}

/** Every envelope on a shape stream, in order (`foldStream` folds away the `type` we assert on). */
async function readEnvelopes(streamUrl: string): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = []
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    out.push(...(body ? (JSON.parse(body) as StreamEnvelope[]) : []))
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (!next || next === offset) break
    offset = next
    if (upToDate) break
  }
  return out
}

const sortedKeys = (rows: Map<string, unknown>) => [...rows.keys()].sort((a, b) => Number(a) - Number(b))

describe('conformance: two schemas, one table name', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
    // Seed both tables with disjoint ids so a cross-schema leak is unmistakable.
    for (const id of [1, 2]) {
      await applyOp(h, 'public.items', { op: 'insert', pk: id, row: { id, name: `pub-${id}` } })
    }
    for (const id of [11, 12]) {
      await applyOp(h, 'other.items', { op: 'insert', pk: id, row: { id, name: `oth-${id}` } })
    }
    await drainEngine(h)
  }, 90000)
  afterAll(async () => {
    await h?.shutdown()
  })

  // Boot sanity: both relations really exist in their own schemas (the oracle's createPgTables had
  // to CREATE SCHEMA for the non-public one) and both were introspected by the engine.
  it('introspects both schemas from CIRCUITS_PG_TABLES', async () => {
    const rows = await pgQuery(
      h,
      `select table_schema from information_schema.tables
        where table_name = 'items' order by table_schema`,
    )
    expect(rows.map((r) => r.table_schema)).toEqual(['other', 'public'])
    for (const t of ['public.items', 'other.items']) {
      const res = await fetch(`${h.engineUrl}/table/${encodeURIComponent(t)}/schema`)
      expect(res.status, t).toBe(200)
      expect(((await res.json()) as { table: string }).table, t).toBe(t)
    }
  }, 60000)

  // (a) A shape on `other.items` sees ONLY that schema's rows, and its envelopes spell the table
  // qualified — the canonical `schema.name`, never the bare name.
  it('serves only the addressed schema, with a qualified envelope type', async () => {
    const other = await createShape(h, { table: 'other.items' })
    await drainEngine(h)
    const rows = await foldStream(other.streamUrl)
    expect(sortedKeys(rows)).toEqual(['11', '12'])
    const envs = await readEnvelopes(other.streamUrl)
    expect(envs.length).toBeGreaterThan(0)
    expect([...new Set(envs.map((e) => e.type))]).toEqual(['other.items'])
  }, 60000)

  // (b) The bare-name sugar resolves to the SAME identity, so it joins the SAME shared shape —
  // `items` and `public.items` are one table, not two separately maintained ones.
  it('shares one shape between the bare name and its explicit public form', async () => {
    const explicit = await createShape(h, { table: 'public.items' })
    const bare = await createShape(h, { table: 'items' })
    expect(bare.shapeId).toBe(explicit.shapeId)
    expect(bare.streamPath).toBe(explicit.streamPath)
    // ...and a same-named table in another schema is a DIFFERENT shape.
    const other = await createShape(h, { table: 'other.items' })
    expect(other.shapeId).not.toBe(explicit.shapeId)

    const rows = await foldStream(explicit.streamUrl)
    expect(sortedKeys(rows)).toEqual(['1', '2'])
  }, 60000)

  // (c) Replication keys by (namespace, relname): an insert into one schema reaches that schema's
  // shape and nothing else, even though the relation NAME is identical.
  it('routes replicated changes to the shape of the writing schema only', async () => {
    const pub = await createShape(h, { table: 'public.items' })
    const other = await createShape(h, { table: 'other.items' })

    await applyOp(h, 'public.items', { op: 'insert', pk: 3, row: { id: 3, name: 'pub-3' } })
    await applyOp(h, 'other.items', { op: 'insert', pk: 13, row: { id: 13, name: 'oth-13' } })
    await drainEngine(h)

    await waitFor(async () => (await foldStream(pub.streamUrl)).has('3'), 'public.items insert to land')
    await waitFor(async () => (await foldStream(other.streamUrl)).has('13'), 'other.items insert to land')

    expect(sortedKeys(await foldStream(pub.streamUrl))).toEqual(['1', '2', '3'])
    expect(sortedKeys(await foldStream(other.streamUrl))).toEqual(['11', '12', '13'])

    // A delete is just as schema-local.
    await applyOp(h, 'other.items', { op: 'delete', pk: 13 })
    await drainEngine(h)
    await waitFor(async () => !(await foldStream(other.streamUrl)).has('13'), 'other.items delete to land')
    expect(sortedKeys(await foldStream(pub.streamUrl))).toEqual(['1', '2', '3'])
  }, 60000)

  // (d) A qualified reference is RESOLVED, never stripped: `private.items` is `private.items`, and
  // since that table is not served the create is refused — not silently answered with
  // `public.items`' rows, which would be a wrong-rows disclosure. ((a) and (b) cover the qualified
  // and bare forms of the tables that ARE served.)
  it('refuses a table in a schema it does not serve instead of answering from public', async () => {
    const res = await fetch(`${h.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'private.items' }),
    })
    expect(res.ok).toBe(false)
    expect(await res.text()).toContain('private.items')
  }, 60000)

  // (e) A reference that is neither `schema.name` nor a bare name is a client error at the boundary,
  // never a lookup that quietly resolves to something else.
  it('rejects a malformed table reference with a 4xx', async () => {
    const res = await fetch(`${h.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'a.b.c' }),
    })
    expect(res.status).toBeGreaterThanOrEqual(400)
    expect(res.status).toBeLessThan(500)
    expect(await res.text()).toMatch(/table/i)
  }, 60000)
})
