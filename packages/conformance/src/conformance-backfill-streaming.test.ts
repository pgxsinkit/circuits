// A backfill is STREAMED, never materialised (issue #13). The engine reads the snapshot over a
// `query_raw` cursor and appends it to the pending shape stream in chunks bounded by
// `CIRCUITS_BACKFILL_APPEND_BYTES`, so its memory is one chunk for a table of any size —
// where before, creating a shape over a large table read every matching row into a `Vec<Row>` first.
//
// Asserted end to end against the live engine, real Postgres and the real durable-streams server:
//
//   1. with a 64 KiB budget, a ~20k-row table's shape really is written in MANY appends — counted
//      by `backfill_chunked_appends_total` on `/metrics`, against a lower bound derived from the
//      data volume, not just "more than one" — and the folded stream equals Postgres exactly, so
//      chunking is invisible in the result. (The durable-streams offset is `{seq}_{byte}` over
//      MESSAGES, not over appends, so the append boundaries are not recoverable from the offsets
//      afterwards; the counter is the direct evidence and this is what it says.)
//   2. a `/v1/shape` snapshot over the same table returns every row;
//   3. `CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS=1` fails THAT create with a clear error and
//      leaves the ENGINE healthy — proved against the engine, not against Postgres: `/ready` is
//      still 200, the ingestor and sequencer still carry a write end to end (`drainEngine`), and a
//      shape created AFTER the failure still receives changes. (The guard is process-wide, so the
//      shape used for that has to be one that takes no backfill — a `changesOnly` feed — or it
//      would trip the same 1 ms timeout and prove nothing.);
//   4. unset (the default), the same create works.

import type { Row, Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, type BootOptions, drainEngine, type Harness } from './harness.js'
import { foldStream, pgQuery, waitFor } from './engine-native.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, n: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

/** Enough rows that a 64 KiB append budget is crossed many times over. */
const ROWS = 20000

const matchAll = { col: 'n', op: 'gte', value: 0 }

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

async function boot(opts: BootOptions = {}): Promise<Harness> {
  h = await bootHarness(schema, opts)
  return h
}

const pg = (sql: string, params: unknown[] = []) => pgQuery(h!, sql, params)

async function seedRows(): Promise<void> {
  await pg(`INSERT INTO items (id, n, label) SELECT g, g, 'label-for-row-' || g FROM generate_series(1, $1) AS g`, [
    ROWS,
  ])
}

interface ShapeResp {
  shapeId: string
  streamPath: string
  streamUrl: string
}

async function createShapeRaw(body: unknown): Promise<{ ok: boolean; status: number; text: string }> {
  const res = await fetch(`${h!.engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { ok: res.ok, status: res.status, text: await res.text() }
}

async function createShape(body: unknown): Promise<ShapeResp> {
  const r = await createShapeRaw(body)
  if (!r.ok) throw new Error(`POST /shapes -> ${r.status} ${r.text}`)
  return JSON.parse(r.text) as ShapeResp
}

async function counter(name: string): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  return Number(((await res.json()) as { counters: Record<string, number> }).counters[name] ?? 0)
}

async function readyStatus(): Promise<{ code: number; status: string }> {
  const res = await fetch(`${h!.engineUrl}/ready`)
  return { code: res.status, status: ((await res.json()) as { status: string }).status }
}

describe('streamed backfills', () => {
  it('writes a large snapshot in several appends and still equals Postgres', async () => {
    await boot({ engineEnv: { CIRCUITS_BACKFILL_APPEND_BYTES: '65536' } })
    await seedRows()

    // The stream's offset must advance in STEPS while the create runs — proof that the snapshot
    // reached storage progressively rather than in one body at the end.
    const before = await counter('backfill_chunked_appends_total')
    const shape = await createShape({ table: 'items', where: matchAll })
    const after = await counter('backfill_chunked_appends_total')
    // A lower bound from the data, not a token "> 1": each row's envelope carries its key, its id,
    // its n and a `label-for-row-N` string — comfortably over 60 bytes serialized — so 20k rows is
    // >1.2 MB and a 64 KiB budget cannot cover it in fewer than ~18 appends. Anything much below
    // that would mean the budget is not really bounding the bodies.
    expect(
      after - before,
      'a 20k-row snapshot under a 64 KiB budget must take many appends, not one',
    ).toBeGreaterThan(10)

    const rows = await foldStream(shape.streamUrl)
    expect(rows.size).toBe(ROWS)
    const oracle = (await pg('SELECT id, n, label FROM items ORDER BY id')) as Row[]
    expect(oracle.length).toBe(ROWS)
    for (const r of oracle) {
      expect(rows.get(String(r.id))).toMatchObject({ id: r.id, n: r.n, label: r.label })
    }

    // Chunking must not have cost the engine its health.
    expect(await readyStatus()).toEqual({ code: 200, status: 'active' })

    // The same counter must be on the Prometheus exposition, not only the JSON one: /metrics/prometheus
    // used to carry the memory/cardinality gauges alone, which made it half a scrape target.
    const prom = await (await fetch(`${h!.engineUrl}/metrics/prometheus`)).text()
    const value = (name: string) => Number(prom.match(new RegExp(`^${name}\\{[^}]*\\} (\\S+)`, 'm'))?.[1] ?? NaN)
    expect(value('engine_backfill_chunked_appends_total')).toBe(after)
    // ...along with the ops gauges this slice added.
    expect(value('engine_sequencer_held_run')).toBe(0)
    expect(value('engine_shutdown_in_progress')).toBe(0)
    expect(prom).toMatch(/^engine_replication_slot_retained_wal_bytes\{/m)
    expect(prom).toMatch(/^engine_replication_confirmed_flush_lag_bytes\{/m)
    expect(prom).toMatch(/^engine_replication_slot_active\{/m)
  })

  it('serves every row through the /v1/shape snapshot over the same table', async () => {
    await boot({ engineEnv: { CIRCUITS_BACKFILL_APPEND_BYTES: '65536' } })
    await seedRows()

    const res = await fetch(`${h!.engineUrl}/v1/shape?table=items&offset=-1`)
    expect(res.status).toBe(200)
    const messages = (await res.json()) as Array<{ key?: string; headers: { operation?: string; control?: string } }>
    const inserts = messages.filter((m) => m.headers.operation === 'insert')
    expect(inserts.length).toBe(ROWS)
    expect(new Set(inserts.map((m) => m.key)).size).toBe(ROWS)
  })

  it('a 1ms statement timeout fails that create with a clear error and leaves the engine healthy', async () => {
    await boot({
      engineEnv: {
        CIRCUITS_BACKFILL_APPEND_BYTES: '65536',
        CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS: '1',
      },
    })
    await seedRows()

    const r = await createShapeRaw({ table: 'items', where: matchAll })
    expect(r.ok, `expected the create to fail under a 1ms statement timeout, got ${r.status} ${r.text}`).toBe(false)
    // Postgres's own words, carried through: "canceling statement due to statement timeout".
    expect(r.text.toLowerCase()).toContain('statement timeout')

    // Nothing was retired and nothing was purged — and that has to be shown against the ENGINE.
    // (Asking Postgres whether a row landed says nothing at all about the engine: Postgres would
    // answer that with the engine dead.)
    //
    // 1. it is still ready;
    expect(await readyStatus()).toEqual({ code: 200, status: 'active' })
    // 2. the ingest path still carries a write end to end — `drainEngine` bumps the sentinel row
    //    and waits for the ENGINE to report having processed it, so it exercises the walsender, the
    //    ingestor, the change log and the sequencer;
    await drainEngine(h!)
    // 3. and a shape created after the failure still receives changes. It has to be a `changesOnly`
    //    feed: the 1 ms guard is process-wide, so anything that takes a backfill would trip the
    //    same timeout, and the point here is the engine's health, not the guard a second time.
    const live = await createShape({ table: 'items', where: matchAll, changesOnly: true })
    await pg('INSERT INTO items (id, n, label) VALUES ($1, 1, $2)', [ROWS + 1, 'after-the-timeout'])
    await waitFor(
      async () => (await foldStream(live.streamUrl)).has(String(ROWS + 1)),
      'a write after a timed-out backfill to reach a shape the engine created afterwards',
    )
    expect((await foldStream(live.streamUrl)).get(String(ROWS + 1))).toMatchObject({
      label: 'after-the-timeout',
    })
  })

  it('the same create works with the guard unset (the default)', async () => {
    await boot({ engineEnv: { CIRCUITS_BACKFILL_APPEND_BYTES: '65536' } })
    await seedRows()
    const shape = await createShape({ table: 'items', where: matchAll })
    expect((await foldStream(shape.streamUrl)).size).toBe(ROWS)
  })
})
