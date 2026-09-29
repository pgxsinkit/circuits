// The replication slot is bound to a catalog epoch (ADR-0004). Upstream creates the slot at boot if
// it is missing and never looks at it again, so a slot lost to a restore, `max_slot_wal_keep_size`,
// a major upgrade or an operator is silently recreated at the current WAL head — and every shape
// misses the gap with no signal at all.
//
// Here the slot is genuinely destroyed underneath a running (and a stopped) engine, and the engine
// has to notice and end the epoch. Under the default policy it resets itself: every shape retired
// (stream closed, then deleted — ADR-0007), a fresh slot, a new `SlotBound` in the durable catalog.
// Under `CIRCUITS_RESET_ON_SLOT_LOSS=false` it refuses instead — fail-closed with a named
// reason, ingest stopped, and `POST /epoch/reset` as the deliberate human act that recovers it.
//
// What must NEVER happen (and is what these tests are really guarding): a fresh slot at the head,
// shapes still being served, and nobody the wiser.

import type { Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { createShape, foldStream, pgQuery, waitFor } from './engine-native.js'
import { bootHarness, type BootOptions, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
    other: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

/** Matches every row either table ever holds here — so "the shape is gone" is never "the shape is empty". */
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

interface EpochView {
  state: 'ok' | 'broken'
  reason: string | null
  systemIdentifier: string | null
  slot: string | null
  boundAt: string | null
}

async function epoch(): Promise<EpochView> {
  const res = await fetch(`${h!.engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`GET /replication/lsn -> ${res.status}`)
  return ((await res.json()) as { epoch: EpochView }).epoch
}

async function counter(name: string): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  return Number(((await res.json()) as { counters: Record<string, number> }).counters[name] ?? 0)
}

async function status(url: string): Promise<number> {
  return (await fetch(url)).status
}

async function healthStatus(): Promise<{ code: number; status: string }> {
  const res = await fetch(`${h!.engineUrl}/v1/health`)
  return { code: res.status, status: ((await res.json()) as { status: string }).status }
}

async function slotExists(): Promise<boolean> {
  return (await pg('SELECT 1 FROM pg_replication_slots WHERE slot_name = $1', [h!.slot])).length > 0
}

/**
 * Destroy the engine's replication slot for real — the thing `max_slot_wal_keep_size`, a restore
 * from backup or an operator's cleanup script does.
 *
 * Postgres refuses to drop a slot a walsender holds, so the engine's ingestor is terminated first
 * and the pair retried until the DROP itself succeeds. Returning on the successful DROP (rather than
 * on "no slot with this name") is what keeps this deterministic: under the auto policy the engine
 * recreates the slot within moments, and a poll for absence would race that and drop the *new*
 * epoch's slot too.
 */
async function destroySlot(): Promise<void> {
  for (let i = 0; i < 200; i++) {
    await pg(
      'SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL',
      [h!.slot],
    ).catch(() => {})
    try {
      await pg('SELECT pg_drop_replication_slot($1)', [h!.slot])
      return
    } catch {
      // "replication slot is active for PID …" — the walsender has not gone yet. Retry.
      await new Promise((r) => setTimeout(r, 50))
    }
  }
  throw new Error(`could not drop replication slot ${h!.slot}`)
}

/** A shape is retired when its record is gone AND its stream is gone (ADR-0007: closed, then deleted). */
async function expectRetired(shapeId: string, streamUrl: string): Promise<void> {
  await waitFor(async () => (await status(`${h!.engineUrl}/shapes/${shapeId}`)) === 404, `shape ${shapeId} to be dropped`)
  await waitFor(async () => (await status(streamUrl)) === 404, `stream ${streamUrl} to be deleted`)
}

/** Create a shape, land a live write on it through replication, and read it back off the stream. */
async function expectLiveShapeWorks(table: 'items' | 'other', id: number): Promise<void> {
  const shape = await createShape(h!, { table, where: matchAll })
  await pg(`INSERT INTO ${table} (id, n) VALUES ($1, $2)`, [id, id])
  await drainEngine(h!)
  await waitFor(
    async () => (await foldStream(shape.streamUrl)).has(String(id)),
    `insert ${id} into ${table} to reach a shape created in the new epoch`,
  )
}

describe('losing the replication slot ends the epoch (ADR-0004)', () => {
  it('auto-reset: a slot dropped under a running engine retires every shape and binds a new epoch', async () => {
    await boot()
    const before = await epoch()
    expect(before.state, 'a healthy engine reports its epoch as ok').toBe('ok')
    expect(before.slot).toBe(h!.slot)
    expect(before.systemIdentifier, 'the cluster identity is recorded, not left null').toBeTruthy()
    expect(before.boundAt).toBeTruthy()

    // Two shapes on two different tables: the epoch is whole-engine, unlike schema drift, so BOTH
    // must go — this is the one case where per-table granularity is wrong.
    await pg('INSERT INTO items (id, n) VALUES (1, 1)')
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)
    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })
    expect(await foldStream(items.streamUrl)).toHaveProperty('size', 1)
    expect(await foldStream(other.streamUrl)).toHaveProperty('size', 1)

    await destroySlot()

    // The ingestor's reconnect finds no slot, so the epoch is over: every shape is retired.
    await expectRetired(items.shapeId, items.streamUrl)
    await expectRetired(other.shapeId, other.streamUrl)

    // A new epoch on a fresh slot of the same name — the StatsD slot gauges keep working precisely
    // because the name does not change. The BINDING is the barrier, not the slot row: the row is
    // visible while Postgres is still finding the slot's consistent point, and the engine records the
    // epoch after that. Waiting on the row would race the rebind and read a stale `boundAt`.
    await waitFor(async () => (await epoch()).boundAt !== before.boundAt, 'a new epoch to be bound')
    expect(await slotExists()).toBe(true)
    const after = await epoch()
    expect(after.state).toBe('ok')
    expect(after.reason).toBeNull()
    expect(after.slot).toBe(h!.slot)
    expect(after.systemIdentifier, 'same cluster, new epoch').toBe(before.systemIdentifier)
    expect(after.boundAt, 'the new epoch has its own binding time').not.toBe(before.boundAt)
    expect(await counter('epoch_resets_total')).toBeGreaterThanOrEqual(1)
    expect(await counter('epoch_breaks_total')).toBeGreaterThanOrEqual(1)

    // And the engine is genuinely ingesting again on the new slot.
    expect((await healthStatus()).status).toBe('active')
    await expectLiveShapeWorks('items', 2)
  }, 90000)

  it('refuse: with RESET_ON_SLOT_LOSS=false the engine fails closed until an operator resets it', async () => {
    await boot({ engineEnv: { CIRCUITS_RESET_ON_SLOT_LOSS: 'false' } })
    await pg('INSERT INTO items (id, n) VALUES (1, 1)')
    await drainEngine(h!)
    const items = await createShape(h!, { table: 'items', where: matchAll })
    expect(await foldStream(items.streamUrl)).toHaveProperty('size', 1)

    await destroySlot()

    // Fail closed with a NAMED reason: the health word is the fleet's (unchanged) `degraded`, and
    // `/replication/lsn` is where an operator reads what actually happened.
    await waitFor(async () => (await epoch()).state === 'broken', 'the engine to declare its epoch broken')
    const broken = await epoch()
    expect(broken.reason).toBe('slot_lost')
    const health = await healthStatus()
    expect(health.code).toBe(503)
    expect(health.status).toBe('degraded')
    // Every shape route refuses rather than serving rows over an epoch the engine cannot vouch for.
    const create = await fetch(`${h!.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', where: matchAll }),
    })
    expect(create.status).toBe(503)
    // Nothing was destroyed while refusing: recovery is the operator's call, not the engine's.
    expect(await status(items.streamUrl)).toBe(200)

    const reset = await fetch(`${h!.engineUrl}/epoch/reset`, { method: 'POST' })
    expect(reset.status).toBe(200)

    // The reset is the same one the auto policy runs: shapes retired, new epoch, ingest resumed.
    await expectRetired(items.shapeId, items.streamUrl)
    const after = await epoch()
    expect(after.state).toBe('ok')
    expect(after.reason).toBeNull()
    expect(after.boundAt).not.toBe(broken.boundAt)
    expect(await slotExists()).toBe(true)
    await waitFor(async () => (await healthStatus()).status === 'active', 'health to recover')
    await expectLiveShapeWorks('items', 2)
  }, 90000)

  it('a slot lost while the engine is DOWN is caught at boot, before anything is resumed', async () => {
    await boot()
    await pg('INSERT INTO items (id, n) VALUES (1, 1)')
    await drainEngine(h!)
    const items = await createShape(h!, { table: 'items', where: matchAll })
    const before = await epoch()

    // The window nothing on the live path can see: the engine is not running when the slot goes.
    await h!.restartEngine(async () => {
      await destroySlot()
    })

    // The boot must not restore the shape — its stream is short an unknown span of changes and no
    // replay can fill it.
    expect(await status(`${h!.engineUrl}/shapes/${items.shapeId}`)).toBe(404)
    expect(await status(items.streamUrl)).toBe(404)
    const after = await epoch()
    expect(after.state).toBe('ok')
    expect(after.slot).toBe(h!.slot)
    expect(after.boundAt, 'boot bound a new epoch rather than adopting the old record').not.toBe(before.boundAt)
    expect(await counter('epoch_resets_total')).toBeGreaterThanOrEqual(1)
    await expectLiveShapeWorks('items', 2)
  }, 90000)

  it('first boot adopts a slot an operator created, and records the binding', async () => {
    // Empty catalog, slot already there: nothing claims an epoch, so this is a genuine first boot
    // and the pre-existing slot is adopted rather than treated as somebody else's.
    await boot({
      beforeEngine: async ({ pgUrl, slot }) => {
        const pgpkg = (await import('pg')).default
        const c = new pgpkg.Client({ connectionString: pgUrl })
        await c.connect()
        try {
          await c.query('SELECT pg_create_logical_replication_slot($1, $2)', [slot, 'pgoutput'])
        } finally {
          await c.end().catch(() => {})
        }
      },
    })

    const e = await epoch()
    expect(e.state).toBe('ok')
    expect(e.slot).toBe(h!.slot)
    expect(e.systemIdentifier).toBeTruthy()
    expect(e.boundAt).toBeTruthy()
    // Adopted, not reset: nothing was retired and no new epoch was needed.
    expect(await counter('epoch_resets_total')).toBe(0)
    expect(await counter('epoch_breaks_total')).toBe(0)
    // …and the adopted slot is the one being streamed.
    await expectLiveShapeWorks('items', 1)
  }, 90000)
})
