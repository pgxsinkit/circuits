import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { Schema } from '@circuits/protocol'

import { bootHarness, drainEngine, type Harness } from './harness.js'
import { createShape, lockTable, pgQuery, tableLockWaiters, waitFor } from './engine-native.js'

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
    child2: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
  },
}

const activeParents = { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } as const

async function readyStatus(h: Harness): Promise<string> {
  return ((await (await fetch(`${h.engineUrl}/ready`)).json()) as { status: string }).status
}

async function streamStatus(streamUrl: string): Promise<number> {
  return (await fetch(`${streamUrl}?offset=-1`)).status
}

describe('native: degradation closes every membership stream, including an in-flight create', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
    await pgQuery(h, 'INSERT INTO parent (id, active) VALUES (1, true)')
    await pgQuery(h, 'INSERT INTO child (id, parent_id) VALUES (1, 1)')
    await drainEngine(h)
  }, 60000)
  afterAll(async () => await h?.shutdown())

  it('a create already in progress cannot return a live handle after degradation', async () => {
    const live = await createShape(h, { table: 'child', where: { col: 'parent_id', in: activeParents } })
    await drainEngine(h)
    expect(await streamStatus(live.streamUrl)).not.toBe(404)

    // Start a second shape before degradation and hold its outer backfill after its snapshot is fixed.
    // Its outcome is settled into a value here so the parked request is never an unhandled rejection:
    // a refusal is one of the two answers this test accepts (see the assertion at the end).
    const createLock = await lockTable(h, 'child2')
    const definition = { table: 'child2', where: { col: 'parent_id', in: activeParents } }
    const creating = createShape(h, definition).then(
      (rec) => ({ ok: true as const, rec }),
      (err: Error) => ({ ok: false as const, err }),
    )
    await waitFor(async () => (await tableLockWaiters(h, 'child2')).length > 0, 'the second create to block')

    // A normal identical request joins the in-flight creator and waits on its public share result.
    // Start it before the fault schedule so it passes the same active-health gate as the creator.
    const joining = createShape(h, definition).then(
      (rec) => ({ ok: true as const, rec }),
      (err: Error) => ({ ok: false as const, err }),
    )

    const queryBackLock = await lockTable(h, 'child')
    try {
      await pgQuery(h, 'UPDATE parent SET active = false WHERE id = 1')

      // Exhaust the production retry schedule through ordinary Postgres connection failures.
      const killed = new Set<number>()
      for (let attempt = 1; attempt <= 5; attempt++) {
        let pid = 0
        await waitFor(async () => {
          pid = (await tableLockWaiters(h, 'child')).find((candidate) => !killed.has(candidate)) ?? 0
          return pid !== 0
        }, `query-back retry ${attempt} to block`)
        killed.add(pid)
        await pgQuery(h, 'SELECT pg_terminate_backend($1)', [pid])
      }

      await waitFor(async () => (await readyStatus(h)) === 'degraded', 'the engine to report degraded')
      await waitFor(async () => (await streamStatus(live.streamUrl)) === 404, 'the live stream to be reaped')
    } finally {
      await queryBackLock.release()
    }

    // The second create started while health was active. Keep it parked until the degradation
    // reaper has run, then let its empty backfill finish.
    await createLock.release()
    const created = await creating
    const joined = await joining

    // Degradation and stream reaping both completed while this create remained parked. It therefore
    // overlapped the mark and must refuse with the typed public error, not return a handle.
    expect(created.ok).toBe(false)
    if (!created.ok) {
      expect(created.err.message).toContain('POST /shapes -> 503')
      expect(created.err.message).toContain('degraded: subquery membership effects were lost')
    }

    // The joiner overlapped the same degradation and must receive the same typed refusal. A generic
    // initialization failure maps to 500 and makes identical normal-client requests disagree.
    expect(joined.ok).toBe(false)
    if (!joined.ok) {
      expect(joined.err.message).toContain('POST /shapes -> 503')
      expect(joined.err.message).toContain('degraded: subquery membership effects were lost')
    }
  }, 60000)
})
