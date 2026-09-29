// Liveness and readiness are different questions, and answering them with one endpoint is how a
// kubelet ends up restarting a pod whose only problem is that nobody has fixed its epoch yet
// (issue #16).
//
//   `GET /health` — LIVENESS. "ok", 200, while the process runs. Nothing else. A kubelet restarts a
//                   pod whose liveness probe fails, so this must never fail for a condition a
//                   restart does not fix.
//   `GET /ready`  — READINESS. 200 `{"status":"active"}` only when the engine can actually serve;
//                   503 with the word that says why otherwise.
//
// The interesting case is `degraded`: with `CIRCUITS_RESET_ON_SLOT_LOSS=false` the engine
// fails closed on a lost slot (ADR-0004) and stays that way until an operator posts `/epoch/reset`.
// Readiness must reflect that (503, so traffic goes elsewhere); liveness must not (200, because
// restarting fixes nothing and would only lose the parked state the operator is about to recover).

import type { Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, type BootOptions, type Harness } from './harness.js'
import { pgQuery, waitFor } from './engine-native.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } },
}

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

async function ready(): Promise<{ code: number; status: string }> {
  const res = await fetch(`${h!.engineUrl}/ready`)
  return { code: res.status, status: ((await res.json()) as { status: string }).status }
}

async function health(): Promise<{ code: number; body: string }> {
  const res = await fetch(`${h!.engineUrl}/health`)
  return { code: res.status, body: await res.text() }
}

/**
 * Destroy the engine's replication slot for real (the same helper `conformance-epoch.test.ts` uses):
 * terminate the walsender holding it, then DROP, retrying until the DROP itself succeeds.
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
      await new Promise((r) => setTimeout(r, 50))
    }
  }
  throw new Error(`could not drop replication slot ${h!.slot}`)
}

describe('readiness vs liveness', () => {
  it('is 200 active on a healthy engine, with liveness agreeing', async () => {
    await boot()
    expect(await ready()).toEqual({ code: 200, status: 'active' })
    expect(await health()).toEqual({ code: 200, body: 'ok' })

    // Readiness must never be cached: an orchestrator polling it has to see the live phase.
    const res = await fetch(`${h!.engineUrl}/ready`)
    expect(res.headers.get('cache-control')).toBe('no-cache, no-store, must-revalidate')
    expect(res.headers.get('content-type')).toBe('application/json')
    await res.text()
  })

  it('reports degraded (503) on a broken epoch while liveness stays ok, and recovers on /epoch/reset', async () => {
    await boot({ engineEnv: { CIRCUITS_RESET_ON_SLOT_LOSS: 'false' } })
    expect(await ready()).toEqual({ code: 200, status: 'active' })

    await destroySlot()

    // The ingestor re-checks the slot before every connection, so the break lands within a backoff.
    await waitFor(async () => (await ready()).status === 'degraded', '/ready to report degraded')
    expect(await ready()).toEqual({ code: 503, status: 'degraded' })

    // Liveness is unmoved: a restart is NOT the fix here (it would lose the parked shapes the
    // operator's reset is about to retire properly), so a kubelet must not be told to restart.
    expect(await health()).toEqual({ code: 200, body: 'ok' })

    const reset = await fetch(`${h!.engineUrl}/epoch/reset`, { method: 'POST' })
    expect(reset.status).toBe(200)
    await reset.text()

    await waitFor(async () => (await ready()).status === 'active', '/ready to recover after /epoch/reset')
    expect(await ready()).toEqual({ code: 200, status: 'active' })
    expect(await health()).toEqual({ code: 200, body: 'ok' })
  })
})
