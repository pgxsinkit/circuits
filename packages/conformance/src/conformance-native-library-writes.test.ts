import { spawn, type ChildProcess } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createCore, type ElectricCore, type ShapeHandle } from '@circuits/api'
import { DurableStreamTestServer } from '@circuits/ds-rust'
import type { Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { foldStream, waitFor } from './engine-native.js'
import { buildEngine } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

const root = join(dirname(fileURLToPath(import.meta.url)), '../../..')

async function spawnLibraryEngine(
  dsUrl: string,
  extraEnv: Record<string, string> = {},
): Promise<{ url: string; proc: ChildProcess }> {
  buildEngine()
  const proc = spawn(join(root, 'target/debug/circuits-engine'), [], {
    env: {
      ...process.env,
      DATABASE_URL: '',
      CIRCUITS_PG_URL: '',
      CIRCUITS_DS_URL: dsUrl,
      CIRCUITS_BIND: '127.0.0.1:0',
      CIRCUITS_TRACE: '0',
      CIRCUITS_LOG: 'warn',
      ...extraEnv,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('library-mode engine did not start')), 20_000)
    let stdout = ''
    proc.stdout!.on('data', (chunk: Buffer) => {
      stdout += chunk.toString()
      const match = stdout.match(/ENGINE_LISTENING (\S+)/)
      if (match) {
        clearTimeout(timer)
        resolve(match[1]!)
      }
    })
    proc.once('exit', (code) => {
      clearTimeout(timer)
      reject(new Error(`library-mode engine exited early with code ${code}`))
    })
  })
  return { url, proc }
}

/** `GET /shapes/{id}` — a pure metadata lookup, deliberately NOT a retention touch. */
async function shapeState(engineUrl: string, id: string): Promise<string | undefined> {
  const res = await fetch(`${engineUrl}/shapes/${encodeURIComponent(id)}`)
  if (!res.ok) return undefined
  return ((await res.json()) as { state?: string }).state
}

describe('native library-mode writes', () => {
  let ds: DurableStreamTestServer | undefined
  let engine: ChildProcess | undefined
  let core: ElectricCore | undefined
  let shape: ShapeHandle | undefined

  afterEach(async () => {
    if (shape) await core?.dropShape(shape.shapeId).catch(() => {})
    engine?.kill('SIGKILL')
    await ds?.stop().catch(() => {})
  })

  it('removes a deleted row from a native materialized shape', async () => {
    ds = new DurableStreamTestServer({ port: 0 })
    const dsUrl = await ds.start()
    const started = await spawnLibraryEngine(dsUrl)
    engine = started.proc
    core = createCore({ dsUrl, engineUrl: started.url })
    await core.defineSchema(schema)

    shape = await core.createShape({ table: 'items' })
    await core.write({ table: 'items', op: 'insert', pk: 1, row: { id: 1, active: true } })
    await waitFor(async () => (await foldStream(shape!.streamUrl)).has('1'), 'initial native insert')

    await core.write({ table: 'items', op: 'delete', pk: 1 })
    // This later write is observed through the same public shape stream, proving the sequencer has
    // already processed the preceding delete before we inspect the materialized result.
    await core.write({ table: 'items', op: 'insert', pk: 2, row: { id: 2, active: true } })
    await waitFor(async () => (await foldStream(shape!.streamUrl)).has('2'), 'barrier native insert')

    expect([...((await foldStream(shape.streamUrl)).keys())].sort()).toEqual(['2'])
  })

  // The live path is covered above by the sequencer's per-key view. The two below cover the paths
  // that view cannot serve directly: replay out of dormancy (which the engine answers with ABSOLUTE
  // emission — upsert-if-it-matches-now, else delete-by-key) and a restart (which rebuilds the view).
  it('retracts a row deleted while the shape was dormant', async () => {
    ds = new DurableStreamTestServer({ port: 0 })
    const dsUrl = await ds.start()
    const started = await spawnLibraryEngine(dsUrl, {
      CIRCUITS_SHAPE_IDLE_SECS: '1',
      CIRCUITS_SHAPE_DORMANT_TTL_SECS: '60',
      CIRCUITS_RETENTION_SWEEP_SECS: '1',
    })
    engine = started.proc
    core = createCore({ dsUrl, engineUrl: started.url })
    await core.defineSchema(schema)

    shape = await core.createShape({ table: 'items' })
    await core.write({ table: 'items', op: 'insert', pk: 1, row: { id: 1, active: true } })
    await core.write({ table: 'items', op: 'insert', pk: 2, row: { id: 2, active: true } })
    await waitFor(async () => (await foldStream(shape!.streamUrl)).has('2'), 'initial native inserts')

    // Release the only subscriber and let the idle sweeper park the shape: its engine state is
    // dropped, the stream is retained, and nothing reaches it until it is touched again.
    await core.dropShape(shape.shapeId)
    await waitFor(
      async () => (await shapeState(started.url, shape!.shapeId)) === 'dormant',
      'shape to go dormant',
    )

    // Mutate WHILE DORMANT. These changes exist only on the change log; the shape learns about
    // them by replaying it, and the replay reads raw envelopes with no before-image on them.
    await core.write({ table: 'items', op: 'delete', pk: 1 })
    await core.write({ table: 'items', op: 'insert', pk: 3, row: { id: 3, active: true } })

    // Rejoin: the same retained shape + stream, reactivated by change-log replay before the create
    // returns. The delete it slept through must have retracted, not lingered as a ghost row.
    const rejoined = await core.createShape({ table: 'items' })
    shape = rejoined
    expect(rejoined.shapeId).toBe(shape.shapeId)
    await waitFor(async () => (await foldStream(rejoined.streamUrl)).has('3'), 'dormant-window insert replayed')
    expect([...((await foldStream(rejoined.streamUrl)).keys())].sort()).toEqual(['2', '3'])
  })

  // NOT a regression guard for absolute emission — it passes with that rule disabled, and the
  // reason is the property this test exists to pin: library mode has no catalog checkpoint to
  // resume from, so a fresh process replays the change log from its ORIGIN and rebuilds the per-key
  // view exactly before the first new change lands. If anyone ever gives library mode a resume
  // point, this goes red and the ghost row comes back.
  it('retracts a row written before a restart', async () => {
    ds = new DurableStreamTestServer({ port: 0 })
    const dsUrl = await ds.start()
    const first = await spawnLibraryEngine(dsUrl)
    engine = first.proc
    core = createCore({ dsUrl, engineUrl: first.url })
    await core.defineSchema(schema)

    const before = await core.createShape({ table: 'items' })
    shape = before
    await core.write({ table: 'items', op: 'insert', pk: 1, row: { id: 1, active: true } })
    await waitFor(async () => (await foldStream(before.streamUrl)).has('1'), 'pre-restart insert')

    // Crash and boot a fresh process against the same durable streams. The new sequencer's per-key
    // view starts empty, so the delete below can carry no before-image from it.
    engine.kill('SIGKILL')
    const second = await spawnLibraryEngine(dsUrl)
    engine = second.proc
    core = createCore({ dsUrl, engineUrl: second.url })
    await core.defineSchema(schema)
    const after = await core.createShape({ table: 'items' })
    shape = after

    // Library mode has no durable shape catalog (the restore is part of the Postgres boot), so the
    // fresh process holds no engine state for the old shape — but it hands out ids from a counter
    // that restarts too, so the re-created shape lands on the SAME retained stream, which still
    // carries the pre-restart row. That is precisely the ghost-row setup: engine state that has
    // forgotten a row, and a stream that has not.
    expect(after.shapeId).toBe(before.shapeId)
    expect(after.streamPath).toBe(before.streamPath)
    expect([...((await foldStream(after.streamUrl)).keys())]).toContain('1')

    await core.write({ table: 'items', op: 'delete', pk: 1 })
    await core.write({ table: 'items', op: 'insert', pk: 2, row: { id: 2, active: true } })
    await waitFor(async () => (await foldStream(after.streamUrl)).has('2'), 'barrier native insert')

    // The delete of the pre-restart key is served absolutely (the view cannot supply an old row),
    // and the shape ends up with exactly the live set — no ghost, no resurrection.
    expect([...((await foldStream(after.streamUrl)).keys())].sort()).toEqual(['2'])
  })
})
