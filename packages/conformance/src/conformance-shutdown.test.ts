// A `SIGTERM` is not a kill (issue #16). The engine drains: `GET /ready` turns 503 first so a load
// balancer stops routing to it, the accept loop closes, every parked long-poll comes back at once,
// the ingestor finishes the commit it is APPENDING, the sequencer finishes its batch and writes a
// final checkpoint, and the process exits 0 — all inside a bounded grace period.
//
// What is asserted here, end to end against the live engine, real Postgres and the real
// durable-streams server:
//
//   1. the drain finishes in seconds, without waiting for a client long-polling durable-streams
//      directly — the engine is not in that request's path, and an engine that waited for its
//      readers would sit out a pod's whole `terminationGracePeriodSeconds`;
//   2. `/ready` answers 503 during the drain while `/health` stays 200 — the liveness/readiness
//      split doing its job;
//   3. the process exits 0 inside the grace period;
//   4. after a restart the shape is STILL maintained (a new insert arrives) and holds no duplicates:
//      streams are never closed or retired on shutdown, and the final checkpoint means the restart
//      does not replay a storm of already-applied changes;
//   5. a `SIGTERM` landing while a large transaction is mid-commit converges after the restart with
//      no duplicates either — the ingestor either finished the chunked append (and acknowledged) or
//      finished nothing (and Postgres re-delivers).

import type { Row, Schema, StreamEnvelope } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { bootHarness, type BootOptions, drainEngine, type Harness } from './harness.js'
import { createShape, foldStream, pgQuery, waitFor } from './engine-native.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, n: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

/** Matches every row the table ever holds here, so "the shape is gone" is never "the shape is empty". */
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

async function readyStatus(url: string): Promise<{ code: number; status: string }> {
  const res = await fetch(`${url}/ready`)
  return { code: res.status, status: ((await res.json()) as { status: string }).status }
}

/** Every envelope on a stream, in order — so a duplicate is visible as a repeated key, not folded away. */
async function streamEnvelopes(streamUrl: string): Promise<StreamEnvelope[]> {
  const all: StreamEnvelope[] = []
  let offset = '-1'
  for (let i = 0; i < 500; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    for (const env of body ? (JSON.parse(body) as StreamEnvelope[]) : []) all.push(env)
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (!next || next === offset) break
    offset = next
    if (upToDate) break
  }
  return all
}

/**
 * Every key on the stream appears EXACTLY once.
 *
 * `foldStream` cannot see a duplicate — it is a Map, so a re-appended row overwrites itself and the
 * fold looks perfect. Duplicates are the whole risk a restart carries (a re-delivered commit, a
 * replayed checkpoint window), so they have to be counted on the raw envelope list. Every row in
 * these tests is written exactly once, so "exactly one envelope per key" is the correct bar; a
 * second envelope for a key means the shutdown cost a client a duplicate.
 */
async function expectEachKeyExactlyOnce(streamUrl: string, expectedKeys: number): Promise<void> {
  const counts = new Map<string, number>()
  for (const e of await streamEnvelopes(streamUrl)) counts.set(e.key, (counts.get(e.key) ?? 0) + 1)
  const repeated = [...counts].filter(([, n]) => n > 1)
  expect(
    repeated.slice(0, 10),
    `${repeated.length} key(s) were appended more than once (first 10 shown)`,
  ).toEqual([])
  expect(counts.size, 'every key that should be on the stream is').toBe(expectedKeys)
}

describe('graceful shutdown (SIGTERM)', () => {
  it('drains readiness, exits 0 without waiting on storage long-polls, and keeps the shape maintained', async () => {
    await boot()
    const shape = await createShape(h!, { table: 'items', where: matchAll })
    await pg('INSERT INTO items (id, n, label) VALUES (1, 1, $1)', ['one'])
    await drainEngine(h!)
    await waitFor(async () => (await foldStream(shape.streamUrl)).has('1'), 'the first insert to reach the shape')

    // The tail of the idle stream, where the long-poll below parks.
    const tail = await fetch(`${shape.streamUrl}?offset=-1`)
    const tailOffset = tail.headers.get('stream-next-offset') ?? '-1'
    await tail.text()

    const t0 = Date.now()
    // A raw durable-streams long-poll, parked directly on the storage server (past the engine
    // entirely). It exists to prove a NEGATIVE: a client tailing storage cannot pin the engine's
    // shutdown, because the engine is not in that request's path at all. Its own window belongs to
    // the durable-streams server and is deliberately not asserted on.
    let rawPollDone = false
    const rawPollAbort = new AbortController()
    const rawPoll = fetch(`${shape.streamUrl}?offset=${encodeURIComponent(tailOffset)}&live=long-poll`, {
      signal: rawPollAbort.signal,
    })
      .then(async (res) => {
        await res.text()
        rawPollDone = true
      })
      .catch(() => {
        // Aborted at the end of the test (see below) — not a completion.
      })

    // Let the request actually reach the storage server and park.
    await new Promise((r) => setTimeout(r, 500))
    expect((await readyStatus(h!.engineUrl)).code).toBe(200)

    const signalledAt = Date.now()
    h!.signalEngine('SIGTERM')

    // `/ready` must go 503 `shutting_down` while `/health` stays 200 — poll fast, the whole drain
    // is meant to be short.
    let sawDraining = false
    let healthStayedOk = true
    const deadline = Date.now() + 5000
    while (Date.now() < deadline) {
      try {
        const r = await readyStatus(h!.engineUrl)
        if (r.code === 503 && r.status === 'shutting_down') {
          sawDraining = true
          healthStayedOk = (await fetch(`${h!.engineUrl}/health`)).status === 200
          break
        }
      } catch {
        break // the port closed before we caught the window
      }
      await new Promise((r) => setTimeout(r, 10))
    }

    expect(sawDraining, '/ready must answer 503 shutting_down during the drain').toBe(true)
    expect(healthStayedOk, '/health (liveness) must stay 200 while draining').toBe(true)

    const exit = await h!.waitForEngineExit(30000)
    const shutdownMs = Date.now() - signalledAt
    expect(exit.code, `engine exited ${JSON.stringify(exit)} after ${Date.now() - t0}ms`).toBe(0)
    expect(exit.signal).toBeNull()
    expect(
      shutdownMs,
      'the whole drain must finish well inside the 25s grace (and inside a k8s terminationGracePeriodSeconds: 30)',
    ).toBeLessThan(15000)
    expect(
      rawPollDone,
      'the engine must not have waited for a client long-polling durable-streams directly',
    ).toBe(false)
    // Cancel it rather than waiting out the storage server's own (much longer) long-poll window —
    // it has already served its purpose.
    rawPollAbort.abort()
    await rawPoll

    // Streams are NOT retired on shutdown: the shape's stream is still there, still holds its row.
    expect((await foldStream(shape.streamUrl)).has('1')).toBe(true)

    // ...and a restart continues it: the restored shape maintains a new insert, and the stream holds
    // no duplicate of the row it already had (the final checkpoint means no replay storm).
    await h!.startEngine()
    await pg('INSERT INTO items (id, n, label) VALUES (2, 2, $1)', ['two'])
    await drainEngine(h!)
    await waitFor(
      async () => (await foldStream(shape.streamUrl)).has('2'),
      'a post-restart insert to reach the shape restored from the catalog',
    )
    await expectEachKeyExactlyOnce(shape.streamUrl, 2)
  })

  it('a SECOND signal during the grace stops waiting and exits non-zero', async () => {
    // A long readiness-drain window makes the grace period observable: the engine is still in it,
    // deliberately holding the port open for a load balancer, when the second signal lands. That is
    // exactly the situation an impatient operator (or a kubelet whose own grace is running out) is
    // in, and the contract is "stop now" — a non-zero exit that says the drain did not complete,
    // not a silent 0.
    await boot({ engineEnv: { CIRCUITS_SHUTDOWN_DRAIN_SECS: '10' } })
    const shape = await createShape(h!, { table: 'items', where: matchAll })
    await pg('INSERT INTO items (id, n, label) VALUES (1, 1, $1)', ['one'])
    await drainEngine(h!)
    await waitFor(async () => (await foldStream(shape.streamUrl)).has('1'), 'the insert to reach the shape')

    h!.signalEngine('SIGTERM')
    // Wait until the drain is observably under way, then interrupt it.
    await waitFor(async () => (await readyStatus(h!.engineUrl)).status === 'shutting_down', '/ready to report draining')
    h!.signalEngine('SIGTERM')

    const exit = await h!.waitForEngineExit(20000)
    expect(exit.code, `a second signal must not exit 0 (got ${JSON.stringify(exit)})`).not.toBe(0)
    expect(exit.code, 'the forced-shutdown exit code').toBe(70)
    expect(h!.engineStderr()).toContain('second SIGTERM')

    // Nothing is corrupted by cutting the drain short: the stream is untouched and a restart
    // resumes it — an unacknowledged commit is re-delivered, the previous checkpoint stands.
    await h!.startEngine()
    await pg('INSERT INTO items (id, n, label) VALUES (2, 2, $1)', ['two'])
    await drainEngine(h!)
    await waitFor(
      async () => (await foldStream(shape.streamUrl)).has('2'),
      'a post-restart insert to reach the shape after a forced shutdown',
    )
    await expectEachKeyExactlyOnce(shape.streamUrl, 2)
  })

  it('a SIGTERM during a large transaction converges after the restart with no duplicates', async () => {
    // The large-transaction knobs from `conformance-large-txn.test.ts`: 4 KiB of buffered changes
    // and a 16 KiB append budget make a few thousand small rows the multi-gigabyte UPDATE ADR-0003
    // is about — the ingestor spills and the commit goes out in many chunks.
    await boot({
      engineEnv: {
        CIRCUITS_TXN_MEMORY_BYTES: '4096',
        CIRCUITS_CHANGES_APPEND_BYTES: '16384',
      },
    })
    const shape = await createShape(h!, { table: 'items', where: matchAll })

    const ROWS = 20000
    // One transaction, big enough that appending it is a long phase rather than an instant: 20k
    // rows against a 16 KiB append budget is well over a thousand chunk POSTs.
    //
    // The signal is sent once the INSERT has COMMITTED, which is when the ingestor is decoding and
    // appending it — the window this test exists for. (Signalling before the commit, as this used
    // to, usually landed before Postgres had delivered anything at all, which tests much less.)
    // Either way the assertions hold: the ingestor is mid-buffer (nothing acknowledged, so the
    // whole transaction is re-delivered) or mid-chunked-append (which runs to completion and is
    // de-duplicated by the sequencer's highwater on replay). Both must converge, exactly once.
    await pg(
      `INSERT INTO items (id, n, label) SELECT g, g, 'row-' || g FROM generate_series(1, $1) AS g`,
      [ROWS],
    )
    h!.signalEngine('SIGTERM')

    const exit = await h!.waitForEngineExit(40000)
    expect(exit.code, `engine exited ${JSON.stringify(exit)}`).toBe(0)

    await h!.startEngine()
    await drainEngine(h!, 60000)
    await waitFor(
      async () => (await foldStream(shape.streamUrl)).size === ROWS,
      `all ${ROWS} rows of the interrupted transaction to converge`,
      60000,
    )

    const rows = await foldStream(shape.streamUrl)
    expect(rows.size).toBe(ROWS)
    const oracle = (await pg('SELECT id, n, label FROM items ORDER BY id')) as Row[]
    expect(oracle.length).toBe(ROWS)
    for (const r of oracle) {
      expect(rows.get(String(r.id))).toMatchObject({ id: r.id, n: r.n, label: r.label })
    }
    // The fold above CANNOT see a duplicate (it is a Map). This can: the interrupted transaction
    // must land exactly once, whether it was re-delivered whole or resumed from the checkpoint.
    await expectEachKeyExactlyOnce(shape.streamUrl, ROWS)
  })
})
