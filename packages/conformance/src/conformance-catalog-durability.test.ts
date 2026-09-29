// The native shape catalog is the restart contract: once POST /shapes succeeds, a crash must not
// turn that acknowledged shape into an unmaintained durable stream. This test puts an ordinary HTTP
// reverse proxy between the engine and durable-streams and makes only catalog appends return 503.
// It uses no engine hooks or private APIs: PostgreSQL SQL, POST/GET /shapes, process restart, and the
// durable stream URL returned by the native create response are the complete reproduction.

import { createServer, request } from 'node:http'

import type { Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery, type ShapeResp, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

const subquerySchema: Schema = {
  tables: {
    parent: {
      columns: { id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
    child: {
      columns: { id: { type: 'int' }, parent_id: { type: 'int' } },
      primaryKey: 'id',
    },
  },
}

interface CatalogFaultProxy {
  url: string
  failedCatalogAppends(): number
  failedShapeRetirements(): number
  lostCatalogResponses(): number
  failCatalogAppends(fail: boolean): void
  failShapeRetirements(fail: boolean): void
  loseNextCatalogResponse(): void
  close(): Promise<void>
}

async function startCatalogFaultProxy(upstreamUrl: string): Promise<CatalogFaultProxy> {
  let fail = false
  let failures = 0
  let failRetirements = false
  let retirementFailures = 0
  let loseNextCatalogResponse = false
  let lostCatalogResponses = 0
  const upstream = new URL(upstreamUrl)
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? '/', upstream)
    if (fail && incoming.method === 'POST' && target.pathname === '/meta/catalog') {
      failures += 1
      incoming.resume()
      outgoing.writeHead(503, { 'content-type': 'text/plain' })
      outgoing.end('catalog temporarily unavailable')
      return
    }
    if (
      failRetirements &&
      target.pathname.startsWith('/shape/') &&
      (incoming.method === 'POST' || incoming.method === 'DELETE')
    ) {
      retirementFailures += 1
      incoming.resume()
      outgoing.writeHead(503, { 'content-type': 'text/plain' })
      outgoing.end('shape retirement temporarily unavailable')
      return
    }

    const loseThisResponse =
      loseNextCatalogResponse && incoming.method === 'POST' && target.pathname === '/meta/catalog'
    if (loseThisResponse) loseNextCatalogResponse = false
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        if (loseThisResponse) {
          // The upstream append has already committed. Consume its success response but report an
          // ordinary transient gateway failure to the engine, reproducing a lost response after a
          // successful non-idempotent request.
          response.resume()
          response.once('end', () => {
            lostCatalogResponses += 1
            outgoing.writeHead(503, { 'content-type': 'text/plain' })
            outgoing.end('catalog response lost after upstream commit')
          })
          return
        }
        outgoing.writeHead(response.statusCode ?? 502, response.headers)
        response.pipe(outgoing)
      },
    )
    forwarded.on('error', (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502, { 'content-type': 'text/plain' })
      outgoing.end(String(error))
    })
    incoming.pipe(forwarded)
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve())
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('catalog fault proxy did not bind TCP')

  return {
    url: `http://127.0.0.1:${address.port}`,
    failedCatalogAppends: () => failures,
    failedShapeRetirements: () => retirementFailures,
    lostCatalogResponses: () => lostCatalogResponses,
    failCatalogAppends: (next) => {
      fail = next
    },
    failShapeRetirements: (next) => {
      failRetirements = next
    },
    loseNextCatalogResponse: () => {
      loseNextCatalogResponse = true
    },
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  }
}

async function readCatalogEvents(dsUrl: string): Promise<Array<{ t?: string; id?: string }>> {
  const events: Array<{ t?: string; id?: string }> = []
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const response = await fetch(`${dsUrl}/meta/catalog?offset=${encodeURIComponent(offset)}`)
    if (response.status === 204) break
    if (!response.ok) throw new Error(`GET meta/catalog -> ${response.status}`)
    const body = (await response.text()).trim()
    if (body) events.push(...(JSON.parse(body) as Array<{ t?: string; id?: string }>))
    const next = response.headers.get('stream-next-offset')
    const upToDate = response.headers.get('stream-up-to-date') !== null
    if (!next || next === offset) break
    offset = next
    if (upToDate) break
  }
  return events
}

async function createAggregate(harness: Harness): Promise<ShapeResp> {
  const response = await fetch(`${harness.engineUrl}/aggregate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'items', fn: 'count' }),
  })
  if (!response.ok) throw new Error(`POST /aggregate -> ${response.status} ${await response.text()}`)
  return (await response.json()) as ShapeResp
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

describe('native catalog durability under a durable-streams status failure', () => {
  it('does not acknowledge a shape whose Created record was rejected by storage', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    await new Promise((resolve) => setTimeout(resolve, 250))

    proxy!.failCatalogAppends(true)
    const creating = createShape(h, { table: 'items' })
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the proxy to reject the Created event')
    proxy!.failCatalogAppends(false)

    // Rejecting creation is a valid fail-closed answer. If creation was acknowledged, however, the
    // handle and stream must be durable across the crash.
    let shape: Awaited<ReturnType<typeof createShape>>
    try {
      shape = await creating
    } catch {
      return
    }

    // The public create already returned success. Recover storage, crash the engine, and use only
    // the handle it returned to check whether that acknowledged subscription survived.
    await h.restartEngine()
    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (2, 20)')
    await drainEngine(h)

    const shapeStatus = await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)
    const rows = await foldStream(shape.streamUrl)
    expect.soft(shapeStatus.status, 'the acknowledged native shape must still be registered after restart').toBe(200)
    expect.soft(rows.has('2'), 'the returned durable stream must still receive PostgreSQL changes').toBe(true)
  }, 90000)

  it('retries mandatory TRUNCATE retirement after durable-streams recovers', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    const shape = await createShape(h, { table: 'items' })
    expect((await foldStream(shape.streamUrl)).has('1')).toBe(true)

    proxy!.failShapeRetirements(true)
    await pgQuery(h, 'TRUNCATE items')
    await waitFor(
      () => Promise.resolve(proxy!.failedShapeRetirements() >= 2),
      'the proxy to reject both close and delete',
    )
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status === 404,
      'the engine to remove the retired shape record',
    )

    // Recovery is entirely external. The engine has already forgotten the shape, but its mandatory
    // retirement must still finish: otherwise the public stream URL remains open with rows that
    // PostgreSQL no longer contains, forever.
    proxy!.failShapeRetirements(false)
    await waitFor(async () => {
      const response = await fetch(shape.streamUrl)
      return response.status === 404 || response.status === 410
    }, 'the stale shape stream to be deleted after durable-streams recovers')
  }, 90000)

  it('does not retire or acknowledge purge before its catalog intent is durable', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        CIRCUITS_SHAPE_IDLE_SECS: '1',
        CIRCUITS_SHAPE_DORMANT_TTL_SECS: '1',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    const shape = await createShape(h, { table: 'items' })
    expect((await foldStream(shape.streamUrl)).has('1')).toBe(true)

    // Keep both halves of the promised retirement from reaching storage. The public request must
    // wait at the Dropped intent before attempting stream retirement or acknowledging success.
    proxy!.failCatalogAppends(true)
    proxy!.failShapeRetirements(true)
    let acknowledged = false
    const deleting = fetch(`${h.engineUrl}/shapes/${shape.shapeId}?purge=true`, { method: 'DELETE' }).then((response) => {
      acknowledged = true
      return response
    })
    await waitFor(
      () => Promise.resolve(proxy!.failedCatalogAppends() > 0),
      'the proxy to reject the Dropped catalog intent',
    )
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(acknowledged).toBe(false)
    expect(proxy!.failedShapeRetirements()).toBe(0)

    proxy!.failCatalogAppends(false)
    const purged = await deleting
    expect(purged.ok, 'the native API acknowledged the purge after Dropped became durable').toBe(true)
    await waitFor(
      () => Promise.resolve(proxy!.failedShapeRetirements() >= 2),
      'the proxy to reject both close and delete',
    )
    expect((await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404)

    // Crash with retirement still retrying. Dropped is already durable, so recovery has an explicit
    // obligation to complete rather than relying on the subscription lease.
    await h.restartEngine(async () => {
      proxy!.failShapeRetirements(false)
    })

    expect((await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404)
    await waitFor(async () => {
      const response = await fetch(shape.streamUrl)
      return response.status === 404 || response.status === 410
    }, 'the purged stream to remain retired after restart')
  }, 90000)

  it('does not acknowledge purge before durability when leases are disabled', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        // Zero is a supported production setting: it disables dormancy and, under ADR-0008, lease
        // expiry. A purge acknowledgement still has to survive a process boundary in this mode.
        CIRCUITS_SHAPE_IDLE_SECS: '0',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const shape = await createShape(h, { table: 'items' })
    proxy!.failCatalogAppends(true)
    proxy!.failShapeRetirements(true)
    let acknowledged = false
    const deleting = fetch(`${h.engineUrl}/shapes/${shape.shapeId}?purge=true`, { method: 'DELETE' }).then((response) => {
      acknowledged = true
      return response
    })
    await waitFor(
      () => Promise.resolve(proxy!.failedCatalogAppends() > 0),
      'the proxy to reject the Dropped catalog intent',
    )
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(acknowledged, 'native purge must wait for its Dropped intent when leases cannot repair it').toBe(false)

    let retryAcknowledged = false
    const retrying = fetch(`${h.engineUrl}/shapes/${shape.shapeId}?purge=true`, { method: 'DELETE' }).then(
      (response) => {
        retryAcknowledged = true
        return response
      },
    )
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(retryAcknowledged, 'a concurrent purge retry must wait for the first request durability barrier').toBe(false)

    proxy!.failCatalogAppends(false)
    const [purged, retried] = await Promise.all([deleting, retrying])
    expect(purged.ok, 'the native API acknowledged the purge after its intent became durable').toBe(true)
    expect(retried.ok).toBe(true)
    await waitFor(
      () => Promise.resolve(proxy!.failedShapeRetirements() >= 2),
      'the proxy to reject both close and delete',
    )

    await h.restartEngine(async () => {
      proxy!.failShapeRetirements(false)
    })
    await new Promise((resolve) => setTimeout(resolve, 2500))

    expect(
      (await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status,
      'an acknowledged purge must not resurrect permanently when the supported zero-lease mode is configured',
    ).toBe(404)
    expect((await fetch(shape.streamUrl)).status).toBe(404)
  }, 90000)

  it('retires a purged stream after the client abandons its durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        // Zero disables lease repair, so the abandoned purge is the ONLY thing that can retire this
        // stream: no background sweep can finish the job for it and hide the leak.
        CIRCUITS_SHAPE_IDLE_SECS: '0',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const shape = await createShape(h, { table: 'items' })
    expect((await fetch(shape.streamUrl)).status).toBe(200)

    // Giving up on a request that is waiting for storage is what a client DOES during an outage.
    // The engine has already taken the shape out of its state and owns a Dropped its writer will
    // land regardless, so the retirement it promised is the engine's obligation alone from here.
    proxy!.failCatalogAppends(true)
    const abort = new AbortController()
    const abandoned = fetch(`${h.engineUrl}/shapes/${shape.shapeId}?purge=true`, {
      method: 'DELETE',
      signal: abort.signal,
    })
    await waitFor(
      () => Promise.resolve(proxy!.failedCatalogAppends() > 0),
      'the proxy to reject the Dropped catalog intent',
    )
    abort.abort()
    await expect(abandoned).rejects.toThrow()
    proxy!.failCatalogAppends(false)

    expect((await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404)
    await waitFor(async () => {
      const dropped = (await readCatalogEvents(h!.dsUrl)).filter(
        (event) => event.t === 'dropped' && event.id === shape.shapeId,
      )
      return dropped.length === 1
    }, 'the abandoned purge to still record exactly one Dropped intent')
    await waitFor(async () => {
      const response = await fetch(shape.streamUrl)
      return response.status === 404 || response.status === 410
    }, 'the abandoned purge to still retire its durable stream in this process')
  }, 90000)

  it('does not acknowledge a join whose shared shape was purged during its durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    const first = await createShape(h, { table: 'items' })
    proxy!.failCatalogAppends(true)
    const joining = createShape(h, { table: 'items' })
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the Joined event to be blocked')

    // The join has passed its lifecycle/schema checks and is waiting only for catalog durability.
    // Purge the shared shape through the native API while that external storage wait is in flight.
    const purging = fetch(`${h.engineUrl}/shapes/${first.shapeId}?purge=true`, { method: 'DELETE' })
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${first.shapeId}`)).status === 404,
      'the shared shape to be purged',
    )

    proxy!.failCatalogAppends(false)
    const [purged, joined] = await Promise.all([purging, joining])
    expect(purged.ok).toBe(true)
    expect((await fetch(first.streamUrl)).status).toBe(404)
    expect(
      (await fetch(`${h.engineUrl}/shapes/${joined.shapeId}`)).status,
      'a successful native join must return a shape that still exists',
    ).toBe(200)
    expect((await fetch(joined.streamUrl)).status, 'a successful native join must return a readable stream').toBe(200)
  }, 90000)

  it('does not acknowledge a first create retired by TRUNCATE during its durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    proxy!.failCatalogAppends(true)
    const creating = createShape(h, { table: 'items' })
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the Created event to be blocked')

    // The create has completed its snapshot and closing schema check, then waits for storage. A
    // public database operation can still retire it in that interval.
    await pgQuery(h, 'TRUNCATE items')
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'TRUNCATE to retire the pending first shape',
    )

    proxy!.failCatalogAppends(false)
    const created = await creating
    expect(
      (await fetch(`${h.engineUrl}/shapes/${created.shapeId}`)).status,
      'a successful native create must still be registered after its durability wait',
    ).toBe(200)
    expect((await fetch(created.streamUrl)).status, 'a successful native create must return a readable stream').toBe(200)
  }, 90000)

  it('does not acknowledge an aggregate retired by TRUNCATE during its durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    await pgQuery(h, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)
    proxy!.failCatalogAppends(true)
    const creating = createAggregate(h)
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the aggregate Created event to be blocked')

    await pgQuery(h, 'TRUNCATE items')
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'TRUNCATE to retire the pending aggregate',
    )

    proxy!.failCatalogAppends(false)
    const created = await creating
    expect(
      (await fetch(`${h.engineUrl}/shapes/${created.shapeId}`)).status,
      'a successful native aggregate create must still be registered after its durability wait',
    ).toBe(200)
    expect((await fetch(created.streamUrl)).status, 'a successful aggregate must return a readable stream').toBe(200)
  }, 90000)

  it('does not acknowledge a subquery shape retired during its durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(subquerySchema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
    })

    await pgQuery(h, 'INSERT INTO parent (id, active) VALUES (1, true)')
    await pgQuery(h, 'INSERT INTO child (id, parent_id) VALUES (1, 1)')
    await drainEngine(h)
    proxy!.failCatalogAppends(true)
    const creating = createShape(h, {
      table: 'child',
      where: {
        col: 'parent_id',
        in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } },
      },
    })
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the subquery Created event to be blocked')

    // Retiring an inner dependency retires the outer subquery shape as well.
    await pgQuery(h, 'TRUNCATE parent')
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'the inner-table TRUNCATE to retire the pending subquery shape',
    )

    proxy!.failCatalogAppends(false)
    const created = await creating
    expect(
      (await fetch(`${h.engineUrl}/shapes/${created.shapeId}`)).status,
      'a successful native subquery create must still be registered after its durability wait',
    ).toBe(200)
    expect((await fetch(created.streamUrl)).status, 'a successful subquery create must return a readable stream').toBe(200)
  }, 90000)

  it('does not apply a non-idempotent catalog mutation twice when only its success response is lost', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        // A long idle window on purpose: these subscriptions are anonymous and never renewed, and
        // this test is about what the FOLD does with a duplicated `Left` — not about leases. Under a
        // second-scale window the sweeper would (correctly, ADR-0008) reclaim the survivor
        // mid-assertion and hide the very thing being measured.
        CIRCUITS_SHAPE_IDLE_SECS: '120',
        CIRCUITS_SHAPE_DORMANT_TTL_SECS: '120',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const first = await createShape(h, { table: 'items' })
    const second = await createShape(h, { table: 'items' })
    const third = await createShape(h, { table: 'items' })
    expect(second.shapeId).toBe(first.shapeId)
    expect(third.shapeId).toBe(first.shapeId)

    // One subscriber leaves. Storage commits that one `Left`, but its success response disappears
    // at the proxy. Retrying the same unkeyed append must not turn one public DELETE into two leaves.
    proxy!.loseNextCatalogResponse()
    const released = await fetch(`${h.engineUrl}/shapes/${first.shapeId}`, { method: 'DELETE' })
    expect(released.ok).toBe(true)
    await waitFor(() => Promise.resolve(proxy!.lostCatalogResponses() === 1), 'the committed response to be lost')
    await waitFor(
      async () =>
        (await readCatalogEvents(h!.dsUrl)).filter((event) => event.t === 'left' && event.id === first.shapeId)
          .length >= 2,
      'the retry to duplicate the committed Left event',
    )

    await h.restartEngine()
    // Two subscribers still exist. Releasing one of them must leave the other subscribed, so the
    // retention lifecycle has no right to retire their shared stream.
    const releasedSecond = await fetch(`${h.engineUrl}/shapes/${first.shapeId}`, { method: 'DELETE' })
    expect(releasedSecond.ok).toBe(true)
    // The reproduction, inverted into the requirement: the duplicated `Left` used to fold as two
    // decrements, leaving a shape with a live subscriber at refcount 0 for retention to evict. It
    // must now never happen, so this wait must TIME OUT.
    await expect(
      waitFor(
        async () => (await fetch(`${h!.engineUrl}/shapes/${first.shapeId}`)).status === 404,
        'the duplicated Left to evict a shape that still has a subscriber',
        15000,
      ),
    ).rejects.toThrow()
    expect(
      (await fetch(`${h.engineUrl}/shapes/${first.shapeId}`)).status,
      'one remaining subscriber must keep its shared native shape alive',
    ).toBe(200)
    expect((await fetch(third.streamUrl)).status, 'the remaining subscriber stream must stay readable').toBe(200)
  }, 90000)

  it('does not leak a subscriber when its join request is aborted during the durability wait', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        CIRCUITS_SHAPE_IDLE_SECS: '1',
        CIRCUITS_SHAPE_DORMANT_TTL_SECS: '1',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const first = await createShape(h, { table: 'items' })
    proxy!.failCatalogAppends(true)
    const abort = new AbortController()
    const abandonedJoin = createShape(h, { table: 'items' }, abort.signal)
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the Joined event to be blocked')

    // This caller never receives a handle and therefore can never release the refcount already
    // taken for it. Cancelling a public HTTP request must undo that provisional subscription.
    abort.abort()
    await expect(abandonedJoin).rejects.toThrow()
    proxy!.failCatalogAppends(false)
    await waitFor(
      async () => (await readCatalogEvents(h!.dsUrl)).some((event) => event.t === 'joined' && event.id === first.shapeId),
      'the abandoned join event to reach the public catalog stream',
    )

    const released = await fetch(`${h.engineUrl}/shapes/${first.shapeId}`, { method: 'DELETE' })
    expect(released.ok).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, 5000))
    expect(
      (await fetch(`${h.engineUrl}/shapes/${first.shapeId}`)).status,
      'the only subscription that ever received a handle was released, so retention must reclaim the shape',
    ).toBe(404)
  }, 90000)

  it('does not acknowledge a release discarded by forced shutdown during a catalog outage', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        CIRCUITS_SHAPE_IDLE_SECS: '1',
        CIRCUITS_SHAPE_DORMANT_TTL_SECS: '1',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const shape = await createShape(h, { table: 'items' })
    proxy!.failCatalogAppends(true)
    let acknowledged = false
    const releasing = fetch(
      `${h.engineUrl}/shapes/${shape.shapeId}?subscription=${encodeURIComponent(shape.subscription!)}`,
      { method: 'DELETE' },
    ).then(
      (response) => {
        acknowledged = true
        return { response }
      },
      (error: unknown) => ({ error }),
    )
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the Left event to be blocked')
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(acknowledged, 'a release whose only durable copy is still blocked must remain pending').toBe(false)

    // A normal SIGTERM cannot drain an unavailable catalog. A second signal is the documented
    // external forced-shutdown path and discards the process's only copy of this unacknowledged Left.
    h.signalEngine('SIGTERM')
    await waitFor(async () => {
      try {
        return (await fetch(`${h!.engineUrl}/ready`)).status === 503
      } catch {
        return false
      }
    }, 'shutdown to enter its readiness drain')
    h.signalEngine('SIGTERM')
    const exit = await h.waitForEngineExit(20000)
    expect(exit.code).toBe(70)
    expect(await releasing).toHaveProperty('error')

    proxy!.failCatalogAppends(false)
    await h.startEngine()
    expect(
      (await readCatalogEvents(h.dsUrl)).filter((event) => event.t === 'left' && event.id === shape.shapeId),
      'the forced exit occurred before the unacknowledged release reached durable storage',
    ).toHaveLength(0)
    const restored = await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)
    expect(restored.status).toBe(200)
    expect(((await restored.json()) as { subscriptions?: number }).subscriptions).toBe(1)

    const retried = await fetch(
      `${h.engineUrl}/shapes/${shape.shapeId}?subscription=${encodeURIComponent(shape.subscription!)}`,
      { method: 'DELETE' },
    )
    expect(retried.ok).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, 5000))
    expect(
      (await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status,
      'the caller can retry the unacknowledged identified release after restart',
    ).toBe(404)
    expect((await fetch(shape.streamUrl)).status).toBe(404)
  }, 90000)

  it('does not acknowledge release before durability when leases are disabled', async () => {
    let proxy: CatalogFaultProxy | undefined
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startCatalogFaultProxy(upstreamUrl)
        return proxy
      },
      engineEnv: {
        CIRCUITS_SHAPE_IDLE_SECS: '0',
        CIRCUITS_RETENTION_SWEEP_SECS: '1',
      },
    })

    const shape = await createShape(h, { table: 'items', subscription: 'review-zero-lease' })
    proxy!.failCatalogAppends(true)
    let acknowledged = false
    const deleting = fetch(
      `${h.engineUrl}/shapes/${shape.shapeId}?subscription=${encodeURIComponent('review-zero-lease')}`,
      { method: 'DELETE' },
    ).then((response) => {
      acknowledged = true
      return response
    })
    await waitFor(() => Promise.resolve(proxy!.failedCatalogAppends() > 0), 'the Left event to be blocked')
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(acknowledged, 'native release must wait for its Left event when leases cannot repair it').toBe(false)

    let retryAcknowledged = false
    const retrying = fetch(
      `${h.engineUrl}/shapes/${shape.shapeId}?subscription=${encodeURIComponent('review-zero-lease')}`,
      { method: 'DELETE' },
    ).then((response) => {
      retryAcknowledged = true
      return response
    })
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(retryAcknowledged, 'a concurrent release retry must wait for the first request durability barrier').toBe(
      false,
    )

    proxy!.failCatalogAppends(false)
    const [released, retried] = await Promise.all([deleting, retrying])
    expect(released.ok, 'the identified native release was acknowledged after its event became durable').toBe(true)
    expect(retried.ok).toBe(true)

    h.signalEngine('SIGTERM')
    await waitFor(async () => {
      try {
        return (await fetch(`${h!.engineUrl}/ready`)).status === 503
      } catch {
        return false
      }
    }, 'shutdown to enter its readiness drain')
    h.signalEngine('SIGTERM')
    expect((await h.waitForEngineExit(20000)).code).toBe(70)

    await h.startEngine()
    await new Promise((resolve) => setTimeout(resolve, 2500))
    const info = await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)
    expect(info.status).toBe(200)
    expect(
      ((await info.json()) as { subscriptions?: number }).subscriptions,
      'the durable named release must still be reflected after restart even though zero disables eviction',
    ).toBe(0)
  }, 90000)
})
