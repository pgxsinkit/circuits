// Native shape creation and release cross an HTTP boundary where a request can commit even though
// its response is lost. These tests put an ordinary reverse proxy in front of the public API and
// reproduce both ambiguous outcomes through the published client; no engine internals are used.

import { createServer, request } from 'node:http'

import { createClient, type CircuitsClient } from '@circuits/client'
import type { Row, Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

interface ResponseLossProxy {
  url: string
  loseNextResponseFor(pathFragment: string): void
  holdNextCreates(count: number): void
  heldCreates(): number
  releaseHeldCreate(index: number): void
  failCreates(fail: boolean): void
  lostResponses(): number
  forwarded(pathFragment: string): number
  close(): Promise<void>
}

async function startResponseLossProxy(upstreamUrl: string): Promise<ResponseLossProxy> {
  const upstream = new URL(upstreamUrl)
  let loseFor: string | undefined
  let lost = 0
  let createsToHold = 0
  let rejectCreates = false
  const heldCreates: Array<() => void> = []
  const forwardedPaths: string[] = []
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? '/', upstream)
    const path = `${target.pathname}${target.search}`
    const loseThisResponse = loseFor !== undefined && path.includes(loseFor)
    if (loseThisResponse) loseFor = undefined

    const forward = () => {
      forwardedPaths.push(path)
      const forwarded = request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
        (response) => {
          if (loseThisResponse && response.statusCode !== undefined && response.statusCode < 400) {
            // The public API has completed the native mutation. Consume that success response, then
            // reproduce a gateway failure between the service and its client.
            response.resume()
            response.once('end', () => {
              lost += 1
              outgoing.writeHead(503, { 'content-type': 'text/plain' })
              outgoing.end('upstream success response lost')
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
    }
    const isLeaseMutation =
      path.includes('shapes.create') || path.includes('aggregate.create') || path.includes('subset.live')
    if (rejectCreates && isLeaseMutation) {
      incoming.resume()
      outgoing.writeHead(503, { 'content-type': 'text/plain' })
      outgoing.end('shape create temporarily unavailable')
    } else if (createsToHold > 0 && path.includes('shapes.create')) {
      createsToHold -= 1
      heldCreates.push(forward)
    } else {
      forward()
    }
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve())
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('response-loss proxy did not bind TCP')

  return {
    url: `http://127.0.0.1:${address.port}`,
    loseNextResponseFor: (pathFragment) => {
      loseFor = pathFragment
    },
    holdNextCreates: (count) => {
      createsToHold = count
    },
    heldCreates: () => heldCreates.length,
    releaseHeldCreate: (index) => {
      const [release] = heldCreates.splice(index, 1)
      if (!release) throw new Error(`no held create at index ${index}`)
      release()
    },
    failCreates: (fail) => {
      rejectCreates = fail
    },
    lostResponses: () => lost,
    forwarded: (pathFragment) => forwardedPaths.filter((path) => path.includes(pathFragment)).length,
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  }
}

let h: Harness | undefined
const clients: CircuitsClient[] = []
const proxies: ResponseLossProxy[] = []

afterEach(async () => {
  for (const client of clients.splice(0)) await client.close().catch(() => {})
  for (const proxy of proxies.splice(0)) await proxy.close().catch(() => {})
  await h?.shutdown()
  h = undefined
})

async function bootWithShortRetention(): Promise<{ client: CircuitsClient; proxy: ResponseLossProxy }> {
  h = await bootHarness(schema, {
    engineEnv: {
      CIRCUITS_SHAPE_IDLE_SECS: '1',
      CIRCUITS_SHAPE_DORMANT_TTL_SECS: '1',
      CIRCUITS_RETENTION_SWEEP_SECS: '1',
    },
  })
  const proxy = await startResponseLossProxy(h.apiUrl)
  proxies.push(proxy)
  const client = createClient({ apiUrl: proxy.url, schema, liveMode: 'long-poll' })
  clients.push(client)
  return { client, proxy }
}

describe('native subscription mutations with an ambiguous HTTP result', () => {
  it('does not release two subscribers when one successful DELETE response is lost and retried', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const first = await client.shape({ table: 'items' })
    const second = await client.shape({ table: 'items' })
    expect(second.handle.shapeId).toBe(first.handle.shapeId)

    // deleteShapeWithRetry is part of the published client. Its first DELETE reaches the native
    // engine and succeeds; only the response is lost, so its retry is the same logical release.
    proxy.loseNextResponseFor('shapes.delete')
    await first.close()
    expect(proxy.lostResponses()).toBe(1)
    expect(proxy.forwarded('shapes.delete')).toBeGreaterThanOrEqual(2)

    // The reproduction, inverted into the requirement: the duplicated release used to take the
    // shared refcount from two to zero and let retention delete the stream under the other live
    // materialization. It must now never happen, so this wait must TIME OUT.
    await expect(
      waitFor(
        async () => (await fetch(`${h!.engineUrl}/shapes/${second.handle.shapeId}`)).status === 404,
        'the retried release to evict the shared shape despite its other live subscriber',
        15000,
      ),
    ).rejects.toThrow()
    expect(
      (await fetch(`${h!.engineUrl}/shapes/${second.handle.shapeId}`)).status,
      'one logical close must leave the other native subscription and stream alive',
    ).toBe(200)
    expect((await fetch(second.handle.streamUrl)).status).toBe(200)
  }, 90000)

  it('does not pin an unreachable subscriber when a successful POST response is lost', async () => {
    const { client, proxy } = await bootWithShortRetention()

    // The engine creates and durably records this subscription, but the caller receives only 503
    // and therefore has no handle it could ever close.
    proxy.loseNextResponseFor('shapes.create')
    await expect(client.shape({ table: 'items' })).rejects.toThrow()
    expect(proxy.lostResponses()).toBe(1)

    // A later normal create joins the same native shape. Once that only returned subscription is
    // closed, retention must be able to reclaim it; the lost response must not create an immortal
    // phantom subscriber.
    const reachable = await client.shape({ table: 'items' })
    await reachable.close()
    await new Promise((resolve) => setTimeout(resolve, 5000))

    expect(
      (await fetch(`${h!.engineUrl}/shapes/${reachable.handle.shapeId}`)).status,
      'a subscription whose create response was lost cannot permanently pin the native shape',
    ).toBe(404)
  }, 90000)

  it('waits for every overlapping renewal before releasing a subscription', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const materialized = await client.shape({ table: 'items' })
    proxy.holdNextCreates(2)

    const firstRenewal = materialized.renew()
    await waitFor(() => Promise.resolve(proxy.heldCreates() === 1), 'the first renewal to be held by the API proxy')
    const secondRenewal = materialized.renew()
    await new Promise((resolve) => setTimeout(resolve, 250))

    let closing: Promise<void>
    if (proxy.heldCreates() === 2) {
      // A keeper that permits overlap must remember BOTH attempts. Complete the newer renewal
      // first; close must still wait for the older request rather than releasing underneath it.
      proxy.releaseHeldCreate(1)
      await secondRenewal
      closing = materialized.close()
      await waitFor(
        () => Promise.resolve(proxy.forwarded('shapes.delete') > 0),
        'an unsafe close to reach the API while the older renewal is still held',
        1000,
      ).catch(() => {})
      proxy.releaseHeldCreate(0)
    } else {
      // Serializing is also correct. close waits for the first request and the already-accepted
      // second one; release both in order and verify DELETE happens only after the complete tail.
      expect(proxy.heldCreates()).toBe(1)
      closing = materialized.close()
      proxy.releaseHeldCreate(0)
      await waitFor(() => Promise.resolve(proxy.heldCreates() === 1), 'the queued second renewal to reach the proxy')
      proxy.releaseHeldCreate(0)
    }
    await Promise.all([firstRenewal, closing])

    const info = await fetch(`${h!.engineUrl}/shapes/${materialized.handle.shapeId}`)
    const body = (await info.json()) as { subscriptions?: number }
    expect(
      body.subscriptions,
      'an older renewal must not land after close and recreate the released subscription',
    ).toBe(0)
  }, 90000)

  it('adopts the fresh handle returned when a late renewal recreates an evicted shape', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const materialized = await client.shape({ table: 'items' })
    const oldId = materialized.handle.shapeId
    const subscription = materialized.handle.subscription!

    // Native reads bypass the engine, so renewal is the only lease signal. Hold renewals outside
    // the engine until the short lease+dormancy windows evict the old shape and stream.
    proxy.failCreates(true)
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${oldId}`)).status === 404,
      'the unrenewed shape to be evicted',
      20000,
    )
    proxy.failCreates(false)

    // This succeeds and creates a fresh native shape, but renew() exposes no new handle and the
    // materialization's stream reader remains attached to oldId.
    await materialized.renew()
    const fresh = await createShape(h!, { table: 'items', subscription })
    expect(fresh.shapeId).not.toBe(oldId)
    await pgQuery(h!, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h!)
    await waitFor(async () => (await foldStream(fresh.streamUrl)).has('1'), 'the fresh shape to receive the row')
    await new Promise((resolve) => setTimeout(resolve, 500))

    expect(
      materialized.currentRows().some((row) => Number(row.id) === 1),
      'a successful late renew must move the materialization to the fresh returned handle',
    ).toBe(true)
  }, 90000)

  it('moves an aggregate reader to the replacement returned by late renewal', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const aggregate = await client.aggregate({ table: 'items', fn: 'count' })
    expect(aggregate.count()).toBe(0)

    proxy.failCreates(true)
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'the unrenewed aggregate to be evicted',
      20000,
    )
    proxy.failCreates(false)
    await aggregate.renew()

    await pgQuery(h!, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h!)
    await waitFor(() => Promise.resolve(aggregate.count() === 1), 'the renewed aggregate reader to receive the row')
  }, 90000)

  it('moves a subset tail to the replacement returned by late renewal', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const subset = await client.subset({ table: 'items', limit: 10 })
    expect(subset.collection.toArray as unknown as Row[]).toEqual([])

    proxy.failCreates(true)
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'the unrenewed subset feed to be evicted',
      20000,
    )
    proxy.failCreates(false)
    await subset.renew()

    await pgQuery(h!, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h!)
    await waitFor(
      () => Promise.resolve((subset.collection.toArray as unknown as Row[]).some((row) => Number(row.id) === 1)),
      'the renewed subset tail to receive the row',
    )
  }, 90000)

  it('recovers the changes a subset missed while its feed was gone', async () => {
    const { client, proxy } = await bootWithShortRetention()
    const subset = await client.subset({ table: 'items', limit: 10 })
    expect(subset.collection.toArray as unknown as Row[]).toEqual([])

    proxy.failCreates(true)
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/s1`)).status === 404,
      'the unrenewed subset feed to be evicted',
      20000,
    )

    // The gap: this row changes while the subscription is lapsed AND its feed evicted, so no feed
    // — neither the dead one nor the changes-only replacement created afterwards — ever carries it.
    // Only re-reading the page can put it back, which is what a successful renew has to do.
    await pgQuery(h!, 'INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h!)

    proxy.failCreates(false)
    await subset.renew()

    await waitFor(
      () => Promise.resolve((subset.collection.toArray as unknown as Row[]).some((row) => Number(row.id) === 1)),
      'the renewed subset to recover the row inserted while its feed was gone',
    )
  }, 90000)
})
