// A subscription is a first-class identity on the native API (ADR-0008): the caller names its claim
// on a shape, repeating the create renews that claim instead of taking a second one, releasing it
// twice is the same act once, and it stays live only while it is renewed within the shape idle
// window. These tests drive the engine's own routes — `POST /shapes`, `GET /shapes/{id}`,
// `DELETE /shapes/{id}?subscription=…` — plus a process restart. No client, no engine internals.

import { createServer, request } from 'node:http'

import type { Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, waitFor } from './engine-native.js'
import { bootHarness, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

let h: Harness | undefined
// The harness owns the durable-streams wrapper it was given and closes it with the engine — closing
// it here as well would race the engine's own keep-alive sockets and hang the teardown.
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

interface LossProxy {
  url: string
  loseNextCatalogResponse(): void
  lostCatalogResponses(): number
  close(): Promise<void>
}

/**
 * A reverse proxy in front of durable-streams that lets exactly one catalog append COMMIT and then
 * answers `503` — the ambiguity every retrying writer has to survive. Ordinary HTTP, no engine
 * hooks: the engine cannot tell this from a write that never happened, so it appends the event
 * again, and the catalog ends up holding two copies of one event.
 */
async function startLossProxy(upstreamUrl: string): Promise<LossProxy> {
  const upstream = new URL(upstreamUrl)
  let loseNext = false
  let lost = 0
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? '/', upstream)
    const lose = loseNext && incoming.method === 'POST' && target.pathname === '/meta/catalog'
    if (lose) loseNext = false
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        if (lose) {
          response.resume()
          response.once('end', () => {
            lost += 1
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
  if (!address || typeof address === 'string') throw new Error('loss proxy did not bind TCP')
  return {
    url: `http://127.0.0.1:${address.port}`,
    loseNextCatalogResponse: () => {
      loseNext = true
    },
    lostCatalogResponses: () => lost,
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  }
}

async function readCatalogEvents(dsUrl: string): Promise<Array<{ t?: string; id?: string; subscription?: string }>> {
  const events: Array<{ t?: string; id?: string; subscription?: string }> = []
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

interface ShapeInfo {
  status: number
  state?: string
  subscriptions?: number
}

async function shapeInfo(id: string): Promise<ShapeInfo> {
  const res = await fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(id)}`)
  if (!res.ok) return { status: res.status }
  const body = (await res.json()) as { state?: string; subscriptions?: number }
  return { status: res.status, state: body.state, subscriptions: body.subscriptions }
}

function release(id: string, subscription?: string): Promise<Response> {
  const query = subscription ? `?subscription=${encodeURIComponent(subscription)}` : ''
  return fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(id)}${query}`, { method: 'DELETE' })
}

/** Second-scale retention, so a lease that is not renewed lapses inside a test. */
const fastRetention = {
  CIRCUITS_SHAPE_IDLE_SECS: '2',
  CIRCUITS_SHAPE_DORMANT_TTL_SECS: '60',
  CIRCUITS_RETENTION_SWEEP_SECS: '1',
}

describe('native subscriptions are identified, idempotent and leased', () => {
  it('repeating a create with the same subscription renews it instead of adding a subscriber', async () => {
    h = await bootHarness(schema)

    const first = await createShape(h, { table: 'items', subscription: 'sub-one' })
    expect(first.subscription, 'the response names the claim the create was recorded under').toBe('sub-one')
    expect(first.leaseSeconds, 'and the window it must be renewed within').toBeGreaterThan(0)

    const again = await createShape(h, { table: 'items', subscription: 'sub-one' })
    expect(again.shapeId, 'a repeat is the same claim on the same shape').toBe(first.shapeId)
    expect(again.streamPath).toBe(first.streamPath)
    expect((await shapeInfo(first.shapeId)).subscriptions, 'one caller is one subscription').toBe(1)

    // A different caller on the same predicate shares the shape and IS a second subscription.
    const other = await createShape(h, { table: 'items', subscription: 'sub-two' })
    expect(other.shapeId).toBe(first.shapeId)
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(2)

    // An engine-minted claim (no subscription sent) is returned so it can be renewed and released.
    const anonymous = await createShape(h, { table: 'items' })
    expect(anonymous.shapeId).toBe(first.shapeId)
    expect(typeof anonymous.subscription).toBe('string')
    expect(anonymous.subscription).not.toBe('sub-one')
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(3)
  }, 90000)

  it('releasing the same subscription twice releases it once, leaving the other subscriber alive', async () => {
    h = await bootHarness(schema)

    const first = await createShape(h, { table: 'items', subscription: 'sub-one' })
    const second = await createShape(h, { table: 'items', subscription: 'sub-two' })
    expect(second.shapeId).toBe(first.shapeId)

    expect((await release(first.shapeId, 'sub-one')).ok).toBe(true)
    // The retry a client makes when its success response was lost in transit.
    expect((await release(first.shapeId, 'sub-one')).ok).toBe(true)
    expect((await release(first.shapeId, 'never-existed')).ok).toBe(true)

    const info = await shapeInfo(first.shapeId)
    expect(info.status, 'the shape still has a subscriber, so it is still there').toBe(200)
    expect(info.subscriptions, 'exactly one release happened').toBe(1)
    expect((await fetch(second.streamUrl)).status, 'and the survivor can still read it').toBe(200)
  }, 90000)

  it('lets a caller renew and release the subscription the engine minted for it', async () => {
    h = await bootHarness(schema)

    // A create that names nothing still gets a claim, and gets told its name — otherwise the caller
    // could only ever release it through the legacy anonymous decrement, which is not retry-safe.
    const anonymous = await createShape(h, { table: 'items' })
    const minted = anonymous.subscription!
    expect(minted.startsWith('~'), 'engine-minted ids are marked').toBe(true)

    // Renewing by that id is the ordinary repeat-create: same handle, still one subscription.
    const renewed = await createShape(h, { table: 'items', subscription: minted })
    expect(renewed.shapeId).toBe(anonymous.shapeId)
    expect(renewed.subscription).toBe(minted)
    expect((await shapeInfo(anonymous.shapeId)).subscriptions).toBe(1)

    // ...and so is releasing it, idempotently.
    expect((await release(anonymous.shapeId, minted)).ok).toBe(true)
    expect((await shapeInfo(anonymous.shapeId)).subscriptions).toBe(0)
    expect((await release(anonymous.shapeId, minted)).ok, 'a retried release is a no-op').toBe(true)
    expect((await shapeInfo(anonymous.shapeId)).subscriptions).toBe(0)
  }, 90000)

  it('lets a caller re-subscribe with an engine-minted id after its lease lapses and the engine restarts', async () => {
    h = await bootHarness(schema, { engineEnv: fastRetention })

    const first = await createShape(h, { table: 'items' })
    const minted = first.subscription!
    expect(minted.startsWith('~')).toBe(true)
    await waitFor(
      async () => (await shapeInfo(first.shapeId)).subscriptions === 0,
      'the engine-minted subscription lease to lapse',
      20000,
    )
    await h.restartEngine()

    // This id came from the native create response. Lapsing removes its claim, not the caller's
    // right to use that returned identity to subscribe again as ADR-0008 documents; and since the
    // `~` prefix is only a marker the engine never validates, a restart has no provenance to lose.
    const renewed = await createShape(h, { table: 'items', subscription: minted })
    expect(renewed.subscription).toBe(minted)
    expect((await shapeInfo(renewed.shapeId)).subscriptions).toBe(1)
  }, 90000)

  it('refuses a subscription id that already belongs to a different shape', async () => {
    h = await bootHarness(schema)

    await createShape(h, { table: 'items', subscription: 'sub-one' })
    const conflicting = await fetch(`${h.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', where: { col: 'n', op: 'gt', value: 5 }, subscription: 'sub-one' }),
    })
    expect(conflicting.status, 'one subscription id names one shape').toBe(409)
    expect(await conflicting.text()).toContain('already belongs to shape')

    // The `~` prefix is a MARKER, not a reserved namespace: the engine never checks whether it
    // minted a given `~` id, so a caller may name one itself and it behaves like any other named
    // subscription. (All such a caller achieves is making its OWN claim the one the legacy
    // anonymous DELETE treats as expendable first.)
    const selfMarked = await fetch(`${h.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', subscription: '~mine' }),
    })
    expect(selfMarked.status, 'a caller-invented `~` id is accepted').toBe(200)
    const marked = (await selfMarked.json()) as { shapeId: string; subscription: string }
    expect(marked.subscription).toBe('~mine')
    expect(marked.shapeId, 'and it claims the shape it named, like any other id').toBe(
      (await createShape(h, { table: 'items', subscription: 'sub-one' })).shapeId,
    )
    expect((await shapeInfo(marked.shapeId)).subscriptions, 'two distinct claims on one shape').toBe(2)
    // Repeat = renew, release = idempotent: the ordinary named-subscription contract.
    await createShape(h, { table: 'items', subscription: '~mine' })
    expect((await shapeInfo(marked.shapeId)).subscriptions).toBe(2)
    expect((await release(marked.shapeId, '~mine')).ok).toBe(true)
    expect((await shapeInfo(marked.shapeId)).subscriptions).toBe(1)
  }, 90000)

  it('lets a lease lapse when nothing renews it, and keeps a renewed one live', async () => {
    h = await bootHarness(schema, { engineEnv: fastRetention })

    const kept = await createShape(h, { table: 'items', subscription: 'renewed' })
    const dropped = await createShape(h, { table: 'items', where: { col: 'n', op: 'gt', value: 5 }, subscription: 'abandoned' })
    expect(dropped.shapeId).not.toBe(kept.shapeId)

    // Renew the first one inside its window while the second one says nothing at all. No DELETE is
    // ever sent for either: the renewal IS the liveness signal on the native path.
    const renewing = setInterval(() => {
      void createShape(h!, { table: 'items', subscription: 'renewed' }).catch(() => {})
    }, 700)
    try {
      await waitFor(
        async () => (await shapeInfo(dropped.shapeId)).subscriptions === 0,
        'the unrenewed lease to lapse',
        20000,
      )
      await waitFor(
        async () => (await shapeInfo(dropped.shapeId)).state === 'dormant',
        'the shape whose last lease lapsed to go dormant',
        20000,
      )
      const alive = await shapeInfo(kept.shapeId)
      expect(alive.subscriptions, 'the renewed subscription is still live').toBe(1)
      expect(alive.state, 'so its shape is still maintained').toBe('active')
    } finally {
      clearInterval(renewing)
    }
  }, 90000)

  it('restores the live subscription set across a restart, so a claim taken before it can be released after', async () => {
    // A generous idle window: this test is about what the catalog restores, not about leases, and a
    // second-scale window would reclaim the restored claims (correctly) mid-assertion.
    h = await bootHarness(schema, {
      engineEnv: { CIRCUITS_SHAPE_IDLE_SECS: '120', CIRCUITS_RETENTION_SWEEP_SECS: '1' },
    })

    const first = await createShape(h, { table: 'items', subscription: 'sub-one' })
    await createShape(h, { table: 'items', subscription: 'sub-two' })
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(2)

    await h.restartEngine()

    await waitFor(
      async () => (await shapeInfo(first.shapeId)).subscriptions === 2,
      'both subscriptions to be restored from the catalog',
      20000,
    )
    // A subscription taken before the restart is releasable BY ID after it — that is what restoring
    // the set (rather than a count) buys: the caller's own name for its claim survived.
    expect((await release(first.shapeId, 'sub-one')).ok).toBe(true)
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(1)
    expect((await release(first.shapeId, 'sub-one')).ok, 'and releasing it again is still a no-op').toBe(true)
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(1)
  }, 90000)

  it('does not release a second claim when one committed release is appended twice', async () => {
    // The other half of the ambiguity, on the WRITER's side: a catalog append whose success
    // response is lost is retried, so the durable log holds the same `Left` twice. Folding both at
    // the next boot used to decrement the shared refcount twice and evict a shape that still had a
    // subscriber. A long idle window keeps leases out of it — this is about the fold.
    let proxy: LossProxy | undefined
    h = await bootHarness(schema, {
      engineEnv: { CIRCUITS_SHAPE_IDLE_SECS: '120', CIRCUITS_RETENTION_SWEEP_SECS: '1' },
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startLossProxy(upstreamUrl)
        return proxy
      },
    })

    const first = await createShape(h, { table: 'items', subscription: 'sub-one' })
    await createShape(h, { table: 'items', subscription: 'sub-two' })
    const third = await createShape(h, { table: 'items', subscription: 'sub-three' })
    expect((await shapeInfo(first.shapeId)).subscriptions).toBe(3)

    proxy!.loseNextCatalogResponse()
    expect((await release(first.shapeId, 'sub-one')).ok).toBe(true)
    await waitFor(() => Promise.resolve(proxy!.lostCatalogResponses() === 1), 'the committed response to be lost')
    await waitFor(
      async () =>
        (await readCatalogEvents(h!.dsUrl)).filter((e) => e.t === 'left' && e.subscription === 'sub-one').length >= 2,
      'the retry to append the committed Left a second time',
    )

    await h.restartEngine()

    // Two claims were taken and one was released, once — however many times the record reached
    // storage.
    await waitFor(
      async () => (await shapeInfo(first.shapeId)).subscriptions === 2,
      'the restart to restore both surviving subscriptions',
      20000,
    )
    expect((await release(first.shapeId, 'sub-two')).ok).toBe(true)
    const info = await shapeInfo(first.shapeId)
    expect(info.status, 'one remaining subscriber keeps the shared shape alive').toBe(200)
    expect(info.subscriptions).toBe(1)
    expect((await fetch(third.streamUrl)).status, 'and its stream stays readable').toBe(200)
  }, 90000)
})
