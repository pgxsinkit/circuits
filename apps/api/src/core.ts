// Circuits "core": the logic behind the tRPC procedures. Writes append State-Protocol
// envelopes directly to the current segment of the durable-streams change log (decoupled from the
// engine, which tails it). Schema definition and shape lifecycle are forwarded to the Rust engine.

import {
  type AggregateDef,
  changesSegmentPath,
  type Op,
  type Row,
  type Schema,
  type ShapeDef,
  type SubsetDef,
  type SubsetResult,
  toTableEnvelope,
  type Value,
} from '@circuits/protocol'

export interface WriteInput {
  table: string
  op: Op
  pk: Value
  row?: Row
  txid?: string
}

export interface ShapeHandle {
  shapeId: string
  table: string
  streamPath: string
  streamUrl: string
  /** The subscription the create was recorded under (ADR-0008). See `@circuits/protocol`. */
  subscription?: string
  /** Seconds a subscription may go unrenewed before the engine releases it (`0` = never). */
  leaseSeconds?: number
}

export interface ElectricCore {
  readonly dsUrl: string
  defineSchema(schema: Schema): Promise<void>
  write(input: WriteInput): Promise<{ txid: string }>
  /**
   * Register a **materialized, live** shape (backfilled + maintained as a durable stream).
   *
   * `subscription` names this caller's claim (ADR-0008): repeating the create with the same id
   * renews it and returns the same handle rather than taking a second claim, which is what makes a
   * create safe to retry after an ambiguous failure. Omitted, the engine mints one and returns it —
   * with no idempotency on that first create.
   */
  createShape(def: ShapeDef, subscription?: string): Promise<ShapeHandle>
  getShape(id: string): Promise<ShapeHandle | null>
  /**
   * Release a subscription on a shape. Genuinely idempotent when `subscription` is given (releasing
   * an id the shape does not hold does nothing), which is why the client may retry it; without one
   * it is the engine's legacy anonymous decrement and a retry releases a second claim.
   */
  dropShape(id: string, subscription?: string): Promise<void>
  /** Run a one-shot **subset query** (ephemeral, non-materialized query-back from Postgres). */
  querySubset(def: SubsetDef): Promise<SubsetResult>
  /**
   * Open the **live tail** for a subset: a non-materialized, changes-only feed on the base predicate
   * (no backfill, no stored set). The client seeds rows from {@link querySubset} and applies this
   * feed's deltas, re-checking view membership — so paging never becomes server-side range state.
   */
  createSubsetFeed(def: Pick<SubsetDef, 'table' | 'where' | 'columns'>, subscription?: string): Promise<ShapeHandle>
  /** Register a scalar **aggregation** (COUNT/SUM/AVG/MIN/MAX) over a filter. Streams a single
   * value maintained incrementally. */
  createAggregate(def: AggregateDef, subscription?: string): Promise<ShapeHandle>
}

export interface CoreOptions {
  dsUrl: string
  engineUrl: string
  /** Injectable for tests; defaults to global fetch. */
  fetch?: typeof fetch
}

export function createCore(opts: CoreOptions): ElectricCore {
  const dsUrl = opts.dsUrl.replace(/\/$/, '')
  const engineUrl = opts.engineUrl.replace(/\/$/, '')
  const doFetch = opts.fetch ?? fetch
  const genTxid = () => globalThis.crypto.randomUUID()

  // Which change-log segment is current (ADR-0006). Cached — in library mode nothing rotates it —
  // and re-resolved whenever an append is refused because the segment was closed.
  let cachedSegment: number | undefined
  async function currentChangesSegment(refresh: boolean): Promise<number> {
    if (cachedSegment !== undefined && !refresh) return cachedSegment
    const res = await doFetch(`${engineUrl}/replication/lsn`)
    if (!res.ok) throw new Error(`engine /replication/lsn -> ${res.status}`)
    const body = (await res.json()) as { changes?: { segment?: number } }
    cachedSegment = body.changes?.segment ?? 0
    return cachedSegment
  }

  async function engineJson<T>(path: string, init: RequestInit): Promise<T> {
    const res = await doFetch(`${engineUrl}${path}`, {
      ...init,
      headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
    })
    if (!res.ok) throw new Error(`engine ${path} -> ${res.status}: ${await res.text()}`)
    return (await res.json()) as T
  }

  return {
    dsUrl,

    async defineSchema(schema) {
      await engineJson('/schema', { method: 'POST', body: JSON.stringify({ schema }) })
    },

    async write(input) {
      const txid = input.txid ?? genTxid()
      const env = toTableEnvelope(input.table, input.op, input.pk, input.row, txid)
      // Library-mode writes go on the single ordered change log (the envelope's `type` carries
      // the table) — same log the replication ingestor feeds in Postgres mode. The log is
      // segmented (ADR-0006), so the write addresses the CURRENT segment, which the engine
      // reports; a segment that turns out to be closed (409/404) means it rotated underneath us,
      // so re-resolve once and retry rather than lose the write.
      for (let attempt = 0; attempt < 2; attempt++) {
        const segment = await currentChangesSegment(attempt > 0)
        const res = await doFetch(`${dsUrl}/${changesSegmentPath(segment)}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify([env]),
        })
        if (res.ok) return { txid }
        if (res.status !== 404 && res.status !== 409 && res.status !== 410) {
          throw new Error(`append changes -> ${res.status}: ${await res.text()}`)
        }
        cachedSegment = undefined
      }
      throw new Error('append changes: the change log kept rotating away from under the write')
    },

    async createShape(def, subscription) {
      return engineJson<ShapeHandle>('/shapes', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          columns: def.columns ?? null,
          ...(subscription ? { subscription } : {}),
        }),
      })
    },

    async getShape(id) {
      const res = await doFetch(`${engineUrl}/shapes/${encodeURIComponent(id)}`)
      if (res.status === 404) return null
      if (!res.ok) throw new Error(`engine /shapes/${id} -> ${res.status}`)
      return (await res.json()) as ShapeHandle
    },

    async dropShape(id, subscription) {
      const query = subscription ? `?subscription=${encodeURIComponent(subscription)}` : ''
      const res = await doFetch(`${engineUrl}/shapes/${encodeURIComponent(id)}${query}`, { method: 'DELETE' })
      if (!res.ok && res.status !== 404) throw new Error(`engine DELETE /shapes/${id} -> ${res.status}`)
    },

    async createSubsetFeed(def, subscription) {
      return engineJson<ShapeHandle>('/shapes', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          columns: def.columns ?? null,
          changesOnly: true,
          ...(subscription ? { subscription } : {}),
        }),
      })
    },

    async createAggregate(def, subscription) {
      return engineJson<ShapeHandle>('/aggregate', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          fn: def.fn,
          col: def.col ?? null,
          ...(subscription ? { subscription } : {}),
        }),
      })
    },

    async querySubset(def) {
      return engineJson<SubsetResult>('/query', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          columns: def.columns ?? null,
          orderBy: def.orderBy ?? null,
          limit: def.limit ?? null,
          offset: def.offset ?? null,
        }),
      })
    },
  }
}
