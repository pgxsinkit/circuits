// Native subset pages are ordered by PostgreSQL, while live deltas are classified against the
// loaded page boundary by the published client. Those two comparisons must agree for every text
// value or an unloaded row can enter (or a loaded row can leave) the window spuriously.

import type { Row, Schema } from '@circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { pgQuery, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, label: { type: 'text' }, n: { type: 'int' } },
      primaryKey: 'id',
    },
  },
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

describe('native subset text ordering', () => {
  it('uses PostgreSQL collation when classifying live rows against the loaded page boundary', async () => {
    h = await bootHarness(schema)

    // Under PostgreSQL C.UTF-8, U+E000 (UTF-8 EE...) sorts before U+1F600 (UTF-8 F0...).
    // JavaScript compares UTF-16 code units instead, where the emoji's leading surrogate D83D sorts
    // before E000. The public page and client-side live-window comparator therefore disagree.
    const postgresFirst = '\uE000'
    const javascriptFirst = '\u{1F600}'
    await pgQuery(h, 'INSERT INTO items (id, label, n) VALUES (1, $1, 0), (2, $2, 0)', [
      postgresFirst,
      javascriptFirst,
    ])
    await drainEngine(h)

    const sub = await h.client.subset({ table: 'items', orderBy: { col: 'label' }, limit: 1 })
    try {
      expect((sub.collection.toArray as unknown as Row[]).map((row) => row.id)).toEqual([1])
      expect(sub.hasMore()).toBe(true)

      // A change to a non-ordering column emits an upsert for the still-unloaded second row. The
      // loaded keyset window ends at row 1, so PostgreSQL says row 2 remains outside it.
      await pgQuery(h, 'UPDATE items SET n = 1 WHERE id = 2')
      await drainEngine(h)
      // A POSITIVE barrier, rather than a wait for row 2 to appear: waiting for the admission was
      // waiting for the DEFECT (a comparator that agrees with PostgreSQL never admits it, so that
      // wait can only time out once this is fixed). Instead, follow row 2's delta with one for a row
      // that IS in the window — the live feed applies them in order, so seeing this one proves the
      // feed has already processed past row 2's and declined it.
      await pgQuery(h, 'UPDATE items SET n = 1 WHERE id = 1')
      await drainEngine(h)
      await waitFor(
        () => (sub.collection.toArray as unknown as Row[]).some((row) => Number(row.id) === 1 && Number(row.n) === 1),
        'the live feed to apply an in-window update that FOLLOWS the out-of-window one',
      )

      expect(
        (sub.collection.toArray as unknown as Row[]).map((row) => row.id),
        'a live delta must use the database ordering and stay outside the not-yet-loaded page',
      ).toEqual([1])
    } finally {
      await sub.close()
    }
  }, 90000)
})
