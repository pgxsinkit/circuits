// The State-Protocol change-event envelope that travels on every table/shape durable stream
// and that `@durable-streams/state`'s createStreamDB consumes. `type` is the table's canonical
// `schema.name` (the collection discriminator — always qualified, see ADR-0002), `key` is the
// stringified primary key, `headers.operation` is the op. See decisions D4.

import { canonicalTable } from "./sql.js";
import type { Op, Row, Value } from "./types.js";

export type Operation = "insert" | "update" | "delete" | "upsert";

export interface StreamEnvelope {
  /** The table's canonical `schema.name` (ADR-0002); never a bare name. */
  type: string;
  key: string;
  /** Present for insert/update/upsert; omitted for delete. */
  value?: Row;
  headers: {
    operation: Operation;
    txid?: string;
    /** Stamped by the server on read; never sent by producers. */
    offset?: string;
    /**
     * Postgres commit LSN (`"HI/LO"` hex) of the change, stamped by the engine on live shape/feed
     * envelopes. Lets a subset client position its live tail at the page snapshot — drop deltas with
     * `lsn < snapshotLsn`. Absent on backfill rows and in library (no-Postgres) mode.
     */
    lsn?: string;
    /**
     * Position of the change within its transaction, stamped by the replication ingestor on
     * table-stream envelopes. `(lsn, seq)` uniquely identifies a change so the engine tailer can
     * skip duplicates from the ingestor's at-least-once redelivery. Not present on shape streams.
     */
    seq?: number;
    /**
     * Transaction-end marker: `true` on the LAST envelope of a transaction, and only there
     * (`docs/adr/0003-ingest-pgoutput-v1-with-spill.md`). The engine's sequencer holds back a
     * trailing `(txid, lsn)` run that is not terminated by it, because a commit too large for one
     * request body reaches the change log as several appends and must still be fanned out — and
     * flushed to shape streams — as ONE transaction. Library-mode writers set it on every envelope
     * (each call is a one-envelope transaction).
     */
    last?: boolean;
  };
}

/**
 * Build the table-stream envelope for an ingest write. `table` accepts the bare-name sugar; the
 * envelope's `type` is always the canonical `schema.name` (ADR-0002), matching what the replication
 * ingestor writes for the same table.
 */
export function toTableEnvelope(table: string, op: Op, pk: Value, row?: Row, txid?: string): StreamEnvelope {
  // `last: true` — one call is one (one-envelope) transaction. The engine's sequencer holds back a
  // run that no envelope terminates (ADR-0003), so a producer that omitted this would be held
  // forever waiting for a chunk that is never coming.
  const headers: StreamEnvelope["headers"] = { operation: op, last: true };
  if (txid !== undefined) headers.txid = txid;
  const env: StreamEnvelope = { type: canonicalTable(table), key: String(pk), headers };
  if (op !== "delete" && row !== undefined) env.value = row;
  return env;
}
