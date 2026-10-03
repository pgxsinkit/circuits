# 0010 — The harness client does not re-subscribe when its stream is retired

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `packages/client/src/index.ts`, `packages/client/src/subset.ts`
Reopen trigger: the first consumer that uses `packages/client` for live shapes, or a conformance test that needs a client to survive a retirement.

Carried over on 2026-09-29 from issue #17 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## Historical finding (recorded 2026-08-22)

ADR-0007 (`docs/adr/0007-retirement-closes-before-delete.md`) states the client contract as a _must_: a shape stream that is retired is **closed, then deleted**, so a subscriber sees `Stream-Closed` (on a read that reaches the close), then 404/410 once the delete lands, and must re-subscribe (a fresh `POST /shapes` → new stream). The engine half is done (every retirement path: evict, purge, failed restore, degraded reap, schema drift, epoch reset). `packages/client` does not yet act on any of the three signals — a `shape()` subscription whose stream is retired stops receiving and has no path back.

## Fix direction

2026-10-03 status refresh: the paragraph above records the earlier finding and is now too broad. Current `shape()` lease renewal calls the original create again and, if the returned handle changes, opens the replacement stream, swaps the materialization and rebinds listeners. The lease-renewal route therefore already supplies recovery after an evicted shape. A source spot check still found no direct stream-closure/404/410 handler which triggers that recreation independently of renewal. Before implementation, reproduce the remaining gap with the actual client, including a zero/non-renewing lease and retirement during a read; do not treat every retirement as an indefinitely stopped subscription. This refresh is not an executed client-level red test or a claim that all retirement recovery is complete.

Classify `Stream-Closed` / 404 / 410 in the client's read transport as **stream-gone** (distinct from 401/403 and from 503 degraded), re-create the shape through the same `POST /shapes` path the subscription used, reset the local fold, and continue; surface it as an event for callers that want to know. The conformance harness already exercises retirement (`conformance-retention.test.ts`, `conformance-schema-drift.test.ts`, `conformance-epoch.test.ts`) — add a client-level case to one of them.

## Not now because

No current consumer of this repository uses `packages/client` for live shapes (pgxsinkit has its own reader; its equivalent item is tracked there). Surfaced while implementing ADR-0007 (`docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).
