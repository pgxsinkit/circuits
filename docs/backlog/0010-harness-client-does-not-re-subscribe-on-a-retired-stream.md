# 0010 — The harness client does not re-subscribe when its stream is retired

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `packages/client/src/index.ts`, `packages/client/src/subset.ts`
Reopen trigger: the first consumer that uses `packages/client` for live shapes, or a conformance test that needs a client to survive a retirement.

Carried over on 2026-09-29 from issue #17 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## The fact

ADR-0007 (`docs/adr/0007-retirement-closes-before-delete.md`) states the client contract as a _must_: a shape stream that is retired is **closed, then deleted**, so a subscriber sees `Stream-Closed` (on a read that reaches the close), then 404/410 once the delete lands, and must re-subscribe (a fresh `POST /shapes` → new stream). The engine half is done (every retirement path: evict, purge, failed restore, degraded reap, schema drift, epoch reset). `packages/client` does not yet act on any of the three signals — a `shape()` subscription whose stream is retired stops receiving and has no path back.

## Fix direction

Classify `Stream-Closed` / 404 / 410 in the client's read transport as **stream-gone** (distinct from 401/403 and from 503 degraded), re-create the shape through the same `POST /shapes` path the subscription used, reset the local fold, and continue; surface it as an event for callers that want to know. The conformance harness already exercises retirement (`conformance-retention.test.ts`, `conformance-schema-drift.test.ts`, `conformance-epoch.test.ts`) — add a client-level case to one of them.

## Not now because

No current consumer of this repository uses `packages/client` for live shapes (pgxsinkit has its own reader; its equivalent item is tracked there). Surfaced while implementing ADR-0007 (`docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).
