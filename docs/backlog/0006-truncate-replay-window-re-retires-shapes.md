# 0006 — A TRUNCATE replayed after a crash re-retires the table's shapes

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `apps/engine/src/engine/drift.rs`, `apps/engine/src/engine/retirement.rs`, the catalog record
Reopen trigger: clients seen re-syncing a table after an engine crash, with a TRUNCATE in the change log just before it.

Carried over on 2026-09-29 from issue #12 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## The fact

A crash in the window between a `TRUNCATE`'s append to the change log and the slot acknowledgement of that commit (≤ 1 s, the status interval) re-delivers the TRUNCATE at the next boot, and the engine **re-retires** that table's shapes — including shapes created after the restart that already reflect the truncation.

## Consequence

One spurious resync of that table's shapes (their streams are closed then deleted; clients re-subscribe and get fresh snapshots). Correct, wasteful, and rare: it needs a crash inside a one-second window that contains a TRUNCATE.

## Fix direction

Retirement on TRUNCATE should only apply to shapes whose snapshot predates the TRUNCATE's position. That needs the shape's `SnapshotGate` (or its seed LSN) on the catalog record so the retirement can be fenced, or a sequencer round-trip during retirement. ADR-0005 notes the window.

## Not now because

Rare and self-correcting. Surfaced in the slice-2b review (`docs/notes/2026-08-21-upstream-issue-triage.md`, follow-ups).
