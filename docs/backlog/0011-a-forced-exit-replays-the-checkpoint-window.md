# 0011 — A forced exit replays the checkpoint window onto shape streams

Status: candidate (recorded 2026-09-29)
Opened: 2026-09-29 · Area: `apps/engine/src/engine/sequencer.rs` (the lazy checkpoint),
`apps/engine/src/shutdown.rs`, `packages/conformance/src/conformance-shutdown.test.ts`
Reopen trigger: a consumer that cannot tolerate a replayed envelope, or a decision to make shape
appends idempotent.

## The fact

- The sequencer persists its position lazily: an `Offset` checkpoint at most every ~2 s of change,
  and only when a read returns. A shutdown that **completes** writes a final checkpoint, so a
  graceful restart replays nothing.
- An exit that cuts the shutdown short does not: a second signal during the grace period (exit 70),
  the grace period running out, a crash, a `SIGKILL`. The restart then resumes from the last lazy
  checkpoint and processes again every change after it, appending the same envelopes to the shape
  streams a second time.
- `shutdown.rs` says so: "A hard stop costs a replay (correct, but wasteful)". The `(lsn, seq)`
  highwater does not prevent it, because the highwater is restored from that same checkpoint.
- A replayed envelope is identical to the original. A reader that folds the stream converges to the
  same rows. A reader at the tail can see a key go back to an earlier value and forward again,
  within one replayed window.

## How it was found

`conformance-shutdown.test.ts` "a SECOND signal during the grace stops waiting and exits non-zero"
asserted that every key is on the stream exactly once after a forced exit. The sequencer stops at the
FIRST signal and sends its final checkpoint to the catalog writer at once, so the assertion held
whenever that append landed before the second signal ended the process. It passed in every local
run (24 of 24, idle and confined to one core) and in three runs on GitHub, and failed in the fourth
(run 36529448124), where the exit won the race: key `1` was on the stream twice.

Reproduced on purpose, 2026-09-29: a `SIGKILL` right after the first row was delivered, with no
`Offset` event yet in `meta/catalog`. After the restart and a second insert the shape stream held:

```
key 1  upsert  {"id":1,"label":"one","n":1}  txid 759  lsn 0/1BB9558
key 1  upsert  {"id":1,"label":"one","n":1}  txid 759  lsn 0/1BB9558
key 2  upsert  {"id":2,"label":"two","n":2}  txid 762  lsn 0/1BB96F8
```

The test now asserts what a forced exit promises: nothing lost, and anything appended twice says
exactly what it said the first time. The two graceful cases still assert exactly once.

## If exactly-once is wanted after a hard stop

Shape appends would have to be idempotent. The Durable Streams protocol has idempotent producers
(producer id, epoch and sequence), and the log server implements them: the sequencer would append
each transaction under a sequence derived from its change-log position, so a replayed append is
recognised and dropped by the log server.
