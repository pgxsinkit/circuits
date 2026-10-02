# 0027 — Gate shutdown checkpointing on an actually completed read

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/sequencer.rs` shutdown checkpoint; boot/catalog restore
Reopen trigger: a never-read sequencer can be started outside the protected restore sequence, a regression removes the boot gate, or a checkpoint test demonstrates an unread task overwriting a valid durable position.

Urgency: parked defense in depth. Existing boot gates prevent the upstream incident, and no corresponding local stale-cursor failure was demonstrated.

## Evidence and priority

Defense-in-depth candidate, not a reproduced local stale-cursor incident. Shutdown checks `reading`, which denotes permission to read, rather than whether a successful unpaused read completed. A permitted reader that only sees failures can still emit its start position. Upstream [#28](https://github.com/indexedlabs/electric-circuits/pull/28), merge [9bd5b3e15d5b07033dd938d27ed834305b7c4bba](https://github.com/indexedlabs/electric-circuits/commit/9bd5b3e15d5b07033dd938d27ed834305b7c4bba), fixed a real startup race and unread shutdown checkpoint there.

Local `ensure_booted`, held restore sequencer and `SchemaIsPostgres` already prevent that upstream racing-create/`changes/0` incident. Neither audit batch removes those protections, and no local corrupt durable restart position was established. Do not classify the whole upstream PR as missing.

## If reopened

Track actual completed unpaused reads, and pin the shutdown checkpoint test to a previously valid catalog position while reads fail or restore is rolled back. Also verify a successful read still writes final position/highwater, a held transaction never advances the published floor, parked schema-compatible corruption preserves its rewind, and epoch reset/discard semantics remain deliberate. This guard does not provide exactly-once delivery after a forced stop; [0011](0011-a-forced-exit-replays-the-checkpoint-window.md) remains separate. Run both gates for implementation.
