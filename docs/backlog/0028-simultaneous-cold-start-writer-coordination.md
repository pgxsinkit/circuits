# 0028 — A slot-busy startup guard is not distributed writer ownership

Status: parked (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/engine/src/engine/mod.rs` setup, `engine/epoch.rs`, deployment topology
Reopen trigger: the deployment deliberately allows overlapping engine instances, or a simultaneous-cold-start test proves two instances passing slot checks and performing stateful setup against one catalog.

Urgency: parked optional ownership work. Under the required single-instance deployment, no simultaneous-cold-start defect was reproduced.

## Fixed behavior and remaining choice

Second batch waits on `Verdict::Busy` before stateful restore/arrangements/ingest and before publication/replica-identity mutation. Upstream source: [8b63db5e4642fb26967c8cd18cfd776c39e49db0](https://github.com/indexedlabs/electric-circuits/commit/8b63db5e4642fb26967c8cd18cfd776c39e49db0). Focused startup and existing epoch tests cover a slot already held by another walsender.

Checking that a slot is currently unheld does not atomically reserve catalog/shape writer authority. Two cold starters might both pass before either acquires the replication connection; that specific local race was not reproduced. This is an optional defense/deployment lead, not a claim the current one-instance topology fails. The [deployment guide](../deployment-postgres.md)'s one replica and `Recreate` requirement remains in force.

## Decision and acceptance

On the trigger, force both starters to the same pre-acquisition barrier with one source/catalog/log server; observe publication mutations, arrangement seeding, catalog writes, stream identity allocation and which process ingests. Do not infer safety from only one eventual walsender winning.

If overlapping writers are needed, choose an ownership/lease or handoff contract with fencing generations and fail-closed loss handling before adopting upstream [#10](https://github.com/indexedlabs/electric-circuits/pull/10)'s managed revision system. Prove that a losing or expired owner cannot append/catalog/create after transfer, with crash/restart and connection-loss tests. Multi-source hosting, runtime authority receipts and TLS are separate product decisions; no wholesale upstream orchestration import is authorized. Keep slot epochs and client-promised catalog durability intact and run both gates on implementation.
