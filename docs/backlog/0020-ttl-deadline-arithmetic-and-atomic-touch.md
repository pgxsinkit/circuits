# 0020 — Checked TTL deadlines and atomic touch/expiry

Status: resolved (fixed 2026-10-03)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/store.rs`, `handlers.rs`
Reopen trigger: an accepted renewal is lost to expiry, a representable persisted deadline fails recovery, or standalone TTL workloads require an active reaper.

Urgency: resolved for the demonstrated deadline and stale-expiry defects; active reaping remains an optional feature. The engine explicitly retires its own streams.

## Reproduced defects

The earlier source-only candidates were reproduced on the local implementation:

- PUT with `Stream-TTL: 18446744073709551615` returned `201`, publishing a stream with an unrepresentable deadline, instead of rejecting the input.
- A deterministic real `Store::get` lookup paused after observing expiry, while an already admitted append completed its renewal and released its operation guard. Resuming the lookup discarded the valid stream using the old observation.
- Recovery accepted a parsed sidecar with an unrepresentable TTL. Unchecked absolute and last-access UNIX conversions shared the same invalid-deadline boundary.

Source: [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410). The local repair adopts checked arithmetic and atomic renewal without importing a reaper or subscription manager.

## Resolution and semantics

Request parsing and the store's create boundary reject an unrepresentable TTL before identity or file allocation. RFC3339 conversion uses checked addition. Recovery validates absolute expiry, last access and sliding deadlines before publishing a stream; invalid sidecars follow the existing quarantine policy, retaining the data, reserved identity and WAL evidence across restarts.

Expiry is inclusive (`now >= deadline`). A GET's expiry check and sliding renewal share the stream lifetime/shared-state locks. HEAD and fork-source lookup do not renew the TTL; a rejected POST does not renew it. Matching PUT renews only a still-alive, unfenced stream, queues metadata persistence, and refuses an expired incarnation rather than reviving it.

Lazy expiry takes nonblocking retirement/metadata barriers and rechecks the deadline under the lifetime boundary before fencing. Busy admitted operations leave expiry retryable. A completed append or accepted read renewal therefore wins over a stale observation. Owned cleanup revalidates the exact incarnation, drains no async operation from a blocking worker, and retains fork-parent bytes until their last child is removed.

Soft expiry publishes its terminal watch only after successful metadata persistence; failure restores admission without an irreversible reader event, allowing access to retry. A failed physical removal keeps the hard-delete fence and stream identity for explicit DELETE retry. Successful physical removal releases ancestor references once. The obsolete non-durable deletion helper, whose detached soft write ignored failures, was removed.

TTL renewal remains metadata-sweep persisted rather than synchronously durable on every GET or matching PUT; a crash before that sweep may restore the older deadline. This repair preserves that existing durability contract. Expiry remains lazy: no active reaper was introduced.

## Regression coverage

Rust tests cover maximum TTL request/store input; all three invalid persisted deadline fields across two recovery passes; RFC3339 maximum-year absolute expiry and restart; persisted sliding renewal; inclusive expiry and renewal immediately before the deadline; deterministic stale-expiry versus admitted-append completion; HTTP HEAD/fork/rejected POST versus matching PUT renewal; failed soft expiry without terminal notification; pinned parent and grandparent lifetime; failed hard unlink, explicit retry and stale-incarnation safety. Existing close-only TTL and retirement/GC regressions remain applicable.

Focused command: `bun run test:durable-streams ttl_` passed all ten matching regressions. Both required gates, `bun run validate` and `bun run validate:full`, passed. The final full gate includes 164 log-server unit tests and eight CLI tests, 240 engine integration cases and 332 protocol cases. Two Rust tests and six protocol cases retain their existing ignores/skips. See the [resolution ledger](0016-indexedlabs-audit-resolution-ledger.md) for the combined batch record.
