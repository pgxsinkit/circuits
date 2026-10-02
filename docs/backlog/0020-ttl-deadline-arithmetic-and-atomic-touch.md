# 0020 — TTL deadline arithmetic and touch/expiry decisions need one safe boundary

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/store.rs` (`expired`, `get`, recovered deadlines), `handlers.rs` TTL parsing/touch
Reopen trigger: an extreme TTL/expiry input reproduces overflow, a racing touch/expiry test loses a valid stream, or standalone TTL workloads need explicit expiry guarantees.

Urgency: conditional medium for standalone TTL workloads; the engine explicitly retires its own streams, and neither proposed failure was reproduced locally.

## Evidence

Source-confirmed, not runtime reproduced. Parsing accepts positive `u64` TTL; expiry uses unchecked `SystemTime + Duration`, and recovered absolute deadlines also need representable conversion. GET's lookup expiry decision and later request touch are separate operations. A racing expiry can use an old deadline instead of the touch that should keep the stream alive. The possible effects are panic/refusal for extreme input or premature retirement; no deployed occurrence was established.

Source: [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410). It introduces checked arithmetic and atomic touch/expiry; active bounded reaping is an optional separate feature. Existing close-only TTL behavior and CORS fixes were already local before this audit. The first two batches change retirement ownership, not the TTL decision itself.

## Boundary and regression requirements

Choose and document whether unrepresentable deadlines reject input or represent non-expiring state; use checked arithmetic consistently for request parsing, runtime deadlines and persisted recovery. A successful touch and retirement's deadline check must linearize under the same lifetime/shared-state boundary. Revalidate the exact incarnation, and keep failed expiry cleanup retryable rather than acknowledging deletion that did not land.

Test maximum inputs, explicit absolute expiry, persisted restart, renewal at the deadline, and a deterministic request-touch versus lazy-expiry race. Verify neither duplicate ancestor release nor unsafe deletion of pinned fork parents. Engine shapes use explicit retirement, so urgency is conditional on standalone TTL use; do not require importing a proactive reaper. Run both gates if implemented.
