# 0021 — Direct DELETE terminal notification is not wired through SSE serving

Status: candidate (recorded 2026-10-02)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/handlers.rs` (`SseSource`), `sse_reactor.rs` (`produce`, wake), `store.rs` deletion watch
Reopen trigger: a direct-DELETE SSE test demonstrates retained delivery or delayed termination, or a consumer needs prompt terminal notification through SSE.

Urgency: low. There is a source gap but no actual SSE reproduction; normal engine close-before-delete already provides its terminal signal.

## What is proved and what is not

Baseline audit reproduced a caught-up **long-poll** remaining pending more than 500 ms after direct DELETE. Second batch fixes long-poll using a sticky deletion watch, including deletion before subscription and failed soft-delete rollback. That observation is not an SSE reproduction.

Current source still has a separate SSE path: inline `SseSource` watches `Tail`, and the Linux offloaded reactor's `produce` reads durable tail/close state and wakes on tail publication. The new deletion watch is not wired through those paths. They retain an Arc to an old incarnation; whether a real client stays connected or reads old bytes after DELETE requires a protocol test. This is a source gap/investigation, not proved SSE data loss or a measured timeout.

Source: upstream [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410)'s deletion signal for inline/reactor subscribers. The engine normally closes before deleting, which already wakes its subscribers; direct DS DELETE and TTL are distinct paths.

## Acceptance

Exercise real caught-up and behind-tail SSE on both inline/fallback and Linux offloaded reactor paths. Delete directly without close; assert documented terminal behavior and prompt permit/socket cleanup. Test deletion before registration, path recreation without following the new incarnation, soft-deleted pinned parents, and failed soft-delete metadata persistence that must not send a false terminal signal. Wire sticky lifecycle notification and reactor wake/termination only after that contract is chosen. Do not substitute the long-poll test for SSE evidence. Keep this independent of a log-server subscription manager and run both repository gates.
