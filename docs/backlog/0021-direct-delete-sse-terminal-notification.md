# 0021 — Direct DELETE terminal notification is not wired through SSE serving

Status: resolved (fixed on `develop`, 2026-10-03)
Opened: 2026-10-02 · Area: `apps/durable-streams/src/handlers.rs` (`SseSource`), `sse_reactor.rs` (`produce`, wake), `store.rs` deletion watch
Reopen trigger: an existing SSE consumer remains pending after terminal deletion, follows a re-created path, or reports durable closure for a deletion; failed soft-delete persistence falsely terminates a consumer; a retained fork loses its inherited data.

Original urgency: low, based on a source gap without an executed SSE reproduction. The trigger fired on 2026-10-03: two actual SSE consumers remained pending after DELETE. Normal engine close-before-delete already supplies a separate terminal signal.

## What is proved and what is not

Baseline audit reproduced a caught-up **long-poll** remaining pending more than 500 ms after direct DELETE. Second batch fixes long-poll using a sticky deletion watch, including deletion before subscription and failed soft-delete rollback. That observation is not an SSE reproduction.

At the audit baseline, inline `SseSource` watched only `Tail`, and the Linux offloaded reactor read durable tail/close state and woke on tail publication. Neither observed the deletion watch. Both retain an Arc to the old incarnation.

Source: upstream [5093702ce3a55007fe201b3615f12d6f65007410](https://github.com/indexedlabs/durable-streams-rust/commit/5093702ce3a55007fe201b3615f12d6f65007410)'s deletion signal for inline/reactor subscribers. The engine normally closes before deleting, which already wakes its subscribers; direct DS DELETE and TTL are distinct paths.

## Executed diagnosis

`bun run test:durable-streams e2e_direct_delete_sse_ -- --nocapture` failed twice, with zero passes and two failures each time, without compile or setup corrections. The inline fixture creates a real fork to force fallback serving, consumes its initial caught-up control and polls the next chunk to Pending before direct DELETE. The reactor fixture hands a real handler registration to a localhost TCP socket and reads its initial control before DELETE. Both DELETEs return 204, publish sticky deletion and remove the public stream while leaving durable closure false. Inline `next_chunk` and reactor `read_to_end` still time out after 500 ms. Reactor client-drop and bounded permit reacquisition complete before final assertions; cleanup cannot turn a timeout verdict green.

The ranked hypotheses were missing deletion observation, missing reactor wake, and pending-output/registration ordering. An inline-only observation probe made inline green while the reactor remained red. This distinguishes the consumer paths rather than using the earlier long-poll failure as SSE evidence.

## Terminal contract

The [audited protocol at its fixed commit](https://github.com/durable-streams/durable-streams/blob/a172acc389351cb3db6deb5cd60e3dec11e7ff39/PROTOCOL.md) permits in-flight read termination after deletion (§5.4); soft-deleted parents return 410 for direct operations while preserving fork data. Its SSE closure signal (§5.8) describes durable closure at the final offset. It does not prescribe a deletion event or a prompt timeout. Prompt transport/source termination is this server's chosen liveness behavior, not a quoted protocol MUST.

Existing consumers stop on the old incarnation's sticky deletion signal without inventing `streamClosed`. A new request resolves through the store's ordinary 404/410 behavior. A re-created path cannot attach an old consumer to the new incarnation. Bytes already emitted or an event completed concurrently with deletion cannot be recalled.

The reactor aborts a connection with queued or partly written output instead of waiting for a slow reader or inserting a terminator into a partial HTTP frame. With no queued output, it attempts the ordinary HTTP zero chunk once and closes even if the write is partial or blocked. Thus a deleted SSE response can be truncated; consumers reconnect from their last complete event/offset. Subscriber cleanup does not depend on draining old backlog.

## Resolution and qualification

Inline `SseSource` owns a deletion receiver, checks it before tail decisions, races both capped-read preparation and range reads against it, checks again before framing, and wakes on it while idle. Terminal publication now wakes the reactor after releasing the watch value lock. The reactor checks the old incarnation before production, after a read, before every write continuation and before tick handling, including subscribers already marked done. Late intake checks the sticky value even when no subscriber list existed at deletion. Its existing close path unlinks the subscriber, advances the slot generation and releases the permit.

Eight new real-handler regressions are in `wal/e2e_tests.rs`:

- The original caught-up inline source and seated reactor socket terminate after direct DELETE without a fake closure event.
- Inline sources admitted before deletion, both caught-up and behind-tail but unpolled, suppress their first frame after deletion and never follow a re-created path; a fresh source delivers only the replacement's bytes.
- Late reactor registrations at both `now` and offset zero retain the old identity after deletion/recreation, terminate without either incarnation's data, and release their permits.
- Failed soft-delete persistence leaves both inline and seated reactor consumers waiting, then a subsequent append reaches each. Successful soft deletion ends the parent's consumer while a child still reads exactly its inherited prefix and own suffix, excluding later parent appends.
- A real sealed/offloaded range read is paused in a wrapper around the local BlobStore backend. DELETE ends its actively pending source before the backend is released. This qualifies cancellation of read preparation, not cancellation of arbitrary already-dispatched storage IO.
- A real behind-tail reactor starts a 1 MiB frame on a socket with a constrained send buffer. DELETE releases its permit within 500 ms before the client drains, then the peer observes EOF or reset with only a prefix received. The test accepts deliberate transport truncation and rejects draining the complete old frame.

All eight passed with the original red-pair scheduling preserved. A qualification setup assertion initially saw one byte (`0`) left from its initial TCP control frame, with the deletion marker still false. Consuming a complete fixture frame before the idle check and before the positive append/delete sequence corrected that setup; no production change was made for it. The fixture helper is bounded by its callers and tailored to those fixed one-chunk payloads, not a general HTTP parser.

Independent review found no blocking issues. Hard-removal failure and lazy TTL expiry use the existing common terminal publication boundary; these eight fixtures do not independently inject those branches through SSE. There is no active expiry reaper, new subscription manager, new dependency, or promise to recall already transmitted data. Normal engine close-before-delete and durable-close framing remain separate and are checked by the complete suites.

Both required gates passed on the final source: `bun run validate` and `bun run validate:full`. They cover 425 engine Rust unit tests and its other binaries, 213 log-server unit tests, eight CLI cases, 82 TS unit tests, 246 engine integration cases across 61 files and 332 protocol cases. Existing two Rust ignores and six protocol skips remain. The focused eight-case loop and complete log-server suite also passed. No dependencies, tools or branches changed; output was captured directly.
