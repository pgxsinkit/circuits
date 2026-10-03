# 0031 — Tail watch publication can regress after close

Status: candidate (source review 2026-10-03; not locally reproduced)
Opened: 2026-10-03 · Area: `apps/durable-streams/src/handlers.rs` tail publication, long-poll and inline SSE
Reopen trigger: a delayed real append callback replaces a newer closed tail notification with an older open tail, and an inline SSE reader misses committed bytes or EOF.

Urgency: investigate after [0030](0030-checkpoint-tail-can-cross-a-rejected-append.md). Potential delivery/liveness defect; no executed counterexample or known production incident. The native engine uses long-poll, whose timeout rechecks shared state. Linux reactor SSE reads shared state directly and does not use the regressing watch payload.

## Source evidence and boundary

Independent review of [0029](0029-general-metadata-writers-capture-speculative-append-state.md) found a preexisting publication gap. `publish_durable_tail` advances `Shared.durable_tail` monotonically and captures `closed_durable` under the shared write lock, then releases it before updating the resident cache and calling `tail_tx.send_replace`. An earlier callback can pause after advancing the shared frontier. A later callback can publish a greater frontier, followed by a successful close. When the earlier callback resumes, it can replace the watch value with its lower byte count and `closed=false`; the monotonic shared check happened before that newer publication.

Waiting long-poll reads the watch payload directly, so it can lose prompt delivery/EOF notification, but its deadline path rechecks `st.tail()`. A newly opened long-poll checks shared state before subscribing and returns a known close immediately. Inline `SseSource::next` uses its watch payload for bytes and closure; shared reads only help its up-to-date/initial-control checks. Its idle heartbeat reports open and its total deadline ends without a shared close recheck. `handle_sse` initializes this receiver from the watch even when its initial shared tail is closed. A new inline SSE consumer after the stale replacement could therefore miss the later bytes/close until reconnect. Forked or tiered streams use this inline path; the Linux reactor reads shared `durable_tail`/`closed_durable` directly. Resident-cache replacement can also regress, with fallback reads providing bytes on a miss.

This pattern predates the dedicated close worker. Committed metadata capture does not qualify ordering of the separate watch/cache publication, and this entry does not claim a demonstrated failure or a selected design.

## Next reproduction and acceptance

Add a test-only hook immediately after the real shared frontier advancement and before cache/watch publication. Hold append A there, complete a later append B and close, then release A. Compare watch and shared state; directly drive inline `EventSource` chunks or use a real fork/tier stream to disable the reactor. Check delivery of B's bytes and terminal control, a waiting long-poll's prompt completion, and a new subscriber after closure. Always release hooks and join writers before assertions/teardown. Demonstrate the failed consumer behavior before implementation.

Consider serializing the frontier/cache/notification publication or deriving consumer decisions from authoritative shared state while keeping watch as a wake mechanism. These are options, not an established design. Preserve monotonic bytes/closure, cache-before-wake, chunk boundaries, close durability, delete identity notification, cancellation and cross-stream progress. Qualify reordered callbacks, concurrent close, inline/reactor SSE and long-poll, then run both validation gates.
