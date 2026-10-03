# 0032 — CLI readiness accepted before the first PUT connection was refused

Status: parked (two occurrences 2026-10-03; cause unresolved after bounded diagnostic qualification)
Opened: 2026-10-03 · Area: `apps/durable-streams/tests/cli_durability_guards.rs`, readiness and child-process diagnostics
Reopen trigger: `direct_delete_terminates_a_waiting_long_poll_over_http` again loses its first connection after readiness, or child/port diagnostics establish a repeatable startup defect.

Urgency: unproved startup/test lead, with no known production incident. Do not treat a passing rerun as proof the original failure was harmless or fixed.

## Executed evidence

During the first `bun run validate` for [0031](0031-tail-watch-publication-can-regress-after-close.md), all 205 log-server unit tests passed (two existing ignores). Four of the five durability/HTTP CLI tests passed, but `direct_delete_terminates_a_waiting_long_poll_over_http` panicked at line 241: the first PUT's `TcpStream::connect` returned `ConnectionRefused` (`Os { code: 111 }`). The preceding `wait_until_listening(port)` probe had already completed a TCP connection, without identifying the listening process. The test never sent that PUT, registered its waiting long-poll or issued DELETE; it did not exercise the changed runtime publication helper.

The original failure command exited 101. Its server stdout/stderr are suppressed and the cleanup guard kills/reaps the child without retaining its natural status at the failed request. No child exit reason, port identity or server error was captured. The temporary test directory was removed on unwind, so this is an observation rather than a retained process/crash image.

`bun run test:durable-streams --test cli_durability_guards direct_delete_terminates_a_waiting_long_poll_over_http -- --nocapture` subsequently passed 21 consecutive times. The complete `cli_durability_guards` file passed three times with default parallelism, five tests per run. No source, diagnostic or request-retry changes were made. These bounded runs did not reproduce the failure and do not identify its cause.

## Candidate causes and next diagnosis

Root presented three falsifiable candidates before follow-up probes:

- Lost port reservation: `unused_local_port()` releases its bound listener before the child binds, and readiness accepts any listener at that address. Concurrent child stderr/status could establish an address conflict or a probe reaching the wrong process.
- Child exit after readiness: retaining its `try_wait()` status and stderr at the first failed request could identify a startup/resource/server failure even with exclusive port ownership.
- Readiness-probe interaction: the probe connects and immediately closes without an HTTP request. Comparing that with a request-bearing probe would distinguish a serving-path interaction if the symptom repeats.

On recurrence, capture the child stderr in memory or inherit it directly, inspect its status at the failed request, and retain the port/child identity. Keep process cleanup bounded, release/reap the child on failure, and do not turn real server errors into successful request retries. Establish an actual red feedback loop before selecting a fix. If exclusive ephemeral binding requires a port-zero startup contract, first inspect the server's bound-address reporting; do not assume the configured `:0` address names the assigned port.

[0014](0014-log-server-lock-test-failed-once-under-load.md) concerns a different second-server lock-test symptom sharing the same port helper. This observation does not establish recurrence of that symptom or fire its reopen trigger. [0031](0031-tail-watch-publication-can-regress-after-close.md) records the validation failure and subsequent gates separately from its reproduced consumer defect.

## Recurrence during 0011 qualification (2026-10-03)

The reopen trigger fired during the final full gate for [0011](0011-a-forced-exit-replays-the-checkpoint-window.md).
All 225 log-server unit tests passed (two existing ignores), then this CLI case again refused its
first PUT connection at line 241; four of the five CLI cases passed. This run exited 101 before the
TS unit, integration and protocol stages. The earlier full attempt had passed the CLI and all 250
integration cases before a separately reproduced sequence-conflict protocol regression, since fixed.
Neither occurrence of this startup refusal exercised DELETE or the new ordinary sequence waiter.

The failing fixture still discarded child stdout/stderr and accepted a TCP connection without
checking child identity or status, so this recurrence also retained no exit reason. Read-only source
review found one bind after initialization and continuous ownership of the same listener by the
accept loop; there is no startup listener-transfer gap. That eliminates a proposed structural
explanation, not the remaining causes. Ranked candidates for the bounded diagnostic qualification
are false readiness (a competing listener or a probe whose local/peer endpoints coincide), child
exit after readiness, and loss of serving lifetime after a probe that reached the child. The
diagnostic amendment records endpoints, child PID/configured port and status, and inherits stderr;
requests remain single attempts. Successful readiness now also checks that the child remains alive,
but a live child does not prove the accepting listener belongs to it. Panic cleanup still kills and
reaps the child before removing its data directory. No production serving code or request-retry
policy changed.

With these diagnostics, the complete five-case CLI file passed five consecutive runs with default
parallelism (25 passing cases, 5.12–5.13 s per file). Read-only review found no diagnostic or cleanup
blocker. This does not identify or repair the two original refusals; their stderr and probe endpoints
cannot be recovered retroactively. This entry stays parked with an explicit recurrence trigger. A
new failure should now retain substantially better evidence before selecting a readiness or serving
repair; the self-connected-probe candidate has not been established by a controlled experiment.

The final `bun run validate:full` for 0011 subsequently passed all five durability CLI cases and
the three other CLI cases, as well as all unit, integration and protocol stages. That green gate
permits closing the separately reproduced delivery repair; it does not resolve this startup lead.
