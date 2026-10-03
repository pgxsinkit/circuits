# 0032 — CLI readiness accepted before the first PUT connection was refused

Status: parked (one occurrence 2026-10-03; not reproduced in bounded qualification)
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
