# 0013 — The change-log "skips the envelopes a migration outran" test times out under load

Status: resolved (2026-10-04; fixture ordering repaired)
Opened: 2026-09-29 · Area: `packages/conformance/src/conformance-change-log-fail-closed.test.ts`
Reopen trigger: the test failing in a gate, locally or on GitHub.

## The fact

- "skips the envelopes a migration outran" waits for the engine's skip counter to move, and
  sometimes times out waiting.
- Run alone it passed 6 of 6. With the integration project's four workers it failed 1 run in 24 on
  `develop` (Rust 1.96.0) and 1 in 12 on the branch that prepares Rust 1.99, with the same symptom.
  So it is not caused by the dependency update, and it depends on load.
- Whether the engine is slow to count the skip or the test's deadline is too short for a loaded
  machine has not been established.

## Original deferral

It fails rarely and a re-run passes. It is recorded so that the next failure is recognised, and
looked into, rather than re-run and forgotten.

## Reopened during 0007 qualification (2026-10-04)

The full validation run stopped in the same case at `conformance-change-log-fail-closed.test.ts:223`:
`timed out waiting for the pre-drift envelopes to be skipped`. The integration project finished
with 252 passed and one failed across 62 files, including all three new 0007 process tests passing.
Format, typecheck, lint and all unit suites had already passed; protocol conformance had not yet
run because the integration script stopped. No production implementation changed in this batch.

The recorded trigger fired. Diagnosis was selected before re-running the full gate: construct
a focused red-capable loop and distinguish a proxy/fixture ordering failure from an actual
sequencer processing failure. No cause, timeout increase or corruption claim is established by
this recurrence alone. Executed evidence and the resolution are retained below.

## Controlled diagnosis

The original focused case passed once (9.44 seconds total), so a blind isolated rerun did not
explain the failure. A temporary test-only proxy gate held a retirement DELETE response after
storage answered. Registry removal was observable as shape 404 while inline drift remained
unfinished. Releasing change-log reads at that boundary reproduced the exact missing stale-schema
counter assertion twice. The second run also proved that the public table schema digest was still
old and the proxy held a page carrying that old digest; neither assertion failed before the
counter timeout. The diagnostic counter budget was shortened to two seconds for this pinned
loop, not increased. The failed full run used the original 20-second budget.

The parent-executed diagnostic command was:

```bash
env CIRCUITS_TEST_PIN_RETIREMENT=1 PATH=/usr/lib/postgresql/18/bin:/home/anton/.local/share/mise/shims:/home/anton/.local/bin:/home/anton/.cargo/bin:/usr/local/bin:/usr/bin:/bin bun run test:integration:harness packages/conformance/src/conformance-change-log-fail-closed.test.ts -t 'skips the envelopes'
```

It exited 1: one failed case, one intentionally filtered-out case, 8.05 seconds total.
The earlier worker-executed pinned red took 7.03 seconds. Both failed at the same semantic
counter assertion. A separate un-escalated fixture start failed because the sandbox prohibited
IPv4 TCP sockets (`Operation not permitted` in the runtime Postgres log); it was not a semantic
red. Two subsequent worker invocations were interrupted while awaiting approval and supplied
no session/result, so they are not counted as executions. The parent then owned execution.

Ranked hypotheses were premature read release on registry 404, a proxy failing to hold the
old page, stale classification failing despite a replaced schema, and ordinary processing/counting
delay. The positive causal probe kept the same held retirement schedule and two-second counter
assertion, but released retirement while keeping reads held, waited for the published schema
digest to change, and only then released reads. It passed (one case, 9.78 seconds total), with
the real sequencer's stale-schema skip warning and counter. This establishes an insufficient
fixture ordering barrier; it does not demonstrate a production decoding or corruption defect.

## Implemented repair and limits

The permanent cases require a held old-digest page before issuing the migration, then wait
for a changed public schema digest before releasing reads. Ordinary and deliberately held
retirement-response schedules both retain the original 20-second skip-counter assertion,
readiness, no-park and fresh-shape convergence checks. The separate corrupt-envelope
park/restart/reset lane remains intact. Temporary environment pinning and the shortened diagnostic
deadline are removed; no production source or dependency configuration changed.

The failed full run did not capture these digest/page probes, so this diagnosis does not claim
every historical timeout had the same cause. The held-response schedule proves the unsafe
barrier directly and makes its correction executable without depending on machine load. Fixture
page buffering is for these small test payloads, not a new production memory bound. The final
focused invocation passed all six cases across this three-case file and 0007's three-case file
(13.02 seconds total). Independent review found no blocker. No temporary environment pin,
shortened assertion budget or debug marker remains in the test source.

Final `bun run validate:full` passed format, typecheck, lint and all unit suites, all 254
integration cases across 62 files (including these three cases), and 332 protocol cases.
The existing two Rust ignores and six protocol skips remain. This resolves the demonstrated
fixture ordering defect; a future skip-counter failure still fires the reopen trigger.
