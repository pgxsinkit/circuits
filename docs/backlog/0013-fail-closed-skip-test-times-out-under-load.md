# 0013 — The change-log "skips the envelopes a migration outran" test times out under load

Status: candidate (recorded 2026-09-29)
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

## Not now because

It fails rarely and a re-run passes. It is recorded so that the next failure is recognised, and
looked into, rather than re-run and forgotten.
