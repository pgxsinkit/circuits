# 0004 — The boot-errors test raced the engine's first retry warning

Status: dropped (fixed when recorded, 2026-09-29; kept for the symptom)
Opened: 2026-09-29 · Area: `packages/conformance/src/conformance-boot-errors.test.ts`,
`apps/engine/src/main.rs` (the boot retry loop)
Reopen trigger: `conformance-boot-errors.test.ts` failing intermittently again on a missing log
line.

## The fact

- "waits for durable-streams that is not up yet instead of exiting EX_CONFIG" failed
  intermittently at `expect(e.stderr()).toContain('durable-streams is unreachable')`: run alone
  three times, it failed twice.
- `/ready` reports `waiting` from the moment the engine is constructed, which is before its first
  connection attempt has failed and been logged. The test read the log as soon as it saw
  `waiting`, so it raced the first warning.
- The engine was not at fault, and nothing about it changed. The test case does not use the log
  server at all.

## What was done

The test now waits for the log line itself, with a deadline (`waitForStderr`), in the two cases
that read a retry warning after seeing `waiting`. Five consecutive runs of the file passed.
