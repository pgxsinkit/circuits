# 0012 — vitest is held at 4.x by the protocol suite

Status: parked (recorded 2026-09-29)
Opened: 2026-09-29 · Area: `package.json` and `apps/durable-streams/package.json` (the `vitest`
pin), `apps/durable-streams/conformance/`
Reopen trigger: a release of `@durable-streams/server-conformance-tests` whose `vitest` range admits
5.x (or that makes `vitest` a peer dependency).

## The fact

- The workspace runs one vitest, pinned at `4.1.11`, the latest 4.x. The latest vitest is `5.0.2`
  (`bun info vitest version`, 2026-09-29).
- `@durable-streams/server-conformance-tests` `0.3.7`, its latest release and the version the log
  server is measured with, lists `vitest` `^4.0.0` as a **dependency**, not a peer. Its
  `runConformanceTests()` registers every test by calling `describe` and `test` from that vitest.
- With vitest 5 as the runner, the package gets its own vitest 4 beside it, and a vitest whose runner
  did not start cannot register tests. Tried on 2026-09-29 with `apps/durable-streams` on `5.0.2`:
  `bun run test:conformance` collected **0 tests** and failed the file with
  `TypeError: Cannot read properties of undefined (reading 'config')` at `runConformanceTests`.
- The harness and the protocol suite share one vitest version, so the root stays on 4.x with it.
  Nothing in the harness itself was tried on vitest 5.

## The options

- Wait for a suite release that accepts vitest 5, then move the whole workspace in one commit. This
  is the current state.
- Put the root harness on vitest 5 and leave `apps/durable-streams` on 4.x: two vitest majors in one
  workspace, which is what the dependency refresh of 2026-09-29 removed.
- Force vitest 5 onto the suite with a resolution override: the suite would run under a vitest it
  does not declare, and a failure there would say nothing about the log server.
