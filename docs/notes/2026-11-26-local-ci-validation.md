# Local and GitHub validation

## Decision

Use `bun run validate:full` as the explicit local pre-release check. GitHub and the local command should use the same suite definitions, build profiles and five log-server configurations. Keep the existing pre-commit check and manual push workflow.

## Why the previous local pass missed the failure

At commit `1847ddb`, GitHub passed the Rust job, the TypeScript unit tests and all five log-server protocol configurations. One of 262 engine conformance tests failed. The local full check ran this same test and passed. [GitHub job](https://github.com/pgxsinkit/circuits/actions/runs/45745054092/job/139658962859)

The purge test observed a successful close notification on the waiting long-poll. Its next GET required an immediate 404 and received 410. Purge acknowledges durable retirement intent. The log server can return 410 after it marks a stream for deletion and before it removes the stream from its map. An existing engine unit test deliberately blocks deletion and verifies that purge can return in the meantime. The integration test should check the purge acknowledgement, the close notification and eventual 404 with a deadline. Keeping the final 404 catches cleanup that never finishes. [Retirement contract](../adr/0007-retirement-closes-before-delete.md)

## Differences to close

| Check | Local full check at `1847ddb` | GitHub at the same commit |
| --- | --- | --- |
| Log-server Rust tests | Cargo test profile | Locked release-durable-streams profile |
| Log-server protocol suite | Default WAL | Default WAL, resident cache, forced read offload, memory, small chunks |
| Engine harness | Debug binaries, PostgreSQL 18, four Vitest workers | Same |

Both machines ran Ubuntu 26.04.1, Rust 1.99.1, Bun 1.4.5, Node 26.13.1 and PostgreSQL 18.7. The PostgreSQL distribution package revisions differed. The local process saw eight CPUs. The historical CI log records a hard file-descriptor limit of 65,536 and omits the CPU count. One local run restricted to four CPUs passed the purge test. Matching resources alone cannot establish ordering between a purge response and storage cleanup.

GitHub specifies four CPUs and 16 GB RAM for the standard public-repository Ubuntu 26.04 x64 runner used here. [GitHub runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories)

## Acceptance checks

- Exercise the ordering that exposed the purge failure on demand. Hold deletion open, verify that purge has acknowledged and the waiting reader got its close notification, release deletion, then require the stream to reach 404 within a deadline. Check nearby lifecycle tests for the same assumption about acknowledgements. A regression that leaves the stream behind must fail.
- Keep ordinary host concurrency. Use a runner-sized CPU budget as an additional way to exercise the affected tests. Record the suite, configuration and resource settings on failure so the failed case can be run by itself.
- Build the release log server once and reuse it for all five protocol configurations. Give each configuration fresh storage. A failed run should retain its original verdict.
- Start the two candidate images with a persistent log volume and Emergent’s public CloudNativePG PostgreSQL 19.0 image, pinned by digest from its `infra/charts/postgres/values.yaml`. Verify an initial row, a replicated update, continuity across a restart, a second update and retirement. Publish the tested image IDs. Emergent owns qualification of its private extension bundle and dependent services.
- Use PostgreSQL 19 for the whole native harness. CI installs it from the PostgreSQL project’s Ubuntu repository. The integration harness uses fast ephemeral clusters, while the image test verifies PostgreSQL durability settings and restarts the persistent database. Both run on the existing standard public-repository runner.

## What qualification exposed

- Parallel HTTP tests could miss the first refusal warning when another request registered its tracing callsite. One subscriber for the test binary and a separate capture buffer per request preserve the assertion under concurrency.
- The membership circuit identified abandoned spill directories by PID. A concurrent run in another PID namespace removed live files. An OS lock now proves ownership, and a two-process regression covers both colliding PIDs and crash cleanup.
- PostgreSQL 19 slot creation in database B waited on an open write transaction in database A. This reproduced the engine startup timeout twice. Giving each integration file its own cluster preserves the deliberate idle-WAL test and the startup deadline.
