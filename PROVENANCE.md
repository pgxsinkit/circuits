# Provenance

This repository was made on 2026-09-29 from two others. Both were forks of projects ElectricSQL
started and no longer maintains. It is maintained here, and tracks neither.

## The engine (`apps/engine`, and the test harness in `packages/` and `apps/api`)

|              |                                                                                             |
| ------------ | ------------------------------------------------------------------------------------------- |
| Came from    | `pgxsinkit/electric-circuits` at `64ebd28c39056744a07e5a258c7163a5d5cbc042`                 |
| Which forked | `electric-sql/electric-circuits` at `b784aaf83b4951215ef58cfdb56660c496b9cf43` (2026-07-23) |
| Licence      | MIT or Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`)                                         |

The history up to `64ebd28` is that repository's, unchanged: upstream's commits, then 47 of ours.

## The log server (`apps/durable-streams`)

|                          |                                                                                                             |
| ------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Came from                | `pgxsinkit/durable-streams-rust` at `ab1a84bad9d4d34912db3f4bedea19f5e79b1bba`                              |
| Which was extracted from | `electric-sql/electric` at `dc07a1e6c8ff459b59ce407cdb6bf2f7fd068f36`, path `packages/durable-streams-rust` |
| Licence                  | Apache-2.0 (`apps/durable-streams/LICENSE`, `apps/durable-streams/NOTICE`)                                  |

Its 30 commits were replayed on top of the engine's history, each with its files placed under
`apps/durable-streams/`. Authors and author dates are the originals. Each replayed commit names the
commit it came from in a `Replayed-From:` trailer, and the tree of the last one is identical to the
tree it was replayed from.

[apps/durable-streams/PROVENANCE.md](apps/durable-streams/PROVENANCE.md) is that repository's own
record, kept as it was written. It describes the extraction and the conformance results at the time.
Its sections on publishing, on the old image workflow and on syncing from upstream describe a
repository that tracked one, and no longer apply.

## What was left behind

The benchmarks, the load generator, the pipeline visualiser, the examples and tutorials, the log
server's npm packaging, and the container builds for them. They remain in the two repositories
above, at the commits named here.

## The protocol

The Durable Streams protocol, its client and its server conformance suite are maintained separately
at `durable-streams/durable-streams`, mirrored at `pgxsinkit/durable-streams`. The log server is
tested against `@durable-streams/server-conformance-tests` from npm, at the exact version in
`apps/durable-streams/package.json`.
