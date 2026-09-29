# 0003 — The log server image's default arguments do not start it

Status: candidate (recorded 2026-09-29)
Opened: 2026-09-29 · Area: `container/Containerfile.durable-streams` (`CMD`),
`apps/durable-streams/src/main.rs` (the `--data-dir` guard)
Reopen trigger: the first person who runs the image with no arguments and reports that it exits, or
any change to the image's entrypoint.

## The fact

- The image's `CMD` is `["--host", "0.0.0.0"]`. The server's default durability is `wal`, and in
  `wal` it refuses to start without an explicit `--data-dir` (exit 2:
  "`--durability wal refuses to start without an explicit --data-dir`"). So
  `podman run ghcr.io/pgxsinkit/circuits/durable-streams` exits at once.
- The guard is deliberate: the default data directory used to be a temporary one, and a durable
  store in a directory that does not survive is a store that silently is not durable.
- The `CMD` predates the guard. It was the same in the repository the log server came from, and the
  comment beside it still said the default was an ephemeral directory until this was recorded.
- Nothing of ours is affected: pgxsinkit's and emergent's compose files pass their own arguments,
  `--data-dir` included.

## The options

- Leave the `CMD` as it is, so that running the image without saying where the data goes fails
  loudly. This is the current state, now documented in the build file and `container/README.md`.
- Give the image a default data directory and declare it a `VOLUME`. It would start with no
  arguments, and a deployment that forgets to mount the volume would lose its streams with the
  container.
- Drop the `CMD` altogether, so the failure is about a missing argument list, not a missing flag.
