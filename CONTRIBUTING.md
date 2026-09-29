# Contributing

## Before changing anything

Read the [README](README.md) for what the repository is, [CONTEXT.md](CONTEXT.md) for its
vocabulary, and [AGENTS.md](AGENTS.md) for the layout and the invariants the engine must hold.
`docs/ARCHITECTURE.md` and `docs/ivm-engine-internals.md` explain the engine's design, and
`apps/durable-streams/ARCHITECTURE.md` the log server's. Read them before changing either.

## Build and test

The toolchain is pinned: Rust by `rust-toolchain.toml`, bun and node by `mise.toml`. Containers are
built with podman. The test harness needs PostgreSQL 18's `initdb` and `pg_ctl` on `PATH`.

```bash
mise install
bun install
bun run engine:test                        # the engine's Rust tests
bun run test:durable-streams               # the log server's Rust tests
bun run typecheck
bun run test                               # every TypeScript suite, engine conformance included
bun run test:durable-streams:conformance   # the protocol suite, against the log server
```

A change to the engine must pass the engine's tests and `bun run test`. A change to the log server
must pass its tests, its conformance run and `bun run test`, because the engine depends on it.

## History

History is one line. Rebase onto the branch you are changing; never merge into it. Keep each commit
to one change, say what it does and why, and add or extend a test for behaviour you change.

## Licence

The engine and the test harness are dual-licensed under [MIT](LICENSE-MIT) or
[Apache 2.0](LICENSE-APACHE); the log server is licensed under
[Apache 2.0](apps/durable-streams/LICENSE). Unless you state otherwise, a contribution is licensed
under the terms of the part it changes, with no additional conditions.
