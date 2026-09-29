# Contributing

## Before changing anything

Read the [README](README.md) for what the repository is, [CONTEXT.md](CONTEXT.md) for its
vocabulary, and [AGENTS.md](AGENTS.md) for the layout and the invariants the engine must hold.
`docs/ARCHITECTURE.md` and `docs/ivm-engine-internals.md` explain the engine's design, and
`apps/durable-streams/ARCHITECTURE.md` the log server's. Read them before changing either.

## Build and test

The toolchain is pinned: Rust by `rust-toolchain.toml`, bun and node by `mise.toml`. Containers are
built with podman. The integration suites need PostgreSQL 18's `initdb` and `pg_ctl` on `PATH`.

```bash
mise install
bun install                                # also installs the git hooks (.githooks/)
bun run validate                           # format, typecheck, lint, unit tests (Rust + TypeScript)
bun run validate:full                      # validate + the engine conformance harness + the protocol suite
```

The scripts only check; `bun run format:write` and `bun run lint:fix` change files. `bun run test`
is the unit tests alone and `bun run test:integration` the suites that boot Postgres, the engine and
the log server. [AGENTS.md](AGENTS.md#build--test) lists the focused scripts.

Every commit must pass `bun run validate`: the pre-commit hook runs it. Every push must pass
`bun run validate:full`: the pre-push hook runs it, and so does CI. A change to the log server is
not proven by its own tests and the protocol suite alone, because the engine depends on it:
`validate:full` runs the engine's conformance harness against the log server built beside it.

## History

History is one line. Rebase onto the branch you are changing; never merge into it. Keep each commit
to one change, say what it does and why, and add or extend a test for behaviour you change.

## Licence

The engine and the test harness are dual-licensed under [MIT](LICENSE-MIT) or
[Apache 2.0](LICENSE-APACHE); the log server is licensed under
[Apache 2.0](apps/durable-streams/LICENSE). Unless you state otherwise, a contribution is licensed
under the terms of the part it changes, with no additional conditions.
