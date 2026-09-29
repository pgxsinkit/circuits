# Project Instructions for AI Agents

## Git Policy

Do not commit or push unless explicitly asked. History is one line: rebase, never merge. At
handoff, report changed files, validation run, and suggested next commands.

## Tools

bun (never npm, pnpm or yarn), podman (never docker), mise for tool versions. Run cargo through
`mise exec -- cargo …` when the shell has not activated mise: the pinned Rust is in
`rust-toolchain.toml`, and a newer rustc crashes while compiling dbsp.

## Build & Test

```bash
bun run engine:test                        # the engine's Rust tests
bun run test:durable-streams               # the log server's Rust tests
bun run typecheck
bun run test                               # every TypeScript suite, engine conformance included (boots its own Postgres)
bun run test:durable-streams:conformance   # the Durable Streams protocol suite, against the log server
```

The harness needs PostgreSQL 18's `initdb` and `pg_ctl` on `PATH`
(`/usr/lib/postgresql/18/bin` on Debian and Ubuntu).

**Finishing a task that touches the engine or the log server requires the suites above green — see
"Testing checklist before claiming done" in AGENTS.md.**

The invariants and the gotchas are in **AGENTS.md**. Read it before touching the engine.

## Architecture Overview

See AGENTS.md (layout and docs index), `docs/ARCHITECTURE.md` and
`apps/durable-streams/ARCHITECTURE.md`. The glossary is `CONTEXT.md`; decisions are in `docs/adr/`.
