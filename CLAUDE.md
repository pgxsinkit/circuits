# Project Instructions for AI Agents

## Git Policy

Do not commit or push unless explicitly asked. History is one line: rebase, never merge. At
handoff, report changed files, validation run, and suggested next commands.

## Tools

bun (never npm, pnpm or yarn), podman (never docker), mise for tool versions. The pinned Rust is in
`rust-toolchain.toml`, and every stable rustc from 1.97.0 to 1.98.1 crashes while compiling dbsp:
the package scripts reach cargo through `scripts/with-toolchain.sh` (`mise exec` when mise is on
`PATH`), so `bun run …` is safe from any shell, but run a bare cargo as `mise exec -- cargo …` when
the shell has not activated mise.

## Build & Test

Scripts are check-default: a bare verb never changes a file (`format:write` and `lint:fix` do).

```bash
bun run validate                           # pre-commit gate (the hook runs it): format, typecheck, lint, test
bun run validate:full                      # explicit full validation and CI: validate + test:integration
bun run test                               # unit only: both crates' Rust tests + the TypeScript unit project; no Postgres
bun run test:integration                   # engine conformance harness (boots its own Postgres) + the protocol suite
```

The integration suites need PostgreSQL 18's `initdb` and `pg_ctl` on `PATH`
(`/usr/lib/postgresql/18/bin` on Debian and Ubuntu). `bun install` installs the pre-commit hook;
there is no local pre-push hook. CI still runs full validation.

**Finishing a task that touches the engine or the log server requires `bun run validate:full` green
— see "Testing checklist before claiming done" in AGENTS.md, which also lists the focused scripts.**

The invariants and the gotchas are in **AGENTS.md**. Read it before touching the engine.

## Architecture Overview

See AGENTS.md (layout and docs index), `docs/ARCHITECTURE.md` and
`apps/durable-streams/ARCHITECTURE.md`. The glossary is `CONTEXT.md`; decisions are in `docs/adr/`.
