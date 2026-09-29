# One repository for the engine and the log server, and no longer a fork

Status: accepted (2026-09-29). Supersedes [ADR-0001](0001-fork-scope-native-path.md).

Status note (2026-09-29): `electric-conformance/` was removed ahead of the rest of the adapter. It could
not run here: it started the stack through a launcher in the benchmarks package, which was not brought
over, and it needed Elixir and a checkout of Electric.

The Circuits engine and the durable-streams log server were two forks in two repositories, each
tracking an upstream that has stopped maintaining it. They are now one repository,
`pgxsinkit/circuits`, maintained here outright. The reasons, and the alternatives that were turned
down, are in pgxsinkit's
[ADR-0065](https://github.com/pgxsinkit/pgxsinkit/blob/main/docs/adr/0065-own-the-circuits-stack.md);
this record holds what follows from it inside this repository.

1. **Two programs, one workspace.** `apps/engine` and `apps/durable-streams` are members of one
   Cargo workspace with one lockfile and one toolchain pin. They stay separate processes and
   separate images: the log server keeps serving clients while the engine restarts.

2. **The pair is tested and released together.** The engine's conformance harness runs the log
   server built from the same commit, never a published build of it. Both images of a release are
   built from one commit and carry the same tags, so a consumer pins the pair with one value.

3. **Upstream compatibility is not a goal.** ADR-0001 kept commits "upstream-shaped" so that any of
   them could be offered upstream, and kept the Electric compatibility adapter for upstream's sake.
   Neither holds any more. There is no refresh path from either upstream, and "matches upstream"
   never justifies the shape of code or of a test.

4. **The native path is the only product surface.** The compatibility adapter (`GET /v1/shape`, the
   `ELECTRIC_*` settings that imitate Electric's, the `electric.*` metric names and
   `electric-conformance/`) is removed in its own change. Tests that reach engine behaviour through
   it are ported to the native path first.

5. **Names.** Nothing carries Electric's name. "Circuits" is the engine; "durable-streams" is the
   protocol the log server implements, and the log server keeps its protocol's name.

6. **Licences stay per crate.** The engine is MIT or Apache-2.0; the log server is Apache-2.0 and
   keeps its `LICENSE` and `NOTICE`. [PROVENANCE.md](../../PROVENANCE.md) records where each came from.

7. **The protocol's conformance suite and specification stay external.** The suite is taken from
   npm at an exact version. They come into this repository only if the log server must do something
   the specification does not allow, or if the package is removed from npm.

## Consequences

- The earlier records (ADR-0001 to ADR-0010, `docs/notes/`, `docs/superpowers/`) keep the names
  they were written with: `electric-circuits`, `ELECTRIC_CIRCUITS_*`.
- The old repositories, `pgxsinkit/electric-circuits` and `pgxsinkit/durable-streams-rust`, are
  left as they are. Nothing here depends on them.
