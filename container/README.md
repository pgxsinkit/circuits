# Container images

Two images, one per program. Both are built with the repository root as the build context, because
the engine and the log server are members of one Cargo workspace.

| Image | Build file | Program | Port |
|---|---|---|---|
| `ghcr.io/pgxsinkit/circuits/engine` | `container/Containerfile.engine` | the engine | 7010 |
| `ghcr.io/pgxsinkit/circuits/durable-streams` | `container/Containerfile.durable-streams` | the log server | 4437 |

```bash
podman build -f container/Containerfile.engine          -t localhost/circuits-engine:dev .
podman build -f container/Containerfile.durable-streams -t localhost/circuits-durable-streams:dev .
```

Both build stages use the Rust version in `rust-toolchain.toml`. The log server is built with its
own release profile (`release-durable-streams`: link-time optimisation, one codegen unit); the
engine with cargo's default release profile.

## Published images

`.github/workflows/images.yml` publishes both from one commit, with the same tags:

| Trigger | Tags |
|---|---|
| push to `develop` | `sha-<short sha>`, `dev` |
| semver tag | `<tag>`, `latest`, `sha-<short sha>` |

Pin the pair with one value, for example `sha-0123abc` for both.

## Configuration

The engine is configured through `CIRCUITS_*` environment variables; the full list is in
[apps/engine/README.md](../apps/engine/README.md) and
[docs/deployment-postgres.md](../docs/deployment-postgres.md). The ones every deployment sets:

| Variable | Meaning |
|---|---|
| `CIRCUITS_DS_URL` | the log server's URL, as the engine reaches it |
| `CIRCUITS_PG_URL` | the Postgres connection string |
| `CIRCUITS_PG_TABLES` | the tables to ingest |
| `CIRCUITS_BIND` | the address the control plane listens on (the image defaults to `0.0.0.0:7010`) |

The replication slot is `circuits` unless `CIRCUITS_PG_SLOT` names another. The engine introspects
its tables at start-up: create them first, or restart the engine after a migration.

The log server is configured through command-line flags; see
[apps/durable-streams/README.md](../apps/durable-streams/README.md). Pass them as the container's
arguments. The image's default arguments are not enough to start it: with the default durability
(`wal`) the server refuses to run without `--data-dir`. Mount a volume and pass, for example,
`--host 0.0.0.0 --port 8791 --data-dir /var/lib/durable-streams`.
