# Releasing

The log server is released with the repository, together with the engine, as a container image:
`ghcr.io/pgxsinkit/circuits/durable-streams`. `.github/workflows/images.yml` builds it from
`container/Containerfile.durable-streams` on every push to `develop` (tags `sha-<short sha>` and
`dev`) and on every semver tag (tags `<tag>`, `latest` and `sha-<short sha>`), after validation
passes. The engine's image is built from the same commit and carries the same tags.

Nothing is published to crates.io, npm or Docker Hub. The `durable-streams` crate, the
`@electric-ax/*` npm packages and the `electricax/durable-streams-server-rust` image are upstream's,
and are not built from this repository.

`version` in `Cargo.toml` (0.1.0) and in `package.json` (0.1.5, what upstream last released) are
upstream's numbers, which its release process kept in step at publish time. Neither is the
version of an image: that is the tag the image was published under.
