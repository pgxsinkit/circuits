#!/usr/bin/env bash
# Runs a command with the toolchain this repository pins: Rust from rust-toolchain.toml, bun and
# node from mise.toml. Every package script that reaches cargo goes through here, directly or
# through the vitest global setups, which build the engine and the log server.
#
# With mise on PATH the command runs under `mise exec`, which resolves the pins for this repository
# whatever the calling environment carries. That matters: a shell, editor or git client that
# activated mise somewhere else exports that place's RUSTUP_TOOLCHAIN, rustup ranks it above
# rust-toolchain.toml, and the rustc it selects can be one that crashes while compiling dbsp (every
# stable from 1.97.0 to 1.98.1 does). Without mise (CI, where actions-rust-lang/setup-rust-toolchain
# installs the toolchain rust-toolchain.toml names) the command runs as it is, and rustup reads the
# pin itself.
#
# Usage: bash scripts/with-toolchain.sh <command> [args...]
set -euo pipefail

if (($# == 0)); then
  echo "usage: bash scripts/with-toolchain.sh <command> [args...]" >&2
  exit 2
fi

mise_bin="$(type -P mise || true)"
if [[ -n "$mise_bin" ]]; then
  exec "$mise_bin" exec -- "$@"
fi
exec "$@"
