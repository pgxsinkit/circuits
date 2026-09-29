# 0015 — Two dbsp settings are read, logged and do nothing

Status: candidate (recorded 2026-09-29)
Opened: 2026-09-29 · Area: `apps/engine/src/config.rs` (`dbsp.dir`, `dbsp.checkpoint_every`),
`apps/engine/src/main.rs`
Reopen trigger: an operator who sets either and expects an effect, or the next change to the
engine's configuration.

## The fact

- `CIRCUITS_DBSP_DIR` is parsed into `config.dbsp.dir`, defaulted to `./data/dbsp/<slot>`, and logged
  at boot as "dbsp arrangements: dir …". Nothing is ever written there: the directory is not even
  created.
- `CIRCUITS_DBSP_CHECKPOINT_SECS` is parsed into `config.dbsp.checkpoint_every` and never read. The
  engine takes no dbsp checkpoint.
- `docs/ARCHITECTURE.md` already calls `CIRCUITS_DBSP_DIR` a former setting. The code and the boot
  log still present it as a live one.

## How it was found

While testing whether an engine on dbsp 0.357 can start on state written by one on 0.318. The answer
was yes, because there is no such state: the only dbsp files are the membership circuit's spill
files (`CIRCUITS_SUBQ_STORAGE_DIR`, or a per-boot temporary directory), a cache nothing reads at
boot. Everything durable is in the log server.

## Fix direction

Remove both settings, their defaults, the boot log line and their tests. A setting that is accepted
and ignored is the one option that should not remain.
