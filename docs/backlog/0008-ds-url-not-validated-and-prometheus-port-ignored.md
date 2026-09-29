# 0008 — `CIRCUITS_DS_URL` is not validated at resolve, and `ELECTRIC_PROMETHEUS_PORT` is accepted but ignored

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `apps/engine/src/config.rs` (`Config::resolve`), `apps/engine/src/main.rs`
Reopen trigger: an operator misled by either, or the removal of the compatibility adapter's settings (ADR-0011), which takes `ELECTRIC_PROMETHEUS_PORT` with it.

Carried over on 2026-09-29 from issue #15 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

As of 2026-09-29 the engine logs at `info` that `ELECTRIC_PROMETHEUS_PORT` is set and not implemented. It still accepts the setting.

## The fact
Two configuration-surface rough edges:

1. **`CIRCUITS_DS_URL` is not parse-validated at config time** the way `CIRCUITS_PG_URL` now is (`pg::parse_pg_url` in `Config::resolve`). An unusable durable-streams URL surfaces as a fatal `reqwest` builder error on first use — still exit 78, just later and with a less direct message.
2. **`ELECTRIC_PROMETHEUS_PORT` is accepted but unimplemented.** `/metrics/prometheus` on the main port is now a complete scrape target (every engine counter/gauge is exported), which makes the missing dedicated listener more visible, and a silently ignored setting is a trap.

## Fix direction
(1) validate the DS URL in `Config::resolve` and refuse with a redacted, named message. (2) Either implement the dedicated listener (a second `axum::serve` on that port serving only `/metrics/prometheus`, joining the same shutdown) or refuse the setting at boot with a pointer to the main-port endpoint — an accepted-and-ignored knob is the one option that should not remain.

## Not now because
Cosmetic; both paths already fail loudly. Surfaced in the slice-6 follow-ups (`docs/notes/2026-08-21-upstream-issue-triage.md`).
