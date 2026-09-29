# 0008 — `CIRCUITS_DS_URL` is not validated at resolve

Status: candidate (recorded 2026-08-22; narrowed 2026-09-29)
Opened: 2026-08-22 · Area: `apps/engine/src/config.rs` (`Config::resolve`)
Reopen trigger: an operator misled by it.

Carried over on 2026-09-29 from issue #15 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

Narrowed on 2026-09-29. The item also covered `ELECTRIC_PROMETHEUS_PORT`, which the engine accepted
and never implemented (it only logged that the setting was ignored). That setting was removed with
the rest of the Electric configuration surface (ADR-0011, commit 3267490), which was this item's
reopen trigger for that half, so only the `CIRCUITS_DS_URL` half remains. `GET /metrics/prometheus`
on the engine's own port is the scrape target, and it exports every engine counter and gauge.

## The fact
**`CIRCUITS_DS_URL` is not parse-validated at config time** the way `CIRCUITS_PG_URL` now is (`pg::parse_pg_url` in `Config::resolve`). An unusable durable-streams URL surfaces as a fatal `reqwest` builder error on first use — still exit 78, just later and with a less direct message.

## Fix direction
Validate the DS URL in `Config::resolve` and refuse with a redacted, named message.

## Not now because
Cosmetic; the path already fails loudly. Surfaced in the slice-6 follow-ups (`docs/notes/2026-08-21-upstream-issue-triage.md`).
