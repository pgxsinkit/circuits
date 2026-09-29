# 0009 — A Postgres error with no SQLSTATE and no io source retries forever at boot

Status: candidate (recorded 2026-08-22)
Opened: 2026-08-22 · Area: `apps/engine/src/pg.rs` (`classify`, `failure_name`), `apps/engine/src/main.rs` (the boot retry loop)
Reopen trigger: a pod found waiting indefinitely on a misconfigured connection string, or `tokio-postgres` exposing the error kind.

Carried over on 2026-09-29 from issue #16 of `pgxsinkit/electric-circuits`, the repository the engine
came from. Checked against the code on that day: still true.

## The fact
`pg::classify` (boot-time fatal-vs-retryable taxonomy) treats a `tokio_postgres::Error` with **no SQLSTATE and no io source** as retryable. That default is right for transport blips, but it also covers at least one genuine misconfiguration: a server demanding a password the URL does not carry (`Kind::Config`). Such a boot retries forever with `/ready = 503 waiting` and a logged attempt every backoff, instead of exiting 78.

## Why it is like this
`tokio_postgres::Error::Kind` is private; the only discriminator for that case is the `Display` string, and matching error text was rejected as too brittle. The wording of `failure_name` is honest ("Postgres returned an error without a SQLSTATE").

## Fix direction
Either upstream a public accessor for the error kind in `tokio-postgres` (or use `is_closed()` plus a conservative Display match for the two or three known `Config` messages), or add a bounded retry budget for no-SQLSTATE errors (e.g. after N minutes of the same no-SQLSTATE failure, exit 78 with the accumulated cause) so a misconfigured pod does not wait forever.

## Not now because
Visible (`waiting` + logs) and rare. Surfaced in the slice-6 follow-ups (`docs/notes/2026-08-21-upstream-issue-triage.md`).
