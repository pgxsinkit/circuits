# Backlog

The documented ledger for work we deliberately are **not** doing now: parked investigations (with
their evidence), improvement candidates, and escape-hatched designs. Same rules as pgxsinkit's
`docs/backlog`:

- One numbered file per item. Entries are **never deleted** — status flips instead, so a symptom
  someone trips over next year finds the prior investigation instead of restarting it.
- Every item carries a **Reopen trigger**: the concrete event or evidence that justifies picking it
  up. Until that fires, the item is settled — do not re-litigate it from scratch.
- `Status: parked` (investigated, evidence recorded, waiting on the trigger) · `candidate`
  (improvement we would take, unscheduled) · `promoted → adr/00xx` (one-line pointer to the ADR that
  superseded it) · `dropped` (decided against; keep the why).
- This directory is an engineering ledger, not user documentation.

## Items

- [0001 — A refused shape create is not logged](0001-refused-shape-create-not-logged.md) — candidate
- [0002 — The harness client retries a dead subscription's renewal at its floor cadence, logging a non-JSON body](0002-harness-client-renewal-retry-storm.md) — candidate
- [0003 — The log server image's default arguments do not start it](0003-log-server-image-default-arguments-do-not-start.md) — candidate
- [0004 — The boot-errors test raced the engine's first retry warning](0004-boot-errors-test-raced-the-first-retry-warning.md) — dropped (fixed)
- [0005 — The reconciler does not pick up new or re-created tables without a restart](0005-reconciler-does-not-pick-up-new-tables.md) — candidate
- [0006 — A TRUNCATE replayed after a crash re-retires the table's shapes](0006-truncate-replay-window-re-retires-shapes.md) — candidate
- [0007 — Three refusal paths have no end-to-end lane](0007-three-refusal-paths-have-no-end-to-end-lane.md) — candidate
- [0008 — `CIRCUITS_DS_URL` is not validated at resolve, and `ELECTRIC_PROMETHEUS_PORT` is accepted but ignored](0008-ds-url-not-validated-and-prometheus-port-ignored.md) — candidate
- [0009 — A Postgres error with no SQLSTATE and no io source retries forever at boot](0009-no-sqlstate-postgres-error-retries-forever.md) — candidate
- [0010 — The harness client does not re-subscribe when its stream is retired](0010-harness-client-does-not-re-subscribe-on-a-retired-stream.md) — candidate

## Not carried over

Four issues of `pgxsinkit/electric-circuits` were open when this repository was made and are not
items here. #4 (`subset()` with limit 0), #5 (`subset()` and NULL sort keys) and #14 (the walsender
connect timeout) were already fixed in the code. #18 is a defect of the compatibility adapter, which
is being removed (ADR-0011).
