# 0034 — Unsequenced output needs separate restart qualification

Status: parked (recorded 2026-10-03; source-derived leads, not reproduced defects)
Opened: 2026-10-03 · Area: aggregate, subquery and library-mode output
Reopen trigger: a fresh semantic replay/lifecycle failure in one of these paths, or an explicit
requirement to extend ordered delivery beyond plain Postgres shapes.

[0011](0011-a-forced-exit-replays-the-checkpoint-window.md) adds durable ordered delivery for plain
Postgres shapes. These paths require distinct identity and ordering models:

- Aggregate restore adopts a fresh snapshot/fold and emits a new absolute seed. Reusing its old
  snapshot gate would double-apply history already represented by that seed. A lead to qualify is
  an old engine's unsequenced append finishing after a replacement engine's fresh seed; this is an
  inferred race, not an executed failure or proof of permanently stale output.
- Subquery shapes retire at boot. Deferred query-backs evaluate current state asynchronously;
  evaluation FIFO is not a prefix of source LSNs, and one source transaction can produce several
  later effect groups. A maximum source token cannot certify all those groups. Live ambiguous
  append retries need qualification independently of boot retirement.
- Library mode has no Postgres LSN/sequence or snapshot witness. Its existing replay behavior is
  outside the new guarantee; stable identity must come from its source contract, not HTTP request
  or page ordinals.

Keep raw output and local lifecycle effects in the oracle, not only final row convergence. Each
path needs a deterministic red before implementation. Do not replace fresh aggregate gates with
original gates, advance the global checkpoint ahead of output, or change client opaque-offset dedup
to hide delivery failures.
