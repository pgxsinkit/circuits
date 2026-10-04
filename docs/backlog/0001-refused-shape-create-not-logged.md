# 0001 — A refused shape create is not logged

Status: resolved (2026-10-03; creation refusals on both native endpoints)
Opened: 2026-09-11 · Area: `apps/engine/src/http.rs` (`create_shape`, `create_aggregate`,
`impl IntoResponse for AppError`)
Reopen trigger: a failed `POST /shapes` or `POST /aggregate` response again lacks its structured
refusal diagnostic, or new evidence shows misleading context or an unbounded diagnostic. Broader
per-request tracing on other endpoints remains a separate requirement.

Selected after the plain Postgres delivery repair in [0011](0011-a-forced-exit-replays-the-checkpoint-window.md),
committed as `a19c915`, under the user's instruction to commit and continue. This is a bounded
operator-facing repair with a recorded incident, ahead of unproved identity leads and optional fork
work. The pool-warning discussion below is historical: clean pool check-in is already fixed and is
not part of this implementation.

## Recorded incident (2026-09-11)

- `create_shape` and `create_aggregate` returned `AppError` on refusal (unknown table, bad predicate,
  a table the engine was not configured to replicate, …). `IntoResponse for AppError` turned that into
  `{ "error": msg }` with the status, and **emitted no log record at any level**. There was no
  request-logging layer on the router either.
- At the time, the caller (pgxsinkit's control plane) mapped every engine error to a `503 sync engine unavailable`
  for its own client and dropped the body too (pgxsinkit backlog 0018). So at INFO the engine
  says nothing while it refuses the same shape once a second.
- Seen on the emergent dev cluster, 2026-09-11: the engine's `CIRCUITS_PG_TABLES` had
  drifted from the client registry; `public.competency_association` was untracked, so every
  `POST /shapes` for the shape on it was refused for over an hour. The engine's log for that hour
  held only the periodic `WARNING: there is no transaction in progress` lines from the metrics poll
  and nothing else — the last real entries were the successful backfills of the tracked tables. The
  refusal was only visible by diffing `GET /tables` against the registry.

## Earlier proposed fix

- Log every `AppError` at `warn!` (status, message, and for shape creates the table and predicate
  summary) when it is converted into a response, or add a `tower_http::TraceLayer` on the router
  that records non-2xx responses with their body. Refusals are rare and operator-actionable; they
  should never be silent.
- While there: the `WARNING: there is no transaction in progress` line every ten seconds is the
  pool's check-in `ROLLBACK` (`pg.rs`, `impl Drop for PooledClient`) fired by the slot-gauge sampler
  (`metrics.rs`, `SLOT_SAMPLE_PERIOD`), which runs a single autocommit `SELECT` and hands the client
  back. Postgres warns on every one, and tokio-postgres surfaces it at INFO. It is noise that buries
  the signal above: skip the `ROLLBACK` when the client is not in a transaction (tokio-postgres does
  not expose that directly; a cheap option is to only issue it for clients that were handed out for
  transactional use), or run the sampler on a dedicated client outside the pool.

## Earlier deferral boundary

Originally deferred until a repeat silent-refusal incident or a need for per-request tracing. The
user's current instruction selects the bounded repair now; the responses themselves are correct,
while the operator's view is missing.

## Current implementation scope (2026-10-03)

Pre-change source review confirmed that the two creation handlers validated subscription input and
called the engine, then converted refusal to AppError without emitting a request diagnostic.
AppError's typed status classification and Retry-After are retained. Axum JSON/type extraction
failures occurred before that AppError path, so they also needed a creation-route boundary.

The selected repair logs one structured warning per failed POST to `/shapes` or `/aggregate`, with
route, status, bounded diagnostic message, canonical table and a bounded structural predicate
summary. Invalid extracted requests have unavailable table/predicate context rather than guessed
fields. Responses, error bodies and Retry-After remain unchanged. Unrelated endpoints and probes
do not acquire blanket warning logging; successful requests do not log their input. The predicate
summary excludes literal values and subscription IDs, and diagnostic strings are bounded/escaped.
Existing error messages can themselves include invalid input values; this is not a promise to
redact every value from a diagnostic cause.

The qualification drives the actual in-process router with a request-scoped tracing subscriber,
first reproducing a silent refusal as a failing warning assertion, then checking both creation
endpoints, pre-engine validation, boot/degradation, extractor responses and bounded context. No new
logging dependency, PostgreSQL change or pool-warning repair was needed.

## Executed regression

Before production edits, `bun run engine:test --test http_endpoints refused_shape_create_logs_one_warning_with_request_context -- --exact`
failed with zero warnings instead of one. The actual router returned the expected unknown-table
400 and error body, so the failed assertion isolates the missing diagnostic rather than a response
or engine failure. An initial expected-body punctuation mismatch was corrected against the existing
response before recording that red; it is a fixture correction, not evidence of a second defect.
Source and this regression directly identify the absent boundary instrumentation, so a speculative
multi-hypothesis diagnosis loop was not needed.

## Resolution and validation

The two handlers accept extraction results explicitly, log only the static rejection category/status
with unavailable context, and return the original Axum rejection response. Extracted requests capture
bounded context before subscription validation or engine calls; failed results log once and delegate
to the original AppError response conversion. There is no global HTTP logger, response-body capture
or change to engine lifecycle/cancellation ownership.

Warnings use `operation`, `route`, numeric `status`, and, when available, `error`, `table` and
`predicate`. Error/table log copies are capped at 1024/256 Unicode characters including an ellipsis;
debug string fields escape controls. Predicate summaries report root kind, visited-node count,
visited depth and truncation, stopping at 64 nodes or depth eight without scanning a wide remainder.
They do not serialize columns, subquery tables, literal values or subscription fields. Error causes
can still include offending input or identifiers; this is bounded diagnosis, not universal redaction.

Seven new tests comprise five router regressions (table-driven across both endpoints) and two
summary/string-bound qualifications. The focused router file passed 15/15 and HTTP unit tests
passed 6/6, including existing typed-error precedence. Cases verify the exact unknown-table response,
subscription refusal before boot admission, booting 503 with Retry-After, degraded 503 without it,
malformed JSON/table/predicate extraction and retained text responses, omitted private predicate/
subscription fields, Unicode/control handling and unrelated-route silence. Successful-create
silence follows the unchanged successful response branch and source review; no dedicated success
logging fixture was added. These in-process tests do not recreate the historical deployment incident.

Coordinator inspection and independent review found no remaining blockers. Both required gates
passed on the final source via `bun run validate:full`: formatting, typecheck, lint, 440 engine Rust
unit tests plus 51 other engine cases, 225 log-server unit tests, eight CLI cases, 82 TS unit tests,
250 engine integration cases across 61 files and 332 protocol cases. The existing two Rust ignores
and six protocol skips remain. Real-engine integration output also contained the new warning events;
that observation supplements the scoped assertions rather than claiming a separate operator-incident
reproduction. No dependencies, tools or branches changed; output was captured directly. The engine
README and architecture record the operator-facing behavior.
