//! Schema drift, TRUNCATE and replica-identity regression: **one** retirement path, four triggers
//! (ADR-0005).
//!
//! The triggers are the pgoutput `Relation` message (drift + identity regression), the pgoutput
//! `Truncate` message, and the background reconciler below (DDL with no following DML — a backfill
//! for a new shape would otherwise run against the stale schema). They all end in
//! [`Engine::retire_dependents`]: every shape whose table IS `t`, plus every subquery shape whose
//! predicate REFERENCES `t`, is purged — the stream closed then deleted (ADR-0007) — and clients
//! recreate. Granularity is per table, never whole-engine: a migration on one table must not
//! resync every table.
//!
//! ## Ordering, and the create that races it
//!
//! Resolution is retire-then-swap: no shape may be maintained for an instant against a schema it
//! was not built from. That leaves one race — a create already past its registration and out at
//! Postgres when the retirement enumerates what to purge. Two things close it:
//!
//! - a per-table **schema generation** (`EngineState::schema_gen`), bumped inside the same critical
//!   section that enumerates the dependents. A create captures the generation of every table it
//!   reads as it registers and re-checks it before returning a handle;
//! - the per-table **resolve lock** ([`Resolving`]), taken before that critical section and held
//!   until the swap. A create registering while it is held is refused outright — otherwise a create
//!   that arrives *after* the enumeration would capture the already-bumped generation, pass its own
//!   closing check, and be orphaned by the `ResetTable` that follows.
//!
//! So a create either lands in the enumeration (and fails its closing check) or is refused.
//!
//! ## Restarting is replay-safe
//!
//! A table with a counts pipeline (`CIRCUITS_DBSP_COUNTS`) has no runtime rebuild:
//! `Arrangements::start` builds the circuit ONCE at boot and seeds it from a group-aggregated
//! snapshot. Rather than keep serving counts computed over a schema — or a row set — that no longer
//! exists, the process **exits** after the retirements and catalog events have landed.
//!
//! Delivery is at-least-once, so the transaction that triggered the exit is re-delivered on the next
//! boot: exiting unconditionally would be an exit loop. The trigger therefore carries its
//! transaction's xid, and the exit is gated on the boot seed's [`crate::pg::SnapshotGate`] — if the
//! seed snapshot already reflects that transaction, the circuit is *correct* and there is nothing to
//! rebuild ([`circuit_needs_rebuild`]). Only a trigger the seed does not cover restarts the process.
//!
//! For the same reason a replayed `TRUNCATE` on a **non**-circuit table re-retires that table's
//! shapes, including shapes created after the boot that already reflect the truncation. The engine
//! cannot tell them apart: a shape's snapshot gate lives inside the sequencer's executor state, not
//! on its record. The window is bounded by the acknowledgement interval (1 s) and the cost is one
//! spurious resync — the same cost the ADR already charges for a truncate.
//!
//! ## When resolution fails
//!
//! Postgres unreachable, a catalog read that errored, or an `ALTER … REPLICA IDENTITY FULL` that
//! could not get its lock: the table becomes **unresolved**. It is removed from the shared decode
//! view (its changes are dropped — no consumer exists, and a future shape backfills from Postgres
//! behind its own gate), creates on it are refused with a retryable error, and a per-table retry
//! task keeps attempting the resolution with backoff. A resolution that succeeds always re-installs
//! the schema when the table was parked, even if the catalog turned out to be unchanged — that
//! install is what un-parks it.
//!
//! What is deliberately NOT a runtime concern: a publication that cannot deliver whole rows. A
//! per-table column list is refused at boot and generated-column publishing is folded into the
//! fingerprint (`pg::inspect_publication`), so the wire and the catalog agree by construction.

use super::*;

use crate::replication::TxnRef;
use crate::schema::{REPLICA_IDENTITY_FULL, SchemaFingerprint, describe_drift};

/// Exit code for the circuit-tier restart. Non-zero so a supervisor treats it as a crash and
/// restarts; distinct from 1 so it is recognisable in `kubectl describe`. Shared with the epoch
/// reset (ADR-0004), which leaves the counts pipelines in the same unrebuildable state.
pub(crate) const EXIT_CIRCUIT_REBUILD: i32 = 75;

/// How long the inline `ALTER … REPLICA IDENTITY FULL` may wait for its `ACCESS EXCLUSIVE` lock.
/// The drift handler runs inside the ingestor, so an unbounded wait would stall EVERY table's
/// ingest behind one long reader of this one; on timeout the table goes unresolved and its retry
/// task tries again.
const IDENTITY_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Backoff bounds for the unresolved-table retry task.
const RETRY_MIN: std::time::Duration = std::time::Duration::from_secs(2);
const RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// `CIRCUITS_SCHEMA_RECONCILE_SECS`, default 60; `0` disables the reconciler.
fn reconcile_interval() -> std::time::Duration {
    let secs =
        std::env::var("CIRCUITS_SCHEMA_RECONCILE_SECS").ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(60);
    std::time::Duration::from_secs(secs)
}

/// Spread retries of many tables apart without pulling in an RNG: ±25% from the clock's sub-second
/// noise. Precision is irrelevant here; only decorrelation matters.
fn jitter(base: std::time::Duration) -> std::time::Duration {
    let nanos =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    let spread = base.as_millis() as u64 / 2;
    let offset = if spread == 0 { 0 } else { nanos as u64 % spread };
    base.saturating_sub(std::time::Duration::from_millis(spread / 2)) + std::time::Duration::from_millis(offset)
}

/// Must the circuit be rebuilt (⇒ the process restarted) for a trigger observed in transaction
/// `txn`, given the boot seed's snapshot `gate` for that table?
///
/// Pure so the decision can be tested without a circuit: `false` only when the seed demonstrably
/// already reflects the triggering transaction — a replay after the restart, whose effect the fresh
/// boot snapshot has already absorbed. No transaction context (the reconciler, a retry) means no
/// evidence, so rebuild.
fn circuit_needs_rebuild(gate: Option<&crate::pg::SnapshotGate>, txn: Option<TxnRef>) -> bool {
    match (gate, txn) {
        (Some(gate), Some(txn)) => !gate.should_skip(0, Some(u64::from(txn.xid))),
        _ => true,
    }
}

/// Where a drift observation came from. The wire can be **behind** the catalog — after a restart the
/// ingestor replays WAL from before a DDL, and Postgres re-sends each `Relation` as the table was AT
/// THAT POINT in the stream — so a wire observation is never authority for anything; only the
/// catalog is.
pub(crate) enum DriftSource {
    /// A pgoutput `Relation` message: what the wire will deliver, as of that point in the stream.
    Relation(SchemaFingerprint),
    /// The reconciler's catalog read.
    Catalog(SchemaFingerprint),
    /// No observation — a retry, or a table the reconciler found absent.
    None,
}

impl DriftSource {
    fn fingerprint(&self) -> Option<&SchemaFingerprint> {
        match self {
            DriftSource::Relation(f) | DriftSource::Catalog(f) => Some(f),
            DriftSource::None => None,
        }
    }

    /// The observation the wire made, if this came from the wire.
    fn from_wire(&self) -> Option<&SchemaFingerprint> {
        match self {
            DriftSource::Relation(f) => Some(f),
            _ => None,
        }
    }
}

/// Per-table serialisation of drift resolutions.
///
/// Triggers **queue** rather than stand down: an `R` for a second migration arriving while the first
/// is being resolved carries a newer observation, and dropping it would leave that migration
/// unresolved until something else noticed. Each waiter then runs its own (cheap: one catalog read)
/// resolution, which is a no-op if the one before it already settled everything.
#[derive(Default)]
pub(crate) struct Resolving {
    locks: std::sync::Mutex<HashMap<TableRef, Arc<tokio::sync::Mutex<()>>>>,
    /// Tables whose resolution is running right now — read by the create gate under the engine-state
    /// lock, so it must be a std lock with no await under it.
    active: std::sync::Mutex<HashSet<TableRef>>,
}

impl Resolving {
    /// Is a resolution running for this table? A create on it must be refused: it would register
    /// after the retirement's enumeration and be orphaned by the `ResetTable` that follows.
    pub(crate) fn is_active(&self, table: &TableRef) -> bool {
        self.active.lock().unwrap().contains(table)
    }
}

/// Test-only handle keeping a table's resolve lock held, so a test can observe what the engine does
/// to a create that arrives mid-resolution (see [`Engine::force_resolve_lock`]).
#[doc(hidden)]
pub struct ResolveLock(#[allow(dead_code)] ResolveGuard);

/// Holds one table's resolve lock, and marks it active for the create gate, until dropped.
struct ResolveGuard {
    resolving: Arc<Resolving>,
    table: TableRef,
    _permit: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for ResolveGuard {
    fn drop(&mut self) {
        self.resolving.active.lock().unwrap().remove(&self.table);
    }
}

/// Clears a table's retry-task slot on every exit path, so a later `unresolved()` can always start
/// a fresh one.
struct RetryGuard {
    retrying: Arc<std::sync::Mutex<HashSet<TableRef>>>,
    table: TableRef,
}

impl Drop for RetryGuard {
    fn drop(&mut self) {
        self.retrying.lock().unwrap().remove(&self.table);
    }
}

/// The ingestor's view of the engine (see [`crate::replication::SchemaEvents`]). Both calls are
/// awaited by the ingestor, so by the time it decodes the next message the dependents are retired
/// and the compiled schema is swapped.
impl crate::replication::SchemaEvents for Engine {
    fn on_schema_drift<'a>(
        &'a self,
        table: &'a TableRef,
        observed: SchemaFingerprint,
        txn: Option<TxnRef>,
    ) -> crate::replication::BoxFuture<'a, ()> {
        Box::pin(async move {
            self.handle_schema_drift(table, DriftSource::Relation(observed), txn).await;
        })
    }

    fn on_truncate<'a>(&'a self, tables: Vec<TableRef>, txn: Option<TxnRef>) -> crate::replication::BoxFuture<'a, ()> {
        Box::pin(async move { self.handle_truncate(tables, txn).await })
    }
}

impl Engine {
    /// Take (waiting if necessary) the per-table resolve lock.
    async fn begin_resolve(&self, table: &TableRef) -> ResolveGuard {
        let lock = self
            .resolving
            .locks
            .lock()
            .unwrap()
            .entry(table.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let permit = lock.lock_owned().await;
        self.resolving.active.lock().unwrap().insert(table.clone());
        ResolveGuard { resolving: self.resolving.clone(), table: table.clone(), _permit: permit }
    }

    /// Handle drift on one table: re-introspect, retire every dependent, swap the compiled schema.
    pub(crate) async fn handle_schema_drift(&self, table: &TableRef, observed: DriftSource, txn: Option<TxnRef>) {
        let Some(url) = self.pg_url.clone() else {
            // Library mode: no fingerprints exist, so nothing can call this.
            return;
        };
        // One resolution at a time per table; the wait is what makes a second trigger see the
        // first's result rather than race it.
        let _guard = self.begin_resolve(table).await;

        let (compiled_before, was_unresolved) = {
            let st = self.state.lock().await;
            (st.tables.get(table).and_then(|ts| ts.fingerprint.clone()), st.unresolved.contains(table))
        };

        // Postgres is the authority on what the table is now.
        let client = match crate::pg::pool_for(&url).get().await {
            Ok(c) => c,
            Err(e) => return self.unresolved(table, &format!("postgres unreachable: {e:#}")).await,
        };
        let mut def = match crate::pg::introspect_opt(&client, table).await {
            Ok(Some(def)) => def,
            Ok(None) => return self.handle_dropped_table(table).await,
            Err(e) => return self.unresolved(table, &format!("re-introspection failed: {e:#}")).await,
        };

        // An identity that is no longer FULL is re-asserted before anything else: the engine cannot
        // retract a row without its full old image.
        //
        // The CATALOG decides, never the wire. A stale `R` replayed after a restart reports the
        // identity as it was at that point in the stream, and acting on that would queue an
        // `ACCESS EXCLUSIVE` request — and retire every dependent — on every replay of a migration
        // that has long since been fixed.
        let mut identity_restored = false;
        if def.fingerprint.as_ref().is_some_and(|f| f.replident != REPLICA_IDENTITY_FULL) {
            if let Err(e) = crate::pg::ensure_replica_identity_full_bounded(&client, table, IDENTITY_LOCK_TIMEOUT).await
            {
                // Usually a lock wait that timed out (a long reader on the table) or a permissions
                // problem. Either way the engine cannot serve the table, and blocking ingest until
                // it can is not an option.
                return self.unresolved(table, &format!("could not restore REPLICA IDENTITY FULL: {e:#}")).await;
            }
            tracing::warn!("restored REPLICA IDENTITY FULL on {table}");
            identity_restored = true;
            match crate::pg::introspect_opt(&client, table).await {
                Ok(Some(fresh)) => def = fresh,
                Ok(None) => return self.handle_dropped_table(table).await,
                Err(e) => {
                    return self
                        .unresolved(table, &format!("re-introspection after REPLICA IDENTITY FULL failed: {e:#}"))
                        .await;
                }
            }
        }
        drop(client);

        let ts = match TableSchema::from_def(table, &def) {
            Ok(ts) => ts,
            Err(e) => {
                // e.g. the migration dropped the primary key. Nothing can be served from it, and no
                // amount of retrying changes that — treat it exactly like a drop.
                tracing::error!("schema drift on {table}: the new schema is unusable ({e:#}); untracking the table");
                return self.handle_dropped_table(table).await;
            }
        };

        // Did POSTGRES move, or only the wire's description of it? A trigger whose re-introspection
        // matches what the engine already compiled is not drift to act on: the compiled schema is
        // already the catalog's, and the only thing that disagreed was a `Relation` message from
        // earlier in the WAL. Retiring there would tear down shapes over a schema that is correct.
        let catalog_changed = match (&compiled_before, &ts.fingerprint) {
            (Some(before), Some(after)) => !before.still_serves(after),
            // One side has no fingerprint to compare: assume it moved rather than assume it didn't.
            _ => true,
        };
        let resolved_something = catalog_changed || identity_restored;
        if resolved_something {
            // Name what moved, before the retirements, so the log reads cause then effect. Prefer
            // the re-introspected difference (what the engine will now serve); fall back to the
            // trigger's own observation, which is the only description left when the two agree
            // again — an identity regression that has just been re-asserted, for instance.
            let describe = |other: Option<&SchemaFingerprint>| match (&compiled_before, other) {
                (Some(before), Some(after)) => describe_drift(before, after),
                _ => Vec::new(),
            };
            let mut what = describe(ts.fingerprint.as_ref());
            if what.is_empty() {
                what = describe(observed.fingerprint());
            }
            tracing::warn!(
                "schema drift on {table}: {}; retiring every dependent shape and recompiling",
                if what.is_empty() { "changed".to_string() } else { what.join("; ") }
            );

            // Retire, THEN swap. Never the other way round.
            self.retire_dependents(table, "schema drift").await;
        } else if was_unresolved {
            tracing::warn!("{table}: re-introspection succeeded; un-parking it");
        } else {
            tracing::debug!(
                "{table}: the replication stream described it differently from the catalog, but the \
                 catalog is unchanged — the ingestor is replaying WAL from before a change the engine \
                 already knows about. Nothing retired."
            );
        }

        // Install the schema when something moved, and ALSO whenever the table was parked: the
        // install is what clears `unresolved` and puts the table back in the decode view, and a
        // parked table whose catalog turns out to be unchanged (a transient Postgres failure, or an
        // identity someone else restored) would otherwise stay parked forever.
        if resolved_something || was_unresolved {
            self.swap_table_schema(table, Some(ts.clone())).await;
        }
        if resolved_something {
            // Durable audit record: this is why those shapes went away.
            if let Some(fp) = ts.fingerprint.clone() {
                self.catalog_tx.send(CatalogEvent::SchemaChanged { table: table.clone(), fingerprint: fp });
            }
            // A wire observation that still disagrees with a FRESH catalog read should be
            // impossible: `pg::inspect_publication` refuses a column list at boot and folds
            // generated-column publishing into the fingerprint. Say so loudly rather than park the
            // table — the engine has no better catalog to consult.
            if let (Some(wire), Some(fresh)) = (observed.from_wire(), ts.fingerprint.as_ref())
                && !fresh.still_serves(wire)
            {
                tracing::error!(
                    "unexpected: the replication stream and the catalog disagree for {table} after \
                     re-introspection ({}); check the publication",
                    describe_drift(fresh, wire).join("; ")
                );
            }
            // The circuit tier cannot be rebuilt at runtime — see the module docs. Only when
            // something actually changed, and only when the boot seed does not already reflect the
            // triggering transaction.
            self.exit_if_circuit_served(table, "schema drift", txn).await;
        }
    }

    /// `TRUNCATE`: the schema is unchanged, but every shape, aggregate and subquery node over the
    /// table holds rows that no longer exist and the engine has no copy from which to synthesise
    /// the deletes. Retire the dependents; clients recreate and backfill the (now empty) table.
    ///
    /// A counts pipeline over the table is in the same position and cannot be reseeded at runtime,
    /// so a truncate of a circuit-served table restarts the process — unless the boot seed already
    /// reflects the truncating transaction, which is exactly the replay case.
    pub(crate) async fn handle_truncate(&self, tables: Vec<TableRef>, txn: Option<TxnRef>) {
        for table in tables {
            // Same lock as a drift resolution: a create must not register between the enumeration
            // and the retirements.
            let _guard = self.begin_resolve(&table).await;
            tracing::warn!("TRUNCATE on {table}: retiring every dependent shape");
            self.retire_dependents(&table, "TRUNCATE").await;
            self.exit_if_circuit_served(&table, "TRUNCATE", txn).await;
        }
    }

    /// The table no longer exists in Postgres. Retire its dependents and forget it everywhere; do
    /// NOT exit — a dropped table is a deliberate act, and the other tables keep working. A table
    /// re-created later is not picked up again until the engine restarts.
    async fn handle_dropped_table(&self, table: &TableRef) {
        tracing::error!(
            "table {table} no longer exists in postgres: retiring every dependent shape and untracking it. \
             A table re-created under the same name is not synced again until the engine restarts."
        );
        self.retire_dependents(table, "table dropped").await;
        self.swap_table_schema(table, None).await;
    }

    /// Mark a table unresolved: its drift is real but could not be settled. Retire its dependents
    /// (they are wrong either way), take it out of the decode view so nothing is served or decoded
    /// on a schema the engine cannot vouch for, and start (once) a retry task.
    async fn unresolved(&self, table: &TableRef, why: &str) {
        tracing::error!(
            "schema of {table} is UNRESOLVED ({why}): its dependent shapes are retired, its changes are \
             dropped, and creates on it are refused until a retry succeeds"
        );
        self.retire_dependents(table, "unresolved schema").await;
        {
            let mut st = self.state.lock().await;
            st.unresolved.insert(table.clone());
            // Out of the decode view only — it stays in `state.tables` so the reconciler keeps
            // reconciling it and API callers get the specific "unresolved" refusal.
            self.tables_shared.write().unwrap().remove(table);
        }
        crate::metrics::metrics().schema_unresolved.fetch_add(1, Ordering::Relaxed);
        // Check-and-claim in ONE critical section, so two concurrent parkings cannot both decide
        // they are the one to start the task (or both decide they are not).
        let claimed = self.retrying.lock().unwrap().insert(table.clone());
        if claimed {
            self.spawn_unresolved_retry(table.clone());
        }
    }

    /// Retry an unresolved table's resolution with backoff, independent of the reconciler knob
    /// (which an operator may have turned off). Exits as soon as the table is resolved — by this
    /// task, by the reconciler, by a `Relation` message, or by being dropped.
    fn spawn_unresolved_retry(&self, table: TableRef) {
        let engine = self.clone();
        tokio::spawn(async move {
            // Clears the slot however this task ends — including a panic — so a later parking of
            // the same table can always start a fresh one.
            let _slot = RetryGuard { retrying: engine.retrying.clone(), table: table.clone() };
            let mut backoff = RETRY_MIN;
            loop {
                tokio::time::sleep(jitter(backoff)).await;
                if !engine.is_unresolved(&table).await {
                    break;
                }
                engine.handle_schema_drift(&table, DriftSource::None, None).await;
                if !engine.is_unresolved(&table).await {
                    tracing::warn!("schema of {table} resolved; it is synced again");
                    break;
                }
                backoff = (backoff * 2).min(RETRY_MAX);
            }
        });
    }

    async fn is_unresolved(&self, table: &TableRef) -> bool {
        self.state.lock().await.unresolved.contains(table)
    }

    /// Bump a table's schema generation exactly as a retirement does, with no Postgres round trip.
    ///
    /// Exposed for tests that drive the create-overtaken-by-a-drift path in isolation; the engine
    /// itself bumps the generation only from [`Self::retire_dependents`], in the same critical
    /// section that enumerates the dependents.
    #[doc(hidden)]
    pub async fn force_schema_generation_bump(&self, table: &TableRef) {
        let mut st = self.state.lock().await;
        *st.schema_gen.entry(table.clone()).or_insert(0) += 1;
    }

    /// Run a full retirement for a table, taking the resolve lock, with no Postgres round trip.
    /// Exposed for tests that need the real enumerate-bump-purge sequence.
    #[doc(hidden)]
    pub async fn force_retire_dependents(&self, table: &TableRef) {
        let _guard = self.begin_resolve(table).await;
        self.retire_dependents(table, "test-forced drift").await;
    }

    /// Hold a table's resolve lock until the returned handle is dropped — the state a create sees
    /// when a drift resolution is running on its table. Test-only.
    #[doc(hidden)]
    pub async fn force_resolve_lock(&self, table: &TableRef) -> ResolveLock {
        ResolveLock(self.begin_resolve(table).await)
    }

    /// Tables whose schema is currently unresolved (`GET /tables`).
    pub async fn unresolved_tables(&self) -> Vec<TableRef> {
        let mut v: Vec<TableRef> = self.state.lock().await.unresolved.iter().cloned().collect();
        v.sort();
        v
    }

    /// Purge every shape that depends on `table`: the shapes ON it (plain, routed, aggregate,
    /// circuit-served) and every subquery shape whose predicate REFERENCES it as an inner table.
    /// `purge_shape` retires the stream (close, then delete) and writes `Dropped`, and the subquery
    /// registry garbage-collects inner nodes at zero refcount.
    ///
    /// The enumeration and the schema-generation bump are ONE critical section; the caller holds the
    /// table's resolve lock across the whole thing. Together that is what makes a concurrent create
    /// either visible here (and purged, then refused by its own closing check) or refused outright.
    async fn retire_dependents(&self, table: &TableRef, why: &str) {
        // Every trigger funnels through here exactly once, so this is the whole `schema_drift_total`
        // count: tables whose dependents were retired, whatever noticed.
        crate::metrics::metrics().schema_drift.fetch_add(1, Ordering::Relaxed);
        let ids: Vec<String> = {
            let mut st = self.state.lock().await;
            let ids = st
                .shapes
                .values()
                .filter(|rec| {
                    rec.table == *table || rec.where_json.as_ref().is_some_and(|w| referenced_tables(w).contains(table))
                })
                .map(|rec| rec.id.clone())
                .collect();
            *st.schema_gen.entry(table.clone()).or_insert(0) += 1;
            ids
        };
        if ids.is_empty() {
            tracing::info!("{why} on {table}: no dependent shapes to retire");
            return;
        }
        tracing::warn!("{why} on {table}: retiring {} dependent shape(s): {}", ids.len(), ids.join(", "));
        for id in &ids {
            if let Err(e) = self.purge_shape(id).await {
                tracing::error!("{why} on {table}: retiring shape {id} failed: {e:#}");
            }
        }
    }

    /// Install (or, with `None`, remove) a table's compiled schema in **every** holder at once —
    /// the ingestor/sequencer's shared view, the engine's own registry and the subquery registry's
    /// copy — and drop the sequencer's executor for it, which is keyed by the old schema. Installing
    /// a schema also clears any unresolved state: this IS the resolution.
    async fn swap_table_schema(&self, table: &TableRef, ts: Option<TableSchema>) {
        let mut st = self.state.lock().await;
        st.unresolved.remove(table);
        match &ts {
            Some(ts) => {
                st.tables.insert(table.clone(), ts.clone());
                self.tables_shared.write().unwrap().insert(table.clone(), ts.clone());
            }
            None => {
                st.tables.remove(table);
                self.tables_shared.write().unwrap().remove(table);
            }
        }
        let snapshot = st.tables.clone();
        if let Some(seq) = st.sequencer.as_ref() {
            let _ = seq.cmd_tx.send(SequencerCmd::ResetTable { table: table.clone() });
        }
        drop(st);
        self.subqueries.lock().await.set_schemas(Arc::new(snapshot));
    }

    /// Restart the process when the affected table has a counts pipeline whose seed does not already
    /// reflect the triggering transaction. Called only after the retirements and the catalog events
    /// have landed. See the module docs for why the xid gate is what makes this replay-safe.
    async fn exit_if_circuit_served(&self, table: &TableRef, why: &str, txn: Option<TxnRef>) {
        let circuit_served =
            self.arrangements.lock().unwrap().as_ref().is_some_and(|arr| arr.counts_group_cols(table).is_some());
        if !circuit_served {
            return;
        }
        let needs_rebuild = {
            let gates = self.arr_gates.read().unwrap();
            circuit_needs_rebuild(gates.get(table), txn)
        };
        if !needs_rebuild {
            tracing::warn!(
                "{why} on {table} (counts pipeline): the boot seed snapshot already reflects \
                 transaction {:?}, so the circuit is correct — this is a re-delivery of a \
                 transaction that already restarted the process. Not restarting again.",
                txn.map(|t| t.xid)
            );
            return;
        }
        tracing::error!(
            "{why} on {table}, which has a counts pipeline: the circuit is built and seeded once at boot \
             and has no runtime rebuild, so its counts no longer describe the table. Restarting the \
             process (exit {EXIT_CIRCUIT_REBUILD}): boot re-introspects, re-seeds the circuit, and \
             restores every other table's shapes from the durable catalog."
        );
        if !self.catalog_tx.drain(std::time::Duration::from_secs(5)).await {
            tracing::error!("catalog writer did not drain before the restart; some drop records may be missing");
        }
        std::process::exit(EXIT_CIRCUIT_REBUILD);
    }

    /// Spawn (once) the background schema reconciler: DDL with **no following DML** produces no
    /// `Relation` message, so nothing on the ingest path would ever notice it — and a backfill for a
    /// shape created afterwards would run against the stale schema. Each tick fingerprints every
    /// tracked table in one query and feeds any mismatch to the same drift handler.
    ///
    /// Postgres mode only, and only while `CIRCUITS_SCHEMA_RECONCILE_SECS` is non-zero.
    /// (Unresolved tables are retried by their own task regardless — see [`Self::unresolved`].)
    pub(crate) fn ensure_schema_reconciler(&self) {
        let Some(url) = self.pg_url.clone() else { return };
        let interval = reconcile_interval();
        if interval.is_zero() {
            tracing::warn!(
                "schema reconciler disabled (CIRCUITS_SCHEMA_RECONCILE_SECS=0): DDL that no \
                 write follows, and ANY primary-key change (the replication stream cannot describe a \
                 primary key), will go unnoticed until the engine restarts"
            );
            return;
        }
        if self.reconciler_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick fires immediately; skip it
            loop {
                tick.tick().await;
                engine.reconcile_schemas(&url).await;
            }
        });
    }

    /// One reconciler pass: compare every tracked table's compiled fingerprint with Postgres's.
    async fn reconcile_schemas(&self, url: &str) {
        // Snapshot (table, compiled fingerprint) pairs. Tables without a fingerprint cannot drift.
        let wanted: Vec<(TableRef, SchemaFingerprint)> = {
            let st = self.state.lock().await;
            st.tables.values().filter_map(|ts| Some((ts.table.clone(), ts.fingerprint.clone()?))).collect()
        };
        if wanted.is_empty() {
            return;
        }
        let tables: Vec<TableRef> = wanted.iter().map(|(t, _)| t.clone()).collect();
        let client = match crate::pg::pool_for(url).get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("schema reconciler: postgres unavailable this tick: {e:#}");
                return;
            }
        };
        let live = match crate::pg::fingerprints(&client, &tables).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("schema reconciler: fingerprint query failed: {e:#}");
                return;
            }
        };
        drop(client);
        for (table, compiled) in wanted {
            match live.get(&table) {
                // Settled and matching: nothing to do. A PARKED table is still visited, so the
                // reconciler is a second way out of `unresolved` alongside its retry task.
                Some(observed) if compiled.still_serves(observed) && !self.is_unresolved(&table).await => {}
                Some(observed) => {
                    self.handle_schema_drift(&table, DriftSource::Catalog(observed.clone()), None).await;
                }
                // Absent from `pg_class` ⇒ dropped. The handler re-checks against Postgres itself.
                None => {
                    self.handle_schema_drift(&table, DriftSource::None, None).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::DsClient;
    use crate::pg::SnapshotGate;
    use crate::schema::{Schema, compile_schema};

    /// A library-mode engine with one table compiled, wired into both schema holders the way
    /// `setup_postgres` does. No durable-streams server behind it: nothing here appends.
    async fn engine_with_items() -> (Engine, TableRef) {
        let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "tables": { "items": { "columns": { "id": {"type":"int"} }, "primaryKey": "id" } }
        }))
        .unwrap();
        let compiled = compile_schema(&schema).unwrap();
        *engine.tables_shared.write().unwrap() = compiled.clone();
        engine.state.lock().await.tables = compiled;
        (engine, TableRef::parse("items").unwrap())
    }

    /// Parking a table takes it out of the decode view and refuses creates on it; **installing a
    /// schema is what un-parks it** — which is why a successful resolution installs even when the
    /// catalog turned out to be unchanged. Without that, a table parked by a transient Postgres
    /// failure at a stale-`Relation` trigger could never come back: every retry would find the
    /// catalog equal to the compiled schema, change nothing, and leave it parked forever.
    #[tokio::test]
    async fn installing_a_schema_unparks_an_unresolved_table() {
        let (engine, items) = engine_with_items().await;
        let ts = engine.state.lock().await.tables[&items].clone();

        engine.unresolved(&items, "test").await;
        assert!(engine.is_unresolved(&items).await);
        assert_eq!(engine.unresolved_tables().await, std::slice::from_ref(&items));
        assert!(
            !engine.tables_shared.read().unwrap().contains_key(&items),
            "a parked table leaves the decode view, so its changes are dropped rather than decoded"
        );
        let err = engine.create_shape(&items, None, None, false, true).await.unwrap_err().to_string();
        assert!(err.contains("unresolved"), "unexpected refusal: {err}");

        // A resolution whose introspection matches what was already compiled still installs.
        engine.swap_table_schema(&items, Some(ts)).await;
        assert!(!engine.is_unresolved(&items).await);
        assert!(engine.unresolved_tables().await.is_empty());
        assert!(engine.tables_shared.read().unwrap().contains_key(&items));
    }

    /// A create on a table whose resolution is running is refused outright: it would register after
    /// the retirement's enumeration, pass its own closing generation check, and be orphaned by the
    /// `ResetTable` that ends the swap.
    #[tokio::test]
    async fn a_create_during_a_resolution_is_refused() {
        let (engine, items) = engine_with_items().await;
        let lock = engine.force_resolve_lock(&items).await;
        let err = engine.create_shape(&items, None, None, false, true).await.unwrap_err().to_string();
        assert!(err.contains("being resolved"), "unexpected refusal: {err}");
        drop(lock);
        // Released: the refusal is not sticky (the create fails later on the absent DS server, not
        // on the gate).
        let err = engine.create_shape(&items, None, None, false, true).await.unwrap_err().to_string();
        assert!(!err.contains("being resolved"), "the gate must clear with the lock: {err}");
    }

    /// A create that overlapped an epoch reset must never be installed (ADR-0004). Two independent
    /// gates say so, and this exercises both against the reset's real critical section.
    ///
    /// A create that arrives while the reset is in flight is refused outright — its backfill would
    /// take a snapshot at an LSN before the new slot's consistent point and permanently miss the
    /// window between them. And a create that had already registered before the reset enumerated its
    /// victims fails its closing check: the reset bumped the epoch generation in that same critical
    /// section, so the create it did not see is rolled back rather than left serving a gap.
    #[tokio::test]
    async fn a_create_overlapping_an_epoch_reset_is_refused_or_rolled_back() {
        let (engine, items) = engine_with_items().await;

        // Captured as a create does, under the lock, before the reset runs.
        let gens = engine.state.lock().await.capture_gens(std::slice::from_ref(&items));
        assert!(engine.ensure_schema_unchanged(&gens).await.is_ok(), "nothing has moved yet");

        let window = engine.force_epoch_reset_window().await;
        // (a) a create arriving now never registers.
        let err = engine.create_shape(&items, None, None, false, true).await.unwrap_err();
        assert!(
            err.downcast_ref::<crate::engine::EpochResetting>().is_some(),
            "a create during a reset must get the typed retryable refusal: {err:#}"
        );
        assert!(engine.state.lock().await.shapes.is_empty(), "and must leave nothing registered");

        // (b) the create that got in first fails its closing check instead.
        let err = engine.ensure_schema_unchanged(&gens).await.unwrap_err().to_string();
        assert!(err.contains("epoch was reset"), "unexpected closing refusal: {err}");

        // The refusal is not sticky: once the reset has bound its new epoch, creates work again
        // (this one fails later, on the absent DS server, not on the gate).
        drop(window);
        let err = engine.create_shape(&items, None, None, false, true).await.unwrap_err();
        assert!(
            err.downcast_ref::<crate::engine::EpochResetting>().is_none(),
            "the gate must clear with the reset: {err:#}"
        );
        // …but a create captured in the OLD epoch stays refused — its generation is gone for good.
        assert!(engine.ensure_schema_unchanged(&gens).await.is_err());
    }

    /// The replay guard, as a pure decision. A trigger whose transaction the boot seed already
    /// reflects must NOT restart the process — that is a re-delivery of the transaction that caused
    /// the previous restart, and exiting again is an exit loop.
    #[test]
    fn a_transaction_the_seed_already_reflects_does_not_rebuild_the_circuit() {
        // `passthrough` skips nothing (it is the no-backfill gate), so it always rebuilds.
        let never_visible = SnapshotGate::passthrough();
        assert!(circuit_needs_rebuild(Some(&never_visible), Some(TxnRef { xid: 42 })));

        // A snapshot whose xmin is above the trigger's xid HAS already absorbed it.
        let after = SnapshotGate::parse("100:100:", "0/0");
        assert!(!circuit_needs_rebuild(Some(&after), Some(TxnRef { xid: 42 })));
        // ...and one taken before it has not.
        assert!(circuit_needs_rebuild(Some(&after), Some(TxnRef { xid: 200 })));
    }

    /// No transaction context (the reconciler, a retry task) is no evidence, so the safe answer is
    /// to rebuild — as it is for a table with no seed gate at all.
    #[test]
    fn without_evidence_the_circuit_is_rebuilt() {
        let after = SnapshotGate::parse("100:100:", "0/0");
        assert!(circuit_needs_rebuild(Some(&after), None));
        assert!(circuit_needs_rebuild(None, Some(TxnRef { xid: 1 })));
        assert!(circuit_needs_rebuild(None, None));
    }
}
