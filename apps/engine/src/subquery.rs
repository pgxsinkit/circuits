//! Subquery support: shared, incrementally-maintained inner-set **nodes** and the cross-table
//! registry that moves outer rows in/out as inner sets change.
//!
//! A shape whose `WHERE` contains `col IN (SELECT proj FROM inner WHERE pred)` (or `NOT IN`) cannot be
//! evaluated row-locally — membership depends on the inner subquery's result set. We maintain that set
//! once per distinct subquery (keyed by a canonical [`SubquerySig`]) as a [`SubqueryNode`]: a map from
//! projected value to the set of inner-row primary keys producing it. A value is "in the set" iff its
//! contributor set is non-empty; tracking contributor pks (not a bare count) makes maintenance
//! reconcile-by-identity — set a row's presence to equal `match(row)` regardless of history.
//!
//! Identical subqueries share one node (the memory win + the sharing the design calls for). Nodes feed
//! dependents — outer shapes or *parent* nodes (a node whose inner `pred` itself references this node) —
//! along edges recorded by connecting column. When a value flips (a bucket goes empty↔non-empty), the
//! registry queries the dependent rows referencing that value and re-evaluates them (see
//! `on_table_delta`, added in a later step). This file (step 6) is the pure in-memory core: node
//! maintenance + the [`SubqueryEval`] read view. No Postgres, no streams yet.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::value::{Tup2, ZWeight};
use anyhow::{Context, Result};

use crate::ds::{DsClient, Envelope};
use crate::heap_size::HeapSize;
use crate::pk_dict::PkDict;
use crate::predicate::{CompiledPredicate, PredicateJson, SubqueryCollector, SubqueryEval, SubquerySig, subquery_sig};
use crate::schema::TableSchema;
use crate::subq_circuit::{Assert, Assertions, PkKey};
use crate::table_ref::TableRef;
use crate::value::{Row, Value};

/// Direction of a value-membership change on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlipDir {
    /// A value's contributor set went empty → non-empty (the value entered the inner set).
    Enter,
    /// A value's contributor set went non-empty → empty (the value left the inner set).
    Leave,
}

/// A single value-membership change emitted by [`SubqueryNode::reconcile_row`]. `value` may be
/// [`Value::Null`] (the null bucket — relevant to `NOT IN`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flip {
    pub value: Value,
    pub dir: FlipDir,
}

/// One maintained inner subquery: `SELECT proj_col FROM inner_table WHERE pred`, as a value set.
///
/// The set itself lives in the **membership circuit** (`crate::subq_circuit`) as the
/// `(node_id, value)` slice of one shared relation; the node keeps only the host-side reverse
/// index (`pk_value`) that makes maintenance reconcile-by-identity — evaluation can depend on
/// *other* nodes' current sets (nested `IN`), so a row's tuple is not a pure function of the
/// row and exact retraction needs the remembered old value.
pub struct SubqueryNode {
    pub sig: SubquerySig,
    pub inner_table: TableRef,
    /// Column index (in `inner_table`) of the projected value.
    pub proj_col: usize,
    /// Column index (in `inner_table`) of the primary key — used to key contributors.
    pub pk_col: usize,
    /// The inner predicate; may reference deeper nodes (evaluated via [`SubqueryEval`]).
    pub pred: Arc<CompiledPredicate>,
    /// The inner `where` as raw JSON (for seeding SQL, which must emit nested `IN (SELECT …)`).
    pub where_json: Option<PredicateJson>,
    /// The node's backfill-snapshot fence: inner deltas already visible to the seed snapshot are
    /// skipped (xid visibility, LSN fallback — see [`crate::pg::SnapshotGate`]).
    pub gate: crate::pg::SnapshotGate,
    /// `Some` while the node is being seeded (three-phase create): raw inner-table deltas that
    /// arrive mid-seed are buffered here and replayed through the seed gate at install — never
    /// applied to a half-seeded set (a snapshot row landing after a fresher delta would be a
    /// stale overwrite). `None` = live.
    ///
    /// Each buffered delta keeps the commit stamp it arrived under ([`BufferedDelta`], the same
    /// type the outer shape's buffer uses), because the replay is a LIVE per-pk decision and is
    /// stamped like every other one (see [`SubqueryNode::recent`]). Today no re-derivation can be
    /// in flight for a node that is mid-seed — [`SubqueryRegistry::queue_node_deferred`] defers it,
    /// and only a freshly minted node ever gets a seed buffer — so the stamp makes the replay's
    /// freshness explicit instead of an unstated consequence of that ordering.
    pub(crate) seed_buffer: Option<Vec<BufferedDelta>>,
    /// Re-derivations a CHILD node's flip aimed at this node while it was still seeding
    /// (`seed_buffer.is_some()`), replayed after the seed is installed — see
    /// [`SubqueryRegistry::queue_node_deferred`]. The parent-node analogue of
    /// [`PendingSubqueryShape::deferred`]: reconciling against an incomplete seed can miss rows,
    /// and later snapshot chunks could overwrite the change with older state.
    pub(crate) deferred: VecDeque<DeferredNodeWork>,
    /// The node's key in the membership circuit (registry-assigned, unique per live node).
    pub node_id: i64,
    /// The template this node is a bind of (see [`crate::predicate::subquery_template`]).
    pub template_key: String,
    /// The lifted parameter literals, positionally aligned with the template's `param_cols`.
    pub(crate) bind: Row,
    /// Number of dependents (shapes + parent nodes) referencing this node; drop the node at 0.
    pub refcount: usize,
    /// **Per-pk recency, for the duration of a node query-back only** — inner-row pk (the key
    /// [`SubqueryRegistry::assert_node_row`] and [`SubqueryRegistry::apply_node_evals`] work in)
    /// → the commit stamp `(lsn, xid)` of the LIVE contribution decision most recently taken for
    /// that pk on this node.
    ///
    /// The node analogue of [`SubqueryShape::recent`], and needed for the same reason: a parent
    /// node's re-derivation ([`requery_and_reconcile_parent`]) reads its inner rows from Postgres
    /// with the registry lock RELEASED, so a direct inner-table change committed after that read's
    /// snapshot is reconciled by [`SubqueryRegistry::on_table_delta`] step 1 in between — and the
    /// re-derivation would then re-assert the old contribution last. That is not a stream append,
    /// but it changes maintained node state, and the DEPENDENT shape appends what the node's flips
    /// say: the divergence is the same permanent one, one level down.
    ///
    /// Populated ONLY while [`inflight_querybacks`](Self::inflight_querybacks) `> 0` and cleared
    /// when it returns to zero — the steady state carries no per-pk map at all.
    pub(crate) recent: HashMap<String, (u64, Option<u64>)>,
    /// How many re-derivations of this node are between their registry-lock release and their
    /// reconcile. Non-zero is exactly the window `recent` has to cover.
    pub(crate) inflight_querybacks: u32,
}

impl HeapSize for SubqueryNode {
    /// `pred` (`Arc<CompiledPredicate>`) is shared with the registry's compiled evaluators, not
    /// uniquely owned by this node — skipped, like every other `Arc<...>` field in this module.
    /// `recent` is bounded by the pks touched while a re-derivation is in flight and is dropped
    /// when the last one finishes, but it is owned heap while it lives, so it is counted;
    /// `inflight_querybacks` (`u32`) is inline.
    fn heap_bytes(&self) -> usize {
        self.sig.heap_bytes()
            + self.inner_table.heap_bytes()
            + self.where_json.heap_bytes()
            + self.gate.heap_bytes()
            + self.seed_buffer.heap_bytes()
            + self.deferred.heap_bytes()
            + self.template_key.heap_bytes()
            + self.bind.heap_bytes()
            + self.recent.heap_bytes()
    }
}

impl SubqueryNode {
    pub fn new(
        sig: SubquerySig,
        inner_table: TableRef,
        proj_col: usize,
        pk_col: usize,
        pred: Arc<CompiledPredicate>,
        node_id: i64,
    ) -> Self {
        SubqueryNode {
            sig,
            inner_table,
            proj_col,
            pk_col,
            pred,
            where_json: None,
            gate: crate::pg::SnapshotGate::passthrough(),
            seed_buffer: None,
            deferred: VecDeque::new(),
            node_id,
            template_key: String::new(),
            bind: Row(Vec::new()),
            refcount: 0,
            recent: HashMap::new(),
            inflight_querybacks: 0,
        }
    }
}

/// One subquery template: the shared evaluation structure for every node (bind) whose inner
/// query differs only in the lifted equality literals — the KeyRouter factoring applied to
/// subqueries. A delta on the inner table is evaluated ONCE per template (residual + param
/// projection), then routed to the single affected bind by hash lookup, instead of one full
/// predicate eval per literal-keyed node.
pub(crate) struct TemplateGroup {
    pub(crate) inner_table: TableRef,
    /// Column index (in `inner_table`) of the projected value (same for every bind).
    proj_col: usize,
    /// The compiled residual (lifted equalities removed, other literals baked in). May contain
    /// nested `IN` leaves, resolved via [`SubqueryEval`] against already-collected child nodes.
    residual: Arc<CompiledPredicate>,
    /// Column indices of the lifted parameters, aligned with each bind `Row`.
    param_cols: Vec<usize>,
    /// bind literals -> the node serving that bind.
    pub(crate) binds: HashMap<Row, SubquerySig>,
    /// pk id -> nodes of this template currently holding a contribution from that pk. An exact
    /// inverted index over the nodes' contributor sets (maintained in lockstep by
    /// `reconcile_node_row`), so a row that stops matching finds its old bind in O(1) instead
    /// of scanning every bind. Keyed by the pk's dictionary id (see [`crate::pk_dict::PkDict`]),
    /// not the pk string — the string lives once in the shared dictionary.
    pk_nodes: HashMap<u32, HashSet<SubquerySig>>,
}

impl HeapSize for TemplateGroup {
    /// `residual` (`Arc<CompiledPredicate>`) is shared across every bind of this template —
    /// skipped, like other `Arc<...>` fields in this module.
    fn heap_bytes(&self) -> usize {
        self.inner_table.heap_bytes()
            + self.param_cols.heap_bytes()
            + self.binds.heap_bytes()
            + self.pk_nodes.heap_bytes()
    }
}

/// Identifies a dependent of a node: an outer shape or a parent node, plus the connecting column on the
/// dependent's table whose value `= the flipped node value` selects the affected rows.
#[derive(Debug, Clone)]
pub enum Dependent {
    /// An outer subquery shape (by registry shape id).
    Shape(String),
    /// A parent node (by signature) whose inner `pred` references this node.
    Node(SubquerySig),
}

impl HeapSize for Dependent {
    fn heap_bytes(&self) -> usize {
        match self {
            Dependent::Shape(id) => id.heap_bytes(),
            Dependent::Node(sig) => sig.heap_bytes(),
        }
    }
}

/// An edge from a node to a dependent: when the node flips `value`, rows of the dependent's table with
/// `connecting_col = value` may change membership.
#[derive(Debug, Clone)]
pub struct Edge {
    pub node_sig: SubquerySig,
    pub dependent: Dependent,
    /// Column index (in the dependent's table) connecting to the node.
    pub connecting_col: usize,
    pub negated: bool,
    /// True iff a NULL entering/leaving the node's set can change this dependent's membership. That is
    /// the case when the `IN` leaf is itself negated (`NOT IN` — SQL: a NULL in the set makes it
    /// UNKNOWN) **or** sits under any `Not{…}` wrapper: with no negation anywhere above the leaf, a
    /// NULL only moves the leaf between FALSE and UNKNOWN, and AND/OR are monotone in
    /// FALSE < UNKNOWN < TRUE, so overall TRUE-ness (inclusion) cannot change. Any negation breaks the
    /// monotonicity, so those dependents must be fully re-derived on a NULL flip.
    pub null_sensitive: bool,
}

impl HeapSize for Edge {
    /// `connecting_col`/`negated`/`null_sensitive` are inline.
    fn heap_bytes(&self) -> usize {
        self.node_sig.heap_bytes() + self.dependent.heap_bytes()
    }
}

/// A registered outer subquery shape: an ordinary materialized shape whose predicate contains
/// `IN (SELECT …)`. The engine emits `upsert`/`delete` envelopes to `stream_path` as membership
/// changes (from outer-row deltas and from inner-set flips).
pub struct SubqueryShape {
    pub shape_id: String,
    pub outer_table: TableRef,
    pub stream_path: String,
    /// The outer predicate (with `InSubquery` leaves resolving against this registry's nodes).
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    /// The shape's backfill-snapshot fence; outer deltas already visible to the backfill are skipped.
    pub gate: crate::pg::SnapshotGate,
    /// Envelopes appended to this shape's stream (backfill + live), for the visualizer's per-node
    /// state. Atomic because the append paths hold `&self`.
    pub emitted: std::sync::atomic::AtomicU64,
    /// This shape's key into the host-side per-feed key set ([`crate::subq_feed::FeedSet`],
    /// `feed_sets`). The set replaces the old `known_members` set: a delete is delivered iff
    /// the set actually retracts, so a "not a member" verdict for a pk the stream never
    /// contained is structurally a no-op — the wake-storm gate (PR #30) with no filter to keep
    /// in sync.
    pub(crate) feed_id: i64,
    /// **Per-pk recency, for the duration of a query-back only** — `pk_dict` id → the commit
    /// stamp `(lsn, xid)` of the LIVE outer-row decision most recently applied to this shape's
    /// stream for that pk.
    ///
    /// A membership query-back reads its candidate rows from Postgres *without* the registry
    /// lock and evaluates them when it reacquires it, so a direct outer-row change committed
    /// after that read's snapshot can be evaluated and emitted in between — and the query-back's
    /// older row would then land last on the stream. Native consumers fold by durable-stream
    /// offset, so "last" is "final": permanent divergence from Postgres. Recording the live
    /// decision's stamp here lets the query-back drop exactly those candidates whose live
    /// decision its own snapshot could not have seen (see [`EmissionSource`]).
    ///
    /// Populated ONLY while [`inflight_querybacks`](Self::inflight_querybacks) `> 0` and cleared
    /// when it returns to zero: with no query-back in flight there is no older read to lose to,
    /// so the steady state carries no per-pk map at all.
    pub(crate) recent: HashMap<u32, (u64, Option<u64>)>,
    /// How many query-backs for this shape are between their registry-lock release and their
    /// evaluation. Non-zero is exactly the window `recent` has to cover.
    pub(crate) inflight_querybacks: u32,
}

impl HeapSize for SubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `emitted` (`AtomicU64`),
    /// `feed_id` (`i64`) and `inflight_querybacks` (`u32`) are inline. `recent` is bounded by the
    /// pks touched while a query-back is in flight and is dropped when the last one finishes, but
    /// it is owned heap while it lives, so it is counted.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes()
            + self.outer_table.heap_bytes()
            + self.stream_path.heap_bytes()
            + self.gate.heap_bytes()
            + self.recent.heap_bytes()
    }
}

/// Where the candidates handed to [`SubqueryRegistry::emit_for_shapes`] came from — which decides
/// how that evaluation relates in TIME to the other evaluations racing for the same stream.
///
/// The emission tail is absolute per pk (upsert if the row matches now, else an idempotent gated
/// delete), so convergence never depended on ordering *between* pks. It does depend on the last
/// evaluation of a given pk being the freshest one, and that is exactly what a query-back cannot
/// promise on its own: it reads Postgres at one snapshot with the registry lock released.
///
/// * `Live`/`Replay` carry the commit stamp of the outer-table change being evaluated. They are
///   the freshest possible verdict for their pks, so while a query-back is in flight they RECORD
///   that stamp in [`SubqueryShape::recent`].
/// * `QueryBack` carries the gate of the read its candidates came from, and DROPS every candidate
///   whose recorded live stamp is not visible to that gate — the live decision was taken on a
///   commit this read could not have seen, so this row is stale and re-emitting it would make an
///   older read the stream's last word.
pub(crate) enum EmissionSource<'a> {
    /// A live outer-table delta from [`SubqueryRegistry::on_table_delta`] step 2.
    Live { lsn: u64, xid: Option<u64> },
    /// A buffered outer-table delta replayed at install ([`SubqueryRegistry::finish_create`]),
    /// with the stamp it was buffered under. Identical to `Live` in effect — a create's deferred
    /// query-back can run immediately after install, so these decisions must be protected the
    /// same way — and kept a distinct variant because the two call sites are not interchangeable.
    Replay { lsn: u64, xid: Option<u64> },
    /// Candidates read from Postgres by a query-back, under `gate`'s snapshot.
    QueryBack { gate: &'a crate::pg::SnapshotGate },
}

/// One table delta buffered by a mid-create shape (outer table) or a mid-seed node (inner table),
/// WITH the commit stamp it arrived under.
///
/// The stamp is retained (it used to be dropped, leaving the install replay unstamped) because the
/// replay is a live decision like any other: a create hands back deferred work that can run a
/// query-back the moment the shape is installed — or a node re-derivation the moment the seed is —
/// and that query-back must be able to tell that the replayed decision is newer than its own read.
/// See [`EmissionSource::Replay`] for the shape half and [`SubqueryNode::recent`] for the node half.
pub(crate) struct BufferedDelta {
    /// Commit LSN of the change (`0` = unknown).
    lsn: u64,
    /// Commit xid, when the source stamped one — the exact half of the visibility test.
    xid: Option<u64>,
    delta: Vec<Tup2<Row, ZWeight>>,
}

impl HeapSize for BufferedDelta {
    /// `lsn`/`xid` are inline.
    fn heap_bytes(&self) -> usize {
        self.delta.heap_bytes()
    }
}

/// A `TableSchema` lookup shared with the engine's compiled schema.
pub type SchemaMap = Arc<HashMap<TableRef, TableSchema>>;

/// One node's inner-set index as [`SubqueryRegistry::node_value_index`] returns it: the true
/// distinct-value count, the node's refcount, up to `cap` `(value, contributor-count)` pairs, and
/// whether that list was truncated.
pub type NodeValueIndex = (usize, usize, Vec<(serde_json::Value, usize)>, bool);

/// Cheap, sampler-safe registry cardinalities (see [`SubqueryRegistry::mem_totals`]).
pub struct MemTotals {
    pub nodes: usize,
    /// Total contributor pks across all nodes (the dominant per-node state).
    pub contributors: usize,
    pub distinct: usize,
    pub shapes: usize,
    pub edges: usize,
    /// Total pks delivered across all shapes' host-side feed sets (Task 2.2).
    pub feed_entries: usize,
}

/// Per-node introspection (served at `GET /subqueries`).
#[derive(Clone, serde::Serialize)]
pub struct NodeStat {
    pub sig: SubquerySig,
    pub inner_table: TableRef,
    pub distinct_values: usize,
    pub refcount: usize,
    /// The template this node is a bind of — equal across nodes that differ only in lifted
    /// equality literals (template-level sharing).
    pub template: String,
}

/// Propagation work that reached a shape while it was still being created, deferred until it is
/// installed (see [`PendingSubqueryShape::deferred`]).
pub enum DeferredShapeWork {
    /// An inner-set value flipped: re-derive the outer rows with `connecting_col = value`.
    Value { connecting_col: usize, value: Value, txid: Option<String> },
    /// A NULL flip on a NULL-sensitive edge: re-derive every outer row.
    Full { txid: Option<String> },
}

impl HeapSize for DeferredShapeWork {
    fn heap_bytes(&self) -> usize {
        match self {
            DeferredShapeWork::Value { value, txid, .. } => value.heap_bytes() + txid.heap_bytes(),
            DeferredShapeWork::Full { txid } => txid.heap_bytes(),
        }
    }
}

/// Propagation work that reached a PARENT NODE while it was still being seeded, deferred until its
/// seed is installed (see [`SubqueryNode::deferred`]).
///
/// The node analogue of [`DeferredShapeWork`], minus the `txid`: re-deriving a node reconciles an
/// in-memory value set and emits nothing, so there is no envelope for a txid to travel on — the
/// flips it produces carry the change onward to the dependents that do emit.
pub enum DeferredNodeWork {
    /// A child value flipped: re-derive the parent's inner rows with `connecting_col = value`.
    Value { connecting_col: usize, value: Value },
    /// A NULL flip on a NULL-sensitive edge: re-derive every inner row of the parent.
    Full,
}

impl HeapSize for DeferredNodeWork {
    fn heap_bytes(&self) -> usize {
        match self {
            DeferredNodeWork::Value { value, .. } => value.heap_bytes(),
            DeferredNodeWork::Full => 0,
        }
    }
}

/// A subquery shape between `begin_create` and `finish_create`: registration exists (so its
/// nodes are refcounted, its edges recorded, and its deltas buffered) but seeding/backfill runs
/// outside the registry lock.
pub struct PendingSubqueryShape {
    pub shape_id: String,
    pub outer_table: TableRef,
    pub stream_path: String,
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    pub changes_only: bool,
    /// This create's node-refcount log (for exact rollback on failure).
    collect_log: Vec<SubquerySig>,
    /// Outer-table deltas buffered while the backfill runs; replayed through the gate at install,
    /// each with the commit stamp it arrived under (see [`BufferedDelta`]).
    buffer: Vec<BufferedDelta>,
    /// Flips that reached this shape's edges before it was installed, replayed at install.
    ///
    /// Phase A commits the shape's edges, so a flip on a node it SHARES with an already-live shape
    /// is propagated immediately — while phase B is still backfilling and there is no shape to
    /// move. The inner change may have committed after the backfill's snapshot, so those outer rows
    /// are in neither the snapshot nor the flip's reach: dropping the flip loses them for good.
    /// Buffering it here is the same answer the outer `buffer` gives to outer deltas.
    deferred: VecDeque<DeferredShapeWork>,
}

impl HeapSize for PendingSubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `changes_only` is inline.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes()
            + self.outer_table.heap_bytes()
            + self.stream_path.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.buffer.heap_bytes()
            + self.deferred.heap_bytes()
    }
}

/// What phase B (Postgres I/O, run WITHOUT the registry lock) needs from `begin_create`.
pub struct BeginCreate {
    /// Fresh nodes this create must seed: `(sig, inner_table, inner where-JSON)`.
    pub seeds: Vec<(SubquerySig, TableRef, Option<PredicateJson>)>,
    /// Schema map snapshot for SQL emission.
    pub schemas: SchemaMap,
}

/// What phase C hands the caller to propagate once the create's registry lock is released. All
/// three are effects the create computed but did not deliver, so all three are barrier-covered
/// (`pendingFlips`) and fail closed exactly like a live flip batch.
pub struct FinishedCreate {
    /// Flips produced by replaying the fresh nodes' buffered inner deltas through their seed gates.
    pub work: VecDeque<(SubquerySig, Flip)>,
    /// Work that reached the SHAPE while it was pending (see [`PendingSubqueryShape::deferred`]),
    /// to run against the now-installed shape.
    pub deferred: VecDeque<DeferredShapeWork>,
    /// Work that reached a fresh PARENT NODE while it was still seeding (see
    /// [`SubqueryNode::deferred`]), to re-derive now that the node's set is real. Each item's flips
    /// are then walked on down the DAG like any other.
    pub node_work: VecDeque<(SubquerySig, DeferredNodeWork)>,
}

/// The cross-table registry of subquery nodes + shapes + edges. Implements [`SubqueryEval`] so a
/// predicate's subquery leaves resolve against the maintained node sets. One per engine, behind a
/// `tokio::Mutex`; every table tailer calls [`on_table_delta`](Self::on_table_delta).
pub struct SubqueryRegistry {
    /// Nodes by canonical signature (shared across identical subqueries).
    pub nodes: HashMap<SubquerySig, SubqueryNode>,
    /// Edges from each node to its dependents, keyed by the node's signature — flip
    /// propagation looks up ONE node's dependents per flip, so this must not be a scan over
    /// every edge in the registry (the propagation-side analogue of template-grouped eval).
    edges: HashMap<SubquerySig, Vec<Edge>>,
    /// Edges appended by the in-flight `begin_create` compile, committed into `edges` only
    /// when the whole registration succeeds (a failed/conflicted compile just clears this —
    /// exact rollback without index bookkeeping).
    staged_edges: Vec<Edge>,
    /// Registered outer subquery shapes by engine shape id.
    pub shapes: HashMap<String, SubqueryShape>,
    /// Necessary-conjunct index over `shapes`, bucketed by outer table — what makes an outer-table
    /// delta cost `O(candidates)` instead of a scan over every registered subquery shape. Kept in
    /// lockstep with `shapes` by [`install_shape`](Self::install_shape) /
    /// [`drop_subquery_shape`](Self::drop_subquery_shape); see [`crate::subq_index`] for the
    /// old-image ∪ new-image probe rule that keeps absolute emission correct.
    shape_index: crate::subq_index::SubqueryShapeIndex,
    /// The membership circuit: every node's value set as one dbsp relation; flip detection is
    /// the circuit's incremental distinct (see `crate::subq_circuit`).
    circuit: crate::subq_circuit::MembershipCircuit,
    /// The global pk dictionary shared by the circuit tier: every contributor / feed key is
    /// `(id, pk_id)` where `pk_id = pk_dict.get_or_insert(pk)`. Ids are minted here (when building
    /// assertions) and resolved back to pk strings here (at the emission seam), so the circuit and
    /// its indexes never store a heap pk string. One instance per engine (per registry).
    pk_dict: Arc<PkDict>,
    /// Next circuit node id (monotonic; ids of dropped nodes are never reused, so a stale
    /// snapshot read can never alias a new node's slice).
    next_node_id: i64,
    /// circuit node id -> node signature (maps circuit flip deltas back to nodes).
    node_by_id: HashMap<i64, SubquerySig>,
    /// Next feed id (per-shape key; monotonic, never reused).
    next_feed_id: i64,
    /// circuit feed id -> shape id (maps feed deltas back to shapes).
    feed_by_id: HashMap<i64, String>,
    /// Host-side per-feed key sets (Task 2.2, dbsp-ds-dh6): per-feed Roaring bitmaps over pk_ids,
    /// implementing the delete gate. A delete is emitted iff `remove(feed_id, pk_id)` returns true.
    /// Mutated only under this registry's lock, synchronously (no `.await`), in the same critical
    /// section as the emission decision — so the emission decision and the bitmap transition are
    /// one indivisible step.
    feed_sets: crate::subq_feed::FeedSet,
    /// Shared evaluation templates (see [`TemplateGroup`]), keyed by
    /// [`crate::predicate::subquery_template`]'s key.
    pub(crate) templates: HashMap<String, TemplateGroup>,
    /// Nodes created but not yet seeded from Postgres (deepest-first).
    pending_seed: Vec<SubquerySig>,
    /// Shapes between `begin_create` and `finish_create` (the three-phase create): their
    /// outer-table deltas are buffered here and replayed through the shape's gate at install.
    pending_shapes: Vec<PendingSubqueryShape>,
    /// Every node-refcount increment made by the in-flight `create_subquery_shape` (one entry per
    /// `collect()` call). On failure the create is rolled back exactly: each logged sig is decremented
    /// once, and nodes that return to zero are removed. The registry mutex is held for the whole
    /// create, so the log can't interleave with another create.
    collect_log: Vec<SubquerySig>,
    ds: DsClient,
    pg_url: Option<String>,
    schemas: SchemaMap,
    /// Ordered emission lanes (see `engine::emission`): membership envelopes are enqueued
    /// under this registry's lock — per-stream enqueue order = eval order — and land on their
    /// streams asynchronously, covered by the `pendingFlips` barrier. `None` (unit tests)
    /// falls back to a direct reliable append.
    lanes: Option<crate::engine::emission::EmissionLanes>,
}

impl HeapSize for SubqueryRegistry {
    /// `circuit` (the dbsp membership relation) is accounted separately — see
    /// [`SubqueryRegistry::circuit_bytes`]'s `bytes_membership_circuit` measurement — so it is
    /// deliberately excluded here to avoid double-counting the same state under two `/memory`
    /// fields. The `pk_dict` is likewise accounted separately (`bytes_pk_dict`, see
    /// [`SubqueryRegistry::pk_dict_bytes`]) — `Arc`-shared and reported once. `feed_sets` is
    /// likewise excluded, accounted separately as `bytes_feed_sets` (see
    /// [`SubqueryRegistry::feed_sets_bytes`]). `ds` (a client
    /// handle) and `schemas` (`Arc`-shared with the engine's compiled
    /// schema) are not uniquely owned; `lanes` holds channel senders, not owned data;
    /// `next_node_id`/`next_feed_id` are inline counters.
    fn heap_bytes(&self) -> usize {
        self.nodes.heap_bytes()
            + self.edges.heap_bytes()
            + self.staged_edges.heap_bytes()
            + self.shapes.heap_bytes()
            + self.shape_index.heap_bytes()
            + self.node_by_id.heap_bytes()
            + self.feed_by_id.heap_bytes()
            + self.templates.heap_bytes()
            + self.pending_seed.heap_bytes()
            + self.pending_shapes.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.pg_url.heap_bytes()
    }
}

impl SubqueryRegistry {
    pub fn new(ds: DsClient, pg_url: Option<String>) -> Self {
        SubqueryRegistry {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            staged_edges: Vec::new(),
            shapes: HashMap::new(),
            shape_index: crate::subq_index::SubqueryShapeIndex::default(),
            circuit: crate::subq_circuit::MembershipCircuit::start().expect("membership circuit failed to start"),
            pk_dict: Arc::new(PkDict::new()),
            next_node_id: 1,
            node_by_id: HashMap::new(),
            next_feed_id: 1,
            feed_by_id: HashMap::new(),
            feed_sets: crate::subq_feed::FeedSet::new(),
            templates: HashMap::new(),
            pending_seed: Vec::new(),
            pending_shapes: Vec::new(),
            collect_log: Vec::new(),
            ds,
            pg_url,
            schemas: Arc::new(HashMap::new()),
            lanes: None,
        }
    }

    /// Apply a contributor assertion batch to the membership circuit and map its member deltas
    /// back to node signatures. Callers hold the registry lock across the await — the circuit
    /// thread never takes this lock, and awaiting the step is what gives every later membership
    /// read read-your-writes over this batch. (The delete gate is no longer a circuit relation —
    /// it lives in [`crate::subq_feed::FeedSet`], mutated synchronously under this same lock.)
    async fn apply_asserts(&mut self, asserts: Assertions) -> Vec<(SubquerySig, Flip)> {
        if asserts.is_empty() {
            return Vec::new();
        }
        self.circuit
            .apply(asserts)
            .await
            .into_iter()
            .filter_map(|d| {
                let sig = self.node_by_id.get(&d.node_id)?.clone();
                let dir = if d.delta > 0 { FlipDir::Enter } else { FlipDir::Leave };
                Some((sig, Flip { value: d.value, dir }))
            })
            .collect()
    }

    /// Build one node's contributor assertion for `pk` and keep the template's `pk_nodes`
    /// inverted index in lockstep. `pk_nodes` IS the record of presence per template (the
    /// circuit's upsert map is the record of the value), so all presence transitions go
    /// through here. Inserts always assert (idempotent; a changed value must flow); absent
    /// stays quiet unless the node actually held the pk.
    fn assert_node_row(&mut self, sig: &SubquerySig, pk: &str, present: Option<Value>) -> Option<Tup2<PkKey, Assert>> {
        let (node_id, tkey) = {
            let node = self.nodes.get(sig)?;
            (node.node_id, node.template_key.clone())
        };
        let pk_id = self.pk_dict.get_or_insert(pk);
        let key = PkKey { id: node_id, pk: pk_id };
        match present {
            Some(v) => {
                if let Some(tpl) = self.templates.get_mut(&tkey) {
                    tpl.pk_nodes.entry(pk_id).or_default().insert(sig.clone());
                }
                Some(Tup2(key, Assert::Insert(v)))
            }
            None => {
                let had = self
                    .templates
                    .get_mut(&tkey)
                    .and_then(|tpl| {
                        let set = tpl.pk_nodes.get_mut(&pk_id)?;
                        let had = set.remove(sig);
                        if set.is_empty() {
                            tpl.pk_nodes.remove(&pk_id);
                        }
                        Some(had)
                    })
                    .unwrap_or(false);
                had.then_some(Tup2(key, Assert::Delete))
            }
        }
    }

    /// Assert a batch of per-pk evaluations against ONE node and return the resulting flips
    /// (seeding replay and flip-driven parent re-derivations, where the caller already
    /// evaluated the node's full predicate per row).
    async fn apply_node_evals(&mut self, sig: &SubquerySig, evals: Vec<(String, Option<Value>)>) -> Vec<Flip> {
        let mut asserts = Assertions::default();
        for (pk, pv) in evals {
            asserts.contributors.extend(self.assert_node_row(sig, &pk, pv));
        }
        // Every assertion belongs to `sig`, so the sig on each flip is redundant here.
        let flips = self.apply_asserts(asserts).await;
        flips.into_iter().map(|(_, f)| f).collect()
    }

    /// Test seam (never compiled into the engine): assert one contributor onto a node exactly as
    /// chunk seeding does, so a cancellation test can park a create with a fresh node
    /// already holding real membership-circuit state.
    #[cfg(test)]
    pub(crate) async fn assert_seed_row_for_test(&mut self, sig: &SubquerySig, pk: &str, value: Value) {
        self.apply_node_evals(sig, vec![(pk.to_string(), Some(value))]).await;
    }

    /// The node's current distinct-value count, read from the circuit snapshot.
    pub(crate) fn circuit_distinct(&self, node_id: i64) -> usize {
        self.circuit.values_for_node(node_id, 0).0
    }

    pub(crate) fn set_lanes(&mut self, lanes: crate::engine::emission::EmissionLanes) {
        self.lanes = Some(lanes);
    }

    /// Deliver membership envelopes to a shape stream in **evaluation order**: enqueue on the
    /// stream's emission lane while the caller holds this registry's lock (per-stream FIFO ⇒
    /// append order = eval order — the "data in the right place" invariant). Without lanes
    /// (unit tests) this awaits a direct reliable append, the pre-lane behavior.
    async fn deliver(&self, stream_path: &str, envs: Vec<Envelope>) {
        match &self.lanes {
            Some(l) => l.enqueue(stream_path, envs),
            None => {
                self.ds.append_reliable(stream_path, &envs).await;
            }
        }
    }

    pub fn set_schemas(&mut self, schemas: SchemaMap) {
        self.schemas = schemas;
    }

    /// Does any node's inner table or any shape's outer table equal `table`? (Fast skip for tailers of
    /// tables not involved in any subquery.)
    pub fn touches(&self, table: &TableRef) -> bool {
        // Called once per replicated envelope, so the shape half is answered by the conjunct
        // index's table buckets in O(1) rather than a scan over every registered shape — the same
        // reason step 2 of `on_table_delta` is indexed. Nodes are shared (one per distinct inner
        // query, orders of magnitude fewer than shapes) and `pending_shapes` holds only in-flight
        // creates, so those two stay linear.
        self.shape_index.has_table(table.as_str())
            || self.nodes.values().any(|n| &n.inner_table == table)
            || self.pending_shapes.iter().any(|p| &p.outer_table == table)
    }

    /// Number of maintained nodes (shared inner sets).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The live **inner-set index** of one node (the visualizer's "see the index" view): up to `cap`
    /// `(value, contributor-count)` pairs, most-shared first, plus the true distinct count, refcount, and
    /// whether the list was truncated. This is the actual engine-maintained set, not derivable from topology.
    pub fn node_value_index(&self, sig: &str, cap: usize) -> Option<NodeValueIndex> {
        let n = self.nodes.get(sig)?;
        let (distinct, vals) = self.circuit.values_for_node(n.node_id, cap);
        let vals: Vec<(serde_json::Value, usize)> = vals.into_iter().map(|(v, c)| (v.to_json(), c)).collect();
        Some((distinct, n.refcount, vals, distinct > cap))
    }

    /// Memory-relevant registry totals: maintained nodes, total contributor pks across all nodes (the
    /// dominant per-node state — one entry per inner row producing a value), distinct values, shapes,
    /// edges, and total feed entries (pks delivered across all shapes' host-side feed sets). Used by
    /// the memory probe to attribute subquery state growth — cheap enough for the 500ms background
    /// sampler (`mem::spawn_sampler`): everything here is already published/derivable per-node index
    /// state or an O(containers) bitmap-length sum, the same class of walk this method always did.
    ///
    /// Does NOT include the byte-level measurements (`bytes_membership_circuit`, `bytes_feed_sets`)
    /// — those are on-demand-only (`GET /memory`); see [`Self::circuit_bytes`].
    pub fn mem_totals(&self) -> MemTotals {
        let mut contributors = 0;
        let mut distinct = 0;
        for n in self.nodes.values() {
            let (d, vals) = self.circuit.values_for_node(n.node_id, usize::MAX);
            contributors += vals.iter().map(|(_, c)| c).sum::<usize>();
            distinct += d;
        }
        MemTotals {
            nodes: self.nodes.len(),
            contributors,
            distinct,
            shapes: self.shapes.len(),
            edges: self.edges_count(),
            feed_entries: self.feed_sets.total_entries(),
        }
    }

    /// Measured owned/on-disk bytes of the membership circuit's published snapshots
    /// (`bytes_membership_circuit` and its `bytes_circuit_integral` / `bytes_circuit_snapshots`
    /// split) — the on-demand-only (`GET /memory`) counterpart to [`Self::mem_totals`]. Never
    /// called from the 500ms background sampler.
    ///
    /// Replaces the former `key_count × 88 B` estimate with dbsp's exact per-batch
    /// `approximate_byte_size` (columnar bytes when resident; on-disk file size when spilled —
    /// see `subq_circuit`'s `SpillConfig`). Cheap: reads the two snapshot slots the circuit
    /// already publishes, no circuit round-trip. See [`crate::subq_circuit::CircuitBytes`] for
    /// what each term covers and the (profiler-only) non-published state it does not. Covers only
    /// the contributor relation now — the per-feed key sets left the circuit (see
    /// [`Self::feed_sets_bytes`]).
    pub fn circuit_bytes(&self) -> crate::subq_circuit::CircuitBytes {
        self.circuit.snapshot_bytes()
    }

    /// Diagnostic only: the membership circuit's full dbsp profiler dump —
    /// `(total_used_bytes, total_storage_size, per-operator profile JSON)`. Heavy (profiler
    /// round-trip through the circuit thread); on-demand only (`GET /debug/dbsp-profile`),
    /// never from the 500 ms sampler.
    pub async fn circuit_profile_dump(&self) -> (usize, usize, String) {
        self.circuit.profile_dump().await
    }

    /// Estimated owned heap of the host-side per-feed key sets (`bytes_feed_sets` in `GET /memory`)
    /// — the delete gate's Roaring bitmaps, moved out of the membership circuit in Task 2.2
    /// (dbsp-ds-dh6). A lower-bound owned floor (serialized bitmap payloads + the outer HashMap
    /// backing store). On-demand only (a per-bitmap payload walk); never on the 500ms sampler path.
    pub fn feed_sets_bytes(&self) -> usize {
        self.feed_sets.heap_bytes()
    }

    /// Estimated owned heap of the global pk dictionary (`bytes_pk_dict` in `GET /memory`) — the
    /// once-per-distinct-pk string storage plus its forward/reverse index. Accounted separately
    /// from the registry's own `heap_bytes` (the dictionary is `Arc`-shared and append-only; this
    /// makes the string-interning trade visible). On-demand only — never on the sampler path.
    pub fn pk_dict_bytes(&self) -> usize {
        self.pk_dict.heap_bytes()
    }

    /// Per-node topology for the introspection endpoint: signature, inner table, current distinct value
    /// count, and the dependent refcount. Two shapes referencing the same subquery show one node with
    /// `refcount == 2` (proves sharing).
    pub fn stats(&self) -> Vec<NodeStat> {
        let mut out: Vec<NodeStat> = self
            .nodes
            .values()
            .map(|n| NodeStat {
                sig: n.sig.clone(),
                inner_table: n.inner_table.clone(),
                distinct_values: self.circuit_distinct(n.node_id),
                refcount: n.refcount,
                template: n.template_key.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.sig.cmp(&b.sig));
        out
    }

    /// Live state summaries for every registry-owned graph node (`node:<sig>` inner sets and
    /// subquery `shape:<sid>` sinks), keyed by graph node id — merged into `GET /state` snapshots
    /// and the tailers' SSE `state` events.
    pub fn state_summaries(&self) -> Vec<(String, crate::engine::NodeStateSummary)> {
        let mut out = Vec::with_capacity(self.nodes.len() + self.shapes.len());
        for (sig, n) in &self.nodes {
            out.push((
                format!("node:{sig}"),
                crate::engine::NodeStateSummary::SubqueryNode {
                    distinct_values: self.circuit_distinct(n.node_id),
                    refcount: n.refcount,
                },
            ));
        }
        for (sid, s) in &self.shapes {
            out.push((
                format!("shape:{sid}"),
                crate::engine::NodeStateSummary::Shape {
                    emitted: s.emitted.load(std::sync::atomic::Ordering::Relaxed),
                },
            ));
        }
        out
    }

    /// Outgoing edges for a node signature. O(that node's own edge list).
    fn edges_of(&self, sig: &SubquerySig) -> Vec<Edge> {
        self.edges.get(sig).cloned().unwrap_or_default()
    }

    /// Every edge in the registry (introspection only — hot paths use [`edges_of`]).
    pub(crate) fn all_edges(&self) -> impl Iterator<Item = &Edge> {
        self.edges.values().flatten()
    }

    /// Total edge count (memory probe + tests).
    pub fn edges_count(&self) -> usize {
        self.edges.values().map(Vec::len).sum()
    }

    /// Commit one edge (staged during creates; direct in tests).
    fn add_edge(&mut self, e: Edge) {
        self.edges.entry(e.node_sig.clone()).or_default().push(e);
    }

    /// Remove a dying node's edge entries: its outgoing list, plus the incoming edges that
    /// point at it from its children's lists (a dependent edge lives under the CHILD's key).
    fn remove_node_edges(&mut self, sig: &SubquerySig, child_sigs: &[SubquerySig]) {
        self.edges.remove(sig);
        for c in child_sigs {
            if let Some(v) = self.edges.get_mut(c) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Node(s) if s == sig));
                if v.is_empty() {
                    self.edges.remove(c);
                }
            }
        }
    }

    // --- registration -------------------------------------------------------------------------

    /// Register an outer subquery shape: compile the outer predicate (creating/deduping nodes + edges),
    /// seed any new nodes from Postgres, backfill the shape, and record it. Idempotent per shape id.
    ///
    /// **Atomic**: on any failure (unknown table, seed error, backfill/append error) every refcount
    /// increment, node, edge, and pending-seed entry made by this call is rolled back, so a failed
    /// create leaves the registry exactly as it was — no half-registered node that would silently
    /// serve wrong (unseeded) membership to a later identical create.
    /// Phase A of the three-phase create (call under the registry lock; brief, in-memory):
    /// compile the predicate (discovering/refcounting nodes — fresh ones start buffering),
    /// record edges, and register a pending shape that buffers its outer-table deltas from this
    /// moment on (no delta can fall between registration and the phase-B snapshot). Returns
    /// what phase B needs. The engine holds subquery initialization admission through phase C or
    /// rollback: an existing node must already be seeded before a new create can reference it.
    pub fn begin_create(
        &mut self,
        shape_id: &str,
        outer_table: &TableRef,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<BeginCreate> {
        let outer_ts = self.schemas.get(outer_table).cloned().context("subquery shape: unknown outer table")?;
        // Compile on a scratch collector so any compile/invariant failure has no effects.
        self.staged_edges.clear();
        self.collect_log.clear();
        let pred = match CompiledPredicate::compile_with(where_json, &outer_ts, self) {
            Ok(p) => Arc::new(p),
            Err(e) => {
                let log = std::mem::take(&mut self.collect_log);
                self.rollback_refs(log);
                return Err(e);
            }
        };
        let log = std::mem::take(&mut self.collect_log);
        // A shared node still seeding means initialization admission was released too early.
        // Fail closed rather than evaluating against incomplete membership.
        let fresh: Vec<SubquerySig> = std::mem::take(&mut self.pending_seed);
        let conflicted =
            log.iter().any(|sig| !fresh.contains(sig) && self.nodes.get(sig).is_some_and(|n| n.seed_buffer.is_some()));
        if conflicted {
            self.rollback_refs(log);
            anyhow::bail!("subquery initialization invariant: shared node is still seeding");
        }
        // Fresh nodes start buffering their inner-table deltas.
        let mut seeds = Vec::with_capacity(fresh.len());
        for sig in &fresh {
            if let Some(n) = self.nodes.get_mut(sig) {
                n.seed_buffer = Some(Vec::new());
                seeds.push((sig.clone(), n.inner_table.clone(), n.where_json.clone()));
            }
        }
        // Shape-level edges (staged with the compile's child edges; committed below).
        for leaf in collect_in_leaves(&pred) {
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Shape(shape_id.to_string()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        // Registration is definitely happening: commit the staged edges.
        for e in std::mem::take(&mut self.staged_edges) {
            self.add_edge(e);
        }
        // Pending shape: outer-table deltas buffer from HERE (before the phase-B snapshot).
        self.pending_shapes.push(PendingSubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: outer_table.clone(),
            stream_path: stream_path.to_string(),
            pred,
            out_cols,
            changes_only,
            collect_log: log,
            buffer: Vec::new(),
            deferred: VecDeque::new(),
        });
        Ok(BeginCreate { seeds, schemas: self.schemas.clone() })
    }

    /// Queue propagation work for a shape that is still being created, if it is one. Returns
    /// whether it was queued — `false` means the shape is installed (or gone) and the caller
    /// proceeds normally.
    ///
    /// Deduped, because the queue is replayed against Postgres one item at a time and a hot inner
    /// table can flip the same value repeatedly during one backfill: a re-derive is absolute, so
    /// running it once at install is running it enough. A full re-derive covers every value, so it
    /// subsumes — and is not added to — anything else in the queue.
    fn queue_deferred(&mut self, shape_id: &str, work: DeferredShapeWork) -> bool {
        let Some(pending) = self.pending_shapes.iter_mut().find(|p| p.shape_id == shape_id) else {
            return false;
        };
        let has_full = pending.deferred.iter().any(|w| matches!(w, DeferredShapeWork::Full { .. }));
        match &work {
            DeferredShapeWork::Full { .. } => {
                if !has_full {
                    pending.deferred.clear();
                    pending.deferred.push_back(work);
                }
            }
            DeferredShapeWork::Value { connecting_col, value, .. } => {
                let dup = pending.deferred.iter().any(|w| {
                    matches!(w, DeferredShapeWork::Value { connecting_col: c, value: v, .. }
                        if c == connecting_col && v == value)
                });
                if !has_full && !dup {
                    pending.deferred.push_back(work);
                }
            }
        }
        true
    }

    /// Queue a re-derivation aimed at a parent node that is still being SEEDED, if it is one.
    /// Returns whether it was queued — `false` means the node is live (or gone) and the caller
    /// reconciles now.
    ///
    /// A flip must never reconcile a mid-seed node: its set is incomplete until phase C finishes
    /// the seed, so the re-derivation can miss rows — and later chunks from a snapshot
    /// OLDER than the flip can overwrite the change. The node and every dependent below
    /// it stay stale for the life of the shape. Deferring is the same answer
    /// [`queue_deferred`](Self::queue_deferred) gives for a not-yet-installed shape.
    ///
    /// Exactly ONE create can own any seeding node: engine admission serializes initialization,
    /// so every seeding node is fresh for one in-flight create and that
    /// create's `finish_create` is the single place this queue is handed back — or, if the create
    /// dies, the node goes and the queue with it.
    ///
    /// Deduped like the shape queue: the replay is absolute, so running a value's re-derive once is
    /// running it enough, and a `Full` re-derive covers every value, so it subsumes the rest.
    fn queue_node_deferred(&mut self, sig: &SubquerySig, work: DeferredNodeWork) -> bool {
        let Some(node) = self.nodes.get_mut(sig) else { return false };
        if node.seed_buffer.is_none() {
            return false;
        }
        let has_full = node.deferred.iter().any(|w| matches!(w, DeferredNodeWork::Full));
        match &work {
            DeferredNodeWork::Full => {
                if !has_full {
                    node.deferred.clear();
                    node.deferred.push_back(work);
                }
            }
            DeferredNodeWork::Value { connecting_col, value } => {
                let dup = node.deferred.iter().any(|w| {
                    matches!(w, DeferredNodeWork::Value { connecting_col: c, value: v }
                        if c == connecting_col && v == value)
                });
                if !has_full && !dup {
                    node.deferred.push_back(work);
                }
            }
        }
        true
    }

    /// Install one snapshot chunk as contributor assertions, without publishing the node. Raw
    /// deltas and child flips still queue while `seed_buffer` is present; initial seed flips are
    /// discarded. Only primary keys and projected values survive this call, never the full rows.
    /// The pending create owns both the host index and circuit state across the step's await, so
    /// its ordinary abort can retract even a chunk whose reply the caller stopped awaiting.
    pub(crate) async fn seed_chunk(&mut self, shape_id: &str, sig: &SubquerySig, rows: Vec<Row>) -> Result<()> {
        let pending = self
            .pending_shapes
            .iter()
            .find(|p| p.shape_id == shape_id)
            .context("seed_chunk: pending shape vanished")?;
        anyhow::ensure!(pending.collect_log.contains(sig), "seed_chunk: node does not belong to create");
        let n = self.nodes.get(sig).context("seed_chunk: node vanished")?;
        anyhow::ensure!(n.seed_buffer.is_some(), "seed_chunk: node is already live");
        let ts = self.schemas.get(&n.inner_table).cloned().context("unknown inner table")?;
        let proj_col = n.proj_col;
        let mut seed = Assertions::default();
        for mut row in rows {
            let pk = ts.key_string(&row).unwrap_or_default();
            let value = row.0.get_mut(proj_col).map(std::mem::take).unwrap_or(Value::Null);
            seed.contributors.extend(self.assert_node_row(sig, &pk, Some(value)));
        }
        let _ = self.apply_asserts(seed).await;
        Ok(())
    }

    /// Phase C (under the registry lock; brief, in-memory + lane enqueues): finish the seeds,
    /// replay every buffered delta through the seed gates, register the shape, and return the work
    /// the caller must propagate (see [`FinishedCreate`]). `seeded` counts the phase-B snapshot
    /// envelopes (for the shape's emitted counter). `seeded_pks` is the backfilled outer rows' pks,
    /// seeding the feed so a later delta that finds one of them no longer matching correctly emits a
    /// delete (not silently dropped as "never known").
    ///
    /// **The invariant that makes the deferred hand-off lossless**: taking the queue off the pending
    /// entry and installing the shape both happen inside ONE synchronous `&mut self` step below, and
    /// the registry mutex is held exclusively for the whole call. So a live flip either queues onto
    /// the pending entry before that step or finds the shape installed after it — there is no state
    /// in between for it to fall through.
    ///
    /// **The invariant that makes it cancellable**: the pending entry stays IN `pending_shapes`
    /// across every await here, so the registry — not this future's stack — owns the create's
    /// rollback state (compile log, buffers, fresh nodes) for the whole of phase C. A create whose
    /// future is dropped at one of these awaits therefore leaves a pending entry
    /// [`abort_create`](Self::abort_create) can unwind exactly, node seeds already asserted into the
    /// membership circuit included. After the atomic tail the create is an ordinary installed shape,
    /// which the same `abort_create` unwinds through the ordinary drop path.
    pub async fn finish_create(
        &mut self,
        shape_id: &str,
        node_seeds: Vec<(SubquerySig, crate::pg::SnapshotGate)>,
        outer_gate: crate::pg::SnapshotGate,
        seeded: u64,
        seeded_pks: std::collections::HashSet<String>,
    ) -> Result<FinishedCreate> {
        anyhow::ensure!(
            self.pending_shapes.iter().any(|p| p.shape_id == shape_id),
            "finish_create: pending shape vanished"
        );
        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        let mut node_work: VecDeque<(SubquerySig, DeferredNodeWork)> = VecDeque::new();
        // 1. The snapshot chunks already landed, but nodes stayed pending. Install each gate
        // before replaying its buffered deltas; no chunk may publish an incompletely seeded node.
        for (sig, gate) in node_seeds {
            let ts = {
                let n = self.nodes.get(&sig).context("finish_create: node vanished")?;
                self.schemas.get(&n.inner_table).cloned().context("unknown inner table")?
            };
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.gate = gate;
            }
            let buffered = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.take()).unwrap_or_default();
            // Replayed one buffered delta at a time, in arrival order, so each keeps its own
            // commit stamp and is recorded as the live decision it is (the outer buffer's replay
            // below does the same). Membership is re-evaluated by identity per pk and is
            // idempotent against the seed for snapshot-visible rows, so replaying every delta is
            // convergent — replaying per delta just also lands the intermediate states.
            for b in buffered {
                let evals = self.node_present_values(&sig, &ts, &b.delta);
                self.record_node_recency(&sig, evals.iter().map(|(pk, _)| pk), b.lsn, b.xid);
                for f in self.apply_node_evals(&sig, evals).await {
                    work.push_back((sig.clone(), f));
                }
            }
            // The node is live from here (its seed and its raw deltas are both in), so a child
            // flip that arrived while it was seeding can finally be re-derived against a set that
            // is not a lie. Taking the queue drains it for good: the node no longer defers.
            if let Some(n) = self.nodes.get_mut(&sig) {
                for w in std::mem::take(&mut n.deferred) {
                    node_work.push_back((sig.clone(), w));
                }
            }
        }
        // 2. Everything that can still fail happens BEFORE the pending entry leaves the registry,
        //    so a failure here returns with the create still exactly unwindable.
        let idx = self
            .pending_shapes
            .iter()
            .position(|p| p.shape_id == shape_id)
            .context("finish_create: pending shape vanished")?;
        let ts = self
            .schemas
            .get(&self.pending_shapes[idx].outer_table)
            .cloned()
            .context("finish_create: unknown outer table")?;
        // The atomic tail: ONE synchronous `&mut self` step, no `.await` inside it — take the
        // pending entry (and with it the deferred queue), register the shape, seed its feed. This
        // is the step the deferred hand-off's losslessness rests on (see the doc comment).
        let mut pending = self.pending_shapes.remove(idx);
        let deferred = std::mem::take(&mut pending.deferred);
        let feed_id = self.next_feed_id;
        self.next_feed_id += 1;
        self.install_shape(SubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: pending.outer_table.clone(),
            stream_path: pending.stream_path.clone(),
            pred: pending.pred.clone(),
            out_cols: pending.out_cols.clone(),
            gate: outer_gate,
            emitted: std::sync::atomic::AtomicU64::new(seeded),
            feed_id,
            recent: HashMap::new(),
            inflight_querybacks: 0,
        });
        // Seed the feed with the backfilled pks (the stream already carries the snapshot) —
        // replaces the old known_members hand-off. This is phase C, under the registry lock,
        // BEFORE the shape is discoverable by any live delta (the spike's §6 riskiest transition):
        // the host-side FeedSet is seeded synchronously here — a `&mut self` op with no `.await` —
        // so no live delta can slip between "shape registered" and "feed seeded".
        for pk in seeded_pks {
            let pk_id = self.pk_dict.get_or_insert(&pk);
            self.feed_sets.insert(feed_id, pk_id);
        }
        // 3. Replay the shape's buffered outer deltas through the gate (absolute emission;
        //    idempotent against the backfill for snapshot-visible rows). Replayed one buffered
        //    delta at a time, in arrival order, so each keeps its own commit stamp: the deferred
        //    work returned below can start a query-back the instant this call releases the lock,
        //    and a replayed decision it must not undo is recognisable only by its stamp. Per-pk
        //    emission is absolute, so replaying per delta rather than folding them into one
        //    verdict lands the same final state — it just also lands the intermediate ones.
        for buffered in std::mem::take(&mut pending.buffer) {
            let candidates = crate::engine::membership::latest_rows_by_pk(&ts, &buffered.delta);
            self.emit_for_shapes(
                &ts,
                vec![(shape_id.to_string(), candidates)],
                None,
                EmissionSource::Replay { lsn: buffered.lsn, xid: buffered.xid },
            )
            .await?;
        }
        Ok(FinishedCreate { work, deferred, node_work })
    }

    /// Install a fully-built outer shape. The shapes map, the feed-id reverse map and the
    /// necessary-conjunct index MUST move together — an index entry outliving its shape is a
    /// wasted probe, but a shape with no index entry is invisible to every later delta (silently
    /// dropped envelopes). One call site so they cannot drift, and one synchronous `&mut self`
    /// step so no live delta can observe a half-installed shape. Registration is only reachable
    /// from `finish_create`, the atomic tail of shape creation: a create that fails or is cancelled
    /// earlier never got here, so [`abort_create`](Self::abort_create) unwinds its still-registered
    /// pending entry — one abandoned after it undoes this install through
    /// [`drop_subquery_shape`](Self::drop_subquery_shape).
    fn install_shape(&mut self, shape: SubqueryShape) {
        self.feed_by_id.insert(shape.feed_id, shape.shape_id.clone());
        self.shape_index.insert(&shape.shape_id, shape.outer_table.as_str(), &shape.pred);
        self.shapes.insert(shape.shape_id.clone(), shape);
    }

    /// The subquery shapes on `table` that a change described by `delta` can possibly move —
    /// exactly the set [`on_table_delta`](Self::on_table_delta) step 2 visits, exposed for tests
    /// and introspection so "shapes visited per change" is assertable.
    ///
    /// `delta` must be the RAW Z-set delta (old `-1` images included), not the per-pk latest fold:
    /// the candidate set is the union over old ∪ new images, which is what keeps a move-out's
    /// absolute delete alive. See [`crate::subq_index`].
    pub(crate) fn outer_candidates(&self, table: &TableRef, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
        self.shape_index.candidates(table.as_str(), delta)
    }

    /// Abort a create in whichever phase it died in. Before the atomic tail of `finish_create` that
    /// means unwinding its pending entry (edges, refcounts, fresh nodes with their buffers and their
    /// deferred queues, and any seed already asserted into the membership circuit); after it the
    /// shape is installed like any other, so the ordinary drop path is the correct undo. A create
    /// cancelled between `finish_create` returning and the engine publishing the shape lands in that
    /// second case.
    ///
    /// The pending branch covers cancellation during chunk installation and phase C:
    /// a dropped future can leave a fresh node holding some or all of its seed.
    /// Removing it without retracting those contributor tuples would leave the
    /// membership circuit carrying a dead node's set forever, so removal goes through the same
    /// retracting path a refcount-0 drop uses — unconditionally, because a node with no state
    /// retracts nothing and the check would cost the same scan as the retraction.
    pub async fn abort_create(&mut self, shape_id: &str) {
        if self.shapes.contains_key(shape_id) {
            self.drop_subquery_shape(shape_id).await;
            return;
        }
        let Some(idx) = self.pending_shapes.iter().position(|p| p.shape_id == shape_id) else {
            return;
        };
        // A cancelled seed step may have submitted its batch without receiving the reply. The
        // contributor snapshot cannot license removal until that FIFO batch has completed.
        self.circuit.barrier().await;
        // Taking the whole entry discards its deferred queue with it: work aimed at a shape that
        // will never exist has nowhere to land and nothing to correct. The same goes for the
        // deferred queue of each fresh node removed below — it dies with the node it was waiting on.
        let pending = self.pending_shapes.remove(idx);
        // Shape edges live under the pred's leaf sigs — remove only there, not a global scan.
        self.remove_shape_edges(&pending.pred, &pending.shape_id);
        let mut asserts = Assertions::default();
        for sig in pending.collect_log {
            let Some(n) = self.nodes.get_mut(&sig) else { continue };
            n.refcount = n.refcount.saturating_sub(1);
            if n.refcount > 0 {
                continue;
            }
            // A cancelled send may also have updated the host index without submitting its
            // assertions. Remove the sig independently of the circuit's published contributors.
            let tkey = n.template_key.clone();
            if let Some(tpl) = self.templates.get_mut(&tkey) {
                tpl.pk_nodes.retain(|_, nodes| {
                    nodes.remove(&sig);
                    !nodes.is_empty()
                });
            }
            // No cascade here (unlike `decref_nodes`): the compile log has one entry per `collect()`
            // call, deeper nodes included, so this walk already visits every node the create
            // referenced — cascading would decrement a child a second time.
            self.remove_node_with_state(&sig, &mut asserts);
            self.pending_seed.retain(|s| s != &sig);
        }
        // The nodes are gone and their ids are never reused, so these retractions have no dependent
        // left to move and their flips are discarded.
        if !asserts.is_empty() {
            let _ = self.circuit.apply(asserts).await;
        }
    }

    /// Remove a shape's dependency edges: they live under the keys of the predicate's IN
    /// leaves, so removal touches only those nodes' lists.
    fn remove_shape_edges(&mut self, pred: &CompiledPredicate, shape_id: &str) {
        for leaf in collect_in_leaves(pred) {
            if let Some(v) = self.edges.get_mut(&leaf.sig) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Shape(id) if id == shape_id));
                if v.is_empty() {
                    self.edges.remove(&leaf.sig);
                }
            }
        }
    }

    /// Drop a removed node's id/template bookkeeping. The circuit retraction is the caller's job —
    /// see [`remove_node_with_state`](Self::remove_node_with_state), which does both for a node
    /// that may hold state.
    fn remove_node_entry(&mut self, node: &SubqueryNode) {
        self.node_by_id.remove(&node.node_id);
        if let Some(tpl) = self.templates.get_mut(&node.template_key) {
            tpl.binds.remove(&node.bind);
            if tpl.binds.is_empty() {
                self.templates.remove(&node.template_key);
            }
        }
    }

    /// Rollback helper for a failed/conflicted `begin_create` compile: drop the staged edges
    /// and undo the node refs made by the aborted compile.
    fn rollback_refs(&mut self, log: Vec<SubquerySig>) {
        self.staged_edges.clear();
        for sig in log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    if let Some(node) = self.nodes.remove(&sig) {
                        self.remove_node_entry(&node);
                    }
                    self.pending_seed.retain(|s| s != &sig);
                }
            }
        }
    }

    /// Remove a subquery shape: drop its edges and decref the nodes it referenced (removing nodes whose
    /// refcount hits zero, and their edges, recursively).
    pub async fn drop_subquery_shape(&mut self, shape_id: &str) {
        let Some(shape) = self.shapes.remove(shape_id) else { return };
        // Sigs this shape pointed at, then drop the shape's edges.
        let sigs: Vec<SubquerySig> = collect_in_leaves(&shape.pred).into_iter().map(|l| l.sig).collect();
        self.remove_shape_edges(&shape.pred, shape_id);
        // Un-file it from the conjunct index in the same step it leaves `shapes` (the inverse of
        // `install_shape`) — a posting for a shape that no longer exists would route deltas at a
        // dead stream path.
        self.shape_index.remove(shape_id);
        // Drop the shape's whole feed (O(1) — no per-pk enumeration, no circuit round-trip) and
        // its id mapping. The stream is being torn down; the `feed_id` is never reused.
        self.feed_by_id.remove(&shape.feed_id);
        self.feed_sets.drop_feed(shape.feed_id);
        self.decref_nodes(sigs).await;
    }

    /// Decrement each sig's refcount, removing (and cascading into the children of) nodes
    /// that reach zero. Removed nodes retract their contributor tuples from the circuit in
    /// one batch at the end; the resulting Leave flips are discarded — a refcount-0 node has
    /// no dependents left to move.
    async fn decref_nodes(&mut self, sigs: Vec<SubquerySig>) {
        let mut stack = sigs;
        let mut asserts = Assertions::default();
        while let Some(sig) = stack.pop() {
            let Some(node) = self.nodes.get_mut(&sig) else { continue };
            node.refcount = node.refcount.saturating_sub(1);
            if node.refcount > 0 {
                continue;
            }
            // Refcount hit zero: retract state, remove node + edges, recurse into its children.
            stack.extend(self.remove_node_with_state(&sig, &mut asserts));
        }
        // Refcount-0 removal ⇒ no dependents remain; the flips are discarded.
        if !asserts.is_empty() {
            let _ = self.circuit.apply(asserts).await;
        }
    }

    /// Remove a node that has reached the end of its life: retract its contributor tuples into
    /// `asserts` (the caller applies one batch), drop its id/template/edge bookkeeping, and return
    /// its child sigs. The one removal path for a node that may hold circuit state — shared by the
    /// refcount-0 drop ([`decref_nodes`](Self::decref_nodes), which recurses into the returned
    /// children) and by [`abort_create`](Self::abort_create), whose fresh node may have taken its
    /// seed before the create was cancelled.
    ///
    /// The contributor slice comes from the circuit's own integral (prefix scan) — there is no host
    /// pk list to drain. Keys are pk ids; the retraction re-asserts the same id, so no dictionary
    /// round-trip is needed here.
    fn remove_node_with_state(&mut self, sig: &SubquerySig, asserts: &mut Assertions) -> Vec<SubquerySig> {
        let Some(node) = self.nodes.remove(sig) else { return Vec::new() };
        let child_sigs: Vec<SubquerySig> = collect_in_leaves(&node.pred).into_iter().map(|l| l.sig).collect();
        for (pk_id, _v) in self.circuit.contributor_entries(node.node_id) {
            if let Some(tpl) = self.templates.get_mut(&node.template_key)
                && let Some(set) = tpl.pk_nodes.get_mut(&pk_id)
            {
                set.remove(sig);
                if set.is_empty() {
                    tpl.pk_nodes.remove(&pk_id);
                }
            }
            asserts.contributors.push(Tup2(PkKey { id: node.node_id, pk: pk_id }, Assert::Delete));
        }
        self.remove_node_entry(&node);
        self.remove_node_edges(sig, &child_sigs);
        child_sigs
    }

    // --- live maintenance ---------------------------------------------------------------------

    /// Process one table delta: update affected nodes (in-memory) and emit outer-shape deltas
    /// synchronously, then **return** the inner-set flips for deferred propagation (the caller
    /// hands them to the engine's flip-propagator task — see [`propagate_flips`]). Deferring the
    /// flip-driven Postgres query-backs is safe because outer membership is emitted absolutely
    /// (upsert-if-matches-now / idempotent delete), so cross-table convergence is order-independent;
    /// the convergence barrier is the processed offset **plus** a drained flip queue. `lsn` is the
    /// change's commit LSN (0 = unknown/never skip).
    pub async fn on_table_delta(
        &mut self,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
        lsn: u64,
        xid: Option<u64>,
        txid: Option<String>,
        mut trace: Option<&mut Vec<crate::trace::TraceHop>>,
    ) -> Result<VecDeque<(SubquerySig, Flip)>> {
        let table = ts.table.clone();
        // Work queue of (node sig, flip) pairs to propagate (BFS up the dependency DAG).
        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        // Trace helper: record a hop once per node id (a shape reached via several flips is one hop).
        let hop = |trace: &mut Option<&mut Vec<crate::trace::TraceHop>>, node: String, outcome: &'static str| {
            if let Some(t) = trace.as_mut() {
                if let Some(prev) = t.iter_mut().find(|h| h.node == node) {
                    if outcome == "passed" {
                        prev.outcome = "passed"; // an earlier dropped hop upgraded by a later emit
                    }
                } else {
                    t.push(crate::trace::TraceHop::new(node, outcome));
                }
            }
        };

        // 1. Templates whose inner table is this table: one residual eval + one bind lookup
        // per touched pk (instead of one full-predicate eval per literal-keyed node), then one
        // circuit step for the whole delta — the circuit's distinct reports the flips.
        let tkeys: Vec<String> =
            self.templates.iter().filter(|(_, t)| t.inner_table == table).map(|(k, _)| k.clone()).collect();
        let mut asserts = Assertions::default();
        let mut live_sigs: Vec<SubquerySig> = Vec::new();
        for tkey in &tkeys {
            let sigs: Vec<SubquerySig> =
                self.templates.get(tkey).map(|t| t.binds.values().cloned().collect()).unwrap_or_default();
            for sig in sigs {
                // Mid-seed: buffer the raw delta for gated replay at install (a half-seeded
                // set must not be reconciled — the snapshot could stale-overwrite a fresher
                // delta).
                if let Some(buf) = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.as_mut()) {
                    // Keep this commit's stamp with the delta: the install replay is a live
                    // decision and has to be able to out-rank a node query-back's older read
                    // (see [`BufferedDelta`]).
                    buf.push(BufferedDelta { lsn, xid, delta: delta.to_vec() });
                    hop(&mut trace, format!("node:{sig}"), "buffered");
                } else if self.nodes.get(&sig).is_some_and(|n| n.gate.should_skip(lsn, xid)) {
                    hop(&mut trace, format!("node:{sig}"), "dropped");
                } else {
                    live_sigs.push(sig);
                }
            }
            let evals = self.template_present(tkey, ts, delta);
            asserts.contributors.extend(self.template_assertions(tkey, evals, lsn, xid));
        }
        let flips = self.apply_asserts(asserts).await;
        for sig in live_sigs {
            let flipped = flips.iter().any(|(s, _)| s == &sig);
            hop(&mut trace, format!("node:{sig}"), if flipped { "passed" } else { "dropped" });
        }
        for f in flips {
            work.push_back(f);
        }

        // 2. Subquery shapes whose outer table is this table: one batch of candidates across
        // every shape — assertions feed the circuit in ONE step, its feed deltas are the
        // deletes, matching candidates are the upserts.
        // Candidates come from the necessary-conjunct index (`crate::subq_index`), bucketed by
        // outer table: a shape whose conjunct no image in this delta satisfies provably cannot
        // change membership for any touched pk, so visiting it would be a full predicate eval
        // that can only conclude "nothing". The probe unions the delta's OLD (`-1`) and NEW
        // (`+1`) images, which is what keeps a move-out's absolute delete alive — see the module
        // docs of `subq_index` for the argument. With a trace subscriber attached the index is
        // bypassed (like the standalone/aggregate tiers) so every shape node still reports a hop.
        let shape_ids: Vec<String> = if trace.is_some() {
            self.shapes.iter().filter(|(_, s)| s.outer_table == table).map(|(id, _)| id.clone()).collect()
        } else {
            self.outer_candidates(&table, delta)
        };
        let mut groups: Vec<(String, Vec<(Row, bool)>)> = Vec::new();
        for id in shape_ids {
            if self.shapes.get(&id).is_some_and(|s| s.gate.should_skip(lsn, xid)) {
                continue;
            }
            groups.push((id, crate::engine::membership::latest_rows_by_pk(ts, delta)));
        }
        // This is the freshest verdict any path can have for these pks: it is derived from the
        // commit itself. `EmissionSource::Live` carries that commit's stamp so a query-back
        // already reading Postgres cannot later overwrite the decision with an older row.
        for (id, emitted, _net) in
            self.emit_for_shapes(ts, groups, txid.clone(), EmissionSource::Live { lsn, xid }).await?
        {
            hop(&mut trace, format!("shape:{id}"), if emitted { "passed" } else { "dropped" });
        }

        // 2b. Pending shapes (mid-create) on this table: buffer for gated replay at install,
        // keeping this commit's stamp with it (the replay is a live decision — see
        // [`BufferedDelta`]).
        for p in self.pending_shapes.iter_mut().filter(|p| p.outer_table == table) {
            p.buffer.push(BufferedDelta { lsn, xid, delta: delta.to_vec() });
        }

        // 3. Flip propagation (the Postgres query-backs) is deferred: the caller enqueues `work`
        // onto the engine's flip-propagator task, which runs [`propagate_flips`] without holding
        // this registry lock across round-trips.
        Ok(work)
    }

    /// For each inner-row pk touched by `delta`, its desired contribution (`Some(proj)` if the row now
    /// matches the node predicate, else `None`). Immutable (reads node sets for `matches_ctx`).
    fn node_present_values(
        &self,
        sig: &SubquerySig,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<Value>)> {
        let (pred, proj) = match self.nodes.get(sig) {
            Some(n) => (n.pred.clone(), n.proj_col),
            None => return Vec::new(),
        };
        // The +1 row (if any) is the row's new state; a pk seen only with -1 was deleted.
        let mut newrow: HashMap<String, Row> = HashMap::new();
        let mut seen: Vec<String> = Vec::new();
        for Tup2(row, w) in delta {
            let pk = ts.key_string(row).unwrap_or_default();
            if !seen.contains(&pk) {
                seen.push(pk.clone());
            }
            if *w > 0 {
                newrow.insert(pk, row.clone());
            }
        }
        seen.into_iter()
            .map(|pk| match newrow.get(&pk) {
                Some(r) => {
                    let pv = if pred.matches_ctx(r, self) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    (pk, pv)
                }
                None => (pk, None),
            })
            .collect()
    }

    /// The per-pk contributions a node RE-DERIVATION should assert: its Postgres rows evaluated
    /// against the node's predicate, MINUS every pk whose live decision the read could not have
    /// seen. `None` = the node vanished while the read ran.
    ///
    /// This is the node tier of the query-back recency fence, the exact analogue of the candidate
    /// filter [`emit_for_shapes`](Self::emit_for_shapes) applies to a shape's query-back, with the
    /// same predicate: `gate.should_skip(lsn, xid)` is "this commit was already visible to that
    /// read's snapshot", so the rows in hand already reflect it and the candidate is current. Its
    /// negation is "the live decision is NEWER than this read": the row is stale, the live verdict
    /// already stands, and re-deriving that pk would re-assert the contribution the verdict just
    /// removed (or withhold one it just added). The node appends nothing itself, but its dependent
    /// shape appends what the node's flips say — so a stale re-assertion is the same permanent
    /// divergence, one level down. (An unstamped live decision — no LSN and no xid — reads as
    /// "newer" and is dropped: only non-Postgres sources leave a change unstamped, and a
    /// re-derivation cannot run without Postgres.)
    fn node_queryback_evals(
        &self,
        sig: &SubquerySig,
        ts: &TableSchema,
        rows: &[Row],
        gate: &crate::pg::SnapshotGate,
    ) -> Option<Vec<(String, Option<Value>)>> {
        let (pred, proj, recent) = {
            let n = self.nodes.get(sig)?;
            (n.pred.clone(), n.proj_col, &n.recent)
        };
        Some(
            rows.iter()
                .filter_map(|r| {
                    let pk = ts.key_string(r).unwrap_or_default();
                    // `recent` empty (nothing decided live during this read) is the common case
                    // and costs nothing: no candidate is re-examined at all.
                    if let Some(&(lsn, xid)) = recent.get(&pk)
                        && !gate.should_skip(lsn, xid)
                    {
                        return None;
                    }
                    let pv = if pred.matches_ctx(r, self) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    Some((pk, pv))
                })
                .collect(),
        )
    }

    /// For each touched pk, the row's target contribution under one template: `Some((node
    /// sig, projected value))` when the latest row matches the residual AND its projected
    /// params hit a registered bind, else `None`. One residual eval + one hash lookup per pk —
    /// the template-sharing eval win over per-node full-predicate evaluation.
    fn template_present(
        &self,
        tkey: &str,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<(SubquerySig, Value)>)> {
        let Some(tpl) = self.templates.get(tkey) else { return Vec::new() };
        crate::engine::membership::latest_rows_by_pk(ts, delta)
            .into_iter()
            .map(|(row, is_new)| {
                let pk = ts.key_string(&row).unwrap_or_default();
                let target = if is_new && tpl.residual.matches_ctx(&row, self) {
                    let params =
                        Row(tpl.param_cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect());
                    tpl.binds
                        .get(&params)
                        .map(|sig| (sig.clone(), row.0.get(tpl.proj_col).cloned().unwrap_or(Value::Null)))
                } else {
                    None
                };
                (pk, target)
            })
            .collect()
    }

    /// Turn one template's per-pk targets into contributor assertions: absent for nodes that
    /// held the pk but are no longer its target, present for the new target. Per node, the
    /// delta is skipped when the node is mid-seed (its raw buffer replays at install) or when
    /// the node's seed gate says the snapshot already contains this change — in both cases
    /// the node's seed is (or will be) the authority, and absolute assertion absorbs any
    /// overlap idempotently.
    ///
    /// This is the per-(node, pk) LIVE decision point, so it is also where node recency is
    /// recorded (see [`SubqueryNode::recent`]): for every bind of this template that has a
    /// re-derivation in flight, this delta decides the pk's contribution ABSOLUTELY — target
    /// bind or not — so the stamp is recorded for each of them, including the pks whose verdict
    /// is "no contribution". Recording only the pks that produced an assertion would miss
    /// exactly the case the fence exists for: a pk the node does not hold yet, whose live
    /// verdict is "still not a contributor", which a stale re-derivation would then admit.
    fn template_assertions(
        &mut self,
        tkey: &str,
        evals: Vec<(String, Option<(SubquerySig, Value)>)>,
        lsn: u64,
        xid: Option<u64>,
    ) -> Vec<Tup2<PkKey, Assert>> {
        let node_applies = |reg: &Self, sig: &SubquerySig| {
            reg.nodes.get(sig).is_some_and(|n| n.seed_buffer.is_none() && !n.gate.should_skip(lsn, xid))
        };
        // The binds whose decisions have an older read to beat. Empty in the steady state (no
        // node re-derivation outstanding), so the recording below costs nothing then.
        let fenced: Vec<SubquerySig> = self
            .templates
            .get(tkey)
            .map(|t| {
                t.binds
                    .values()
                    .filter(|sig| {
                        self.nodes.get(*sig).is_some_and(|n| n.inflight_querybacks > 0) && node_applies(self, sig)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut asserts = Vec::new();
        for (pk, target) in evals {
            // This commit decided this pk's contribution on every fenced bind (it is the target
            // of at most one of them, and a non-contributor to the rest), so all of them record
            // it — a re-derivation reading Postgres older than this commit must drop the pk
            // whichever bind it is re-deriving.
            for sig in &fenced {
                if let Some(n) = self.nodes.get_mut(sig) {
                    n.recent.insert(pk.clone(), (lsn, xid));
                }
            }
            // A pk with no interned id was never asserted, so it can hold no contribution — probe
            // without minting (a never-member delete must not grow the dictionary).
            let holders: Vec<SubquerySig> = self
                .pk_dict
                .get(&pk)
                .and_then(|pk_id| self.templates.get(tkey).and_then(|t| t.pk_nodes.get(&pk_id)))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            for sig in holders {
                if target.as_ref().is_some_and(|(tsig, _)| tsig == &sig) {
                    continue; // still the target; the insert below carries the fresh value
                }
                if node_applies(self, &sig) {
                    asserts.extend(self.assert_node_row(&sig, &pk, None));
                }
            }
            if let Some((sig, v)) = target
                && node_applies(self, &sig)
            {
                asserts.extend(self.assert_node_row(&sig, &pk, Some(v)));
            }
        }
        asserts
    }

    /// Mark a query-back for `shape_id` as in flight — called under the lock the query-back is
    /// about to RELEASE for its Postgres read, so every live decision taken while that read (and
    /// the evaluation that follows it) is outstanding lands in [`SubqueryShape::recent`].
    /// Increment first, release second: the reverse order would leave a hole exactly the size of
    /// the race being closed.
    fn begin_queryback(&mut self, shape_id: &str) {
        if let Some(s) = self.shapes.get_mut(shape_id) {
            s.inflight_querybacks = s.inflight_querybacks.saturating_add(1);
        }
    }

    /// Balance [`begin_queryback`](Self::begin_queryback) — under the lock, on EVERY exit path of
    /// the query-back (including the read's error path and the "nothing came back" path).
    /// A shape dropped in the meantime has nothing to decrement.
    ///
    /// The last in-flight query-back to finish clears `recent`: with no outstanding read there is
    /// nothing left that could overwrite a live decision, so keeping per-pk stamps would only be
    /// unbounded growth. This is what makes the map cost proportional to a query-back window
    /// rather than to the shape.
    fn end_queryback(&mut self, shape_id: &str) {
        if let Some(s) = self.shapes.get_mut(shape_id) {
            s.inflight_querybacks = s.inflight_querybacks.saturating_sub(1);
            if s.inflight_querybacks == 0 {
                s.recent = HashMap::new();
            }
        }
    }

    /// [`begin_queryback`](Self::begin_queryback), one tier down: mark a re-derivation of `sig`'s
    /// inner rows as in flight, under the lock it is about to RELEASE for its Postgres read, so
    /// every live contribution decision taken while that read is outstanding lands in
    /// [`SubqueryNode::recent`]. Increment first, release second — the reverse order would leave a
    /// hole exactly the size of the race being closed.
    fn begin_node_queryback(&mut self, sig: &SubquerySig) {
        if let Some(n) = self.nodes.get_mut(sig) {
            n.inflight_querybacks = n.inflight_querybacks.saturating_add(1);
        }
    }

    /// Balance [`begin_node_queryback`](Self::begin_node_queryback) — under the lock, on EVERY exit
    /// path of the re-derivation (the read's error path and the "node vanished" path included), or
    /// the node would keep recording recency (and dropping candidates) forever. A node dropped in
    /// the meantime has nothing to decrement. The last one out clears `recent`: with no outstanding
    /// read there is nothing left that could re-assert a stale contribution.
    fn end_node_queryback(&mut self, sig: &SubquerySig) {
        if let Some(n) = self.nodes.get_mut(sig) {
            n.inflight_querybacks = n.inflight_querybacks.saturating_sub(1);
            if n.inflight_querybacks == 0 {
                n.recent = HashMap::new();
            }
        }
    }

    /// Record a batch of LIVE per-pk contribution decisions for one node — a no-op unless a
    /// re-derivation of that node is in flight, which is the only window the map has to cover.
    /// (The template path records inline; this is for the seed-buffer replay, which decides one
    /// node's pks directly rather than through a template's binds. See
    /// [`SubqueryNode::seed_buffer`] for why that replay is stamped at all.)
    fn record_node_recency<'a>(
        &mut self,
        sig: &SubquerySig,
        pks: impl Iterator<Item = &'a String>,
        lsn: u64,
        xid: Option<u64>,
    ) {
        let Some(n) = self.nodes.get_mut(sig) else { return };
        if n.inflight_querybacks == 0 {
            return;
        }
        for pk in pks {
            n.recent.insert(pk.clone(), (lsn, xid));
        }
    }

    /// Deferred-propagation helper: snapshot what a query-back needs (brief lock scope at the
    /// call site — see the free functions below).
    fn snapshot_for_table(&self, table: &TableRef) -> Result<TableSchema> {
        self.schemas.get(table).cloned().with_context(|| format!("unknown table '{table}'"))
    }

    /// The ONE emission tail (spec §5): evaluate each shape's candidates against the current
    /// membership snapshot, transition the host-side per-feed key set ([`crate::subq_feed::FeedSet`])
    /// in the same synchronous step, and deliver — **upserts for every matching candidate** (an
    /// update to a continuing member must flow; upserts are always safe for readers), **deletes
    /// only where the feed actually retracts** (`FeedSet::remove` returns true — a "not a member"
    /// verdict for a pk the stream never contained gates to nothing, so the spurious delete that
    /// used to wake idle long-polls is structurally impossible). Emission is absolute per pk, so
    /// deferred flip propagation converges regardless of timing.
    ///
    /// Callers hold the registry lock for the whole call (eval + FeedSet mutation + lane enqueue),
    /// which is what keeps per-stream append order = evaluation order AND makes each emission
    /// decision atomic with its bitmap transition (borrow-checker-enforced — no `.await` between).
    /// `candidates` are each shape's touched rows: `(latest row, still-exists)`.
    /// Returns per shape whether anything was delivered (trace hops).
    ///
    /// **Per-stream order is not enough on its own.** Holding the lock makes append order equal
    /// evaluation order, but a query-back's candidates were READ before it took the lock, so
    /// "evaluated last" and "freshest" are different things for exactly the pks a live commit
    /// touched in between. `source` closes that gap ([`EmissionSource`]): a live/replayed
    /// decision records its commit stamp for every pk it evaluates while a query-back is in
    /// flight, and a query-back drops every candidate whose recorded stamp its own snapshot could
    /// not see. Both halves are needed — recording only the pks that produced an envelope would
    /// let a stale query-back upsert re-add a row whose live verdict was "not a member, and it
    /// was never in the feed", which emits nothing and is precisely the divergence seen in
    /// practice.
    async fn emit_for_shapes(
        &mut self,
        ts: &TableSchema,
        groups: Vec<(String, Vec<(Row, bool)>)>,
        txid: Option<String>,
        source: EmissionSource<'_>,
    ) -> Result<Vec<(String, bool, i64)>> {
        // Phase 1: evaluate each candidate against the current membership snapshot and, in the
        // SAME synchronous step (no `.await`), transition the host-side FeedSet — building the
        // member upserts and the gated deletes. A delete is emitted for a shape's pk **iff**
        // `feed_sets.remove` returns true (the pk was actually in the feed): the check-and-set IS
        // the emission decision, in one indivisible expression under the registry lock, so the
        // spurious delete that used to wake idle long-polls is structurally impossible (the
        // wake-storm gate, PR #30). Upserts are delivered for every current member unconditionally.
        let mut staged: Vec<(String, Vec<Row>)> = Vec::new();
        let mut deletes: HashMap<String, Vec<String>> = HashMap::new();
        // The commit stamp a live/replayed decision records for the pks it evaluates; `None` for a
        // query-back, which reads this map instead of writing it.
        let live_stamp: Option<(u64, Option<u64>)> = match &source {
            EmissionSource::Live { lsn, xid } | EmissionSource::Replay { lsn, xid } => Some((*lsn, *xid)),
            EmissionSource::QueryBack { .. } => None,
        };
        for (shape_id, candidates) in groups {
            let Some(shape) = self.shapes.get(&shape_id) else { continue };
            let (pred, feed_id) = (shape.pred.clone(), shape.feed_id);
            // Recency is only worth recording while some query-back for THIS shape is between its
            // Postgres read and its evaluation; outside that window there is no older read that
            // could overwrite this decision, so the map stays empty (and no pk id is minted for a
            // non-member that would otherwise never need one).
            let record_recency = shape.inflight_querybacks > 0 && live_stamp.is_some();
            // A query-back's candidates are pre-filtered against the live decisions taken since
            // its snapshot. `should_skip(lsn, xid)` is "this commit was already visible to that
            // snapshot" — i.e. the read already reflects it, so the candidate row is current and
            // the query-back may evaluate it. Its negation is "the live decision is NEWER than
            // this read": the row in hand is stale, the live verdict already stands, and this
            // evaluation must not touch the pk at all. (An unstamped live decision — no LSN and
            // no xid — reads as "newer" and is dropped: only non-Postgres sources leave a change
            // unstamped, and a query-back cannot run without Postgres.)
            let candidates: Vec<(Row, bool)> = match &source {
                // `recent` empty (nothing decided live during this read) is the common case and
                // costs nothing: no pk is recomputed for the whole candidate set.
                EmissionSource::QueryBack { gate } if !shape.recent.is_empty() => {
                    let recent = &shape.recent;
                    candidates
                        .into_iter()
                        .filter(|(row, _)| {
                            let Ok(pk) = ts.key_string(row) else { return true };
                            match self.pk_dict.get(&pk).and_then(|id| recent.get(&id)) {
                                Some(&(lsn, xid)) => gate.should_skip(lsn, xid),
                                None => true,
                            }
                        })
                        .collect()
                }
                EmissionSource::QueryBack { .. } | EmissionSource::Live { .. } | EmissionSource::Replay { .. } => {
                    candidates
                }
            };
            let mut members: Vec<Row> = Vec::new();
            let mut evaluated: Vec<u32> = Vec::new();
            for (row, exists) in candidates {
                let pk = match ts.key_string(&row) {
                    Ok(pk) => pk,
                    Err(_) => continue,
                };
                if exists && pred.matches_ctx(&row, self) {
                    // Current member: deliver the upsert and record presence in the feed.
                    let pk_id = self.pk_dict.get_or_insert(&pk);
                    self.feed_sets.insert(feed_id, pk_id);
                    if record_recency {
                        evaluated.push(pk_id);
                    }
                    members.push(row);
                } else {
                    // Non-member. The delete is emitted only if the pk was actually in the feed;
                    // the dictionary is probed WITHOUT minting — a never-interned pk can never
                    // have been a member, so it gates with no id allocated (the same
                    // probe-without-mint rationale as `template_assertions`).
                    if let Some(pk_id) = self.pk_dict.get(&pk) {
                        if self.feed_sets.remove(feed_id, pk_id) {
                            deletes.entry(shape_id.clone()).or_default().push(pk);
                        }
                        if record_recency {
                            evaluated.push(pk_id);
                        }
                    } else if record_recency {
                        // A never-interned non-member emits nothing — and still has to win over a
                        // query-back holding an older row for this pk, which WOULD emit an upsert
                        // (the empty-shape row-moved-away case). Recording it needs an id, so this
                        // is the one place a non-member mints one; it is bounded by the pks a live
                        // commit touches while a query-back is in flight.
                        evaluated.push(self.pk_dict.get_or_insert(&pk));
                    }
                }
            }
            if let (Some(stamp), Some(shape)) = (live_stamp, self.shapes.get_mut(&shape_id)) {
                for pk_id in evaluated {
                    shape.recent.insert(pk_id, stamp);
                }
            }
            staged.push((shape_id, members));
        }
        // Phase 3: build + deliver per shape (still under the caller's lock — enqueue order
        // on each stream's FIFO lane is evaluation order).
        let mut results = Vec::with_capacity(staged.len());
        for (shape_id, members) in staged {
            let Some(shape) = self.shapes.get(&shape_id) else { continue };
            let dels = deletes.remove(&shape_id).unwrap_or_default();
            let net = members.len() as i64 - dels.len() as i64;
            let mut envs = crate::engine::translate_output(
                ts,
                members.into_iter().map(|r| (r, 1)).collect(),
                txid.clone(),
                None,
                shape.out_cols.as_deref().map(Vec::as_slice),
            );
            envs.extend(crate::engine::delete_envelopes(ts, dels, txid.clone()));
            if envs.is_empty() {
                results.push((shape_id, false, 0));
                continue;
            }
            shape.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let path = shape.stream_path.clone();
            self.deliver(&path, envs).await;
            results.push((shape_id, true, net));
        }
        Ok(results)
    }
}

// --- deferred flip propagation ------------------------------------------------------------------
//
// Runs on the engine's flip-worker pool (semaphore-bounded, `CIRCUITS_FLIP_WORKERS`), NOT
// inside the table tailers, so the flip-driven Postgres query-backs neither sit on the tailer
// hot path nor serialize on a single task. Two invariants make this sound:
//
//  * **Deferral**: every emission is absolute (per pk: upsert if the row matches *now*, else
//    idempotent delete), so a propagation that runs later re-derives from the then-current
//    Postgres and node state and converges regardless of when — or on which worker — it runs.
//    The convergence barrier gains one term: the engine's pending counter must drain to zero
//    (`GET /replication/lsn` → `pendingFlips`), and that counter also covers emission-lane
//    batches until they LAND on their streams.
//  * **Eval+enqueue atomicity + per-stream FIFO**: membership evaluation and the enqueue of the
//    resulting envelopes happen under one registry-lock scope, and each shape stream drains
//    through exactly one ordered emission lane (`engine::emission`). Per-stream append order
//    therefore equals evaluation order — a move evaluated at time t1 can never land *after* an
//    emission evaluated at t2 > t1 for the same pk (which would leave the stream's last word
//    stale — permanent divergence). This is the same guarantee the old
//    hold-the-lock-across-append design gave, without network under the lock and without a
//    single-task bottleneck. Postgres round-trips run outside the lock, concurrently.
//  * **Per-pk recency across the read**: evaluation order is not freshness order for a query-back,
//    whose candidate rows were READ before it took the lock. A live outer-row change committed
//    after that read's snapshot can be evaluated in between, and the query-back would then
//    evaluate its older row last. So while a query-back is in flight the shape records the commit
//    stamp of every live decision it applies, and the query-back drops the candidates whose stamp
//    its own snapshot could not see (`SubqueryShape::recent`, `EmissionSource`). Without this the
//    two invariants above still leave the stream's last word for that pk stale forever — native
//    consumers fold by durable-stream offset, so an older LSN on the envelope repairs nothing.

/// How many times a flip batch's propagation is attempted before the engine gives up on the batch
/// — and on itself (see [`propagate_with_retry`]).
pub const FLIP_ATTEMPTS: u32 = 5;
/// Delay after the first failed attempt; doubled per attempt, capped at [`FLIP_BACKOFF_MAX`]. The
/// point is to sit out a transient Postgres fault (a killed backend, a failover, a lock timeout)
/// long enough that the retry has a different answer. Shrunk under `cfg(test)` so the exhaustion
/// test costs milliseconds instead of spending the production schedule's several seconds waiting.
const FLIP_BACKOFF_START: std::time::Duration = std::time::Duration::from_millis(if cfg!(test) { 1 } else { 50 });
const FLIP_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_millis(if cfg!(test) { 8 } else { 2000 });

/// [`propagate_flips`], retried on failure — the only form callers should use.
///
/// A retry RESUMES the walk rather than restarting it: `work` is drained in place and a failed
/// attempt leaves behind exactly what it did not finish (the failing item back at the front), so
/// the next attempt picks up where the last one stopped. Restarting from the original roots would
/// be silently wrong — reconciling a parent node CONSUMES the transition that produced its flips,
/// so a re-walk from the roots finds the parent already moved, derives nothing, and reports `Ok`
/// while the dependents those flips would have moved never move. Re-walking the finished edges of
/// the restored item is harmless: emission is absolute per pk.
pub async fn propagate_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
    txid: Option<String>,
    lsn: Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_flips(registry, work, txid.clone(), lsn.clone(), trace_tx).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "subquery flip propagation failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} flip(s) left to walk): {e:#}",
            work.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// [`propagate_deferred_shape_work`], on the same retry schedule (and with the same fail-closed
/// treatment by the caller) as the live walk: work queued during a create is exactly as
/// unreproducible once dropped as work derived from a live flip, so it gets the same guarantees.
pub async fn propagate_deferred_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    work: &mut VecDeque<DeferredShapeWork>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_deferred_shape_work(registry, shape_id, work).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "deferred propagation for shape '{shape_id}' failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} item(s) left): {e:#}",
            work.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// Run the work that reached a shape while it was being created, now that it is installed.
///
/// Only that shape: the flip's walk already visited every other dependent of the flipped node at
/// the time, so re-walking `edges_of` would redo their query-backs for nothing. Drained in place
/// with the failed item restored at the front, exactly like [`propagate_flips`], so the retry
/// resumes rather than restarts.
///
/// No trace is emitted. A flip trace lights the whole path — source `table:` → flipped `node:` →
/// dependent — and a deferred item deliberately carries neither the flipped node's signature nor
/// the table the change entered through; the walk that queued it already lit that path for the
/// dependents it could move.
pub async fn propagate_deferred_shape_work(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    work: &mut VecDeque<DeferredShapeWork>,
) -> Result<()> {
    while let Some(item) = work.pop_front() {
        let res = match &item {
            DeferredShapeWork::Value { connecting_col, value, txid } => {
                move_shape_for_value(registry, shape_id, *connecting_col, value, txid.clone()).await.map(|_| ())
            }
            DeferredShapeWork::Full { txid } => rederive_shape(registry, shape_id, txid.clone()).await,
        };
        if let Err(e) = res {
            work.push_front(item);
            return Err(e);
        }
    }
    Ok(())
}

/// [`propagate_deferred_node_work`], on the same retry schedule (and with the same fail-closed
/// treatment by the caller) as every other propagation: a re-derivation a create deferred is
/// exactly as unreproducible once dropped as a live flip's.
pub async fn propagate_deferred_node_with_retry(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, DeferredNodeWork)>,
    walk: &mut VecDeque<(SubquerySig, Flip)>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let mut backoff = FLIP_BACKOFF_START;
    let mut attempt = 1u32;
    loop {
        let e = match propagate_deferred_node_work(registry, work, walk, trace_tx).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        tracing::warn!(
            "deferred node propagation failed (attempt {attempt} of {FLIP_ATTEMPTS}, {} re-derivation(s) + {} flip(s) left): {e:#}",
            work.len(),
            walk.len()
        );
        if attempt >= FLIP_ATTEMPTS {
            return Err(e);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(FLIP_BACKOFF_MAX);
        attempt += 1;
    }
}

/// Run the re-derivations that reached a parent NODE while its create was still seeding it, now
/// that its set is installed — then walk whatever they flip on down the DAG, so the dependents
/// below (the create's own shape included, installed by now) move too.
///
/// Both queues are drained in place and both are resumable, so the retry picks up where the failed
/// attempt stopped rather than restarting: a failed `requery_and_reconcile_parent` fails at its
/// Postgres read, BEFORE it consumes the transition it was called for, so its item goes back at the
/// front unchanged; and once a re-derivation has produced flips they live on `walk`, which
/// [`propagate_flips`] drains with the same discipline. Restarting instead would find the parent
/// already reconciled and derive nothing while its dependents never moved.
pub async fn propagate_deferred_node_work(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, DeferredNodeWork)>,
    walk: &mut VecDeque<(SubquerySig, Flip)>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    while let Some((sig, item)) = work.pop_front() {
        let filter = match &item {
            DeferredNodeWork::Value { connecting_col, value } => Some((*connecting_col, value)),
            DeferredNodeWork::Full => None,
        };
        match requery_and_reconcile_parent(registry, &sig, filter).await {
            Ok(Some((_inner, flips))) => {
                for f in flips {
                    walk.push_back((sig.clone(), f));
                }
            }
            // The node was dropped while the work waited: nothing to re-derive, nothing to walk.
            Ok(None) => {}
            Err(e) => {
                work.push_front((sig, item));
                return Err(e);
            }
        }
    }
    // No trace is emitted for the re-derivation itself, for the same reason the deferred shape
    // replay emits none: the walk that queued it already lit the source table → flipped node path.
    // The flips it produced light their own hops from this node down.
    propagate_flips(registry, walk, None, None, trace_tx).await
}

/// Propagate a batch of inner-set flips up the dependency DAG (BFS), querying back affected rows.
/// `work` is the walk's whole state and is drained IN PLACE: on failure the item being processed
/// goes back at the front and the rest is untouched, so the caller's retry can resume (see
/// [`propagate_with_retry`], which is what callers should use).
pub async fn propagate_flips(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
    txid: Option<String>,
    lsn: Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    while let Some((sig, flip)) = work.pop_front() {
        if let Err(e) = propagate_one(registry, &sig, &flip, &txid, &lsn, trace_tx, work).await {
            work.push_front((sig, flip));
            return Err(e);
        }
    }
    Ok(())
}

/// One flip's worth of the walk: move every dependent of the flipped node, pushing any parent-node
/// flips it derives onto `work`. Split out of [`propagate_flips`] so a failure anywhere inside it
/// unwinds to exactly one place — the caller that puts the flip back on the queue.
async fn propagate_one(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    sig: &SubquerySig,
    flip: &Flip,
    txid: &Option<String>,
    lsn: &Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
) -> Result<()> {
    // The flipped inner-set node's dependents, plus the table its change entered through: the
    // head of the propagation path each dependent's trace lights (`table:<t>` → `node:<sig>` →
    // dependent). Fetched under one lock so a concurrent drop can't split them.
    let (edges, source_table) = {
        let reg = registry.lock().await;
        (reg.edges_of(sig), reg.nodes.get(sig).map(|n| n.inner_table.clone()))
    };
    for edge in edges {
        // A NULL-value flip only matters to NULL-sensitive dependents — a `NOT IN` leaf, or an
        // `IN` leaf under any `Not{…}` (SQL: a NULL in the set makes the leaf UNKNOWN, which
        // negation turns into a membership change). It can shift *every* dependent row, so
        // re-derive the dependent fully; NULL-insensitive dependents can't change (AND/OR are
        // monotone over FALSE < UNKNOWN < TRUE), so skip.
        if matches!(flip.value, Value::Null) {
            if edge.null_sensitive {
                rederive_dependent(registry, &edge, txid.clone(), work).await?;
            }
            continue;
        }
        match &edge.dependent {
            Dependent::Shape(id) => {
                let moved = move_shape_for_value(registry, id, edge.connecting_col, &flip.value, txid.clone()).await?;
                // Light the whole path only when the shape actually moved rows: source
                // `table:<t>` → the flipped `node:<sig>` → this `shape:<id>`.
                if let (Some((outer, net)), Some(src)) = (moved, source_table.as_ref()) {
                    emit_flip_trace(
                        trace_tx,
                        &outer,
                        src,
                        sig,
                        format!("shape:{id}"),
                        vec![id.clone()],
                        net,
                        lsn.clone(),
                        txid.clone(),
                    );
                }
            }
            Dependent::Node(parent_sig) => {
                // `None` = the parent vanished, or it is a fresh node its own create is still
                // seeding — in which case the re-derivation was queued on the node and runs when
                // that create installs the seed (see `queue_node_deferred`).
                let new_flips =
                    requery_and_reconcile_parent(registry, parent_sig, Some((edge.connecting_col, &flip.value)))
                        .await?;
                if let Some((_inner, flips)) = new_flips {
                    // A nested `IN`: connect the flipped child `node:<sig>` to the parent
                    // `node:<parent_sig>` it re-derived, so the propagation reads through. The
                    // parent's own downstream shape lights when its flips reach a shape edge.
                    if let (false, Some(src)) = (flips.is_empty(), source_table.as_ref()) {
                        emit_flip_trace(
                            trace_tx,
                            src,
                            src,
                            sig,
                            format!("node:{parent_sig}"),
                            Vec::new(),
                            flip_net(&flips),
                            lsn.clone(),
                            txid.clone(),
                        );
                    }
                    for f in flips {
                        work.push_back((parent_sig.clone(), f));
                    }
                }
            }
        }
    }
    Ok(())
}

/// An inner-set value `v` flipped for an outer shape: query the outer rows with `connecting_col = v`,
/// re-evaluate the full shape predicate, and append `upsert` (matches) / `delete` (doesn't) by pk.
/// Returns `Some((outer_table, net_weight))` when envelopes were appended — the shape's own table
/// (the event's `table`) and the net membership change (for the trace dot's label/colour) — or
/// `None` when nothing moved.
async fn move_shape_for_value(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    connecting_col: usize,
    value: &Value,
    txid: Option<String>,
) -> Result<Option<(TableRef, i64)>> {
    // Brief lock: snapshot what the query-back needs.
    let (ts, pg_url) = {
        let mut reg = registry.lock().await;
        // A shape whose create is still in flight has edges (phase A) but no installed shape to
        // move: the flip waits on its pending entry and runs at install, instead of falling through
        // to a `shapes` miss that would silently drop it.
        if reg.queue_deferred(
            shape_id,
            DeferredShapeWork::Value { connecting_col, value: value.clone(), txid: txid.clone() },
        ) {
            return Ok(None);
        }
        let Some(shape) = reg.shapes.get(shape_id) else { return Ok(None) };
        let snapshot = (reg.snapshot_for_table(&shape.outer_table)?, reg.pg_url.clone());
        // In flight from here until the evaluation below: live decisions taken in that window are
        // stamped on the shape and beat whatever this read returns.
        reg.begin_queryback(shape_id);
        snapshot
    };
    // Every exit from here on must decrement — including the read's error path — or the shape
    // would keep recording recency (and dropping candidates) forever. There is no guard object
    // that can do it: the registry mutex is async and cannot be taken in `Drop`.
    let (rows, gate) = match query_candidates(&pg_url, &ts, connecting_col, value).await {
        Ok(r) => r,
        Err(e) => {
            registry.lock().await.end_queryback(shape_id);
            return Err(e);
        }
    };
    if rows.is_empty() {
        registry.lock().await.end_queryback(shape_id);
        return Ok(None);
    }
    // Evaluate + assert + deliver atomically under the lock, through the ONE emission tail
    // (candidates from a query-back all still exist), filtered against the live decisions taken
    // since `gate`'s snapshot.
    let mut reg = registry.lock().await;
    let candidates: Vec<(Row, bool)> = rows.into_iter().map(|r| (r, true)).collect();
    let emitted = reg
        .emit_for_shapes(&ts, vec![(shape_id.to_string(), candidates)], txid, EmissionSource::QueryBack { gate: &gate })
        .await;
    reg.end_queryback(shape_id);
    Ok(match emitted?.first() {
        Some((_, true, net)) => Some((ts.table.clone(), *net)),
        _ => None,
    })
}

/// Re-query a parent node's inner rows — `Some((col, v))` = only rows with
/// `connecting_col = v` (a value flip), `None` = every row (a NULL re-derive) — then
/// re-evaluate the parent's full predicate and reconcile. Returns `Some((inner_table,
/// flips))`, or `None` if the parent vanished. The shared body of both flip-driven parent
/// paths: the fetch differs, the eval+reconcile never does.
async fn requery_and_reconcile_parent(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    parent_sig: &SubquerySig,
    filter: Option<(usize, &Value)>,
) -> Result<Option<(TableRef, Vec<Flip>)>> {
    let (ts, pg_url) = {
        let mut reg = registry.lock().await;
        // Same as `move_shape_for_value`'s pending-shape check, one tier down: a parent node whose
        // own create is still seeding it has edges (phase A) but no set to reconcile against. The
        // work waits on the node and runs at install, instead of re-deriving against an empty set
        // whose seed — older than this flip — would then be installed over the change.
        if reg.queue_node_deferred(
            parent_sig,
            match filter {
                Some((col, value)) => DeferredNodeWork::Value { connecting_col: col, value: value.clone() },
                None => DeferredNodeWork::Full,
            },
        ) {
            return Ok(None);
        }
        let Some(n) = reg.nodes.get(parent_sig) else { return Ok(None) };
        let snapshot = (reg.snapshot_for_table(&n.inner_table)?, reg.pg_url.clone());
        // In flight from here until the reconcile below: live contribution decisions taken in that
        // window are stamped on the node and beat whatever this read returns.
        reg.begin_node_queryback(parent_sig);
        snapshot
    };
    // Every exit from here on must decrement — the read's error path included — or the node would
    // keep recording recency forever. There is no guard object that can do it: the registry mutex
    // is async and cannot be taken in `Drop` (same reason as `move_shape_for_value`).
    let read = match filter {
        Some((col, value)) => query_candidates(&pg_url, &ts, col, value).await,
        None => query_all(&pg_url, &ts).await,
    };
    let (rows, gate) = match read {
        Ok(r) => r,
        Err(e) => {
            registry.lock().await.end_node_queryback(parent_sig);
            return Err(e);
        }
    };
    let mut reg = registry.lock().await;
    let Some(evals) = reg.node_queryback_evals(parent_sig, &ts, &rows, &gate) else {
        reg.end_node_queryback(parent_sig);
        return Ok(None);
    };
    let flips = reg.apply_node_evals(parent_sig, evals).await;
    reg.end_node_queryback(parent_sig);
    Ok(Some((ts.table.clone(), flips)))
}

/// Re-derive a dependent fully (used for NULL flips on negated edges): re-query every candidate row
/// of the dependent's table and reconcile/emit. Rare (projections are typically non-null).
async fn rederive_dependent(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    edge: &Edge,
    txid: Option<String>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
) -> Result<()> {
    match &edge.dependent {
        Dependent::Shape(id) => rederive_shape(registry, id, txid).await?,
        Dependent::Node(parent_sig) => {
            // Full re-derive of the parent: same eval+reconcile as a value flip, fetching
            // every row instead of one connecting value's candidates.
            if let Some((_table, flips)) = requery_and_reconcile_parent(registry, parent_sig, None).await? {
                for f in flips {
                    work.push_back((parent_sig.clone(), f));
                }
            }
        }
    }
    Ok(())
}

/// Re-derive an outer shape from scratch: re-query every candidate row of its table and let the
/// emission tail decide each one's membership. Used for a NULL flip on a NULL-sensitive edge (which
/// can move any row, whatever its connecting value) and to replay a `Full` deferred item.
async fn rederive_shape(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    txid: Option<String>,
) -> Result<()> {
    let (ts, pg_url) = {
        let mut reg = registry.lock().await;
        // Same as `move_shape_for_value`: a shape still being created is queued, not dropped — its
        // edges are live but its `shapes` entry does not exist yet.
        if reg.queue_deferred(shape_id, DeferredShapeWork::Full { txid: txid.clone() }) {
            return Ok(());
        }
        let Some(s) = reg.shapes.get(shape_id) else { return Ok(()) };
        let snapshot = (reg.snapshot_for_table(&s.outer_table)?, reg.pg_url.clone());
        reg.begin_queryback(shape_id);
        snapshot
    };
    // Same accounting discipline as `move_shape_for_value`: decrement on every path.
    let (rows, gate) = match query_all(&pg_url, &ts).await {
        Ok(r) => r,
        Err(e) => {
            registry.lock().await.end_queryback(shape_id);
            return Err(e);
        }
    };
    // Full re-derive: every row is a candidate; the ONE emission tail decides — except for pks a
    // live commit decided after this read's snapshot, which it drops.
    let mut reg = registry.lock().await;
    let candidates: Vec<(Row, bool)> = rows.into_iter().map(|r| (r, true)).collect();
    let emitted = reg
        .emit_for_shapes(&ts, vec![(shape_id.to_string(), candidates)], txid, EmissionSource::QueryBack { gate: &gate })
        .await;
    reg.end_queryback(shape_id);
    emitted?;
    Ok(())
}

// Candidate-row resolution (arrangement snapshot → pooled Postgres fallback) is the shared
// membership kernel's — one implementation for this registry and for circuit cohort serving.
use crate::engine::membership::{query_rows_all as query_all, query_rows_by_col as query_candidates};

/// Net membership change carried by a batch of parent-node flips (enters +1, leaves −1), for the
/// trace dot's label/colour.
fn flip_net(flips: &[Flip]) -> i64 {
    flips
        .iter()
        .map(|f| match f.dir {
            FlipDir::Enter => 1,
            FlipDir::Leave => -1,
        })
        .sum()
}

/// Broadcast a lossy trace event lighting the WHOLE path a deferred inner-set flip travelled: the
/// source inner `table:<t>` the change entered through, the flipped inner-set `node:<sig>` (its
/// `IN-SET ARRANGE`/distinct), and the re-derived `dependent` (`shape:<sid>` for an outer subquery
/// shape, or a parent `node:<sig>` for nested `IN`). The originating envelope's own trace stops at
/// the inner-set node — the propagator moves the dependent out of band, after the query-backs — so
/// without this the visualizer flashes the source, fades the moved shape off that direct change's
/// path, and never pulses the serving edge (an edge pulses only when both endpoints flash). One
/// synthetic weighted row carries the net membership change so the travelling dot is labelled +1 /
/// −1 and coloured. Best-effort and zero-cost when no one is subscribed, mirroring the in-engine
/// trace path.
///
/// `event_table` is the table the event is *about* (the dependent shape's own table, matching the
/// direct-change trace's `table`); `source_table` is where the change entered and heads the hop path
/// (they differ: a `project_members` change moves an `issues` shape).
// Everything after `trace_tx` goes into the one `TraceEvent` this builds (its stamp, table, hop
// path, shapes and net weight). The three callers hold those as separate locals, so a parameter
// struct would only move the same list into a struct literal at each of them.
#[allow(clippy::too_many_arguments)]
fn emit_flip_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    event_table: &TableRef,
    source_table: &TableRef,
    node_sig: &SubquerySig,
    dependent: String,
    shapes: Vec<String>,
    net: i64,
    lsn: Option<String>,
    txid: Option<String>,
) {
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let ev = crate::trace::TraceEvent {
        lsn,
        txid,
        table: event_table.clone(),
        // One synthetic weighted row carrying the net change: a single flip can move many outer
        // rows, so the payload is not one table row — left empty, weighted by the net.
        delta: vec![crate::trace::TraceDelta { row: serde_json::json!({}), w: net }],
        hops: vec![
            crate::trace::TraceHop::new(format!("table:{source_table}"), "passed"),
            crate::trace::TraceHop::new(format!("node:{node_sig}"), "passed"),
            crate::trace::TraceHop::new(dependent, "passed"),
        ],
        shapes,
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

impl SubqueryCollector for SubqueryRegistry {
    /// Discover (or dedupe) a subquery node: compile its inner predicate (recursively collecting deeper
    /// nodes), record its child edges, and queue it for seeding. Returns the canonical signature.
    fn collect(&mut self, table: &TableRef, project: &str, where_: Option<&PredicateJson>) -> Result<SubquerySig> {
        let sig = subquery_sig(table, project, where_);
        if let Some(n) = self.nodes.get_mut(&sig) {
            n.refcount += 1;
            self.collect_log.push(sig.clone());
            return Ok(sig);
        }
        let inner_ts = self.schemas.get(table).cloned().context("subquery: unknown inner table")?;
        let inner_pred = match where_ {
            Some(w) => CompiledPredicate::compile_with(w, &inner_ts, self)?,
            None => CompiledPredicate::MatchAll,
        };
        // Record edges from each child node to THIS node (so a child flip re-derives this node's rows).
        for leaf in collect_in_leaves(&inner_pred) {
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Node(sig.clone()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        let proj_col = inner_ts.column_index(project)?;
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        // Template registration: lift the equality literals into a bind (coerced to the
        // column types, same as leaf compilation) and share the residual across binds.
        let (tkey, bind_literals, residual_json) = crate::predicate::subquery_template(table, project, where_);
        let mut param_cols = Vec::with_capacity(bind_literals.len());
        let mut bind_vals = Vec::with_capacity(bind_literals.len());
        for (col, lit) in &bind_literals {
            let idx = inner_ts.column_index(col)?;
            param_cols.push(idx);
            bind_vals.push(Value::literal_from_json(lit, inner_ts.column_type(idx))?);
        }
        let bind = Row(bind_vals);
        if !self.templates.contains_key(&tkey) {
            // The residual is compiled with a sig-only collector: any nested IN inside it was
            // already collected (and refcounted) by the full-pred compile above; collecting
            // again would double-count.
            struct SigOnly;
            impl SubqueryCollector for SigOnly {
                fn collect(&mut self, t: &TableRef, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                    Ok(subquery_sig(t, p, w))
                }
            }
            let residual = if residual_json.is_empty() {
                CompiledPredicate::MatchAll
            } else {
                CompiledPredicate::compile_with(&PredicateJson::And { and: residual_json }, &inner_ts, &mut SigOnly)?
            };
            self.templates.insert(
                tkey.clone(),
                TemplateGroup {
                    inner_table: table.clone(),
                    proj_col,
                    residual: Arc::new(residual),
                    param_cols: param_cols.clone(),
                    binds: HashMap::new(),
                    pk_nodes: HashMap::new(),
                },
            );
        }
        if let Some(tpl) = self.templates.get_mut(&tkey) {
            tpl.binds.insert(bind.clone(), sig.clone());
        }
        let mut node =
            SubqueryNode::new(sig.clone(), table.clone(), proj_col, inner_ts.pk_index, Arc::new(inner_pred), node_id);
        node.where_json = where_.cloned();
        node.template_key = tkey;
        node.bind = bind;
        node.refcount = 1;
        self.nodes.insert(sig.clone(), node);
        self.node_by_id.insert(node_id, sig.clone());
        self.collect_log.push(sig.clone());
        self.pending_seed.push(sig.clone());
        Ok(sig)
    }
}

impl SubqueryEval for SubqueryRegistry {
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, value))
    }
    fn has_null(&self, sig: &SubquerySig) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, &Value::Null))
    }
}

/// One `IN (SELECT …)` leaf found in a compiled predicate, with the context needed to build its
/// dependency edge.
pub struct InLeaf {
    pub col: usize,
    pub sig: SubquerySig,
    pub negated: bool,
    /// leaf negated OR under any `Not{…}` wrapper — see [`Edge::null_sensitive`].
    pub null_sensitive: bool,
}

/// Find all `IN (SELECT …)` leaves in a compiled predicate, tracking whether each sits under a `Not`
/// (which makes it NULL-sensitive even when the leaf itself isn't negated — `NOT (x IN S)` flips
/// membership when a NULL enters `S`, exactly like `x NOT IN S`).
pub fn collect_in_leaves(p: &CompiledPredicate) -> Vec<InLeaf> {
    let mut out = Vec::new();
    fn go(p: &CompiledPredicate, under_not: bool, out: &mut Vec<InLeaf>) {
        match p {
            CompiledPredicate::And(v) | CompiledPredicate::Or(v) => v.iter().for_each(|c| go(c, under_not, out)),
            CompiledPredicate::Not(b) => go(b, true, out),
            CompiledPredicate::InSubquery { col, sig, negated } => out.push(InLeaf {
                col: *col,
                sig: sig.clone(),
                negated: *negated,
                null_sensitive: *negated || under_not,
            }),
            _ => {}
        }
    }
    go(p, false, &mut out);
    out
}

/// Does a JSON predicate contain any `IN (SELECT …)` subquery?
pub fn predicate_has_subquery(p: &PredicateJson) -> bool {
    match p {
        PredicateJson::In { .. } => true,
        PredicateJson::And { and } => and.iter().any(predicate_has_subquery),
        PredicateJson::Or { or } => or.iter().any(predicate_has_subquery),
        PredicateJson::Not { not } => predicate_has_subquery(not),
        PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => false,
    }
}

/// Every table referenced by a JSON predicate's subqueries (inner tables, recursively).
pub fn referenced_tables(p: &PredicateJson) -> Vec<TableRef> {
    let mut out = Vec::new();
    fn go(p: &PredicateJson, out: &mut Vec<TableRef>) {
        match p {
            PredicateJson::In { subquery, .. } => {
                if !out.contains(&subquery.table) {
                    out.push(subquery.table.clone());
                }
                if let Some(w) = &subquery.where_ {
                    go(w, out);
                }
            }
            PredicateJson::And { and } => and.iter().for_each(|c| go(c, out)),
            PredicateJson::Or { or } => or.iter().for_each(|c| go(c, out)),
            PredicateJson::Not { not } => go(not, out),
            PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => {}
        }
    }
    go(p, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry with one MatchAll node registered the way `collect()` would: node map,
    /// node_by_id, and a unit-bind template — so `on_table_delta`, `apply_node_evals`, and the
    /// `SubqueryEval` reads all work.
    fn registry_with_node(sig: &str) -> SubqueryRegistry {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        insert_test_node(&mut reg, sig);
        reg
    }

    fn insert_test_node(reg: &mut SubqueryRegistry, sig: &str) {
        let node_id = reg.next_node_id;
        reg.next_node_id += 1;
        let mut node = SubqueryNode::new(sig.into(), "t".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll), node_id);
        let tkey = format!("tpl:{sig}");
        node.template_key = tkey.clone();
        node.refcount = 1;
        reg.nodes.insert(sig.into(), node);
        reg.node_by_id.insert(node_id, sig.into());
        reg.templates.insert(
            tkey,
            TemplateGroup {
                inner_table: "t".into(),
                proj_col: 0,
                residual: Arc::new(CompiledPredicate::MatchAll),
                param_cols: Vec::new(),
                binds: [(Row(Vec::new()), sig.to_string())].into_iter().collect(),
                pk_nodes: HashMap::new(),
            },
        );
    }

    /// The trace reports an inner-table delta's effect on a subquery node: `passed` when the
    /// inner set flipped (a value entered/left), `dropped` when it didn't change.
    #[tokio::test]
    async fn trace_subquery_node_hops() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap()
        };
        let mut reg = SubqueryRegistry::new(crate::ds::DsClient::new("http://127.0.0.1:1"), None);
        insert_test_node(&mut reg, "sig1");

        // A new row projects value 1 into the inner set -> Enter flip -> passed.
        let delta = vec![Tup2(Row(vec![Value::Int(1)]), 1)];
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "passed"),
            "expected passed node hop, got {hops:?}"
        );

        // The same row again: the value is already present -> no flip -> dropped.
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "dropped"),
            "expected dropped node hop, got {hops:?}"
        );
    }

    /// A deferred inner-set flip that moves a dependent shape must light the WHOLE propagation path
    /// — the source inner `table:`, the flipped `node:<sig>` (IN-SET ARRANGE/distinct), and the
    /// re-derived `shape:<sid>` — so the visualizer animates the moved shape instead of fading it
    /// off the direct change's path (the "leaving a project" bug).
    #[test]
    fn flip_trace_lights_source_node_and_shape() {
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);
        // A `project_members` delete flipped inner-set value; the `issues` shape s1 lost 103 rows.
        let sig = "project_members|project_id|L(user_id,Eq,1)".to_string();
        emit_flip_trace(
            &trace_tx,
            &"issues".into(),
            &"project_members".into(),
            &sig,
            "shape:s1".into(),
            vec!["s1".into()],
            -103,
            Some("0/1A2B3C".into()),
            Some("777".into()),
        );

        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        // The event is about the dependent shape's table; the path still heads at the source.
        assert_eq!(ev["table"], "public.issues");
        // Carries the originating write's lsn/txid, so the activity log can group this deferred
        // flip event together with the direct-change event that triggered it (same commit).
        assert_eq!(ev["lsn"], "0/1A2B3C");
        assert_eq!(ev["txid"], "777");
        let outcome = |node: &str| {
            ev["hops"].as_array().unwrap().iter().find(|h| h["node"] == node).map(|h| h["outcome"].clone())
        };
        assert_eq!(outcome("table:public.project_members"), Some(serde_json::json!("passed")), "source lit");
        assert_eq!(outcome(&format!("node:{sig}")), Some(serde_json::json!("passed")), "subquery node lit");
        assert_eq!(outcome("shape:s1"), Some(serde_json::json!("passed")), "dependent shape lit");
        assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s1")]);
        assert_eq!(ev["delta"][0]["w"], -103, "the dot carries the real net −103 leave, not 0");
    }

    /// **A failed walk keeps its own remains.** The DAG walk is not restartable from its roots:
    /// reconciling a parent node consumes the transition that produced its flips, so a retry from
    /// the roots finds the parent already moved, derives nothing, and reports success while the
    /// dependents those flips would have moved never move — lost effects that also bypass the
    /// fail-closed path, because nothing errored the second time.
    ///
    /// What makes the retry sound is that the queue survives the failure: whatever the attempt did
    /// not finish, including the item that failed, is still there for the next attempt. Asserted
    /// here directly, since a partial walk through a nested DAG needs a live Postgres to reach.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_propagation_leaves_its_work_for_the_retry() {
        let ts = issues_ts();
        // No `pg_url`: the first query-back fails, as an outage makes it fail.
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.add_edge(Edge {
            node_sig: sig.clone(),
            dependent: Dependent::Shape("s1".into()),
            connecting_col: 1,
            negated: false,
            null_sensitive: false,
        });
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> = [
            (sig.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter }),
            (sig.clone(), Flip { value: Value::Int(200), dir: FlipDir::Enter }),
        ]
        .into_iter()
        .collect();
        let err = propagate_flips(&registry, &mut work, None, None, &trace_tx).await;

        assert!(err.is_err(), "the query-back cannot succeed without Postgres");
        assert_eq!(work.len(), 2, "a failed attempt must not consume the work it did not finish");
        assert_eq!(work[0].1.value, Value::Int(100), "the failed item goes back at the front");
        assert_eq!(work[1].1.value, Value::Int(200), "and the untouched item keeps its place");
    }

    /// The same, through the retrying entry point every caller uses: exhausting the attempts still
    /// hands the whole unfinished walk back, so the caller can report exactly what was lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exhausted_retry_still_returns_the_unfinished_work() {
        let ts = issues_ts();
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.add_edge(Edge {
            node_sig: sig.clone(),
            dependent: Dependent::Shape("s1".into()),
            connecting_col: 1,
            negated: false,
            null_sensitive: false,
        });
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(sig.clone(), Flip { value: Value::Int(100), dir: FlipDir::Enter })].into_iter().collect();
        let err = propagate_with_retry(&registry, &mut work, None, None, &trace_tx).await;

        assert!(err.is_err(), "postgres never comes back; every attempt fails");
        assert_eq!(work.len(), 1, "the abandoned flip is still on the queue for the report");
    }

    /// `outer_t(gid, id)` + `inner_t(gid, id)`: enough for `begin_create` to compile
    /// `gid IN (SELECT gid FROM inner_t)` and register a pending shape.
    fn creating_schemas() -> HashMap<TableRef, TableSchema> {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        [("outer_t", mk("outer_t")), ("inner_t", mk("inner_t"))]
            .into_iter()
            .map(|(t, ts)| (TableRef::parse(t).unwrap(), ts))
            .collect()
    }

    fn in_inner_t() -> PredicateJson {
        serde_json::from_value(serde_json::json!({
            "col":"gid","in":{"table":"inner_t","project":"gid"}
        }))
        .unwrap()
    }

    /// A registry parked exactly where phase B runs: edges committed, shape not installed.
    fn registry_mid_create() -> (SubqueryRegistry, BeginCreate) {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        reg.set_schemas(Arc::new(creating_schemas()));
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &in_inner_t(), None, false).unwrap();
        (reg, begin)
    }

    /// **A flip that arrives mid-create waits, it is not dropped.** Phase A commits the shape's
    /// edges, so a flip on a node the shape SHARES with a live sibling reaches this shape while
    /// phase B is still backfilling — and the backfill's snapshot may predate the inner change.
    /// Finding no installed shape and moving on would lose those rows for the life of the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_reaching_a_shape_mid_create_waits_on_its_pending_entry() {
        let (reg, _begin) = registry_mid_create();
        let registry = tokio::sync::Mutex::new(reg);

        // No `pg_url`: had the flip fallen through to its query-back instead of being queued, this
        // would be an error rather than "nothing moved".
        let moved = move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None).await.unwrap();
        assert!(moved.is_none(), "nothing moves: the shape is not installed yet");
        // A hot inner table flips the same value repeatedly; the replay is absolute, so once is enough.
        move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None).await.unwrap();
        move_shape_for_value(&registry, "s1", 0, &Value::Int(8), None).await.unwrap();
        {
            let reg = registry.lock().await;
            assert!(reg.shapes.is_empty(), "the create has not installed anything");
            let queued = &reg.pending_shapes[0].deferred;
            assert_eq!(queued.len(), 2, "both distinct values queued; the repeat deduped");
            assert!(matches!(queued[0], DeferredShapeWork::Value { value: Value::Int(7), .. }));
            assert!(matches!(queued[1], DeferredShapeWork::Value { value: Value::Int(8), .. }));
        }

        // A NULL flip re-derives every row, so it subsumes anything queued before it.
        rederive_shape(&registry, "s1", None).await.unwrap();
        let reg = registry.lock().await;
        let queued = &reg.pending_shapes[0].deferred;
        assert_eq!(queued.len(), 1, "the full re-derive replaced the per-value work");
        assert!(matches!(queued[0], DeferredShapeWork::Full { .. }));
    }

    /// `finish_create` hands the queue back with the pending entry removed and the shape installed,
    /// all in one `&mut self` step: after it, a flip finds the shape and runs straight through, and
    /// the caller has the only copy of what arrived while it could not.
    #[tokio::test(flavor = "multi_thread")]
    async fn finish_create_hands_back_the_queued_work() {
        let (reg, begin) = registry_mid_create();
        let seeds: Vec<(SubquerySig, crate::pg::SnapshotGate)> =
            begin.seeds.iter().map(|(sig, _, _)| (sig.clone(), crate::pg::SnapshotGate::passthrough())).collect();
        let registry = tokio::sync::Mutex::new(reg);
        move_shape_for_value(&registry, "s1", 0, &Value::Int(7), None).await.unwrap();

        let mut reg = registry.into_inner();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();

        assert!(finished.work.is_empty(), "no buffered inner deltas to replay");
        assert_eq!(finished.deferred.len(), 1, "the queued flip comes back for the caller to run");
        assert!(finished.node_work.is_empty(), "no flip reached a node mid-seed");
        assert!(reg.pending_shapes.is_empty(), "the pending entry is gone, and its queue with it");
        assert!(reg.shapes.contains_key("s1"), "the shape is installed: later flips run straight through");
    }

    /// `outer_t(gid, id)` + `mid_t(gid, id)` + `deep_t(gid, id)`: enough to compile the NESTED
    /// `gid IN (SELECT gid FROM mid_t WHERE gid IN (SELECT gid FROM deep_t))`, which registers two
    /// fresh nodes — a deepest one and a PARENT node depending on it.
    fn nested_schemas() -> HashMap<TableRef, TableSchema> {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        ["outer_t", "mid_t", "deep_t"].into_iter().map(|t| (TableRef::parse(t).unwrap(), mk(t))).collect()
    }

    /// A registry parked in a NESTED create's phase B: both fresh nodes seeding (deepest first),
    /// the child→parent edge committed, no shape installed. Returns the registry, the deepest
    /// node's sig and the parent node's.
    fn registry_mid_nested_create() -> (SubqueryRegistry, SubquerySig, SubquerySig) {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        reg.set_schemas(Arc::new(nested_schemas()));
        let where_json: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "gid",
            "in": {
                "table": "mid_t",
                "project": "gid",
                "where": { "col": "gid", "in": { "table": "deep_t", "project": "gid" } }
            }
        }))
        .unwrap();
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false).unwrap();
        assert_eq!(begin.seeds.len(), 2, "two fresh nodes, deepest first");
        let (deep, mid) = (begin.seeds[0].0.clone(), begin.seeds[1].0.clone());
        (reg, deep, mid)
    }

    /// **A flip that reaches a still-seeding parent NODE waits, it is not reconciled.** The node's
    /// set is incomplete until phase C finishes its seed, so re-deriving now misses rows — and the
    /// seed, taken from a snapshot older than the flip, is then installed over the change, leaving
    /// the node and everything below it stale for the life of the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flip_reaching_a_seeding_parent_node_waits_on_it() {
        let (reg, deep, mid) = registry_mid_nested_create();
        let mid_id = reg.nodes[&mid].node_id;
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);

        // No `pg_url`: had the flip fallen through to its query-back instead of being deferred,
        // this walk would fail rather than complete quietly.
        let flip = |v: i64| -> VecDeque<(SubquerySig, Flip)> {
            [(deep.clone(), Flip { value: Value::Int(v), dir: FlipDir::Enter })].into_iter().collect()
        };
        propagate_flips(&registry, &mut flip(7), None, None, &trace_tx).await.unwrap();
        // A hot inner table flips the same value repeatedly during one seed; the re-derive is
        // absolute, so running it once at install is running it enough.
        propagate_flips(&registry, &mut flip(7), None, None, &trace_tx).await.unwrap();
        {
            let reg = registry.lock().await;
            let queued = &reg.nodes[&mid].deferred;
            assert_eq!(queued.len(), 1, "queued once on the parent node; the repeat deduped");
            assert!(matches!(queued[0], DeferredNodeWork::Value { connecting_col: 0, value: Value::Int(7) }));
            assert_eq!(reg.circuit_distinct(mid_id), 0, "the seeding node's set is untouched");
            assert!(reg.nodes[&mid].seed_buffer.is_some(), "and it is still seeding");
        }

        // A full re-derive covers every value, so it subsumes what was queued before it.
        assert!(requery_and_reconcile_parent(&registry, &mid, None).await.unwrap().is_none());
        let reg = registry.lock().await;
        let queued = &reg.nodes[&mid].deferred;
        assert_eq!(queued.len(), 1, "the full re-derive replaced the per-value work");
        assert!(matches!(queued[0], DeferredNodeWork::Full));
    }

    /// `finish_create` hands the node's deferred work back once that node's seed and its buffered
    /// raw deltas are both in — the first moment re-deriving it against Postgres means anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn finish_create_hands_back_the_node_work() {
        let (reg, deep, mid) = registry_mid_nested_create();
        let seeds: Vec<(SubquerySig, crate::pg::SnapshotGate)> =
            [&deep, &mid].into_iter().map(|sig| (sig.clone(), crate::pg::SnapshotGate::passthrough())).collect();
        let registry = tokio::sync::Mutex::new(reg);
        let (trace_tx, _rx) = tokio::sync::broadcast::channel(16);
        let mut work: VecDeque<(SubquerySig, Flip)> =
            [(deep.clone(), Flip { value: Value::Int(7), dir: FlipDir::Enter })].into_iter().collect();
        propagate_flips(&registry, &mut work, None, None, &trace_tx).await.unwrap();

        let mut reg = registry.into_inner();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();

        assert_eq!(finished.node_work.len(), 1, "the parent node's re-derivation comes back");
        assert_eq!(finished.node_work[0].0, mid, "aimed at the parent node that deferred it");
        assert!(reg.nodes[&mid].deferred.is_empty(), "taken, not copied");
        assert!(reg.nodes[&mid].seed_buffer.is_none(), "and the node is live, so nothing defers again");
    }

    /// A create cancelled after chunk assertions and before activation — one fresh node's seed in
    /// the membership circuit, the shape not installed — leaves nothing behind. The rollback state
    /// is the registry's (the pending entry never left it), so the abort can retract the partial
    /// seed as well as unwind the refcounts, edges and templates.
    #[tokio::test(flavor = "multi_thread")]
    async fn abort_after_a_partial_phase_c_leaves_no_state() {
        let (mut reg, deep, mid) = registry_mid_nested_create();
        let (deep_id, mid_id) = (reg.nodes[&deep].node_id, reg.nodes[&mid].node_id);
        // Phase C got as far as installing the deepest node's seed, then the request was dropped.
        reg.apply_node_evals(&deep, vec![("1".to_string(), Some(Value::Int(7)))]).await;
        assert_eq!(reg.circuit_distinct(deep_id), 1, "the partial seed really landed");

        reg.abort_create("s1").await;

        assert!(reg.pending_shapes.is_empty(), "no pending entry");
        assert!(reg.shapes.is_empty(), "and no installed shape either");
        assert!(reg.nodes.is_empty(), "both fresh nodes are gone");
        assert_eq!(reg.edges_count(), 0, "no edge outlives the create that staged it");
        assert!(reg.templates.is_empty(), "no template bind outlives its only node");
        assert!(reg.pending_seed.is_empty(), "nothing is left waiting to be seeded");
        assert_eq!(reg.circuit_distinct(deep_id), 0, "the partial seed was retracted");
        assert_eq!(reg.circuit_distinct(mid_id), 0);
    }

    /// A dropped apply can still be queued on the circuit thread. Rollback must wait before
    /// inspecting the contributor snapshot; otherwise the late assertion escapes its retraction.
    #[tokio::test(flavor = "multi_thread")]
    async fn abort_retracts_a_seed_assertion_cancelled_while_queued() {
        cancelled_seed_send(false).await;
        cancelled_seed_send(true).await;
    }

    async fn cancelled_seed_send(before_enqueue: bool) {
        let (mut reg, deep, existing) = if before_enqueue {
            let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
            reg.set_schemas(Arc::new(creating_schemas()));
            let predicate = |value| {
                serde_json::from_value(serde_json::json!({
                    "col":"gid", "in":{"table":"inner_t","project":"gid", "where":{"col":"gid","op":"eq","value":value}}
                }))
                .unwrap()
            };
            let begin =
                reg.begin_create("existing", &"outer_t".into(), "shape/existing", &predicate(7), None, true).unwrap();
            let existing_sig = begin.seeds[0].0.clone();
            reg.seed_chunk("existing", &existing_sig, vec![Row(vec![Value::Int(7), Value::Int(999)])]).await.unwrap();
            reg.finish_create(
                "existing",
                vec![(existing_sig.clone(), crate::pg::SnapshotGate::passthrough())],
                crate::pg::SnapshotGate::passthrough(),
                0,
                Default::default(),
            )
            .await
            .unwrap();
            let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &predicate(8), None, true).unwrap();
            let sig = begin.seeds[0].0.clone();
            assert_eq!(reg.nodes[&sig].template_key, reg.nodes[&existing_sig].template_key);
            (reg, sig, Some(existing_sig))
        } else {
            let (reg, deep, _) = registry_mid_nested_create();
            (reg, deep, None)
        };
        let id = reg.nodes[&deep].node_id;
        let release = reg.circuit.pause_for_test().await;
        if before_enqueue {
            reg.circuit.fill_queue_for_test();
        }
        {
            let apply = reg.seed_chunk("s1", &deep, vec![Row(vec![Value::Int(7), Value::Int(1)])]);
            tokio::pin!(apply);
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(apply.as_mut().poll(cx).is_pending())).await);
        }
        // The snapshot is still empty even though the insertion has been queued.
        assert_eq!(reg.circuit_distinct(id), 0);
        {
            let rollback = reg.abort_create("s1");
            tokio::pin!(rollback);
            let pending =
                std::future::poll_fn(|cx| std::task::Poll::Ready(rollback.as_mut().poll(cx).is_pending())).await;
            release.send(()).unwrap();
            if pending {
                rollback.await;
            }
        }
        // Flush the worker with an unrelated assertion before checking the old id. This is a
        // test observation barrier, not the missing rollback barrier being tested.
        reg.circuit
            .apply(crate::subq_circuit::Assertions {
                contributors: vec![Tup2(
                    crate::subq_circuit::PkKey { id: 999, pk: 999 },
                    crate::subq_circuit::Assert::Delete,
                )],
            })
            .await;
        assert_eq!(reg.circuit_distinct(id), 0, "the queued assertion must not outlive rollback");
        if let Some(existing) = existing {
            assert_eq!(reg.nodes.len(), 1, "only the pre-existing node survives");
            assert!(reg.circuit.contains(reg.nodes[&existing].node_id, &Value::Int(7)));
            assert_eq!(reg.nodes[&existing].refcount, 1);
            assert_eq!(reg.circuit.contributor_entries(reg.nodes[&existing].node_id).len(), 1);
            let template = &reg.templates[&reg.nodes[&existing].template_key];
            assert_eq!(template.pk_nodes.len(), 1, "the cancelled send's pk must leave the retained template");
            assert!(template.pk_nodes.values().all(|sigs| sigs.len() == 1 && sigs.contains(&existing)));
        } else {
            assert!(reg.nodes.is_empty());
            assert!(reg.templates.is_empty());
        }
    }

    /// Account row staging independently of retained contributor/key state. The legacy baseline
    /// uses the old collect loop (append every chunk, retaining both nested node vectors); the
    /// new path feeds those same chunks directly to the registry and retains no full-row vector.
    #[tokio::test(flavor = "multi_thread")]
    async fn wide_nested_seed_stages_one_chunk_and_defers_live_work() {
        const ROWS: usize = 2048;
        const CHUNK: usize = 16;
        const PAYLOAD: usize = 4096;
        let chunk = |start: usize| -> Vec<Row> {
            (start..start + CHUNK)
                .map(|i| Row(vec![Value::Int(i as i64), Value::Int(i as i64), Value::Text("x".repeat(PAYLOAD))]))
                .collect()
        };
        let legacy: Vec<Vec<Row>> = (0..2)
            .map(|_| {
                let mut all = Vec::new();
                for start in (0..ROWS).step_by(CHUNK) {
                    all.append(&mut chunk(start));
                }
                all
            })
            .collect();
        let legacy_bytes = legacy.heap_bytes();
        drop(legacy);

        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let mut schemas = nested_schemas();
        for name in ["mid_t", "deep_t"] {
            let def = serde_json::from_value(serde_json::json!({
                "columns": {"gid":{"type":"int"},"id":{"type":"int"},"payload":{"type":"text"}}, "primaryKey":"id"
            }))
            .unwrap();
            schemas.insert(name.into(), TableSchema::from_def(&name.into(), &def).unwrap());
        }
        reg.set_schemas(Arc::new(schemas));
        let predicate = serde_json::from_value(serde_json::json!({
            "col":"gid", "in":{"table":"mid_t","project":"gid", "where":{"col":"gid","in":{"table":"deep_t","project":"gid"}}}
        })).unwrap();
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &predicate, None, true).unwrap();
        let mut peak = 0;
        for (sig, _, _) in &begin.seeds {
            for start in (0..ROWS).step_by(CHUNK) {
                let rows = chunk(start);
                peak = peak.max(rows.heap_bytes());
                reg.seed_chunk("s1", sig, rows).await.unwrap();
                assert!(reg.nodes[sig].seed_buffer.is_some(), "chunks do not publish nodes");
                assert!(reg.shapes.is_empty());
            }
        }
        assert!(legacy_bytes >= 2 * ROWS * PAYLOAD);
        assert!(peak <= CHUNK * (PAYLOAD + 256));
        assert!(legacy_bytes > peak * 200);
        let (deep, _, _) = &begin.seeds[0];
        let (mid, _, _) = &begin.seeds[1];
        let ts = reg.schemas[&"deep_t".into()].clone();
        // A live deletion during the seed window buffers raw old images; it must not remove a
        // partially installed member before all chunks have landed.
        let old = Row(vec![Value::Int(7), Value::Int(7), Value::Text("x".repeat(PAYLOAD))]);
        assert!(reg.on_table_delta(&ts, &[Tup2(old, -1)], 0x200, Some(50), None, None).await.unwrap().is_empty());
        assert_eq!(reg.nodes[deep].seed_buffer.as_ref().unwrap().len(), 1);
        assert!(reg.circuit.contains(reg.nodes[deep].node_id, &Value::Int(7)));
        assert!(reg.queue_node_deferred(mid, DeferredNodeWork::Value { connecting_col: 0, value: Value::Int(7) }));
        let seeds = begin
            .seeds
            .iter()
            .map(|(sig, _, _)| (sig.clone(), crate::pg::SnapshotGate::parse("40:45:", "0/100")))
            .collect();
        let finished = reg
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, Default::default())
            .await
            .unwrap();
        assert!(!reg.circuit.contains(reg.nodes[deep].node_id, &Value::Int(7)));
        assert_eq!(finished.work.len(), 1, "buffered raw deletion replays after the gate installs");
        assert_eq!(finished.node_work.len(), 1, "child work waits for the complete parent seed");
        eprintln!(
            "0022 row-staging: legacy_nested_bytes={legacy_bytes} chunk_peak_bytes={peak} contributor_rows={}",
            2 * ROWS
        );
        reg.abort_create("s1").await;
        assert!(reg.nodes.is_empty());
    }

    /// The deferred replay is resumable for the same reason the live walk is: a failed item goes
    /// back at the front, so the retry redoes exactly what did not land and nothing else.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_deferred_item_is_restored_for_the_retry() {
        let ts = issues_ts();
        // No `pg_url`: the first query-back fails, as an outage makes it fail.
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        insert_membership_shape(&mut reg, "s1", &sig, 1);
        reg.set_schemas(Arc::new([(TableRef::parse("issues").unwrap(), ts)].into_iter().collect()));
        let registry = tokio::sync::Mutex::new(reg);

        let mut work: VecDeque<DeferredShapeWork> = [
            DeferredShapeWork::Value { connecting_col: 1, value: Value::Int(100), txid: None },
            DeferredShapeWork::Full { txid: None },
        ]
        .into_iter()
        .collect();
        let res = propagate_deferred_shape_work(&registry, "s1", &mut work).await;

        assert!(res.is_err(), "the query-back cannot succeed without Postgres");
        assert_eq!(work.len(), 2, "a failed replay must not consume the work it did not finish");
        assert!(
            matches!(work[0], DeferredShapeWork::Value { value: Value::Int(100), .. }),
            "the failed item goes back at the front"
        );
    }

    /// Gating: a flip that changes no dependent membership emits nothing. Here a NULL flip reaches a
    /// plain (non-negated) `IN` dependent, which NULL can't move — `propagate_flips` skips it, so no
    /// path lights and the visualizer fades nothing spuriously.
    #[tokio::test]
    async fn flip_no_op_emits_no_trace() {
        let mut reg = registry_with_node("sig1");
        reg.add_edge(Edge {
            node_sig: "sig1".into(),
            dependent: Dependent::Shape("s7".into()),
            connecting_col: 0,
            negated: false,
            null_sensitive: false,
        });
        let reg = tokio::sync::Mutex::new(reg);
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);

        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        work.push_back(("sig1".into(), Flip { value: Value::Null, dir: FlipDir::Enter }));
        propagate_flips(&reg, &mut work, None, None, &trace_tx).await.unwrap();
        assert!(trace_rx.try_recv().is_err(), "a NULL flip on a non-null-sensitive dependent emits nothing");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_enter_and_leave_on_first_and_last_contributor() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        let evals = |pk: &str, v: Option<Value>| vec![(pk.to_string(), v)];
        assert_eq!(
            reg.apply_node_evals(&sig, evals("a", Some(Value::Int(5)))).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Enter }]
        );
        assert!(reg.contains(&sig, &Value::Int(5)));
        // second contributor to the same value -> no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("b", Some(Value::Int(5)))).await, vec![]);
        // removing one of two -> still present, no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("a", None)).await, vec![]);
        assert!(reg.contains(&sig, &Value::Int(5)));
        // removing the last -> Leave
        assert_eq!(
            reg.apply_node_evals(&sig, evals("b", None)).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Leave }]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_value_change_emits_leave_then_enter() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        let mut flips = reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(7)))]).await;
        flips.sort_by(|a, b| a.value.cmp(&b.value));
        assert_eq!(
            flips,
            vec![
                Flip { value: Value::Int(5), dir: FlipDir::Leave },
                Flip { value: Value::Int(7), dir: FlipDir::Enter },
            ]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
        assert!(reg.contains(&sig, &Value::Int(7)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_same_value_is_a_noop() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        assert_eq!(reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await, vec![]);
        // unchanged absence is also a no-op
        assert_eq!(reg.apply_node_evals(&sig, vec![("z".into(), None)]).await, vec![]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_bucket_tracks_has_null() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Null))]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Enter }]
        );
        assert!(reg.has_null(&sig));
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), None)]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Leave }]
        );
        assert!(!reg.has_null(&sig));
    }

    /// `NOT (x IN S)` is exactly as NULL-sensitive as `x NOT IN S`: a NULL entering `S` turns the leaf
    /// UNKNOWN, and the enclosing NOT converts that into a membership change. The edge must record it,
    /// or a NULL flip silently skips the re-derivation and members go stale.
    #[test]
    fn null_sensitivity_tracks_not_wrappers_and_negated_leaves() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&"outer_t".into(), &def).unwrap()
        };
        struct Rec;
        impl crate::predicate::SubqueryCollector for Rec {
            fn collect(&mut self, t: &TableRef, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                Ok(crate::predicate::subquery_sig(t, p, w))
            }
        }
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile_with(&serde_json::from_value(j).unwrap(), &ts, &mut Rec).unwrap()
        };
        let in_sub = serde_json::json!({"col":"gid","in":{"table":"outer_t","project":"gid"}});

        // plain IN: not NULL-sensitive (FALSE↔UNKNOWN can't change inclusion without negation)
        let leaves = collect_in_leaves(&compile(in_sub.clone()));
        assert!(!leaves[0].negated && !leaves[0].null_sensitive);

        // NOT IN leaf: NULL-sensitive
        let mut neg = in_sub.clone();
        neg["negated"] = serde_json::json!(true);
        let leaves = collect_in_leaves(&compile(neg));
        assert!(leaves[0].negated && leaves[0].null_sensitive);

        // IN under a Not wrapper: NULL-sensitive even though the leaf isn't negated
        let leaves = collect_in_leaves(&compile(serde_json::json!({"not": in_sub.clone()})));
        assert!(!leaves[0].negated && leaves[0].null_sensitive);

        // IN nested under Not(And(...)): still NULL-sensitive
        let leaves = collect_in_leaves(&compile(serde_json::json!({
            "not": {"and": [ {"col":"id","op":"gt","value":0}, in_sub ]}
        })));
        assert!(leaves[0].null_sensitive);
    }

    /// A failed create (here: no Postgres to seed from) must roll the registry back to exactly its
    /// prior state — no orphaned node, edge, or pending-seed entry that a later identical create
    /// would silently join and read unseeded (wrong) membership from.
    #[tokio::test]
    async fn failed_create_rolls_back_nodes_and_edges() {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
        };
        let mut schemas = HashMap::new();
        schemas.insert(TableRef::parse("outer_t").unwrap(), mk("outer_t"));
        schemas.insert(TableRef::parse("inner_t").unwrap(), mk("inner_t"));
        // No pg_url: node seeding must fail after collect() has already registered the node.
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        reg.set_schemas(Arc::new(schemas));
        let where_json: PredicateJson = serde_json::from_value(serde_json::json!({
            "col":"gid","in":{"table":"inner_t","project":"gid"}
        }))
        .unwrap();
        // Three-phase: begin registers (nodes buffering, edges, pending shape); a phase-B
        // failure is rolled back exactly by abort_create.
        let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false).unwrap();
        assert_eq!(begin.seeds.len(), 1, "one fresh node to seed");
        assert_eq!(reg.nodes.len(), 1);
        assert!(reg.nodes.values().all(|n| n.seed_buffer.is_some()), "fresh node buffers");
        assert!(reg.touches(&"outer_t".into()), "pending shape routes its outer table");
        reg.abort_create("s1").await;
        assert_eq!(reg.nodes.len(), 0, "aborted create left an orphaned node");
        assert_eq!(reg.edges_count(), 0, "aborted create left orphaned edges");
        assert_eq!(reg.pending_seed.len(), 0, "aborted create left a pending seed");
        assert!(reg.shapes.is_empty());
        assert!(reg.pending_shapes.is_empty());
    }

    /// The per-feed key set is the fix for the live-poll wake-storm bug, now structural:
    /// `emit_for_shapes` computes an *absolute* membership verdict for every touched pk, but a
    /// delete envelope is built ONLY where the host-side FeedSet actually retracts — a "not a
    /// member" verdict for a pk the stream never contained gates to nothing (`remove` returns
    /// false), so the spurious delete that used to wake every idle long-poll cannot be emitted at
    /// all (there is no filter left to get out of sync). The end-to-end drop is asserted via
    /// `emit_for_shapes`; the gate proper (a known member's delete opens, a repeat nets nothing)
    /// is asserted by poking the FeedSet directly — its verdicts are the delete gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_relation_drops_deletes_for_never_known_pks() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap();
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        // A shape whose predicate never matches (Not(MatchAll)): every candidate verdict is
        // "not a member".
        insert_outer_shape(&mut reg, "s1", "t", CompiledPredicate::Not(Box::new(CompiledPredicate::MatchAll)));
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);

        // A "leave" for a pk this feed never contained: nothing is emitted (no lanes are
        // configured, so an emission would attempt a real append and fail loudly; emitted
        // stays 0 and the result reports nothing delivered).
        let results = reg
            .emit_for_shapes(
                &ts,
                vec![("s1".to_string(), vec![(row(1), true)])],
                None,
                EmissionSource::Live { lsn: 1, xid: None },
            )
            .await
            .unwrap();
        assert_eq!(results, vec![("s1".to_string(), false, 0)], "never-member delete must be dropped");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Seed pk 1 as a member (backfill hand-off), then the same verdict is a GENUINE leave.
        // The pk id must match the one `emit_for_shapes` would probe for row(1)'s pk string, so
        // build the key through the SAME dictionary.
        let pk_id = reg.pk_dict.get_or_insert("1");
        let feed_id = reg.shapes["s1"].feed_id;
        reg.feed_sets.insert(feed_id, pk_id);
        assert!(
            reg.feed_sets.remove(feed_id, pk_id),
            "a known member's delete must gate OPEN (remove returns true → retraction)"
        );

        // And once retracted, a repeat delete gates closed again.
        assert!(
            !reg.feed_sets.remove(feed_id, pk_id),
            "repeat delete for an already-removed pk must gate closed (remove returns false)"
        );
    }

    /// Focused regression: `emit_for_shapes` must mint a `pk_dict` id ONLY for a genuine member
    /// (mirrors the existing probe-without-minting rationale for `template_assertions`'s
    /// `pk_dict.get` lookups). A delete or non-matching candidate for a pk never seen before must
    /// leave the dictionary's `len()` unchanged — no forward/reverse slot for a pk that can never
    /// be present in the feed relation — while a genuinely matching candidate DOES mint.
    #[tokio::test(flavor = "multi_thread")]
    async fn never_member_candidate_does_not_mint_pk_dict_id() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def(&"t".into(), &def).unwrap();
        // A real (fake) DS server: the final phase genuinely emits an upsert, which would retry
        // forever against an unreachable URL.
        let (ds_url, _store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        insert_outer_shape(&mut reg, "s1", "t", CompiledPredicate::MatchAll);
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);
        assert_eq!(reg.pk_dict.len(), 0);

        // A delete for a brand-new pk (42), never interned before: `exists = false` forces
        // `member = false` regardless of the (always-true) predicate. The fix must skip minting.
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(42), false)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 0, "a never-member delete candidate must not mint a pk_dict id");

        // A non-matching (but existing) candidate for another brand-new pk (43) must likewise
        // skip minting: swap in a never-matching predicate for this check. (Poking `pred` in
        // place would desync the conjunct index — safe ONLY because this test calls
        // `emit_for_shapes` directly and never probes the index. Production always re-files
        // through `install_shape`.)
        reg.shapes.get_mut("s1").unwrap().pred =
            Arc::new(CompiledPredicate::Not(Box::new(CompiledPredicate::MatchAll)));
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(43), true)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 0, "a non-matching candidate must not mint a pk_dict id");

        // A genuinely matching insert (pk 44) DOES mint exactly one id.
        reg.shapes.get_mut("s1").unwrap().pred = Arc::new(CompiledPredicate::MatchAll);
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(row(44), true)])],
            None,
            EmissionSource::Live { lsn: 1, xid: None },
        )
        .await
        .unwrap();
        assert_eq!(reg.pk_dict.len(), 1, "a matching candidate must mint exactly one pk_dict id");
    }

    // --- Task 0.3 harness: a minimal fake durable-streams server ------------------------------
    //
    // `emit_for_shapes`'s `deliver()` falls back to `DsClient::append_reliable` when no lanes are
    // configured (unit tests): a genuinely non-empty emission would otherwise retry forever
    // against `http://unused`. `tests/live_poll_deadline.rs` and `tests/params_shape.rs` solve
    // this the same way — spawn a real (local) axum server and point a real `DsClient` at it —
    // reused verbatim here so appends actually land and can be inspected, instead of asserting
    // only the `emitted` counter.

    #[derive(Clone, Default)]
    struct DsStore(Arc<std::sync::Mutex<HashMap<String, Vec<Envelope>>>>);

    async fn fake_ds_handler(
        axum::extract::State(store): axum::extract::State<DsStore>,
        req: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::http::{Method, StatusCode};
        use axum::response::IntoResponse;
        match *req.method() {
            Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
            Method::POST => {
                let path = req.uri().path().trim_start_matches('/').to_string();
                let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await.unwrap_or_default();
                if let Ok(envs) = serde_json::from_slice::<Vec<Envelope>>(&body) {
                    store.0.lock().unwrap().entry(path).or_default().extend(envs);
                }
                StatusCode::OK.into_response()
            }
            Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    /// Boots the fake server on an ephemeral port; returns its base URL and the shared store of
    /// every envelope POSTed to it, keyed by stream path (e.g. `"shape/s1"`).
    async fn spawn_fake_ds() -> (String, DsStore) {
        let store = DsStore::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().fallback(fake_ds_handler).with_state(store.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), store)
    }

    /// The operations (`"upsert"`/`"delete"`) delivered to one stream path so far, in append order.
    fn ops_for(store: &DsStore, path: &str) -> Vec<String> {
        store
            .0
            .lock()
            .unwrap()
            .get(path)
            .map(|v| v.iter().map(|e| e.headers.operation.clone()).collect())
            .unwrap_or_default()
    }

    /// `issues(id, project_id)`, matching the brief's example shape:
    /// `issues WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = ...)`.
    fn issues_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "project_id": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&"issues".into(), &def).unwrap()
    }

    fn issue(id: i64, project_id: i64) -> Row {
        Row(vec![Value::Int(id), Value::Int(project_id)])
    }

    /// Registers a shape whose predicate is `project_id IN (node sig)` — the outer half of the
    /// brief's example, wired to a membership node inserted with [`insert_test_node`].
    fn insert_membership_shape(reg: &mut SubqueryRegistry, shape_id: &str, sig: &SubquerySig, project_col: usize) {
        insert_outer_shape(
            reg,
            shape_id,
            "issues",
            CompiledPredicate::InSubquery { col: project_col, sig: sig.clone(), negated: false },
        );
    }

    /// Register an outer shape with an arbitrary predicate through the SAME installation path
    /// production uses (`install_shape`), so the necessary-conjunct index is populated exactly as
    /// `finish_create` would populate it.
    fn insert_outer_shape(reg: &mut SubqueryRegistry, shape_id: &str, outer_table: &str, pred: CompiledPredicate) {
        let feed_id = reg.next_feed_id;
        reg.next_feed_id += 1;
        reg.install_shape(SubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: outer_table.into(),
            stream_path: format!("shape/{shape_id}"),
            pred: Arc::new(pred),
            out_cols: None,
            gate: crate::pg::SnapshotGate::passthrough(),
            emitted: std::sync::atomic::AtomicU64::new(0),
            feed_id,
            recent: HashMap::new(),
            inflight_querybacks: 0,
        });
    }

    /// A realistic LinearLite-style outer predicate: `project_id = k AND project_id IN (SELECT …)`.
    /// `access_leaf` lifts the equality as the necessary conjunct; the `IN` leaf is not row-local
    /// and can never be one.
    fn keyed_membership_pred(project_id: i64, sig: &SubquerySig) -> CompiledPredicate {
        CompiledPredicate::And(vec![
            CompiledPredicate::Cmp { col: 1, op: crate::predicate::LeafOp::Eq, value: Value::Int(project_id) },
            CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: false },
        ])
    }

    /// bead dbsp-ds-g5p, regression 1/2 — **THE correctness trap of the conjunct index.**
    ///
    /// Outer membership is emitted ABSOLUTELY (§3.3): every touched pk gets its *current*
    /// verdict, and a move-out is a delete the feed gate opens. So a candidate probe that only
    /// looked at the row's NEW image would skip the shape the row just LEFT — and the delete
    /// would never be built. The probe must union the old (`-1`) and new (`+1`) images of the
    /// delta; this test moves a row out of the indexed conjunct's value and demands the delete.
    #[tokio::test(flavor = "multi_thread")]
    async fn move_out_delete_survives_the_conjunct_index() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // User 1 is a member of BOTH projects, so the only thing that moves the row is the
        // indexed equality conjunct — isolating exactly what the index decides.
        reg.apply_node_evals(
            &sig,
            vec![("pm-1".into(), Some(Value::Int(100))), ("pm-2".into(), Some(Value::Int(200)))],
        )
        .await;
        insert_outer_shape(&mut reg, "s100", "issues", keyed_membership_pred(100, &sig));
        insert_outer_shape(&mut reg, "s200", "issues", keyed_membership_pred(200, &sig));

        // Enter s100: issue 1 lands in project 100.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, None, None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s100"), vec!["upsert"]);

        // The move: UPDATE project_id 100 -> 200, as `apply_envelope` builds it (old `-1`, new
        // `+1`). The NEW image fails s100's indexed conjunct entirely — s100 is only reachable
        // through the old image.
        let delta = vec![Tup2(issue(1, 100), -1), Tup2(issue(1, 200), 1)];
        assert!(
            reg.outer_candidates(&"issues".into(), &delta).contains(&"s100".to_string()),
            "the shape the row LEFT must be a candidate — via the delta's old image"
        );
        reg.on_table_delta(&ts, &delta, 2, None, None, None).await.unwrap();
        assert_eq!(
            ops_for(&store, "shape/s100"),
            vec!["upsert", "delete"],
            "the move-out delete must still be emitted after the index skips non-candidates"
        );
        assert_eq!(ops_for(&store, "shape/s200"), vec!["upsert"], "and the move-in upsert lands");
    }

    /// bead dbsp-ds-g5p, regression 2/2 — per-change cost is `O(candidates)`, not
    /// `O(#subquery shapes)`. `on_table_delta` step 2 visits exactly `outer_candidates`, so
    /// asserting on that set IS asserting shapes-visited. Also pins the two other bucketing
    /// properties: shapes on another outer table are never even considered, and a shape with no
    /// indexable conjunct (a bare `IN`, or a `NOT IN` under a negation) stays an unconditional
    /// candidate — skipping those would be unsound.
    #[tokio::test(flavor = "multi_thread")]
    async fn outer_candidates_do_not_scale_with_shape_count() {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);

        const N: i64 = 500;
        for k in 0..N {
            insert_outer_shape(&mut reg, &format!("s{k}"), "issues", keyed_membership_pred(k, &sig));
        }
        // A shape on a DIFFERENT outer table: the `shapes.iter().filter(outer_table == table)`
        // global scan this replaces would have walked past all of these.
        insert_outer_shape(&mut reg, "other", "comments", keyed_membership_pred(7, &sig));
        assert_eq!(reg.shapes.len(), N as usize + 1);

        let cands = reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 7), 1)]);
        assert_eq!(cands, vec!["s7".to_string()], "1 of {N} shapes visited, not {N}");

        // Un-indexable predicates must stay unconditional candidates.
        insert_outer_shape(
            &mut reg,
            "bare_in",
            "issues",
            CompiledPredicate::InSubquery { col: 1, sig: sig.clone(), negated: false },
        );
        insert_outer_shape(
            &mut reg,
            "not_in",
            "issues",
            CompiledPredicate::Not(Box::new(keyed_membership_pred(7, &sig))),
        );
        let mut cands = reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 4242), 1)]);
        cands.sort();
        assert_eq!(
            cands,
            vec!["bare_in".to_string(), "not_in".to_string()],
            "a predicate with no necessary conjunct can never be skipped"
        );

        // Dropping a shape un-files it: no stale candidate, and the freed id is safe to re-mint.
        reg.drop_subquery_shape("s7").await;
        assert!(
            reg.outer_candidates(&"issues".into(), &[Tup2(issue(1, 7), 1)]).iter().all(|s| s != "s7"),
            "a dropped shape must leave no index entry behind"
        );
    }

    /// G2 loop test 1/2 — the delete-gate's OPEN-failure half: a delete for a pk that was
    /// **never** a member of the feed must be dropped (zero appends, zero wake), never a
    /// spurious delete. `project_members` (user 1) only ever contains project 100; an issue in
    /// project 999 is written then deleted without ever entering the `issues` shape's feed.
    #[tokio::test(flavor = "multi_thread")]
    async fn never_member_delete_is_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // The only project user 1 is a member of.
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s1", &sig, 1);

        // Write an issue in a NON-matching project (999): never a member, so nothing is emitted.
        let work = reg.on_table_delta(&ts, &[Tup2(issue(1, 999), 1)], 1, None, None, None).await.unwrap();
        assert!(work.is_empty(), "an outer-table delta never queues node-flip propagation");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Delete it: still never a member -> the delete-gate must drop it (no spurious wake).
        reg.on_table_delta(&ts, &[Tup2(issue(1, 999), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(
            reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "never-member delete must be dropped"
        );
        assert!(
            ops_for(&store, "shape/s1").is_empty(),
            "the shape's stream must receive ZERO appends for a never-member pk"
        );
    }

    /// G2 loop test 2/2 — the delete-gate's CLOSED-failure half: a delete for a pk that WAS a
    /// genuine member must never be dropped, on either exit path — (a) the row itself is
    /// deleted, (b) the row survives but the membership node it depended on flips the pk out
    /// (`project_members`'s row for user 1 is removed: "the user loses the project") — and a pk
    /// that re-enters after leaving must re-emit (the feed relation, not a one-shot latch).
    #[tokio::test(flavor = "multi_thread")]
    async fn genuine_member_delete_is_never_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s2", &sig, 1);
        fn emitted(reg: &SubqueryRegistry) -> u64 {
            reg.shapes["s2"].emitted.load(std::sync::atomic::Ordering::Relaxed)
        }

        // Enter: an issue in the matching project.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 1);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert"]);

        // Exit (a): the row itself is deleted -> exactly one delete emission.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 2, "exactly one more envelope: the row-delete emission");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete"]);

        // A pk that re-enters after leaving must re-emit — the feed relation gates on current
        // membership, it is not a one-shot "already told you" latch.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 3, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 3, "a re-entering pk must re-emit");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert"]);

        // Exit (b): the row is untouched, but the user loses the project — the membership node's
        // only contributor is withdrawn, flipping project 100 out of the inner set. In
        // production `propagate_flips`/`move_shape_for_value` discovers this and queries
        // Postgres back for the outer rows with `project_id = 100` to re-evaluate; this harness
        // has no Postgres, so the flip is driven directly and the still-present candidate row is
        // handed to the same `emit_for_shapes` re-derivation tail that query-back would call —
        // exactly the "membership flip" exit path, distinct from a row delete.
        let flips = reg.apply_node_evals(&sig, vec![("pm-1".into(), None)]).await;
        assert_eq!(flips, vec![Flip { value: Value::Int(100), dir: FlipDir::Leave }]);
        let results = reg
            .emit_for_shapes(
                &ts,
                vec![("s2".to_string(), vec![(issue(1, 100), true)])],
                None,
                EmissionSource::Live { lsn: 1, xid: None },
            )
            .await
            .unwrap();
        assert_eq!(
            results,
            vec![("s2".to_string(), true, -1)],
            "exactly one delete emission for the membership-flip exit path"
        );
        assert_eq!(emitted(&reg), 4);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert", "delete"]);
    }

    // --- query-back vs. live ordering (per-pk recency) -----------------------------------------
    //
    // A membership query-back reads its candidate rows from Postgres with the registry lock
    // RELEASED, so a direct outer-row change committed after that read can be evaluated and
    // emitted in between. Without a fence the query-back's older row is evaluated last and
    // becomes the stream's last word — and native consumers fold by durable-stream offset, so
    // "last" is final. These three tests pin the fence: live decisions stamp the pks they touch
    // while a query-back is in flight, and a query-back drops the pks whose stamp its own
    // snapshot could not have seen.

    /// A shape over `project_id = 100 AND project_id IN <node>`, its node containing 100, with a
    /// fake durable-streams server so appends actually land and can be inspected.
    async fn recency_fixture() -> (TableSchema, DsStore, SubqueryRegistry) {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_outer_shape(&mut reg, "s1", "issues", keyed_membership_pred(100, &sig));
        (ts, store, reg)
    }

    /// (a) With a query-back in flight, a live decision for pk 1 is stamped, and a query-back
    /// holding an OLDER read of that pk drops it — while one whose snapshot already saw the live
    /// commit applies its row as usual.
    ///
    /// The live decision here emits nothing at all (the row moved out of the shape and the feed
    /// never contained it), which is exactly the case an emission-driven fence would miss: the
    /// stale query-back's upsert would be the stream's only word about pk 1, and Postgres says
    /// the row is not in the shape.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_query_back_cannot_overwrite_a_newer_live_decision() {
        let (ts, store, mut reg) = recency_fixture().await;

        // A query-back for the flipped value is between its Postgres read and its evaluation.
        reg.begin_queryback("s1");

        // Meanwhile the row moves out of project 100, in a transaction (xid 50) the read below
        // cannot see. Absolute verdict: not a member — and nothing is emitted, because the feed
        // never contained pk 1.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1), Tup2(issue(1, 999), 1)], 0x200, Some(50), None, None)
            .await
            .unwrap();
        assert!(ops_for(&store, "shape/s1").is_empty(), "a never-member verdict emits nothing");
        let pk_id = reg.pk_dict.get("1").expect("the live decision must record recency for pk 1");
        assert_eq!(
            reg.shapes["s1"].recent.get(&pk_id),
            Some(&(0x200u64, Some(50u64))),
            "the live decision's commit stamp must be recorded while a query-back is in flight"
        );

        // The in-flight read: its snapshot (xmin 40, xmax 45) predates xid 50, so its row for
        // pk 1 — still in project 100 — is stale. It must be dropped, not emitted.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &stale },
        )
        .await
        .unwrap();
        assert!(
            ops_for(&store, "shape/s1").is_empty(),
            "a query-back row older than the live decision for that pk must be dropped"
        );
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // A query-back whose snapshot (xmin 60) DID see xid 50 is not stale: whatever it read is
        // at least as new as the live decision (here: the row moved back into project 100), so
        // it applies normally.
        let fresh = crate::pg::SnapshotGate::parse("60:70:", "0/300");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &fresh },
        )
        .await
        .unwrap();
        assert_eq!(
            ops_for(&store, "shape/s1"),
            vec!["upsert"],
            "a query-back that already saw the live commit must still apply its row"
        );
    }

    /// (b) The map is bounded by the in-flight window: nothing is recorded with no query-back
    /// outstanding, and the last one to finish clears what was recorded.
    #[tokio::test(flavor = "multi_thread")]
    async fn recency_is_recorded_only_while_a_query_back_is_in_flight() {
        let (ts, store, mut reg) = recency_fixture().await;

        // No query-back in flight: an ordinary live delta records nothing.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, Some(10), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert"]);
        assert!(reg.shapes["s1"].recent.is_empty(), "with nothing to protect against, no per-pk state is kept");

        // Two query-backs in flight; a live delta now records for the pks it evaluates.
        reg.begin_queryback("s1");
        reg.begin_queryback("s1");
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 2, Some(11), None, None).await.unwrap();
        assert_eq!(reg.shapes["s1"].recent.len(), 1);

        // The first to finish does NOT clear: the other read is still outstanding.
        reg.end_queryback("s1");
        assert_eq!(reg.shapes["s1"].recent.len(), 1, "one query-back is still in flight");

        // The last one does.
        reg.end_queryback("s1");
        assert!(reg.shapes["s1"].recent.is_empty(), "the last query-back out clears the map");
        assert_eq!(reg.shapes["s1"].inflight_querybacks, 0);

        // An unbalanced decrement (a shape whose query-back failed twice, or a dropped shape)
        // must not wrap the counter into a permanently "in flight" state.
        reg.end_queryback("s1");
        assert_eq!(reg.shapes["s1"].inflight_querybacks, 0);
        reg.end_queryback("gone");
    }

    /// (c) The same fence protects a live LEAVE: once the delete has been emitted, a stale
    /// query-back must not resurrect the row with an upsert — the stream's last word for that pk
    /// has to stay the newer verdict.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_leave_is_not_undone_by_a_stale_query_back() {
        let (ts, store, mut reg) = recency_fixture().await;
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, Some(10), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert"]);

        reg.begin_queryback("s1");
        // The row is deleted in a newer transaction (xid 50): the feed retracts, a delete lands.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 0x200, Some(50), None, None).await.unwrap();
        assert_eq!(ops_for(&store, "shape/s1"), vec!["upsert", "delete"]);

        // The in-flight read still has the row. Applying it would make "upsert" the last word.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        reg.emit_for_shapes(
            &ts,
            vec![("s1".to_string(), vec![(issue(1, 100), true)])],
            None,
            EmissionSource::QueryBack { gate: &stale },
        )
        .await
        .unwrap();
        reg.end_queryback("s1");
        assert_eq!(
            ops_for(&store, "shape/s1"),
            vec!["upsert", "delete"],
            "the stale query-back must not resurrect a row the live path just retracted"
        );
    }

    // --- node re-derivation vs. live ordering (per-pk recency, one tier down) ------------------
    //
    // The same race as above, on a membership NODE: `requery_and_reconcile_parent` reads the
    // node's inner rows from Postgres with the registry lock released, so a direct inner-table
    // change committed after that read is reconciled by `on_table_delta` step 1 in between, and
    // the re-derivation would re-assert the old contribution last. The node appends nothing
    // itself, but its dependent shape appends what the node's flips say, so the divergence is the
    // same permanent one.

    /// `t(gid, id)` — the projected value and the pk of a membership node's inner table (columns
    /// are positioned in sorted order, so `gid` is 0 and `id` is 1, matching `insert_test_node`).
    fn inner_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "gid": {"type":"int"}, "id": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&"t".into(), &def).unwrap()
    }

    fn inner_row(gid: i64, id: i64) -> Row {
        Row(vec![Value::Int(gid), Value::Int(id)])
    }

    /// (a) With a re-derivation of the node in flight, the live contribution decisions it races
    /// are stamped per pk, and a re-derivation holding an OLDER read of those pks drops them —
    /// while one whose snapshot already saw the live commit applies its rows as usual.
    ///
    /// pk 2 is the case the conformance schedule hits and an assertion-driven fence would miss:
    /// the node never held it, so the live verdict ("still not a contributor") asserts nothing at
    /// all — and the stale read's row for it would otherwise be admitted as a fresh contribution.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_node_requery_cannot_overwrite_a_newer_live_contribution() {
        let ts = inner_ts();
        let sig: SubquerySig = "t|gid|MatchAll".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("1".into(), Some(Value::Int(100)))]).await;
        assert!(reg.contains(&sig, &Value::Int(100)));

        // A re-derivation of this node is between its Postgres read and its reconcile.
        reg.begin_node_queryback(&sig);

        // Meanwhile both inner rows are deleted in a transaction (xid 50) that read cannot see.
        // Absolute verdict: neither pk contributes — and for pk 2, which the node never held,
        // that verdict asserts nothing.
        reg.on_table_delta(
            &ts,
            &[Tup2(inner_row(100, 1), -1), Tup2(inner_row(200, 2), -1)],
            0x200,
            Some(50),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(!reg.contains(&sig, &Value::Int(100)), "the live delta retracted pk 1");
        assert_eq!(
            reg.nodes[&sig].recent.get("1"),
            Some(&(0x200u64, Some(50u64))),
            "the live decision's commit stamp must be recorded while a re-derivation is in flight"
        );
        assert_eq!(
            reg.nodes[&sig].recent.get("2"),
            Some(&(0x200u64, Some(50u64))),
            "including the pk whose verdict is 'no contribution', which asserts nothing"
        );

        // The in-flight read still has both rows; its snapshot (xmin 40, xmax 45) predates xid 50.
        let stale = crate::pg::SnapshotGate::parse("40:45:", "0/100");
        let evals = reg
            .node_queryback_evals(&sig, &ts, &[inner_row(100, 1), inner_row(200, 2)], &stale)
            .expect("the node is live");
        assert!(evals.is_empty(), "every candidate older than its live decision must be dropped");
        reg.apply_node_evals(&sig, evals).await;
        assert!(!reg.contains(&sig, &Value::Int(100)), "a stale re-derivation must not re-admit a value");
        assert!(!reg.contains(&sig, &Value::Int(200)), "nor admit one the live verdict refused");

        // A read whose snapshot (xmin 60) DID see xid 50 is not stale: whatever it read is at
        // least as new as the live decision (here: both rows are back), so it applies normally.
        let fresh = crate::pg::SnapshotGate::parse("60:70:", "0/300");
        let evals = reg
            .node_queryback_evals(&sig, &ts, &[inner_row(100, 1), inner_row(200, 2)], &fresh)
            .expect("the node is live");
        reg.apply_node_evals(&sig, evals).await;
        assert!(reg.contains(&sig, &Value::Int(100)));
        assert!(reg.contains(&sig, &Value::Int(200)));
        reg.end_node_queryback(&sig);
    }

    /// (b) The node's map is bounded by the in-flight window exactly like the shape's: nothing is
    /// recorded with no re-derivation outstanding, and the last one to finish clears what was.
    #[tokio::test(flavor = "multi_thread")]
    async fn node_recency_is_recorded_only_while_a_requery_is_in_flight() {
        let ts = inner_ts();
        let sig: SubquerySig = "t|gid|MatchAll".into();
        let mut reg = registry_with_node(&sig);

        // Nothing in flight: an ordinary live delta records nothing.
        reg.on_table_delta(&ts, &[Tup2(inner_row(100, 1), 1)], 1, Some(10), None, None).await.unwrap();
        assert!(reg.contains(&sig, &Value::Int(100)));
        assert!(reg.nodes[&sig].recent.is_empty(), "with nothing to protect against, no per-pk state is kept");

        // Two re-derivations in flight; a live delta now records for the pks it decides.
        reg.begin_node_queryback(&sig);
        reg.begin_node_queryback(&sig);
        reg.on_table_delta(&ts, &[Tup2(inner_row(100, 1), 1)], 2, Some(11), None, None).await.unwrap();
        assert_eq!(reg.nodes[&sig].recent.len(), 1);

        // The first to finish does NOT clear: the other read is still outstanding.
        reg.end_node_queryback(&sig);
        assert_eq!(reg.nodes[&sig].recent.len(), 1, "one re-derivation is still in flight");

        // The last one does.
        reg.end_node_queryback(&sig);
        assert!(reg.nodes[&sig].recent.is_empty(), "the last re-derivation out clears the map");
        assert_eq!(reg.nodes[&sig].inflight_querybacks, 0);

        // An unbalanced decrement (a failed read, or a node dropped mid-flight) must not wrap the
        // counter into a permanently "in flight" state.
        reg.end_node_queryback(&sig);
        assert_eq!(reg.nodes[&sig].inflight_querybacks, 0);
        reg.end_node_queryback(&"gone".to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_eval_reads_node_sets() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(1))), ("b".into(), Some(Value::Null))]).await;
        assert!(reg.contains(&sig, &Value::Int(1)));
        assert!(!reg.contains(&sig, &Value::Int(2)));
        assert!(reg.has_null(&sig));
        // unknown sig -> empty
        assert!(!reg.contains(&"other".to_string(), &Value::Int(1)));
    }
}
