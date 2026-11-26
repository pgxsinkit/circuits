//! Predicate AST (deserialized from the control-plane JSON, mirroring `@circuits/protocol`)
//! compiled to a positional evaluator applied directly to each Z-set delta.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::heap_size::HeapSize;
use crate::schema::TableSchema;
use crate::table_ref::TableRef;
use crate::value::{Row, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LeafOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    /// SQL `LIKE` (case-sensitive; `%` = any sequence, `_` = any single char). `NOT LIKE` is
    /// `Not(Like)`.
    Like,
}

/// Inner subquery reference: `(SELECT project FROM table WHERE where)`. `where` may itself contain
/// `In` leaves (nested subqueries). Single column only.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubqueryJson {
    /// The inner table. Canonicalised at ingress by [`TableRef`]'s `Deserialize`, so a bare
    /// `"table": "groups"` in a request body means `public.groups` and the signature/SQL below
    /// carry the qualified form.
    pub table: TableRef,
    pub project: String,
    #[serde(rename = "where", default)]
    pub where_: Option<Box<PredicateJson>>,
}

/// JSON predicate shape: a leaf `{col,op,value}`, a null test `{col, isNull}`, a combinator
/// `{and|or:[...]}` / `{not:{}}`, or a subquery leaf `{col, in:{table,project,where?}, negated?}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum PredicateJson {
    Leaf {
        col: String,
        op: LeafOp,
        value: serde_json::Value,
    },
    /// SQL null test: `col IS NULL` (`isNull: true`) / `col IS NOT NULL` (`isNull: false`). A separate
    /// form because it is the one predicate that is TRUE *on* a NULL cell — no comparison can express
    /// it under three-valued logic (every cmp over NULL is UNKNOWN, and NOT UNKNOWN stays UNKNOWN).
    IsNull {
        col: String,
        #[serde(rename = "isNull")]
        is_null: bool,
    },
    And {
        and: Vec<PredicateJson>,
    },
    Or {
        or: Vec<PredicateJson>,
    },
    Not {
        not: Box<PredicateJson>,
    },
    In {
        col: String,
        #[serde(rename = "in")]
        subquery: SubqueryJson,
        #[serde(default)]
        negated: bool,
    },
}

impl HeapSize for SubqueryJson {
    fn heap_bytes(&self) -> usize {
        self.table.heap_bytes() + self.project.heap_bytes() + self.where_.heap_bytes()
    }
}

impl HeapSize for PredicateJson {
    /// `op`/`is_null`/`negated` are inline (`Copy`); everything else recurses.
    fn heap_bytes(&self) -> usize {
        match self {
            PredicateJson::Leaf { col, op: _, value } => col.heap_bytes() + value.heap_bytes(),
            PredicateJson::IsNull { col, is_null: _ } => col.heap_bytes(),
            PredicateJson::And { and } => and.heap_bytes(),
            PredicateJson::Or { or } => or.heap_bytes(),
            PredicateJson::Not { not } => not.heap_bytes(),
            PredicateJson::In { col, subquery, negated: _ } => col.heap_bytes() + subquery.heap_bytes(),
        }
    }
}

/// Canonical signature for a subquery node: identical subqueries (same inner table, projected column,
/// and structurally-equal `where`) produce equal signatures, so they share one maintained node.
pub type SubquerySig = String;

/// Stable, order-insensitive canonicalization of a predicate (AND/OR children sorted by their own
/// canonical form). Used to build a [`SubquerySig`] so two equivalent subqueries dedupe to one node.
pub fn canonical_pred(p: &PredicateJson) -> String {
    match p {
        PredicateJson::Leaf { col, op, value } => {
            format!("L({col},{op:?},{})", serde_json::to_string(value).unwrap_or_default())
        }
        PredicateJson::IsNull { col, is_null } => format!("U({col},{is_null})"),
        PredicateJson::And { and } => {
            let mut cs: Vec<String> = and.iter().map(canonical_pred).collect();
            cs.sort();
            format!("A({})", cs.join(","))
        }
        PredicateJson::Or { or } => {
            let mut cs: Vec<String> = or.iter().map(canonical_pred).collect();
            cs.sort();
            format!("O({})", cs.join(","))
        }
        PredicateJson::Not { not } => format!("N({})", canonical_pred(not)),
        PredicateJson::In { col, subquery, negated } => {
            format!(
                "I({col},{negated},{})",
                subquery_sig(&subquery.table, &subquery.project, subquery.where_.as_deref())
            )
        }
    }
}

/// The canonical signature of a subquery node.
pub fn subquery_sig(table: &TableRef, project: &str, where_: Option<&PredicateJson>) -> SubquerySig {
    let w = where_.map(canonical_pred).unwrap_or_default();
    format!("{table}|{project}|{w}")
}

/// Template identity for a subquery: the literal values of the inner WHERE's **top-level
/// non-NULL equality conjuncts over distinct columns** are lifted out as parameters (the
/// "bind"), and everything else — ranges, OR, NOT, IS NULL, nested IN, duplicate columns —
/// stays baked into the canonical residual. Two subqueries that differ only in those lifted
/// literals share one template (`user_id = 1` / `user_id = 2` → one template, binds `(1)` /
/// `(2)`), which is what lets the registry evaluate a delta once per template (one residual
/// eval + one bind hash-lookup) instead of once per literal-keyed node — the same
/// factoring `equality_template` applies to routed shapes.
///
/// Returns `(template_key, bind, residual)`; the bind is sorted by column name (stable), and
/// is empty when nothing lifts (the template then has a single unit bind). The residual is
/// the un-lifted conjuncts, cloned, for the caller to compile as the template's shared
/// filter (empty ⇒ match-all).
pub fn subquery_template(
    table: &TableRef,
    project: &str,
    where_: Option<&PredicateJson>,
) -> (String, Vec<(String, serde_json::Value)>, Vec<PredicateJson>) {
    // Flatten top-level AND chains (a conjunct of a conjunct is still top-level).
    fn conjuncts<'a>(p: &'a PredicateJson, out: &mut Vec<&'a PredicateJson>) {
        match p {
            PredicateJson::And { and } => and.iter().for_each(|c| conjuncts(c, out)),
            other => out.push(other),
        }
    }
    let mut lifted: Vec<(String, serde_json::Value)> = Vec::new();
    let mut residual: Vec<PredicateJson> = Vec::new();
    if let Some(w) = where_ {
        let mut flat = Vec::new();
        conjuncts(w, &mut flat);
        // Deterministic lifting regardless of author order: visit conjuncts in canonical
        // order, lift the first non-NULL equality per column, residual-ize the rest (a
        // duplicate-column equality — `a=1 AND a=2`, a degenerate contradiction — stays a
        // residual conjunct rather than rejecting the template).
        flat.sort_by_key(|p| canonical_pred(p));
        for p in flat {
            match p {
                PredicateJson::Leaf { col, op: LeafOp::Eq, value }
                    if !value.is_null() && !lifted.iter().any(|(c, _)| c == col) =>
                {
                    lifted.push((col.clone(), value.clone()));
                }
                other => residual.push(other.clone()),
            }
        }
    }
    lifted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut res_canon: Vec<String> = residual.iter().map(canonical_pred).collect();
    res_canon.sort();
    let cols = lifted.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>().join(",");
    let key = format!("{table}|{project}|P({cols})|A({})", res_canon.join(","));
    (key, lifted, residual)
}

/// Collects (and dedupes) subquery nodes encountered while compiling a predicate. The engine's
/// registry implements this to create/seed nodes; `NoSubqueries` errors (used by paths that don't
/// support subqueries, e.g. subset queries).
pub trait SubqueryCollector {
    /// Register or dedupe a subquery node and return its canonical signature.
    fn collect(&mut self, table: &TableRef, project: &str, where_: Option<&PredicateJson>) -> Result<SubquerySig>;
}

/// A collector that rejects subqueries — for predicate paths where they are not supported.
pub struct NoSubqueries;
impl SubqueryCollector for NoSubqueries {
    fn collect(&mut self, _t: &TableRef, _p: &str, _w: Option<&PredicateJson>) -> Result<SubquerySig> {
        anyhow::bail!("subqueries are not supported here")
    }
}

/// Resolves subquery-node membership during evaluation (`matches_ctx`). The engine's registry
/// implements this over its maintained node sets.
pub trait SubqueryEval {
    /// Is `value` a member of the node's current set?
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool;
    /// Does the node's set currently contain a NULL value? (Makes `x NOT IN set` UNKNOWN.)
    fn has_null(&self, sig: &SubquerySig) -> bool;
}

/// A [`SubqueryEval`] that panics — guards `matches()` (the non-subquery path) against ever reaching
/// an `InSubquery` arm. Subquery shapes must use `matches_ctx` with the real registry.
struct PanicEval;
impl SubqueryEval for PanicEval {
    fn contains(&self, _: &SubquerySig, _: &Value) -> bool {
        panic!("matches() reached a subquery predicate; use matches_ctx with the registry")
    }
    fn has_null(&self, _: &SubquerySig) -> bool {
        panic!("matches() reached a subquery predicate; use matches_ctx with the registry")
    }
}

/// A necessary conjunct extracted from a predicate — see [`CompiledPredicate::access_leaf`].
#[derive(Debug, Clone, PartialEq)]
pub enum AccessLeaf {
    /// `col = value` (value non-NULL).
    Eq { col: usize, value: Value },
    /// `col > value` (`strict`) or `col >= value`.
    Lower { col: usize, value: Value, strict: bool },
    /// `col < value` (`strict`) or `col <= value`.
    Upper { col: usize, value: Value, strict: bool },
}

impl HeapSize for AccessLeaf {
    /// `col`/`strict` are inline; only the literal `value` owns heap.
    fn heap_bytes(&self) -> usize {
        match self {
            AccessLeaf::Eq { col: _, value }
            | AccessLeaf::Lower { col: _, value, strict: _ }
            | AccessLeaf::Upper { col: _, value, strict: _ } => value.heap_bytes(),
        }
    }
}

/// Compiled predicate over positional columns. `MatchAll` is used when a shape has no `where`.
#[derive(Debug, Clone)]
pub enum CompiledPredicate {
    MatchAll,
    Cmp {
        col: usize,
        op: LeafOp,
        value: Value,
    },
    /// SQL null test — the one leaf that is TRUE on a NULL cell (two-valued, never UNKNOWN).
    IsNull {
        col: usize,
        is_null: bool,
    },
    And(Vec<CompiledPredicate>),
    Or(Vec<CompiledPredicate>),
    Not(Box<CompiledPredicate>),
    /// `col IN (subquery)` (or `NOT IN` when `negated`). The inner predicate lives in the registry node
    /// keyed by `sig`; evaluation consults it via [`SubqueryEval`].
    InSubquery {
        col: usize,
        sig: SubquerySig,
        negated: bool,
    },
}

impl CompiledPredicate {
    /// Compile against `ts`, rejecting subqueries (back-compat for non-subquery paths).
    pub fn compile(p: &PredicateJson, ts: &TableSchema) -> Result<Self> {
        Self::compile_with(p, ts, &mut NoSubqueries)
    }

    /// Compile against `ts`, registering any `IN (SELECT …)` subqueries via `collector` (which creates
    /// the maintained nodes). The compiled `InSubquery` leaves reference nodes by signature.
    pub fn compile_with(p: &PredicateJson, ts: &TableSchema, collector: &mut dyn SubqueryCollector) -> Result<Self> {
        Ok(match p {
            PredicateJson::Leaf { col, op, value } => {
                let idx = ts.column_index(col)?;
                // Leaf literals (incl. substituted `$N` param values, always strings) coerce to the
                // column type — Postgres unknown-literal semantics. A timestamp/date
                // literal is also respelled as a cell of that column is, and one the engine cannot
                // read is refused here rather than compared as text (see `pg_temporal`); a `LIKE`
                // pattern is text whatever the column, and is never rewritten.
                let v = if *op == LeafOp::Like {
                    Value::literal_from_json(value, ts.column_type(idx))?
                } else {
                    Value::literal_for_column(value, ts.column_type(idx), ts.pg_type_of(col)).map_err(|e| {
                        match ts.pg_type_of(col) {
                            Some(declared) => e.context(format!("predicate on column '{col}' ({declared})")),
                            None => e.context(format!("predicate on column '{col}'")),
                        }
                    })?
                };
                CompiledPredicate::Cmp { col: idx, op: *op, value: v }
            }
            PredicateJson::IsNull { col, is_null } => {
                let idx = ts.column_index(col)?;
                CompiledPredicate::IsNull { col: idx, is_null: *is_null }
            }
            PredicateJson::And { and } => {
                CompiledPredicate::And(and.iter().map(|p| Self::compile_with(p, ts, collector)).collect::<Result<_>>()?)
            }
            PredicateJson::Or { or } => {
                CompiledPredicate::Or(or.iter().map(|p| Self::compile_with(p, ts, collector)).collect::<Result<_>>()?)
            }
            PredicateJson::Not { not } => CompiledPredicate::Not(Box::new(Self::compile_with(not, ts, collector)?)),
            PredicateJson::In { col, subquery, negated } => {
                let idx = ts.column_index(col)?;
                let sig = collector.collect(&subquery.table, &subquery.project, subquery.where_.as_deref())?;
                CompiledPredicate::InSubquery { col: idx, sig, negated: *negated }
            }
        })
    }

    /// Compile an optional predicate; `None` -> match all rows. Rejects subqueries.
    pub fn compile_opt(p: Option<&PredicateJson>, ts: &TableSchema) -> Result<Self> {
        match p {
            Some(p) => Self::compile(p, ts),
            None => Ok(CompiledPredicate::MatchAll),
        }
    }

    /// Compile an optional predicate with a subquery collector; `None` -> match all rows.
    pub fn compile_opt_with(
        p: Option<&PredicateJson>,
        ts: &TableSchema,
        collector: &mut dyn SubqueryCollector,
    ) -> Result<Self> {
        match p {
            Some(p) => Self::compile_with(p, ts, collector),
            None => Ok(CompiledPredicate::MatchAll),
        }
    }

    /// Sharing template: if this predicate is a pure conjunction of **non-null equality** leaves
    /// over **distinct columns**, return the `(column index, literal)` pairs sorted by column
    /// index. Shapes with equal column-index sets share one family circuit (see
    /// `docs/ivm-engine-internals.md` §3.1).
    ///
    /// Returns `None` for everything else — ranges, `neq`, `OR`, `NOT`, `MatchAll`, a `col = NULL`
    /// literal (SQL `= NULL` is UNKNOWN, never a key match), or a duplicate column (`a=1 AND a=2`,
    /// a degenerate contradiction) — all of which keep using a standalone per-shape circuit.
    pub fn equality_template(&self) -> Option<Vec<(usize, Value)>> {
        fn collect(p: &CompiledPredicate, acc: &mut Vec<(usize, Value)>) -> bool {
            match p {
                CompiledPredicate::Cmp { col, op: LeafOp::Eq, value } if !matches!(value, Value::Null) => {
                    acc.push((*col, value.clone()));
                    true
                }
                CompiledPredicate::And(ps) => ps.iter().all(|p| collect(p, acc)),
                _ => false,
            }
        }
        let mut acc = Vec::new();
        if !collect(self, &mut acc) || acc.is_empty() {
            return None;
        }
        acc.sort_by_key(|a| a.0);
        if acc.windows(2).any(|w| w[0].0 == w[1].0) {
            return None; // duplicate column — degenerate, not a shareable single key
        }
        Some(acc)
    }

    /// A single **necessary conjunct** for this predicate: a leaf the predicate implies, so a row on
    /// which the leaf is not TRUE can never match. Used to index standalone shapes by `(column, op)` —
    /// a change then only visits shapes whose necessary conjunct it satisfies, instead of every shape.
    ///
    /// Extraction walks the top-level `AND` chain (recursively through nested `AND`s — a conjunct of a
    /// conjunct is still necessary) and prefers an equality leaf (hash lookup) over a range bound
    /// (ordered scan). Returns `None` for predicates with no such leaf (`OR`/`NOT` at the top,
    /// `LIKE`, `!=`, `IS NULL`, `MatchAll`, subqueries, or a NULL literal — `cmp` with NULL is never
    /// TRUE, so such shapes match nothing anyway); those shapes stay on the fallback scan list.
    pub fn access_leaf(&self) -> Option<AccessLeaf> {
        fn leaf_of(p: &CompiledPredicate) -> Option<AccessLeaf> {
            let CompiledPredicate::Cmp { col, op, value } = p else { return None };
            if matches!(value, Value::Null) {
                return None;
            }
            match op {
                LeafOp::Eq => Some(AccessLeaf::Eq { col: *col, value: value.clone() }),
                LeafOp::Gt => Some(AccessLeaf::Lower { col: *col, value: value.clone(), strict: true }),
                LeafOp::Gte => Some(AccessLeaf::Lower { col: *col, value: value.clone(), strict: false }),
                LeafOp::Lt => Some(AccessLeaf::Upper { col: *col, value: value.clone(), strict: true }),
                LeafOp::Lte => Some(AccessLeaf::Upper { col: *col, value: value.clone(), strict: false }),
                LeafOp::Neq | LeafOp::Like => None,
            }
        }
        fn collect(p: &CompiledPredicate, best: &mut Option<AccessLeaf>) {
            match p {
                CompiledPredicate::And(ps) => {
                    for q in ps {
                        collect(q, best);
                    }
                }
                _ => {
                    if let Some(l) = leaf_of(p) {
                        let better = match (&best, &l) {
                            (None, _) => true,
                            (Some(AccessLeaf::Eq { .. }), _) => false,
                            (Some(_), AccessLeaf::Eq { .. }) => true,
                            _ => false,
                        };
                        if better {
                            *best = Some(l);
                        }
                    }
                }
            }
        }
        let mut best = None;
        collect(self, &mut best);
        best
    }

    /// Filter membership under SQL `WHERE` semantics: a row is included iff the predicate is TRUE.
    /// UNKNOWN (from a NULL operand) and FALSE both exclude the row. Panics if the predicate contains a
    /// subquery — use [`matches_ctx`](Self::matches_ctx) for those.
    pub fn matches(&self, row: &Row) -> bool {
        self.eval_ctx(row, &PanicEval) == Tri::True
    }

    /// Filter membership with subquery resolution: subquery leaves consult `ev` (the registry's node
    /// sets). Equivalent to [`matches`](Self::matches) for subquery-free predicates.
    pub fn matches_ctx(&self, row: &Row, ev: &dyn SubqueryEval) -> bool {
        self.eval_ctx(row, ev) == Tri::True
    }

    /// Three-valued evaluation (TRUE / FALSE / UNKNOWN), mirroring Postgres so the engine and the
    /// oracle agree even in the presence of NULLs. `ev` resolves subquery membership.
    fn eval_ctx(&self, row: &Row, ev: &dyn SubqueryEval) -> Tri {
        match self {
            CompiledPredicate::MatchAll => Tri::True,
            CompiledPredicate::Cmp { col, op, value } => {
                let cell = row.0.get(*col).unwrap_or(&Value::Null);
                cmp(cell, *op, value)
            }
            // `IS [NOT] NULL` is two-valued: TRUE/FALSE by the cell's null-ness, never UNKNOWN.
            CompiledPredicate::IsNull { col, is_null } => {
                let cell = row.0.get(*col).unwrap_or(&Value::Null);
                Tri::from_bool(matches!(cell, Value::Null) == *is_null)
            }
            // AND: FALSE dominates; else UNKNOWN if any UNKNOWN; else TRUE (empty AND => TRUE).
            CompiledPredicate::And(ps) => {
                let mut acc = Tri::True;
                for p in ps {
                    match p.eval_ctx(row, ev) {
                        Tri::False => return Tri::False,
                        Tri::Unknown => acc = Tri::Unknown,
                        Tri::True => {}
                    }
                }
                acc
            }
            // OR: TRUE dominates; else UNKNOWN if any UNKNOWN; else FALSE (empty OR => FALSE).
            CompiledPredicate::Or(ps) => {
                let mut acc = Tri::False;
                for p in ps {
                    match p.eval_ctx(row, ev) {
                        Tri::True => return Tri::True,
                        Tri::Unknown => acc = Tri::Unknown,
                        Tri::False => {}
                    }
                }
                acc
            }
            // NOT TRUE = FALSE, NOT FALSE = TRUE, NOT UNKNOWN = UNKNOWN. The UNKNOWN case is the fix
            // that makes `NOT (col = x)` over a NULL cell keep the row out, exactly as Postgres does.
            CompiledPredicate::Not(p) => p.eval_ctx(row, ev).not(),
            // `x IN set`: NULL x -> UNKNOWN; else membership. `x NOT IN set`: NULL x -> UNKNOWN; a set
            // containing NULL makes it UNKNOWN (SQL); else the complement. Mirrors Postgres exactly.
            CompiledPredicate::InSubquery { col, sig, negated } => {
                let cell = row.0.get(*col).unwrap_or(&Value::Null);
                if matches!(cell, Value::Null) {
                    return Tri::Unknown;
                }
                let present = ev.contains(sig, cell);
                if !negated {
                    Tri::from_bool(present)
                } else if ev.has_null(sig) {
                    Tri::Unknown
                } else {
                    Tri::from_bool(!present)
                }
            }
        }
    }
}

/// SQL three-valued truth value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    fn from_bool(b: bool) -> Tri {
        if b { Tri::True } else { Tri::False }
    }
    fn not(self) -> Tri {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

/// Compare a cell against a literal under SQL three-valued semantics: any NULL operand yields
/// UNKNOWN (mirrors Postgres and `@circuits/protocol`'s evaluator).
fn cmp(cell: &Value, op: LeafOp, value: &Value) -> Tri {
    if matches!(cell, Value::Null) || matches!(value, Value::Null) {
        return Tri::Unknown;
    }
    let truth = match op {
        LeafOp::Eq => cell == value,
        LeafOp::Neq => cell != value,
        LeafOp::Like => match (cell, value) {
            (Value::Text(c), Value::Text(p)) => like_match(c, p),
            _ => return Tri::Unknown,
        },
        LeafOp::Lt | LeafOp::Lte | LeafOp::Gt | LeafOp::Gte => {
            // A type mismatch has no ordering; treat as UNKNOWN (literals are column-typed, so this
            // does not arise in practice).
            let Some(ord) = ordering(cell, value) else { return Tri::Unknown };
            // TEST-ONLY: the `off_by_one_cmp` fault makes `<=`/`>=` strict, so rows exactly on a
            // boundary literal are mishandled. No-op unless CIRCUITS_FAULT=off_by_one_cmp.
            let off_by_one = matches!(crate::fault::active(), crate::fault::Fault::OffByOneCmp);
            match op {
                LeafOp::Lt => ord.is_lt(),
                LeafOp::Lte => {
                    if off_by_one {
                        ord.is_lt()
                    } else {
                        ord.is_le()
                    }
                }
                LeafOp::Gt => ord.is_gt(),
                LeafOp::Gte => {
                    if off_by_one {
                        ord.is_gt()
                    } else {
                        ord.is_ge()
                    }
                }
                _ => unreachable!(),
            }
        }
    };
    Tri::from_bool(truth)
}

/// SQL `LIKE` matching: `%` = any sequence (including empty), `_` = exactly one char, everything else
/// literal (case-sensitive). Iterative backtracking over chars — patterns here are short.
pub(crate) fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star_p, mut star_t): (Option<usize>, usize) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_p = Some(pi);
            star_t = ti;
            pi += 1;
        } else if let Some(sp) = star_p {
            pi = sp + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.partial_cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Text(x), Value::Text(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableDef, TableSchema};

    fn users() -> TableSchema {
        let json = serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "age": {"type":"int"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        });
        let def: TableDef = serde_json::from_value(json).unwrap();
        TableSchema::from_def(&"users".into(), &def).unwrap()
    }

    fn row(ts: &TableSchema, j: serde_json::Value) -> Row {
        ts.row_from_json(j.as_object().unwrap()).unwrap()
    }

    // The access leaf is a conjunct the predicate implies — an equality when one exists (hash
    // lookup), else a range bound, else None (fallback scan): the standalone index depends on
    // "leaf not TRUE on a row ⇒ predicate not TRUE on that row".
    #[test]
    fn access_leaf_extraction() {
        let ts = users();
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), &ts).unwrap()
        };
        let name_idx = ts.column_index("name").unwrap();
        let age_idx = ts.column_index("age").unwrap();

        // A bare equality leaf, and an equality preferred over a range inside a conjunction —
        // including a nested AND (a conjunct of a conjunct is still necessary).
        assert_eq!(
            compile(serde_json::json!({"col":"name","op":"eq","value":"a"})).access_leaf(),
            Some(AccessLeaf::Eq { col: name_idx, value: Value::Text("a".into()) })
        );
        assert_eq!(
            compile(serde_json::json!({"and":[
                {"col":"age","op":"gt","value":18},
                {"and":[{"col":"name","op":"eq","value":"a"}, {"col":"name","op":"like","value":"a%"}]}
            ]}))
            .access_leaf(),
            Some(AccessLeaf::Eq { col: name_idx, value: Value::Text("a".into()) })
        );
        // Range-only conjunction -> a bound.
        assert_eq!(
            compile(serde_json::json!({"col":"age","op":"gte","value":21})).access_leaf(),
            Some(AccessLeaf::Lower { col: age_idx, value: Value::Int(21), strict: false })
        );
        // OR / NOT / LIKE / != / match-all have no necessary leaf.
        assert_eq!(
            compile(serde_json::json!({"or":[
                {"col":"age","op":"gt","value":18}, {"col":"name","op":"eq","value":"a"}
            ]}))
            .access_leaf(),
            None
        );
        assert_eq!(compile(serde_json::json!({"col":"name","op":"neq","value":"a"})).access_leaf(), None);
        assert_eq!(CompiledPredicate::MatchAll.access_leaf(), None);
    }

    // A string leaf literal coerces to the column type (Postgres unknown-literal semantics), so
    // `'18'` works against an int column and `'true'` against a bool column.
    #[test]
    fn string_literal_coerces_to_column_type() {
        let ts = users();
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), &ts).unwrap()
        };
        let r = row(&ts, serde_json::json!({"id":1,"name":"a","age":18,"active":true}));
        assert!(compile(serde_json::json!({"col":"age","op":"gte","value":"18"})).matches(&r)); // '18' -> int
        assert!(compile(serde_json::json!({"col":"active","op":"eq","value":"true"})).matches(&r)); // 'true' -> bool
        assert!(compile(serde_json::json!({"col":"name","op":"eq","value":"a"})).matches(&r)); // 'a' -> text
        // an uncoercible string against an int column is a (400) error, not a silent mismatch
        assert!(
            CompiledPredicate::compile(
                &serde_json::from_value::<PredicateJson>(serde_json::json!({"col":"age","op":"eq","value":"abc"}))
                    .unwrap(),
                &ts,
            )
            .is_err()
        );
    }

    #[test]
    fn equality_and_comparison() {
        let ts = users();
        let p: PredicateJson =
            serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap();
        let cp = CompiledPredicate::compile(&p, &ts).unwrap();
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":2,"name":"b","age":20,"active":false}))));

        let p2: PredicateJson = serde_json::from_value(serde_json::json!({"col":"age","op":"gte","value":18})).unwrap();
        let cp2 = CompiledPredicate::compile(&p2, &ts).unwrap();
        assert!(cp2.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":18,"active":true}))));
        assert!(!cp2.matches(&row(&ts, serde_json::json!({"id":2,"name":"b","age":17,"active":true}))));
    }

    #[test]
    fn boolean_combinators() {
        let ts = users();
        let p: PredicateJson = serde_json::from_value(serde_json::json!({
            "and": [
                {"col":"active","op":"eq","value":true},
                {"or": [ {"col":"age","op":"gt","value":30}, {"not": {"col":"name","op":"eq","value":"bob"}} ]}
            ]
        }))
        .unwrap();
        let cp = CompiledPredicate::compile(&p, &ts).unwrap();
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"alice","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":2,"name":"bob","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":3,"name":"alice","age":20,"active":false}))));
    }

    #[test]
    fn match_all_when_no_predicate() {
        let ts = users();
        let cp = CompiledPredicate::compile_opt(None, &ts).unwrap();
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":1,"active":false}))));
    }

    #[test]
    fn equality_template_extraction() {
        let ts = users();
        let tpl = |j: serde_json::Value| {
            CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), &ts)
                .unwrap()
                .equality_template()
        };
        let cols =
            |t: &TableSchema, names: &[&str]| names.iter().map(|n| t.column_index(n).unwrap()).collect::<Vec<_>>();

        // single equality qualifies
        let t = tpl(serde_json::json!({"col":"name","op":"eq","value":"alice"})).unwrap();
        assert_eq!(t.iter().map(|(c, _)| *c).collect::<Vec<_>>(), cols(&ts, &["name"]));
        assert_eq!(t[0].1, Value::Text("alice".into()));

        // AND of equalities -> sorted by column index, distinct columns
        let t = tpl(serde_json::json!({"and":[
            {"col":"name","op":"eq","value":"a"}, {"col":"active","op":"eq","value":true}
        ]}))
        .unwrap();
        let mut want = cols(&ts, &["name", "active"]);
        want.sort();
        assert_eq!(t.iter().map(|(c, _)| *c).collect::<Vec<_>>(), want);

        // nested And flattens
        assert!(
            tpl(serde_json::json!({"and":[
                {"and":[{"col":"name","op":"eq","value":"a"}]}, {"col":"age","op":"eq","value":1}
            ]}))
            .is_some()
        );

        // non-qualifying shapes -> None
        assert!(tpl(serde_json::json!({"col":"age","op":"gte","value":18})).is_none()); // range
        assert!(tpl(serde_json::json!({"col":"name","op":"neq","value":"a"})).is_none()); // neq
        assert!(tpl(serde_json::json!({"or":[{"col":"name","op":"eq","value":"a"}]})).is_none()); // or
        assert!(tpl(serde_json::json!({"not":{"col":"name","op":"eq","value":"a"}})).is_none()); // not
        assert!(
            tpl(serde_json::json!({"and":[
                {"col":"name","op":"eq","value":"a"}, {"col":"age","op":"gt","value":1}
            ]}))
            .is_none()
        ); // mixed eq + range
        assert!(
            tpl(serde_json::json!({"and":[
                {"col":"age","op":"eq","value":1}, {"col":"age","op":"eq","value":2}
            ]}))
            .is_none()
        ); // duplicate column
        // MatchAll
        assert!(CompiledPredicate::MatchAll.equality_template().is_none());
    }

    /// A collector that records every (table, project, where-sig) it sees, for assertions.
    struct RecordCollector {
        sigs: Vec<SubquerySig>,
    }
    impl SubqueryCollector for RecordCollector {
        fn collect(&mut self, t: &TableRef, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
            let sig = subquery_sig(t, p, w);
            self.sigs.push(sig.clone());
            Ok(sig)
        }
    }

    /// A test eval: a fixed membership set per sig, plus a null flag.
    struct MockEval {
        set: std::collections::HashSet<Value>,
        null: bool,
    }
    impl SubqueryEval for MockEval {
        fn contains(&self, _sig: &SubquerySig, v: &Value) -> bool {
            self.set.contains(v)
        }
        fn has_null(&self, _sig: &SubquerySig) -> bool {
            self.null
        }
    }

    #[test]
    fn subquery_signature_is_stable_and_order_insensitive() {
        // identical subqueries -> identical sig
        let a = subquery_sig(
            &"parent".into(),
            "id",
            Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
        );
        let b = subquery_sig(
            &"parent".into(),
            "id",
            Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
        );
        assert_eq!(a, b);
        // AND child order does not change the sig
        let p1: PredicateJson = serde_json::from_value(serde_json::json!({"and":[
            {"col":"active","op":"eq","value":true}, {"col":"x","op":"gt","value":1}
        ]}))
        .unwrap();
        let p2: PredicateJson = serde_json::from_value(serde_json::json!({"and":[
            {"col":"x","op":"gt","value":1}, {"col":"active","op":"eq","value":true}
        ]}))
        .unwrap();
        assert_eq!(subquery_sig(&"t".into(), "id", Some(&p1)), subquery_sig(&"t".into(), "id", Some(&p2)));
        // a different inner where -> different sig
        let p3: PredicateJson =
            serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":false})).unwrap();
        assert_ne!(subquery_sig(&"parent".into(), "id", Some(&p3)), a);
        // a different project / table -> different sig
        assert_ne!(subquery_sig(&"parent".into(), "name", None), subquery_sig(&"parent".into(), "id", None));
    }

    /// Template extraction factors literals OUT of the identity (the route-join model applied
    /// to subqueries): same query shape + different literals ⇒ one template key, distinct
    /// binds; non-equality structure stays in the residual with literals baked in.
    #[test]
    fn subquery_template_lifts_equality_literals() {
        let w = |j: serde_json::Value| serde_json::from_value::<PredicateJson>(j).unwrap();

        // Different literals, same shape -> same key, different binds.
        let (k1, b1, _) = subquery_template(
            &"pm".into(),
            "project_id",
            Some(&w(serde_json::json!({"col":"user_id","op":"eq","value":1}))),
        );
        let (k2, b2, _) = subquery_template(
            &"pm".into(),
            "project_id",
            Some(&w(serde_json::json!({"col":"user_id","op":"eq","value":2}))),
        );
        assert_eq!(k1, k2, "one template per shape");
        assert_ne!(b1, b2);
        assert_eq!(b1, vec![("user_id".to_string(), serde_json::json!(1))]);

        // Multi-column AND: binds sorted by column name; conjunct order irrelevant.
        let (k3, b3, _) = subquery_template(
            &"pm".into(),
            "p",
            Some(&w(serde_json::json!({"and":[
                {"col":"user_id","op":"eq","value":1}, {"col":"status","op":"eq","value":"active"}
            ]}))),
        );
        let (k4, b4, _) = subquery_template(
            &"pm".into(),
            "p",
            Some(&w(serde_json::json!({"and":[
                {"col":"status","op":"eq","value":"x"}, {"col":"user_id","op":"eq","value":9}
            ]}))),
        );
        assert_eq!(k3, k4);
        assert_eq!(b3.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), vec!["status", "user_id"]);
        assert_eq!(b4[0].1, serde_json::json!("x"));

        // Ranges / OR / NOT / IS NULL / nested IN stay residual (literals baked in) — a
        // different range literal is a DIFFERENT template.
        let (k5, b5, r5) = subquery_template(
            &"t".into(),
            "id",
            Some(&w(serde_json::json!({"and":[
                {"col":"user_id","op":"eq","value":1}, {"col":"age","op":"gt","value":18}
            ]}))),
        );
        let (k6, _, _) = subquery_template(
            &"t".into(),
            "id",
            Some(&w(serde_json::json!({"and":[
                {"col":"user_id","op":"eq","value":2}, {"col":"age","op":"gt","value":18}
            ]}))),
        );
        let (k7, _, _) = subquery_template(
            &"t".into(),
            "id",
            Some(&w(serde_json::json!({"and":[
                {"col":"user_id","op":"eq","value":1}, {"col":"age","op":"gt","value":21}
            ]}))),
        );
        assert_eq!(k5, k6, "same residual range literal -> same template");
        assert_ne!(k5, k7, "different residual literal -> different template");
        assert_eq!(b5.len(), 1, "only the equality lifts");
        assert_eq!(r5.len(), 1, "the range conjunct is the residual");

        // NULL equality literal never lifts (SQL `= NULL` is UNKNOWN, not a key).
        let (_, b8, r8) =
            subquery_template(&"t".into(), "id", Some(&w(serde_json::json!({"col":"user_id","op":"eq","value":null}))));
        assert!(b8.is_empty());
        assert_eq!(r8.len(), 1, "the NULL equality stays residual");

        // No where -> empty params, stable key; distinct from a lifted one.
        let (k9, b9, r9) = subquery_template(&"t".into(), "id", None);
        assert!(b9.is_empty());
        assert!(r9.is_empty(), "no where -> empty residual (match-all)");
        assert_ne!(k9, k1);
        assert_eq!(k9, subquery_template(&"t".into(), "id", None).0);

        // Duplicate column (`a=1 AND a=2`, degenerate): one lift, the other residual —
        // deterministic regardless of author order.
        let (k10, b10, _) = subquery_template(
            &"t".into(),
            "id",
            Some(&w(serde_json::json!({"and":[
                {"col":"a","op":"eq","value":1}, {"col":"a","op":"eq","value":2}
            ]}))),
        );
        let (k11, b11, _) = subquery_template(
            &"t".into(),
            "id",
            Some(&w(serde_json::json!({"and":[
                {"col":"a","op":"eq","value":2}, {"col":"a","op":"eq","value":1}
            ]}))),
        );
        assert_eq!(k10, k11);
        assert_eq!(b10, b11);
        assert_eq!(b10.len(), 1);
    }

    #[test]
    fn compiles_in_subquery_leaf_and_collects_node() {
        let ts = users();
        let p: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "id",
            "in": { "table": "groups", "project": "gid", "where": {"col":"name","op":"eq","value":"a"} },
            "negated": true
        }))
        .unwrap();
        let mut c = RecordCollector { sigs: Vec::new() };
        let cp = CompiledPredicate::compile_with(&p, &ts, &mut c).unwrap();
        match cp {
            CompiledPredicate::InSubquery { col, ref sig, negated } => {
                assert_eq!(col, ts.column_index("id").unwrap());
                assert!(negated);
                let want = subquery_sig(
                    &"groups".into(),
                    "gid",
                    Some(&serde_json::from_value(serde_json::json!({"col":"name","op":"eq","value":"a"})).unwrap()),
                );
                assert_eq!(sig, &want);
                assert!(sig.starts_with("public.groups|gid|"), "node signatures carry the canonical table: {sig}");
            }
            _ => panic!("expected InSubquery, got {cp:?}"),
        }
        assert_eq!(c.sigs.len(), 1);
        // subqueries are rejected without a collector
        assert!(CompiledPredicate::compile(&p, &ts).is_err());
        // equality_template never treats a subquery as a shareable equality key
        let mut c2 = RecordCollector { sigs: Vec::new() };
        assert!(CompiledPredicate::compile_with(&p, &ts, &mut c2).unwrap().equality_template().is_none());
    }

    #[test]
    fn matches_ctx_in_and_not_in_with_null_semantics() {
        let ts = users();
        let mut c = RecordCollector { sigs: Vec::new() };
        let in_pred = CompiledPredicate::compile_with(
            &serde_json::from_value(serde_json::json!({"col":"id","in":{"table":"g","project":"gid"}})).unwrap(),
            &ts,
            &mut c,
        )
        .unwrap();
        let not_in = CompiledPredicate::compile_with(
            &serde_json::from_value(serde_json::json!({"col":"id","negated":true,"in":{"table":"g","project":"gid"}}))
                .unwrap(),
            &ts,
            &mut c,
        )
        .unwrap();
        let mk = |id: i64| row(&ts, serde_json::json!({"id":id,"name":"a","age":1,"active":true}));
        let null_row = row(&ts, serde_json::json!({"id":null,"name":"a","age":1,"active":true}));

        let ev = MockEval { set: [Value::Int(1), Value::Int(2)].into_iter().collect(), null: false };
        assert!(in_pred.matches_ctx(&mk(1), &ev)); // 1 in set
        assert!(!in_pred.matches_ctx(&mk(3), &ev)); // 3 not in set
        assert!(!in_pred.matches_ctx(&null_row, &ev)); // NULL IN -> UNKNOWN -> excluded
        assert!(!not_in.matches_ctx(&mk(1), &ev)); // 1 in set -> NOT IN false
        assert!(not_in.matches_ctx(&mk(3), &ev)); // 3 not in set -> NOT IN true
        assert!(!not_in.matches_ctx(&null_row, &ev)); // NULL NOT IN -> UNKNOWN

        // set contains NULL -> NOT IN is UNKNOWN for everyone (SQL gotcha)
        let ev_null = MockEval { set: [Value::Int(1)].into_iter().collect(), null: true };
        assert!(!not_in.matches_ctx(&mk(3), &ev_null));
        assert!(in_pred.matches_ctx(&mk(1), &ev_null)); // positive IN unaffected by null in set
    }

    #[test]
    fn three_valued_null_logic() {
        let ts = users();
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), &ts).unwrap()
        };
        // Rows with a NULL `name` / NULL `age`.
        let null_name = row(&ts, serde_json::json!({"id":1,"name":null,"age":20,"active":true}));
        let null_age_active = row(&ts, serde_json::json!({"id":2,"name":"a","age":null,"active":true}));
        let null_age_inactive = row(&ts, serde_json::json!({"id":3,"name":"a","age":null,"active":false}));

        // Leaf over NULL -> UNKNOWN -> excluded (eq and neq alike).
        assert!(!compile(serde_json::json!({"col":"name","op":"eq","value":"alpha"})).matches(&null_name));
        assert!(!compile(serde_json::json!({"col":"name","op":"neq","value":"alpha"})).matches(&null_name));

        // THE FIX: NOT(name = 'alpha') over a NULL name is NOT UNKNOWN = UNKNOWN -> excluded.
        // The old two-valued evaluator wrongly returned true here and would have leaked the row.
        let not_eq = compile(serde_json::json!({"not":{"col":"name","op":"eq","value":"alpha"}}));
        assert!(!not_eq.matches(&null_name));
        // ...but NOT(name = 'alpha') over a concrete non-match is TRUE.
        assert!(not_eq.matches(&row(&ts, serde_json::json!({"id":9,"name":"bob","age":20,"active":true}))));

        // AND: TRUE AND UNKNOWN = UNKNOWN -> excluded; FALSE AND UNKNOWN = FALSE -> excluded.
        let and = compile(
            serde_json::json!({"and":[{"col":"active","op":"eq","value":true},{"col":"age","op":"gt","value":18}]}),
        );
        assert!(!and.matches(&null_age_active));
        assert!(!and.matches(&null_age_inactive));

        // OR: TRUE OR UNKNOWN = TRUE -> included; FALSE OR UNKNOWN = UNKNOWN -> excluded.
        let or = compile(
            serde_json::json!({"or":[{"col":"active","op":"eq","value":true},{"col":"age","op":"gt","value":100}]}),
        );
        assert!(or.matches(&null_age_active));
        assert!(!or.matches(&null_age_inactive));
    }

    #[test]
    fn like_match_basics() {
        assert!(like_match("abc", "a%"));
        assert!(like_match("abc", "_bc"));
        assert!(!like_match("abc", "a_d"));
        assert!(like_match("abc", "%"));
        assert!(like_match("abc", "abc"));
        assert!(!like_match("abcd", "abc"));
    }
    // --- timestamp literals --------------------------------------------------------------------
    //
    // A `timestamptz` column is coarse `Text` here: its cells are Postgres's own text output
    // (`2026-10-19 16:00:00+00`), and the engine compares text in code-point order. Postgres
    // compares the same predicate as instants (the backfill and every subset page run it in SQL),
    // so a literal in any other spelling of the same instant makes the live path disagree with the
    // snapshot the client was just handed. The app sends ISO 8601 (`2026-10-19T07:00:00Z`): 'T'
    // sorts after ' ', so every cell on the literal's own UTC date compares below it, whatever its
    // time. Observed against the gateway on 2026-11-16: 17 of 40 edits to events inside a week's
    // window never reached the feed — exactly the 17 on the window's first UTC date.

    /// `events(id, starts timestamptz, local timestamp, day date)` as introspection builds it:
    /// coarse `Text` columns that carry their Postgres type name.
    fn events_ts() -> TableSchema {
        let mut def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": {
                "id": {"type":"int"}, "starts": {"type":"text"}, "local": {"type":"text"}, "day": {"type":"text"},
                "title": {"type":"text"}
            },
            "primaryKey": "id"
        }))
        .unwrap();
        for (col, pg) in [("starts", "timestamptz"), ("local", "timestamp"), ("day", "date"), ("title", "text")] {
            def.columns.get_mut(col).unwrap().pg_type = Some(pg.into());
        }
        TableSchema::from_def(&"events".into(), &def).unwrap()
    }

    /// One event whose cells are spelled the way Postgres prints them (session `TimeZone` UTC).
    fn event_at(ts: &TableSchema, starts: &str) -> Row {
        let local = starts.trim_end_matches("+00");
        let day = &starts[..10];
        row(ts, serde_json::json!({"id":1,"starts":starts,"local":local,"day":day,"title":"t"}))
    }

    fn compile_on(ts: &TableSchema, j: serde_json::Value) -> CompiledPredicate {
        CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), ts).unwrap()
    }

    /// The reproduction: a week window whose lower bound is `2026-10-19T07:00:00Z` (midnight
    /// Pacific, as the app writes it) and an event at 16:00 UTC that same day. Postgres says the
    /// event is inside the window; the live path must say the same.
    #[test]
    fn a_timestamptz_bound_in_iso_8601_admits_a_row_later_the_same_utc_day() {
        let ts = events_ts();
        let event = event_at(&ts, "2026-10-19 16:00:00+00");
        let window = compile_on(
            &ts,
            serde_json::json!({"and":[
                {"col":"starts","op":"gte","value":"2026-10-19T07:00:00Z"},
                {"col":"starts","op":"lt","value":"2026-10-26T07:00:00Z"}
            ]}),
        );
        assert!(window.matches(&event), "16:00Z on the bound's date is after a 07:00Z lower bound");

        // The mirror image on the upper bound: the same instant must be outside a window that
        // ends at 07:00Z that day.
        let earlier_window = compile_on(
            &ts,
            serde_json::json!({"and":[
                {"col":"starts","op":"gte","value":"2026-10-12T07:00:00Z"},
                {"col":"starts","op":"lt","value":"2026-10-19T07:00:00Z"}
            ]}),
        );
        assert!(!earlier_window.matches(&event), "16:00Z is not before a 07:00Z upper bound on the same date");
    }

    /// Every spelling of one instant compares the same: `Z`, a numeric offset, fractional seconds,
    /// a space separator, and Postgres's own output.
    #[test]
    fn timestamptz_literals_compare_by_instant_whatever_their_spelling() {
        let ts = events_ts();
        let event = event_at(&ts, "2026-10-19 16:00:00+00");
        for same_instant in [
            "2026-10-19T16:00:00Z",
            "2026-10-19t16:00:00z",
            "2026-10-19T16:00:00.000Z",
            "2026-10-19T16:00:00+00:00",
            "2026-10-19T09:00:00-07:00",
            "2026-10-20T01:30:00+09:30",
            "2026-10-19 16:00:00+00",
            "2026-10-19 16:00:00",
            "2026-10-19T16:00Z",
        ] {
            let eq = compile_on(&ts, serde_json::json!({"col":"starts","op":"eq","value":same_instant}));
            assert!(eq.matches(&event), "{same_instant} is the event's instant");
            let lte = compile_on(&ts, serde_json::json!({"col":"starts","op":"lte","value":same_instant}));
            let gte = compile_on(&ts, serde_json::json!({"col":"starts","op":"gte","value":same_instant}));
            assert!(lte.matches(&event) && gte.matches(&event), "{same_instant}: <= and >= both hold at equality");
            let neq = compile_on(&ts, serde_json::json!({"col":"starts","op":"neq","value":same_instant}));
            assert!(!neq.matches(&event), "{same_instant} is not a different instant");
        }
        for (earlier, later) in [
            ("2026-10-19T15:59:59.999Z", "2026-10-19T16:00:00.001Z"),
            ("2026-10-19T08:59:59-07:00", "2026-10-19T09:00:01-07:00"),
        ] {
            let after = compile_on(&ts, serde_json::json!({"col":"starts","op":"gt","value":earlier}));
            let before = compile_on(&ts, serde_json::json!({"col":"starts","op":"lt","value":later}));
            assert!(after.matches(&event), "the event is after {earlier}");
            assert!(before.matches(&event), "the event is before {later}");
        }
        // Fractional seconds: Postgres prints them with trailing zeros trimmed.
        let fractional = event_at(&ts, "2026-10-19 16:00:00.5+00");
        let eq = compile_on(&ts, serde_json::json!({"col":"starts","op":"eq","value":"2026-10-19T16:00:00.500Z"}));
        assert!(eq.matches(&fractional));
    }

    /// `timestamp` and `date` columns follow Postgres's cast of the literal to the column type: a
    /// zone on a `timestamp` literal is ignored, and a `date` keeps only the literal's date fields.
    #[test]
    fn timestamp_and_date_literals_take_the_column_types_spelling() {
        let ts = events_ts();
        let event = event_at(&ts, "2026-10-19 16:00:00+00");
        let local = compile_on(&ts, serde_json::json!({"col":"local","op":"gte","value":"2026-10-19T07:00:00"}));
        assert!(local.matches(&event), "a timestamp cell at 16:00 is after 07:00 the same day");
        let local_eq = compile_on(&ts, serde_json::json!({"col":"local","op":"eq","value":"2026-10-19T16:00:00Z"}));
        assert!(local_eq.matches(&event), "Postgres ignores a zone when casting to timestamp");
        let day = compile_on(&ts, serde_json::json!({"col":"day","op":"eq","value":"2026-10-19T07:00:00Z"}));
        assert!(day.matches(&event), "a date column keeps the literal's date");
    }

    /// The index tiers compare the access leaf's value against cells as text, so the literal has
    /// to be stored in the cells' own spelling — Postgres's output form.
    #[test]
    fn a_timestamptz_access_leaf_is_stored_in_postgres_output_form() {
        let ts = events_ts();
        let starts = ts.column_index("starts").unwrap();
        assert_eq!(
            compile_on(&ts, serde_json::json!({"col":"starts","op":"gte","value":"2026-10-19T07:00:00Z"}))
                .access_leaf(),
            Some(AccessLeaf::Lower { col: starts, value: Value::Text("2026-10-19 07:00:00+00".into()), strict: false })
        );
        assert_eq!(
            compile_on(&ts, serde_json::json!({"col":"starts","op":"eq","value":"2026-10-19T00:00:00.250-07:00"}))
                .access_leaf(),
            Some(AccessLeaf::Eq { col: starts, value: Value::Text("2026-10-19 07:00:00.25+00".into()) })
        );
    }

    /// What must NOT change: a plain text column keeps its literal byte for byte, whatever it looks
    /// like; a `LIKE` pattern is never rewritten; and the infinities stay as Postgres prints them.
    #[test]
    fn text_columns_like_patterns_and_the_infinities_are_left_as_they_are() {
        let ts = events_ts();
        let title = ts.column_index("title").unwrap();
        let starts = ts.column_index("starts").unwrap();
        for text in ["2026-10-19T07:00:00Z", "next tuesday", ""] {
            assert_eq!(
                compile_on(&ts, serde_json::json!({"col":"title","op":"eq","value":text})).access_leaf(),
                Some(AccessLeaf::Eq { col: title, value: Value::Text(text.into()) })
            );
        }
        for untouched in ["infinity", "-infinity"] {
            assert_eq!(
                compile_on(&ts, serde_json::json!({"col":"starts","op":"eq","value":untouched})).access_leaf(),
                Some(AccessLeaf::Eq { col: starts, value: Value::Text(untouched.into()) }),
                "{untouched:?} is not rewritten"
            );
        }
        let like = compile_on(&ts, serde_json::json!({"col":"starts","op":"like","value":"2026-10-19T%"}));
        assert!(matches!(like, CompiledPredicate::Cmp { value: Value::Text(ref v), .. } if v == "2026-10-19T%"));
    }

    /// Postgres reads one instant in more spellings than ISO 8601 with `Z`: a name for UTC, an
    /// offset with a one-digit hour or no colon, a zone straight after the date. The snapshot query
    /// is handed the literal as written, so the live path has to read each of these as SQL does.
    #[test]
    fn timestamptz_literals_in_the_other_spellings_postgres_reads_compare_by_instant() {
        let ts = events_ts();
        let starts = ts.column_index("starts").unwrap();
        let event = event_at(&ts, "2026-10-19 16:00:00+00");
        for same_instant in [
            "2026-10-19 16:00:00 UTC",
            "2026-10-19T16:00:00 gmt",
            "2026-10-19 16:00:00.0 zulu",
            "2026-10-19 21:00:00+5",
            "2026-10-19 21:00:00 +5",
            "2026-10-19 11:00:00-5",
            "2026-10-19 21:30:00+530",
            "2026-10-19 21:30:00+5:30",
        ] {
            let eq = compile_on(&ts, serde_json::json!({"col":"starts","op":"eq","value":same_instant}));
            assert!(eq.matches(&event), "{same_instant} is the event's instant");
            let neq = compile_on(&ts, serde_json::json!({"col":"starts","op":"neq","value":same_instant}));
            assert!(!neq.matches(&event), "{same_instant} is not a different instant");
        }
        // A zone after the date alone is midnight in that zone: `2026-10-19+02` is 22:00Z the day
        // before, and `2026-10-20 UTC` is the next midnight.
        let window = compile_on(
            &ts,
            serde_json::json!({"and":[
                {"col":"starts","op":"gte","value":"2026-10-19+02"},
                {"col":"starts","op":"lt","value":"2026-10-20 UTC"}
            ]}),
        );
        assert!(window.matches(&event));
        assert!(!window.matches(&event_at(&ts, "2026-10-18 21:59:59+00")));
        assert_eq!(
            compile_on(&ts, serde_json::json!({"col":"starts","op":"gte","value":"2026-10-19 07:00:00 UTC"}))
                .access_leaf(),
            Some(AccessLeaf::Lower { col: starts, value: Value::Text("2026-10-19 07:00:00+00".into()), strict: false })
        );
    }

    /// A literal for a timestamp or date column that the engine cannot read is REFUSED when the
    /// predicate is compiled. Kept as raw text it would be compared in code-point order against
    /// cells in Postgres's spelling, while the snapshot's SQL reads it as an instant: the two
    /// halves of one subset would disagree and nothing would say so.
    #[test]
    fn a_timestamp_literal_the_engine_cannot_read_is_refused_not_compared_as_text() {
        let ts = events_ts();
        for (col, pg_type) in [("starts", "timestamptz"), ("local", "timestamp"), ("day", "date")] {
            for literal in [
                // Postgres reads these, from zone tables and settings the engine does not have.
                "2026-10-19 07:00:00 PST",
                "2026-10-19 07:00:00 America/Los_Angeles",
                "Oct 19 2026",
                "tomorrow",
                // Postgres rejects these too.
                "next tuesday",
                "2026-13-40T07:00:00Z",
                "",
            ] {
                for wrapped in [
                    serde_json::json!({"col": col, "op": "gte", "value": literal}),
                    serde_json::json!({"not": {"or": [{"col": "id", "op": "eq", "value": 1},
                        {"col": col, "op": "eq", "value": literal}]}}),
                ] {
                    let pred: PredicateJson = serde_json::from_value(wrapped).unwrap();
                    let refused = match CompiledPredicate::compile(&pred, &ts) {
                        Err(refused) => format!("{refused:#}"),
                        Ok(_) => panic!("{literal:?} on a {pg_type} column must be refused"),
                    };
                    assert!(
                        refused.contains(&format!("'{col}'")) && refused.contains(pg_type) && refused.contains(literal),
                        "the refusal names the column, its type and the literal: {refused}"
                    );
                }
            }
        }
    }

    /// A literal for an ARRAY, RANGE or MULTIRANGE of timestamps or dates is refused whatever it
    /// says. Postgres reads the literal's elements as instants (`{"2026-10-19 01:00:00-07"}` equals
    /// a row printed `{"2026-10-19 08:00:00+00"}`), so comparing the literal's text would split the
    /// snapshot from the live path. Reading one would mean Postgres's array and range input syntax,
    /// quoting and range canonicalisation included; the engine does not.
    #[test]
    fn a_literal_for_an_array_or_range_of_timestamps_is_refused() {
        let mut def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": {
                "id": {"type":"int"}, "marks": {"type":"text"}, "span": {"type":"text"}, "days": {"type":"text"},
                "spans": {"type":"text"}, "tags": {"type":"text"}
            },
            "primaryKey": "id"
        }))
        .unwrap();
        for (col, pg) in [
            ("marks", "_timestamptz"),
            ("span", "tstzrange"),
            ("days", "_date"),
            ("spans", "tsmultirange"),
            ("tags", "_text"),
        ] {
            def.columns.get_mut(col).unwrap().pg_type = Some(pg.into());
        }
        let ts = TableSchema::from_def(&"events".into(), &def).unwrap();
        for (col, pg_type, literal) in [
            ("marks", "_timestamptz", r#"{"2026-10-19 01:00:00-07"}"#),
            ("marks", "_timestamptz", r#"{"2026-10-19 08:00:00+00"}"#),
            ("span", "tstzrange", r#"["2026-10-19 08:00:00+00","2026-10-20 08:00:00+00")"#),
            ("days", "_date", "{2026-10-19}"),
            ("spans", "tsmultirange", "{}"),
        ] {
            for op in ["eq", "neq", "gte"] {
                let pred: PredicateJson =
                    serde_json::from_value(serde_json::json!({"col": col, "op": op, "value": literal})).unwrap();
                let refused = match CompiledPredicate::compile(&pred, &ts) {
                    Err(refused) => format!("{refused:#}"),
                    Ok(_) => panic!("{literal:?} on a {pg_type} column must be refused"),
                };
                assert!(
                    refused.contains(&format!("'{col}'"))
                        && refused.contains(pg_type)
                        && refused.contains("array, range"),
                    "the refusal names the column and its type, and says what is not read: {refused}"
                );
            }
        }
        // An array of anything else is the application's text, as before.
        let pred: PredicateJson =
            serde_json::from_value(serde_json::json!({"col": "tags", "op": "eq", "value": "{a,b}"})).unwrap();
        assert!(CompiledPredicate::compile(&pred, &ts).is_ok());
    }
}
