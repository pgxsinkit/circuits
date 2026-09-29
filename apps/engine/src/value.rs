//! The dynamically-typed Z-set element types: scalar `Value`, positional `Row`,
//! and the weighted-delta pair `Tup2<Row, ZWeight>`.
//!
//! `Tup2`/`ZWeight` are dbsp's own (re-exported), and `Value`/`Row` carry the full
//! `dbsp::DBData` derive stack (rkyv archive + SizeOf + IsNone) so they can be keys and
//! values in the storage-backed arrangements (`src/arrangements.rs`). The engine's hand
//! rolled executors keep using them as plain Rust values; only the arrangement layer
//! exercises the archive impls (batches serialized to layer files).

use anyhow::{Context, Result, bail};
use feldera_macros::IsNone;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::heap_size::HeapSize;
use crate::schema::ColumnType;

/// Signed multiplicity of a Z-set element: `+1` insert, `-1` delete.
pub use dbsp::ZWeight;

/// A weighted pair, the element of a Z-set delta (`Tup2(row, weight)`).
pub use dbsp::utils::Tup2;

/// A scalar cell value. `Float` wraps `OrderedFloat` because a bare `f64` is not
/// `Eq`/`Ord`/`Hash` and so could not be a map key (aggregate multisets, routing indexes).
#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize, Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub enum Value {
    #[default]
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(OrderedFloat<f64>),
}

/// Integers a JSON **number** round-trips through IEEE-754 double without loss: `2^53 - 1`. Beyond
/// it every JSON parser that decodes numbers as doubles (every JavaScript one) silently drops the
/// low digits, so an exact value has to leave the number space — the same rule the aggregate `SUM`
/// encoding already follows (`engine::executors::AggSum::to_json`).
pub const JSON_EXACT_INT_MAX: u64 = 9_007_199_254_740_991;

impl Value {
    /// Parse a JSON scalar into a `Value` of the given column type. `null` -> `Null`.
    ///
    /// An `int` column also accepts a **decimal string** — that is what
    /// [`to_json`](Self::to_json) emits beyond [`JSON_EXACT_INT_MAX`], so the encoding round-trips.
    pub fn from_json(j: &serde_json::Value, ty: ColumnType) -> Result<Value> {
        if j.is_null() {
            return Ok(Value::Null);
        }
        Ok(match ty {
            ColumnType::Int => match j {
                serde_json::Value::String(s) => {
                    Value::Int(s.parse().with_context(|| format!("expected an integer, got '{s}'"))?)
                }
                _ => Value::Int(j.as_i64().context("expected an integer")?),
            },
            ColumnType::Float => Value::Float(OrderedFloat(j.as_f64().context("expected a float")?)),
            ColumnType::Text => Value::Text(j.as_str().context("expected a string")?.to_string()),
            ColumnType::Bool => Value::Bool(j.as_bool().context("expected a bool")?),
        })
    }

    /// Type a **predicate literal** (not a data cell) against a column type, leniently: a string
    /// literal is coerced into the target type (`'5'` → int 5, `'t'` → bool true, …), matching
    /// Postgres unknown-literal coercion, so a string literal can compare against a non-text column.
    /// Typed JSON (number/bool) stays strict — same as [`from_json`], so a bare `5` against a text
    /// column still errors.
    pub fn literal_from_json(j: &serde_json::Value, ty: ColumnType) -> Result<Value> {
        if let serde_json::Value::String(s) = j {
            return Ok(match ty {
                ColumnType::Text => Value::Text(s.clone()),
                ColumnType::Int => Value::Int(s.parse().with_context(|| format!("invalid integer literal '{s}'"))?),
                ColumnType::Float => {
                    Value::Float(OrderedFloat(s.parse().with_context(|| format!("invalid float literal '{s}'"))?))
                }
                ColumnType::Bool => match s.as_str() {
                    "t" | "true" | "TRUE" | "True" => Value::Bool(true),
                    "f" | "false" | "FALSE" | "False" => Value::Bool(false),
                    _ => bail!("invalid boolean literal '{s}'"),
                },
            });
        }
        Value::from_json(j, ty)
    }

    /// Parse a stringified primary-key (the durable-stream event `key`) into a typed `Value`.
    pub fn from_key_string(s: &str, ty: ColumnType) -> Result<Value> {
        Ok(match ty {
            ColumnType::Int => Value::Int(s.parse().context("pk is not an integer")?),
            ColumnType::Float => Value::Float(OrderedFloat(s.parse().context("pk is not a float")?)),
            ColumnType::Text => Value::Text(s.to_string()),
            ColumnType::Bool => Value::Bool(s.parse().context("pk is not a bool")?),
        })
    }

    /// The wire encoding of a cell: everything a JSON number can carry exactly stays a number, and
    /// an integer outside that range (`|v| > `[`JSON_EXACT_INT_MAX`]) becomes a decimal **string**.
    ///
    /// Postgres `bigint` reaches `2^63-1`, so a single cell can already exceed what a JavaScript
    /// JSON parser reproduces — and the engine does not hand back a silently rounded number. This is
    /// the same rule the aggregate `SUM` encoding follows; it applies to every row value the engine
    /// serialises (shape-stream envelopes, `/query` pages, subset pages, MIN/MAX of an int column).
    /// [`from_json`](Self::from_json) accepts the string form back, so the encoding round-trips.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Int(i) if i.unsigned_abs() <= JSON_EXACT_INT_MAX => (*i).into(),
            Value::Int(i) => serde_json::Value::String(i.to_string()),
            Value::Float(f) => serde_json::json!(f.0),
            Value::Text(s) => s.clone().into(),
            Value::Bool(b) => (*b).into(),
        }
    }

    /// String form used as the durable-stream event `key` (the primary key).
    pub fn to_key_string(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.0.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
        }
    }
}

/// A row is a positional vector of cell values; the schema gives names to the positions.
#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize, Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get(&self, idx: usize) -> Result<&Value> {
        self.0.get(idx).with_context(|| format!("column index {idx} out of range"))
    }
}

impl HeapSize for Value {
    /// Only `Text` owns heap (the `String`); every other variant is inline.
    fn heap_bytes(&self) -> usize {
        match self {
            Value::Text(s) => s.heap_bytes(),
            Value::Null | Value::Int(_) | Value::Bool(_) | Value::Float(_) => 0,
        }
    }
}

impl HeapSize for Row {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

/// `Tup2` is dbsp's own type (foreign), but `HeapSize` is ours — allowed under the orphan
/// rule. Only the two Z-set delta shapes the engine actually stores are covered (widening this
/// blindly to `Tup2<A, B>` would need `B: HeapSize` too, which `ZWeight` (a bare `i64`) already
/// satisfies via the leaf macro, but keeping the impl concrete avoids accidentally covering
/// unrelated `Tup2` uses elsewhere with unreviewed semantics).
impl HeapSize for Tup2<Row, ZWeight> {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire rule for `int` cells: a JSON number while it round-trips through a double, an exact
    /// decimal string beyond — and `from_json` accepts the string back, so a value survives a
    /// serialise/parse cycle unchanged.
    #[test]
    fn an_int_beyond_2_53_keeps_its_exact_wire_form() {
        let exact = |v: i64| Value::Int(v).to_json();
        assert_eq!(exact(0), serde_json::json!(0));
        assert_eq!(exact(JSON_EXACT_INT_MAX as i64), serde_json::json!(9_007_199_254_740_991i64));
        assert_eq!(exact(-(JSON_EXACT_INT_MAX as i64)), serde_json::json!(-9_007_199_254_740_991i64));
        // 2^53 + 1: the first value a JavaScript JSON parser cannot reproduce.
        assert_eq!(exact(9_007_199_254_740_993), serde_json::json!("9007199254740993"));
        assert_eq!(exact(i64::MIN), serde_json::json!("-9223372036854775808"));

        for v in [0i64, 1, -1, JSON_EXACT_INT_MAX as i64, 9_007_199_254_740_993, i64::MAX, i64::MIN] {
            let round = Value::from_json(&Value::Int(v).to_json(), ColumnType::Int).unwrap();
            assert_eq!(round, Value::Int(v), "int {v} must survive the wire encoding");
        }
        // Only integers change form; the other types are unaffected.
        assert_eq!(Value::Float(OrderedFloat(1.5)).to_json(), serde_json::json!(1.5));
        assert_eq!(Value::Text("9007199254740993".into()).to_json(), serde_json::json!("9007199254740993"));
    }

    /// A non-numeric string against an int column is still an error — the string form is an exact
    /// integer encoding, not a lenient cast.
    #[test]
    fn a_non_numeric_string_is_not_an_int() {
        assert!(Value::from_json(&serde_json::json!("nope"), ColumnType::Int).is_err());
    }
}

/// Best-effort sanity check used by JSON parsing paths.
pub fn ensure_object(j: &serde_json::Value) -> Result<&serde_json::Map<String, serde_json::Value>> {
    match j.as_object() {
        Some(m) => Ok(m),
        None => bail!("expected a JSON object, got {j}"),
    }
}
