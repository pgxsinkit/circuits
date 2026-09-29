//! Table identity: a Postgres relation is `(schema, name)`, and its canonical spelling is
//! `schema.name` — everywhere (ADR-0002).
//!
//! One table has ONE spelling: the envelope `type` on the change log and shape streams, the `table`
//! field of every native-API request/response, the durable catalog's `rec.table`, sharing
//! signatures, trace/graph node ids and StatsD tags all carry `TableRef`'s canonical `Display` form.
//! `users` and `public.users` are the same table, so they must never become two separately
//! maintained shapes.
//!
//! A bare name is accepted **only at ingress** as sugar for `public.<name>` and canonicalised right
//! there ([`TableRef::parse`] is the single parse rule; there is no second one). Everything past the
//! boundary is typed.
//!
//! ## Representation
//!
//! The canonical string is stored once (`schema.name`) with the separator's byte offset, rather than
//! two independent `String`s. That keeps the type to one allocation, makes an invalid value
//! unconstructible (the fields are private; every path in goes through validation), and — the
//! ergonomic reason — gives [`TableRef::as_str`] a borrow of the canonical form for the sites that
//! genuinely need a `&str`: the `Arc<str>` bucket keys in [`crate::subq_index`], trace/graph node
//! ids, StatsD tags, `format!` messages. [`TableRef::schema`] / [`TableRef::name`] hand out the two
//! parts.

use std::fmt;

use anyhow::{Result, bail};

/// The schema a bare table name resolves to.
pub const PUBLIC_SCHEMA: &str = "public";

/// The engine's own reserved schema. **No table identity may live here**, so no envelope the
/// ingestor produces can ever collide with the engine's own control envelopes on the change log
/// (`__circuits.control`, ADR-0006) — the ADR's "a spelling no tracked table can produce" is
/// enforced at the one place table identities are built, rather than assumed.
pub const RESERVED_SCHEMA: &str = "__circuits";

/// A schema-qualified table identity. `Display`/[`as_str`](TableRef::as_str) render the canonical
/// `schema.name` (unquoted, exactly one dot); [`quote_qualified`](TableRef::quote_qualified) renders
/// the SQL form `"schema"."name"`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRef {
    /// `schema.name`, validated at construction.
    canon: String,
    /// Byte offset of the separating `.` within `canon`.
    dot: usize,
}

/// Reject a schema the engine reserves for itself (see [`RESERVED_SCHEMA`]) as well as everything
/// [`check_part`] rejects. Every path that names a schema goes through here.
fn check_schema(schema: &str) -> Result<()> {
    check_part("schema", schema)?;
    if schema == RESERVED_SCHEMA {
        bail!(
            "'{RESERVED_SCHEMA}' is reserved by the engine (its change-log control envelopes are typed \
             '{RESERVED_SCHEMA}.*'); no table may be tracked in it"
        );
    }
    Ok(())
}

/// Reject anything that would make the canonical `schema.name` form ambiguous or need quoting: an
/// empty part, an embedded dot (which would spell a second separator), or a double quote (which
/// [`TableRef::quote_qualified`] would have to escape into a name that no longer round-trips through
/// [`TableRef::parse`]).
fn check_part(what: &str, part: &str) -> Result<()> {
    if part.is_empty() {
        bail!("table reference has an empty {what}");
    }
    if part.contains('.') {
        bail!("table reference {what} '{part}' contains a '.'; use exactly one schema.name separator");
    }
    if part.contains('"') {
        bail!("table reference {what} '{part}' contains a '\"'; quoted identifiers are not supported");
    }
    // `*` is [`TableSelector`]'s wildcard, not an identifier character. Without this, `*.*` would
    // parse as the schema literally named `*` and `foo.*bar` as a table literally named `*bar` —
    // both silently selecting nothing instead of being reported as the typos they are.
    if part.contains('*') {
        bail!(
            "table reference {what} '{part}' contains a '*'; the only wildcard forms are `schema.*` \
             and `*` (= `public.*`)"
        );
    }
    Ok(())
}

impl TableRef {
    /// Build from an explicit schema + name. Errors on an empty/dotted/quoted part.
    pub fn new(schema: &str, name: &str) -> Result<Self> {
        check_schema(schema)?;
        check_part("name", name)?;
        Ok(TableRef { canon: format!("{schema}.{name}"), dot: schema.len() })
    }

    /// Build a `public.<name>` reference (the bare-name sugar, made explicit).
    pub fn public(name: &str) -> Result<Self> {
        TableRef::new(PUBLIC_SCHEMA, name)
    }

    /// The one parse rule for a textual table reference (ADR-0002):
    /// - `schema.name` → exactly as given,
    /// - `name` (no dot) → `public.name`,
    /// - anything else (empty part, more than one dot, quotes) → an error.
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            bail!("empty table reference");
        }
        match s.split_once('.') {
            Some((schema, name)) => TableRef::new(schema, name),
            None => TableRef::public(s),
        }
    }

    /// The schema part (`public` for a bare-name ingress).
    pub fn schema(&self) -> &str {
        &self.canon[..self.dot]
    }

    /// The table part.
    pub fn name(&self) -> &str {
        &self.canon[self.dot + 1..]
    }

    /// The canonical `schema.name` string — what goes on the wire, in the catalog, in signatures,
    /// trace ids and metrics tags.
    pub fn as_str(&self) -> &str {
        &self.canon
    }

    /// True for a `public.*` table (the default schema).
    pub fn is_public(&self) -> bool {
        self.schema() == PUBLIC_SCHEMA
    }

    /// The SQL identifier form: `"schema"."name"`, each part double-quoted with internal quotes
    /// doubled (a no-op here — [`check_part`] rejects quotes — but the rule is the shared one).
    pub fn quote_qualified(&self) -> String {
        format!("{}.{}", crate::pg::quote_ident(self.schema()), crate::pg::quote_ident(self.name()))
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canon)
    }
}

impl fmt::Debug for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TableRef({})", self.canon)
    }
}

impl std::str::FromStr for TableRef {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        TableRef::parse(s)
    }
}

/// Test-only convenience so unit tests can write `"users".into()` / `table: "t".into()`. Panics on
/// an invalid reference — a test that spells one is a bug in the test, not a runtime path.
#[cfg(test)]
impl From<&str> for TableRef {
    fn from(s: &str) -> Self {
        TableRef::parse(s).expect("valid table reference in test")
    }
}

impl crate::heap_size::HeapSize for TableRef {
    fn heap_bytes(&self) -> usize {
        self.canon.capacity()
    }
}

impl serde::Serialize for TableRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.canon)
    }
}

/// Deserialization IS the ingress canonicalisation: every request body / query param / catalog
/// record that carries a table goes through [`TableRef::parse`], so a bare name becomes
/// `public.<name>` and `a.b.c` is a deserialization error (⇒ 4xx) rather than a silently wrong
/// lookup deeper in.
impl<'de> serde::Deserialize<'de> for TableRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = TableRef;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a table reference (`schema.name`, or a bare name meaning `public.<name>`)")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<TableRef, E> {
                TableRef::parse(v).map_err(|e| E::custom(format!("{e:#}")))
            }
        }
        d.deserialize_str(V)
    }
}

/// What one `CIRCUITS_PG_TABLES` entry selects.
///
/// `*` (or an empty setting) means every `public` table with a primary key — deliberately NOT every
/// schema: introspect-all runs `ALTER TABLE … REPLICA IDENTITY FULL`, and doing that across managed
/// system schemas is not the engine's call. `schema.*` opts a specific schema in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableSelector {
    /// Every table with a primary key in this schema.
    AllIn(String),
    /// Exactly this table.
    One(TableRef),
}

impl TableSelector {
    /// Parse one entry: `*` / `schema.*` / `schema.name` / `name` (bare ⇒ `public.name`).
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s == "*" {
            return Ok(TableSelector::AllIn(PUBLIC_SCHEMA.to_string()));
        }
        if let Some(schema) = s.strip_suffix(".*") {
            check_schema(schema)?;
            return Ok(TableSelector::AllIn(schema.to_string()));
        }
        Ok(TableSelector::One(TableRef::parse(s)?))
    }
}

impl fmt::Display for TableSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSelector::AllIn(schema) => write!(f, "{schema}.*"),
            TableSelector::One(t) => write!(f, "{t}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine's control envelopes on the change log are typed `__circuits.control` (ADR-0006),
    /// and every reader skips them BY TYPE. That is only sound if no tracked table can ever carry
    /// that type — so the schema is refused wherever a table identity or selector is built, not
    /// merely assumed to be unused.
    #[test]
    fn the_reserved_schema_can_never_name_a_table() {
        let err = TableRef::parse("__circuits.control").expect_err("the control type is not a table");
        assert!(err.to_string().contains("reserved"), "{err}");
        assert!(TableRef::new("__circuits", "anything").is_err());
        // ...including through the config surface, table-by-table or wholesale.
        assert!(TableSelector::parse("__circuits.items").is_err());
        let err = TableSelector::parse("__circuits.*").expect_err("nor a wildcard over it");
        assert!(err.to_string().contains("reserved"), "{err}");
        // A table merely NAMED like it, in a real schema, is ordinary data.
        assert!(TableRef::parse("public.__circuits").is_ok());
        assert!(TableRef::parse("__circuits_archive.items").is_ok());
    }

    #[test]
    fn bare_name_is_public_sugar() {
        let t = TableRef::parse("users").unwrap();
        assert_eq!(t.schema(), "public");
        assert_eq!(t.name(), "users");
        assert_eq!(t.to_string(), "public.users");
        assert_eq!(t.as_str(), "public.users");
        assert!(t.is_public());
    }

    #[test]
    fn qualified_is_taken_as_given() {
        let t = TableRef::parse("other.items").unwrap();
        assert_eq!(t.schema(), "other");
        assert_eq!(t.name(), "items");
        assert_eq!(t.to_string(), "other.items");
        assert!(!t.is_public());
    }

    /// One spelling per table: the bare sugar and the explicit form are the SAME value, so they
    /// hash/compare equal and can never become two separately maintained shapes.
    #[test]
    fn bare_and_qualified_public_are_one_identity() {
        assert_eq!(TableRef::parse("users").unwrap(), TableRef::parse("public.users").unwrap());
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TableRef::parse("users").unwrap());
        assert!(set.contains(&TableRef::parse("public.users").unwrap()));
        // ...and a same-named table in another schema is a DIFFERENT identity.
        assert!(!set.contains(&TableRef::parse("other.users").unwrap()));
    }

    #[test]
    fn display_round_trips_through_parse() {
        for s in ["users", "public.users", "other.items"] {
            let t = TableRef::parse(s).unwrap();
            assert_eq!(TableRef::parse(&t.to_string()).unwrap(), t);
        }
    }

    #[test]
    fn rejects_malformed_references() {
        for bad in [
            "a.b.c", "", "   ", ".", "a.", ".b", "a..b", "\"a\".b", "a.\"b\"", "us\"ers", "*", "*.*", "foo.*bar",
            "*.items",
        ] {
            assert!(TableRef::parse(bad).is_err(), "expected {bad:?} to be rejected");
        }
    }

    #[test]
    fn quote_qualified_double_quotes_each_part() {
        assert_eq!(TableRef::parse("users").unwrap().quote_qualified(), r#""public"."users""#);
        assert_eq!(TableRef::parse("other.items").unwrap().quote_qualified(), r#""other"."items""#);
    }

    #[test]
    fn new_rejects_parts_that_would_re_parse_differently() {
        assert!(TableRef::new("a.b", "c").is_err());
        assert!(TableRef::new("a", "b.c").is_err());
        assert!(TableRef::new("", "c").is_err());
        assert!(TableRef::new("a", "").is_err());
    }

    #[test]
    fn selector_parses_star_forms() {
        assert_eq!(TableSelector::parse("*").unwrap(), TableSelector::AllIn("public".into()));
        assert_eq!(TableSelector::parse("other.*").unwrap(), TableSelector::AllIn("other".into()));
        assert_eq!(
            TableSelector::parse("items").unwrap(),
            TableSelector::One(TableRef::parse("public.items").unwrap())
        );
        assert_eq!(
            TableSelector::parse("other.items").unwrap(),
            TableSelector::One(TableRef::parse("other.items").unwrap())
        );
        assert!(TableSelector::parse(".*").is_err());
        assert!(TableSelector::parse("a.b.c").is_err());
        // A `*` anywhere but the two wildcard forms is a typo, not an identifier.
        assert!(TableSelector::parse("*.*").is_err());
        assert!(TableSelector::parse("foo.*bar").is_err());
        assert!(TableSelector::parse("*.items").is_err());
    }

    #[test]
    fn serde_round_trips_and_canonicalises() {
        let t: TableRef = serde_json::from_str("\"items\"").unwrap();
        assert_eq!(t.to_string(), "public.items");
        assert_eq!(serde_json::to_string(&t).unwrap(), "\"public.items\"");
        assert!(serde_json::from_str::<TableRef>("\"a.b.c\"").is_err());
        // Map keys go through the same rule (the library-mode `Schema.tables` object).
        let m: std::collections::BTreeMap<TableRef, u8> =
            serde_json::from_str(r#"{"items":1,"other.items":2}"#).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m[&TableRef::parse("public.items").unwrap()], 1);
        assert_eq!(m[&TableRef::parse("other.items").unwrap()], 2);
    }
}
