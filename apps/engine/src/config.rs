//! Boot configuration resolved from the environment: the `CIRCUITS_*` variables (see the engine's
//! `README.md`). Resolution is a pure function of an env getter ([`Config::resolve`]) so it is
//! unit-testable without touching the process environment.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::table_ref::{TableRef, TableSelector};
use crate::txn_buffer::TxnBufferConfig;

/// Fully-resolved boot configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Postgres connection string (`CIRCUITS_PG_URL`; enables Postgres mode).
    pub pg_url: Option<String>,
    /// Durable-streams base URL (`CIRCUITS_DS_URL`; required for a real run, set by the entrypoint).
    pub ds_url: Option<String>,
    /// HTTP bind address for the control plane (`CIRCUITS_BIND`).
    pub bind: String,
    /// `tracing` EnvFilter string (`CIRCUITS_LOG`).
    pub log_filter: String,
    /// Logical-replication slot name (`CIRCUITS_PG_SLOT`).
    pub slot: String,
    /// Tables to replicate (`CIRCUITS_PG_TABLES`): `schema.name`, a bare name (=
    /// `public.<name>`), or `schema.*` / `*` for "every table with a primary key in that schema"
    /// (`*` and an empty setting both mean `public.*` — see [`TableSelector`]). Malformed entries
    /// are dropped with a warning rather than crashing the boot.
    pub tables: Vec<TableSelector>,
    /// Legacy replication poll interval (ms). Unused since the ingestor streams pgoutput (push
    /// delivery); still parsed so existing `CIRCUITS_PG_POLL_MS` settings are accepted.
    pub poll_ms: u64,
    /// Max pooled Postgres connections for backfills/query-backs (`CIRCUITS_PG_POOL_SIZE`, default 20).
    pub db_pool_size: usize,
    /// Register the introspection surface (`/trace` SSE + `/graph`(`/node`) + `/state`(`/node`) —
    /// the pipeline-visualizer backend). `CIRCUITS_TRACE=0|false|off` disables it: the routes
    /// are never registered, so nothing can subscribe and the hot-path trace gating stays on its
    /// zero-subscriber fast path. Default on. Note: the surface is unauthenticated either way.
    pub trace: bool,
    /// dbsp-backed table arrangements (always built; see `arrangements.rs`). The circuit is
    /// mandatory infrastructure — the sub-knobs below tune it, but it can no longer be turned off.
    pub dbsp: DbspConfig,
    /// Large-transaction handling on the ingest path (ADR-0003): the per-transaction memory cap
    /// before the buffer spills to disk, the spill directory, and the byte budget for one append.
    pub txn: TxnBufferConfig,
    /// Backfill streaming: the byte budget for one backfill append and the off-by-default
    /// slow-backfill `statement_timeout`.
    pub backfill: crate::pg::BackfillConfig,
    /// How long a graceful shutdown may take before it is forced
    /// (`CIRCUITS_SHUTDOWN_GRACE_SECS`).
    pub shutdown_grace: Duration,
    /// How long the HTTP server keeps accepting after a signal, answering `GET /ready` with 503, so
    /// a load balancer's probe sees the drain (`CIRCUITS_SHUTDOWN_DRAIN_SECS`). Comes out
    /// of `shutdown_grace`, not on top of it.
    pub shutdown_ready_drain: Duration,
}

/// Settings for the dbsp arrangement layer (all under `CIRCUITS_DBSP*`).
#[derive(Clone, Debug)]
pub struct DbspConfig {
    /// State directory (`CIRCUITS_DBSP_DIR`; default `./data/dbsp/<slot>` — slot-keyed so parallel
    /// engines and different source databases never share dbsp state).
    pub dir: std::path::PathBuf,
    /// Storage-cache budget in MiB (`CIRCUITS_DBSP_CACHE_MIB`).
    pub cache_mib: Option<usize>,
    /// Spill threshold in KiB (`CIRCUITS_DBSP_MIN_STORAGE_KB`; default 1024 = 1 MiB;
    /// 0 spills everything eligible).
    pub min_storage_bytes: Option<usize>,
    /// Memory ceiling in MiB driving dbsp's pressure-based spilling (`CIRCUITS_DBSP_MAX_RSS_MB`).
    pub max_rss_bytes: Option<u64>,
    /// Checkpoint cadence in seconds (`CIRCUITS_DBSP_CHECKPOINT_SECS`; default 60; 0 = only
    /// at shutdown).
    pub checkpoint_every: Option<Duration>,
    /// Extra lookup indexes beyond the per-table primary key: `table.column[,table.column…]`
    /// (`CIRCUITS_DBSP_INDEXES`). Deprecated and ignored. The table part may itself be
    /// qualified (`schema.name.column`), so the COLUMN is split off the END.
    pub indexes: Vec<(TableRef, String)>,
    /// Counts pipelines: `table:col+col[,table:col…]` (`CIRCUITS_DBSP_COUNTS`). The circuit
    /// maintains a live COUNT per distinct group projection; COUNT aggregates whose predicate
    /// decomposes over these columns are served from the groups.
    pub counts: Vec<(TableRef, Vec<String>)>,
}

fn nonempty(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

impl Config {
    /// Resolve configuration from an env getter. Pure (no process-env access) so precedence is
    /// testable. `Err` is a boot-fatal misconfiguration (an unparseable `CIRCUITS_PG_TABLES`
    /// entry, or a large-transaction knob that could never work — see [`TxnBufferConfig::resolve`]).
    pub fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<Config> {
        let g = |k: &str| nonempty(get(k));

        // Postgres URL. Parsed here (parsing is pure — no I/O) so an unusable one is a NAMED boot
        // refusal rather than a connect that fails identically forever: to the boot classifier a
        // `Config::from_str` failure looks exactly like "the database is not up yet" (no SQLSTATE,
        // no server answer), so without this a typo would back off and re-parse the same broken
        // string every 30 s for ever.
        let pg_url = g("CIRCUITS_PG_URL");
        if let Some(url) = pg_url.as_deref() {
            crate::pg::parse_pg_url(url).context("CIRCUITS_PG_URL")?;
        }
        let ds_url = g("CIRCUITS_DS_URL");

        // Bind address. CIRCUITS_BIND always wins. Otherwise Postgres mode (a deployment) binds
        // 0.0.0.0:3000 and library mode an ephemeral local port.
        let bind = if let Some(b) = g("CIRCUITS_BIND") {
            b
        } else if pg_url.is_some() {
            "0.0.0.0:3000".to_string()
        } else {
            "127.0.0.1:0".to_string()
        };

        // Log filter: CIRCUITS_LOG, a raw EnvFilter; else info.
        let log_filter = g("CIRCUITS_LOG").unwrap_or_else(|| "info".into());

        let slot = g("CIRCUITS_PG_SLOT").unwrap_or_else(|| "circuits".to_string());

        // `schema.name` / bare name (= `public.<name>`) / `schema.*` / `*`. An empty setting leaves
        // the list empty, which `setup_postgres` reads as `public.*` (introspect all).
        //
        // A malformed entry is FATAL, never skipped: skipping it would silently leave a table out of
        // replication — every shape on it refused, every change to it invisible — for a typo, while
        // the neighbouring failure mode (a well-formed name for a table that does not exist) already
        // aborts the boot at introspection. Loud and symmetric beats quietly half-configured.
        let raw_tables = g("CIRCUITS_PG_TABLES").unwrap_or_default();
        let mut tables: Vec<TableSelector> = Vec::new();
        let mut table_errors: Vec<String> = Vec::new();
        for entry in raw_tables.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match TableSelector::parse(entry) {
                Ok(sel) => tables.push(sel),
                Err(e) => table_errors.push(format!("'{entry}': {e:#}")),
            }
        }
        if !table_errors.is_empty() {
            bail!(
                "CIRCUITS_PG_TABLES has {} unusable entr{} ({}). Each entry must be \
                 `schema.name`, a bare `name` (meaning `public.<name>`), `schema.*` (every table with \
                 a primary key in that schema), or `*` (= `public.*`).",
                table_errors.len(),
                if table_errors.len() == 1 { "y" } else { "ies" },
                table_errors.join("; "),
            );
        }

        let poll_ms = g("CIRCUITS_PG_POLL_MS").and_then(|s| s.trim().parse().ok()).unwrap_or(50);

        let db_pool_size =
            g("CIRCUITS_PG_POOL_SIZE").and_then(|s| s.trim().parse::<usize>().ok()).filter(|n| *n >= 1).unwrap_or(20);

        let trace = g("CIRCUITS_TRACE")
            .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off"))
            .unwrap_or(true);

        // The dbsp counts circuit is always built — it is mandatory infrastructure, no longer
        // gated by an on/off flag. It maintains only the configured COUNT groupings (`_COUNTS`);
        // row data lives in Postgres, not here. `_INDEXES` is deprecated and ignored (it configured
        // the removed per-table row arrangements); empty `_INDEXES`/`_COUNTS` are valid.
        let dbsp = DbspConfig {
            // Default dir is keyed by the replication slot: dbsp state is only valid for the
            // database identity it was built from, and parallel engines (conformance harnesses)
            // get disjoint state dirs for free.
            dir: g("CIRCUITS_DBSP_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::Path::new("./data").join("dbsp").join(&slot)),
            cache_mib: g("CIRCUITS_DBSP_CACHE_MIB").and_then(|s| s.trim().parse().ok()),
            min_storage_bytes: Some(
                g("CIRCUITS_DBSP_MIN_STORAGE_KB").and_then(|s| s.trim().parse::<usize>().ok()).unwrap_or(1024) * 1024,
            ),
            max_rss_bytes: g("CIRCUITS_DBSP_MAX_RSS_MB")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|mb| mb * 1024 * 1024),
            checkpoint_every: match g("CIRCUITS_DBSP_CHECKPOINT_SECS").and_then(|s| s.trim().parse::<u64>().ok()) {
                Some(0) => None,
                Some(s) => Some(Duration::from_secs(s)),
                None => Some(Duration::from_secs(60)),
            },
            indexes: g("CIRCUITS_DBSP_INDEXES")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| {
                    // `table.column`, where `table` may itself be `schema.name` — the COLUMN is the
                    // last dotted part, so split from the right.
                    let (t, c) = s.trim().rsplit_once('.')?;
                    Some((TableRef::parse(t.trim()).ok()?, c.trim().to_string()))
                })
                .collect(),
            counts: g("CIRCUITS_DBSP_COUNTS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| {
                    let (t, cols) = s.trim().split_once(':')?;
                    let cols: Vec<String> =
                        cols.split('+').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
                    if cols.is_empty() { None } else { Some((TableRef::parse(t.trim()).ok()?, cols)) }
                })
                .collect(),
        };

        // Large transactions (ADR-0003). Boot-fatal on an unusable setting: a memory cap or append
        // budget that was meant to be applied and silently was not is the worst of both worlds.
        let txn = TxnBufferConfig::resolve(&g).context("large-transaction configuration")?;

        // Streamed backfills. Same stance as the large-transaction knobs: a budget that was meant
        // to be applied and silently was not is worse than a refused boot.
        let d = crate::pg::BackfillConfig::default();
        let append_bytes = match g("CIRCUITS_BACKFILL_APPEND_BYTES") {
            None => d.append_bytes,
            Some(raw) => raw.trim().parse::<u64>().map_err(|_| {
                anyhow::anyhow!("CIRCUITS_BACKFILL_APPEND_BYTES must be a byte count, got '{}'", raw.trim())
            })?,
        };
        if append_bytes == 0 {
            bail!(
                "CIRCUITS_BACKFILL_APPEND_BYTES must be a positive byte count (it bounds one \
                 backfill append's request body); 0 would make every shape unbackfillable"
            );
        }
        if append_bytes > crate::txn_buffer::DS_MAX_BODY_BYTES {
            bail!(
                "CIRCUITS_BACKFILL_APPEND_BYTES is {append_bytes}, above the durable-streams \
                 request-body cap of {} bytes; an append that large could never land",
                crate::txn_buffer::DS_MAX_BODY_BYTES
            );
        }
        let statement_timeout_ms = match g("CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS") {
            None => d.statement_timeout_ms,
            Some(raw) => raw.trim().parse::<u64>().map_err(|_| {
                anyhow::anyhow!(
                    "CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS must be a whole number of \
                     milliseconds (0 = off), got '{}'",
                    raw.trim()
                )
            })?,
        };
        let backfill = crate::pg::BackfillConfig { append_bytes, statement_timeout_ms };

        let shutdown_grace = crate::shutdown::resolve_grace(&g).context("shutdown configuration")?;
        let shutdown_ready_drain = crate::shutdown::resolve_ready_drain(&g).context("shutdown configuration")?;
        if shutdown_ready_drain >= shutdown_grace {
            bail!(
                "CIRCUITS_SHUTDOWN_DRAIN_SECS ({}s) must be less than \
                 CIRCUITS_SHUTDOWN_GRACE_SECS ({}s): the drain comes OUT of the grace, and \
                 spending all of it advertising 503 leaves nothing to finish an in-flight commit in",
                shutdown_ready_drain.as_secs(),
                shutdown_grace.as_secs(),
            );
        }

        Ok(Config {
            pg_url,
            ds_url,
            bind,
            log_filter,
            slot,
            tables,
            poll_ms,
            db_pool_size,
            trace,
            dbsp,
            txn,
            backfill,
            shutdown_grace,
            shutdown_ready_drain,
        })
    }

    /// Resolve from the real process environment.
    pub fn from_env() -> Result<Config> {
        Config::resolve(|k| std::env::var(k).ok())
    }

    /// The resolved configuration with the Postgres URL's credentials redacted — safe to log.
    pub fn redacted(&self) -> String {
        format!(
            "bind={} pg_url={} ds_url={} slot={} trace={} log={} \
             txn_memory_bytes={} changes_append_bytes={} txn_spill_dir={} backfill_append_bytes={} \
             backfill_statement_timeout_ms={} shutdown_grace={:?} shutdown_ready_drain={:?}",
            self.bind,
            self.pg_url.as_deref().map(redact_url).unwrap_or_else(|| "<none>".into()),
            self.ds_url.as_deref().unwrap_or("<none>"),
            self.slot,
            self.trace,
            self.log_filter,
            self.txn.memory_bytes,
            self.txn.append_bytes,
            self.txn.spill_dir.display(),
            self.backfill.append_bytes,
            self.backfill.statement_timeout_ms,
            self.shutdown_grace,
            self.shutdown_ready_drain,
        )
    }
}

/// Redact `user:pass@` credentials from a Postgres/URL connection string for logging.
fn redact_url(url: &str) -> String {
    // scheme://user:pass@host/... -> scheme://***@host/...
    //
    // Split at the LAST `@`, not the first: a password may legally contain one (`p@ss`), and
    // splitting at the first would keep everything after it — i.e. the tail of the password — in a
    // line that exists precisely to be safe to log. A host name cannot contain an `@`, so the last
    // one always ends the userinfo.
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.rsplit_once('@') {
            Some((_creds, host)) => format!("{scheme}://***@{host}"),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Config {
        try_cfg(pairs).expect("valid test config")
    }

    fn try_cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let map: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Config::resolve(move |k| map.get(k).cloned())
    }

    /// Postgres mode comes from `CIRCUITS_PG_URL` alone; `DATABASE_URL` is not read.
    #[test]
    fn pg_url_is_circuits_pg_url() {
        let c = cfg(&[("CIRCUITS_PG_URL", "postgres://circuits")]);
        assert_eq!(c.pg_url.as_deref(), Some("postgres://circuits"));
        assert_eq!(cfg(&[("DATABASE_URL", "postgres://other")]).pg_url, None);
        assert_eq!(cfg(&[]).pg_url, None);
    }

    /// A connection string the driver cannot parse refuses the boot HERE, where every other
    /// unusable setting is refused — never in the connect loop, which cannot tell it apart from a
    /// database that has not come up yet and would retry it for ever.
    #[test]
    fn an_unparseable_pg_url_refuses_the_boot() {
        let e = Config::resolve(|k| match k {
            "CIRCUITS_PG_URL" => Some("postgres://u@host:notaport/db".into()),
            _ => None,
        })
        .expect_err("an unusable connection string must not resolve");
        let msg = format!("{e:#}");
        assert!(msg.contains("unusable Postgres URL"), "{msg}");
    }

    /// A password containing an `@` must not leak its tail into the "safe to log" config line.
    #[test]
    fn redaction_splits_at_the_last_at_not_the_first() {
        assert_eq!(redact_url("postgres://u:p@ss@host:5432/db"), "postgres://***@host:5432/db");
        assert_eq!(redact_url("postgres://u:p@host/db"), "postgres://***@host/db");
        assert_eq!(redact_url("postgres://host/db"), "postgres://host/db", "no userinfo, nothing to redact");
        assert_eq!(redact_url("not a url"), "not a url");
    }

    #[test]
    fn pg_url_tolerates_sslmode_disable() {
        // We don't strip it — tokio-postgres accepts sslmode in the conn string. Just confirm it
        // passes through verbatim so the connect string is unchanged.
        let url = "postgresql://postgres:password@proxy:5433/postgres?sslmode=disable";
        let c = cfg(&[("CIRCUITS_PG_URL", url)]);
        assert_eq!(c.pg_url.as_deref(), Some(url));
    }

    #[test]
    fn bind_precedence() {
        // nothing set -> dev default
        assert_eq!(cfg(&[]).bind, "127.0.0.1:0");
        // Postgres mode, no bind -> 0.0.0.0:3000
        assert_eq!(cfg(&[("CIRCUITS_PG_URL", "postgres://x")]).bind, "0.0.0.0:3000");
        // CIRCUITS_BIND always wins
        assert_eq!(cfg(&[("CIRCUITS_BIND", "127.0.0.1:9"), ("CIRCUITS_PG_URL", "postgres://x")]).bind, "127.0.0.1:9");
    }

    #[test]
    fn log_filter() {
        assert_eq!(cfg(&[]).log_filter, "info");
        // CIRCUITS_LOG passes through verbatim
        assert_eq!(cfg(&[("CIRCUITS_LOG", "circuits_engine=debug")]).log_filter, "circuits_engine=debug");
    }

    #[test]
    fn slot_name() {
        assert_eq!(cfg(&[]).slot, "circuits");
        assert_eq!(cfg(&[("CIRCUITS_PG_SLOT", "custom")]).slot, "custom");
    }

    #[test]
    fn pg_pool_size() {
        assert_eq!(cfg(&[]).db_pool_size, 20);
        assert_eq!(cfg(&[("CIRCUITS_PG_POOL_SIZE", "5")]).db_pool_size, 5);
        // A pool of no connections could never serve a backfill: the default stands.
        assert_eq!(cfg(&[("CIRCUITS_PG_POOL_SIZE", "0")]).db_pool_size, 20);
        assert_eq!(cfg(&[("CIRCUITS_PG_POOL_SIZE", "many")]).db_pool_size, 20);
    }

    #[test]
    fn trace_flag() {
        assert!(cfg(&[]).trace, "introspection defaults on");
        assert!(!cfg(&[("CIRCUITS_TRACE", "0")]).trace);
        assert!(!cfg(&[("CIRCUITS_TRACE", "false")]).trace);
        assert!(!cfg(&[("CIRCUITS_TRACE", "off")]).trace);
        assert!(cfg(&[("CIRCUITS_TRACE", "1")]).trace);
        assert!(cfg(&[("CIRCUITS_TRACE", "true")]).trace);
    }

    #[test]
    fn dbsp_circuit_is_always_built() {
        // The circuit is mandatory infrastructure: no on/off flag. With nothing configured it
        // still resolves, with an empty index/counts config and a slot-keyed default state dir.
        let c = cfg(&[]);
        assert!(c.dbsp.indexes.is_empty(), "empty _INDEXES is valid");
        assert!(c.dbsp.counts.is_empty(), "empty _COUNTS is valid");
        assert!(c.dbsp.dir.ends_with("dbsp/circuits"), "default dir is slot-keyed: {:?}", c.dbsp.dir);
        assert_eq!(c.dbsp.checkpoint_every, Some(Duration::from_secs(60)));
        assert_eq!(c.dbsp.min_storage_bytes, Some(1024 * 1024));
    }

    #[test]
    fn dbsp_tunables_parse() {
        let c = cfg(&[
            ("CIRCUITS_DBSP_DIR", "/tmp/dbsp"),
            ("CIRCUITS_DBSP_INDEXES", "todos.list_id, list_members.user_id"),
            ("CIRCUITS_DBSP_COUNTS", "todos:list_id+done"),
            ("CIRCUITS_DBSP_CHECKPOINT_SECS", "0"),
            ("CIRCUITS_DBSP_MIN_STORAGE_KB", "2048"),
        ]);
        assert_eq!(c.dbsp.dir, std::path::PathBuf::from("/tmp/dbsp"));
        assert_eq!(
            c.dbsp.indexes,
            vec![
                (TableRef::parse("public.todos").unwrap(), "list_id".to_string()),
                (TableRef::parse("public.list_members").unwrap(), "user_id".to_string()),
            ]
        );
        assert_eq!(
            c.dbsp.counts,
            vec![(TableRef::parse("public.todos").unwrap(), vec!["list_id".to_string(), "done".to_string()])]
        );
        assert_eq!(c.dbsp.checkpoint_every, None, "0 means checkpoint only at shutdown");
        assert_eq!(c.dbsp.min_storage_bytes, Some(2048 * 1024));
    }

    /// The selector grammar, and the fact that a typo is FATAL rather than quietly dropped — a
    /// skipped entry would leave that table out of replication with nothing but a log line to say so.
    #[test]
    fn pg_tables_selectors_parse_and_typos_are_fatal() {
        use crate::table_ref::TableRef;
        let c = cfg(&[("CIRCUITS_PG_TABLES", "items, other.items , reporting.*, *")]);
        assert_eq!(
            c.tables,
            vec![
                TableSelector::One(TableRef::parse("public.items").unwrap()),
                TableSelector::One(TableRef::parse("other.items").unwrap()),
                TableSelector::AllIn("reporting".into()),
                TableSelector::AllIn("public".into()),
            ]
        );
        assert!(cfg(&[]).tables.is_empty(), "empty setting stays empty (setup_postgres reads it as public.*)");

        for bad in ["a.b.c", "items, a.b.c", "*.*", "foo.*bar", "."] {
            let err = try_cfg(&[("CIRCUITS_PG_TABLES", bad)])
                .expect_err(&format!("{bad:?} must abort the boot, not be skipped"));
            let msg = format!("{err:#}");
            assert!(msg.contains("CIRCUITS_PG_TABLES"), "{msg}");
            assert!(msg.contains("schema.*"), "the message must state the rule: {msg}");
        }
    }
}
