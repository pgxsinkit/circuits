// Adapted from indexedlabs/electric-circuits PR #27, final commit d37951100fcfb681a3f0da260ff992681b60a164.
// https://github.com/indexedlabs/electric-circuits/pull/27
//! Settled snapshots: the record of what the sequencer has fanned out, the shared poller that
//! learns when those transactions become visible, and the bounded wait a snapshot takes when it
//! excludes one of them.
//!
//! PostgreSQL makes a transaction visible to new snapshots only when its backend leaves the
//! ProcArray, AFTER the commit record is flushed — and with synchronous replication, after the
//! standby acknowledged it, which can take arbitrarily long. Logical decoding reads the flushed
//! record, so the engine can sequence a transaction T (and append it to every shape and subset
//! feed) while T is still invisible. A snapshot taken in that window excludes T, and T's envelopes
//! are already behind whatever position the snapshot is paired with — a backfill's `BeginShape`
//! point or a subset client's HEAD offset — so T is in neither the snapshot nor the live tail after
//! it. The xid gate cannot help: it decides what to skip, not what was never delivered.
//!
//! So every snapshot a backfill or query-back reads is **settled** ([`begin_settled_snapshot`]):
//!
//! * The sequencer notes each transaction's xid, per table it touched, BEFORE it fans the
//!   transaction out ([`SequencedXids::note`]).
//! * A snapshot is checked only against the tables its read depends on ([`SettleScope`]), as their
//!   record stood just BEFORE the snapshot was opened (the poller may prune a transaction that
//!   became visible after the snapshot was taken, before it is checked), and only at the places a
//!   sequenced transaction can hide from it: the snapshot's `xip` list and
//!   `[xmax, highest sequenced]`. A sequenced transaction below `xmin`, or between `xmin` and `xmax`
//!   and not in `xip`, is visible by definition. A transaction the engine never sequenced — an
//!   open writer pinning `xmin`, however old — is never looked at, so it can never block a settle.
//! * A snapshot that excludes a sequenced transaction is rolled back, its pooled connection is given
//!   back, and the caller waits (bounded by `CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS`, pool
//!   wait for the retake included) until the shared [poller](SequencedXids) has seen those
//!   transactions visible. It then takes a connection again and one more snapshot, which is settled
//!   by construction (see [`begin_settled_snapshot`]).
//! * Waiters are admitted up to `CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS`; a request past that
//!   fails at once with a retryable [`SnapshotUnsettled`] (HTTP 503 + `Retry-After`).
//! * The record is bounded (`CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS`). Past the bound it gives up
//!   its oldest chunks and leaves a per-table **overflow fence** (the highest xid given up): until the
//!   poller sees `xmin` pass the fence, a snapshot of that table is served only if its own `xmin` is
//!   past it, and answers a retryable 503 otherwise (see [`SequencedXids::enforce_bound`]).
//!
//! `pg_xact_status()` cannot detect the window by itself: it reports such a transaction as
//! `in progress`, because it consults the ProcArray before the clog.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use super::{PooledClient, SnapshotFences, SnapshotGate, open_snapshot};
use crate::table_ref::TableRef;

// ---- the record: per-table sparse bitmaps of sequenced xids -------------------------------------

/// xids per chunk. A chunk is a 1024-bit bitmap (128 bytes) plus its key, so a DENSE backlog costs
/// ~1 bit per transaction (16M transactions ≈ 2 MiB) and a sparse one — a few transactions held by a
/// synchronous standby while everything around them became visible — costs one chunk each.
const CHUNK_SHIFT: u32 = 10;
const CHUNK_BITS: u64 = 1 << CHUNK_SHIFT;
const CHUNK_WORDS: usize = (CHUNK_BITS / 64) as usize;

/// Bytes one chunk costs (bitmap + key + count), for the reported footprint.
const CHUNK_BYTES: u64 = (CHUNK_WORDS as u64) * 8 + 16;

/// Default for `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS`: 16M transactions, ~2 MiB when dense.
pub const DEFAULT_SETTLE_MAX_XIDS: u64 = 16 * 1024 * 1024;

/// The smallest accepted `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS`: 2^20 transactions (1024
/// chunks, ~144 KiB). The poller forgets what it sees visible every tick, so only a poller outage
/// long enough for a million transactions (or a thousand tables' worth of 1024-xid windows) can
/// reach the bound — and past it the affected tables answer 503 until it recovers.
pub const MIN_SETTLE_MAX_XIDS: u64 = 1 << 20;

/// The largest accepted `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS`. Every recorded xid is
/// compared modulo 2^32 against the newest one, which is sound only while they span less than 2^31;
/// capping the record at 2^30 transactions keeps it well inside that.
pub const MAX_SETTLE_MAX_XIDS: u64 = 1 << 30;

/// One 1024-xid slice of a table's record. `key` is the unwrapped xid divided by [`CHUNK_BITS`].
#[derive(Clone)]
struct Chunk {
    key: u64,
    bits: [u64; CHUNK_WORDS],
    len: u32,
}

impl Chunk {
    fn new(key: u64) -> Self {
        Chunk { key, bits: [0; CHUNK_WORDS], len: 0 }
    }

    fn start(&self) -> u64 {
        self.key << CHUNK_SHIFT
    }

    fn get(&self, bit: u64) -> bool {
        self.bits[(bit / 64) as usize] & (1u64 << (bit % 64)) != 0
    }

    /// Set `bit`; `true` if it was not set.
    fn set(&mut self, bit: u64) -> bool {
        let w = &mut self.bits[(bit / 64) as usize];
        let m = 1u64 << (bit % 64);
        if *w & m != 0 {
            return false;
        }
        *w |= m;
        self.len += 1;
        true
    }

    /// Clear `bit`; `true` if it was set.
    fn clear(&mut self, bit: u64) -> bool {
        let w = &mut self.bits[(bit / 64) as usize];
        let m = 1u64 << (bit % 64);
        if *w & m == 0 {
            return false;
        }
        *w &= !m;
        self.len -= 1;
        true
    }

    /// The highest set xid (unwrapped), if any.
    fn max(&self) -> Option<u64> {
        let (i, &w) = self.bits.iter().enumerate().rev().find(|(_, w)| **w != 0)?;
        Some(self.start() + i as u64 * 64 + 63 - u64::from(w.leading_zeros()))
    }

    /// Every set xid (unwrapped) at or after `from`.
    fn push_from(&self, from: u64, out: &mut Vec<u64>) {
        let start = self.start();
        let first = from.saturating_sub(start);
        if first >= CHUNK_BITS {
            return;
        }
        for (i, &word) in self.bits.iter().enumerate().skip((first / 64) as usize) {
            let mut w = word;
            if i as u64 == first / 64 {
                w &= !0u64 << (first % 64);
            }
            while w != 0 {
                let b = w.trailing_zeros() as u64;
                out.push(start + i as u64 * 64 + b);
                w &= w - 1;
            }
        }
    }
}

/// One table's sequenced-but-not-yet-seen-visible xids: chunks sorted by key.
#[derive(Default, Clone)]
struct TableXids {
    chunks: VecDeque<Chunk>,
    len: u64,
}

impl TableXids {
    /// Where `key` is (or would be inserted). New xids are almost always the highest, so the back is
    /// checked before a binary search.
    fn find(&self, key: u64) -> std::result::Result<usize, usize> {
        match self.chunks.back() {
            None => Err(0),
            Some(last) if last.key == key => Ok(self.chunks.len() - 1),
            Some(last) if last.key < key => Err(self.chunks.len()),
            Some(_) => self.chunks.binary_search_by_key(&key, |c| c.key),
        }
    }

    /// Record unwrapped xid `v`. Returns (newly recorded, chunks added).
    fn insert(&mut self, v: u64) -> (bool, usize) {
        let key = v >> CHUNK_SHIFT;
        let bit = v & (CHUNK_BITS - 1);
        let (idx, added) = match self.find(key) {
            Ok(i) => (i, 0),
            Err(i) => {
                self.chunks.insert(i, Chunk::new(key));
                (i, 1)
            }
        };
        let newly = self.chunks[idx].set(bit);
        if newly {
            self.len += 1;
        }
        (newly, added)
    }

    fn contains(&self, v: u64) -> bool {
        match self.find(v >> CHUNK_SHIFT) {
            Ok(i) => self.chunks[i].get(v & (CHUNK_BITS - 1)),
            Err(_) => false,
        }
    }

    /// Forget `v`. Returns (was recorded, chunks removed).
    fn remove(&mut self, v: u64) -> (bool, usize) {
        let Ok(i) = self.find(v >> CHUNK_SHIFT) else { return (false, 0) };
        if !self.chunks[i].clear(v & (CHUNK_BITS - 1)) {
            return (false, 0);
        }
        self.len -= 1;
        if self.chunks[i].len == 0 {
            self.chunks.remove(i);
            return (true, 1);
        }
        (true, 0)
    }

    /// Keep only what a snapshot with `xmax` and (sorted) `xip` does NOT see: xids in `xip`, and xids
    /// at or after `xmax`. Everything else recorded here is committed (it was decoded) and below
    /// `xmax` outside `xip`, hence visible — permanently. Returns (xids forgotten, chunks removed).
    fn prune(&mut self, xmax: u64, xip: &[u64]) -> (u64, usize) {
        let mut forgotten = 0u64;
        for chunk in self.chunks.iter_mut() {
            let start = chunk.start();
            if start >= xmax {
                break; // chunks are sorted: nothing from here on is below xmax
            }
            // The xip entries inside this chunk (xip is sorted).
            let lo = xip.partition_point(|&x| x < start);
            let hi = xip.partition_point(|&x| x < start + CHUNK_BITS);
            let mut keep = [0u64; CHUNK_WORDS];
            for &x in &xip[lo..hi] {
                let b = x - start;
                keep[(b / 64) as usize] |= 1u64 << (b % 64);
            }
            let below = (xmax - start).min(CHUNK_BITS); // bits [0, below) are below xmax
            let mut len = 0u32;
            for (i, word) in chunk.bits.iter_mut().enumerate() {
                let first = i as u64 * 64;
                let below_mask = if below >= first + 64 {
                    !0u64
                } else if below <= first {
                    0
                } else {
                    (1u64 << (below - first)) - 1
                };
                *word &= !below_mask | keep[i];
                len += word.count_ones();
            }
            forgotten += u64::from(chunk.len - len);
            chunk.len = len;
        }
        let before = self.chunks.len();
        self.chunks.retain(|c| c.len > 0);
        self.len -= forgotten;
        (forgotten, before - self.chunks.len())
    }

    /// Recorded xids a snapshot does not see — those in its `xip`, and those at or after its `xmax`
    /// — appended to `out` (unwrapped).
    fn unsettled(&self, xmax: u64, xip: &[u64], out: &mut Vec<u64>) {
        if self.len == 0 {
            return;
        }
        for &x in xip {
            if x < xmax && self.contains(x) {
                out.push(x);
            }
        }
        let from = self.chunks.partition_point(|c| c.start() + CHUNK_BITS <= xmax);
        for chunk in self.chunks.iter().skip(from) {
            chunk.push_from(xmax, out);
        }
    }
}

/// A snapshot's visibility fences, unwrapped into the record's 64-bit xid space.
struct Fences {
    xmin: u64,
    xmax: u64,
    /// Sorted.
    xip: Vec<u64>,
}

#[derive(Default)]
struct Record {
    tables: HashMap<Box<str>, TableXids>,
    /// The newest unwrapped xid ever noted: every 32-bit xid is unwrapped relative to it. `None`
    /// until the first note.
    anchor: Option<u64>,
    /// Recorded xids, all tables.
    len: u64,
    /// Chunks, all tables — what the bound is enforced on.
    chunks: usize,
    /// Overflow fences: per table, the highest xid the bound gave up (unwrapped). A snapshot of the
    /// table is served only if its `xmin` is past it; the poller clears it once a snapshot's is.
    fences: HashMap<Box<str>, u64>,
    peak_len: u64,
    /// When the last bound drop was logged (the warning is rate-limited).
    last_drop_log: Option<Instant>,
    dropped_since_log: u64,
}

/// A 32-bit xid, placed in the 64-bit space of everything recorded: the value within 2^31 of
/// `anchor` (the newest xid noted) that has these low 32 bits. That is exactly PostgreSQL's
/// modulo-2^32 comparison (`TransactionIdPrecedes`), so a transaction from just after an epoch
/// boundary lands above one from just before it. Sound while every live xid is within 2^31 of the
/// newest, which PostgreSQL's wraparound protection guarantees for anything that can still be
/// committing.
fn unwrap_xid(anchor: Option<u64>, xid: u32) -> u64 {
    match anchor {
        // First xid ever: park it one epoch up, so older xids unwrap without going below 0.
        None => (1u64 << 32) | u64::from(xid),
        Some(a) => {
            let d = i64::from(xid.wrapping_sub(a as u32) as i32);
            (a as i64 + d) as u64
        }
    }
}

/// `gate`'s fences, unwrapped against `anchor` (see [`unwrap_xid`]).
fn fences_at(anchor: Option<u64>, gate: &SnapshotGate) -> Fences {
    let mut xip: Vec<u64> = gate.xip.iter().map(|&x| unwrap_xid(anchor, x as u32)).collect();
    xip.sort_unstable();
    Fences { xmin: unwrap_xid(anchor, gate.xmin as u32), xmax: unwrap_xid(anchor, gate.xmax as u32), xip }
}

impl Record {
    /// See [`unwrap_xid`].
    fn unwrap(&self, xid: u32) -> u64 {
        unwrap_xid(self.anchor, xid)
    }

    fn fences(&self, gate: &SnapshotGate) -> Fences {
        fences_at(self.anchor, gate)
    }

    fn contains(&self, table: &str, v: u64) -> bool {
        self.tables.get(table).is_some_and(|t| t.contains(v))
    }

    /// Is `v` still unresolved on `table`: recorded, or possibly given up under its overflow fence?
    fn unresolved(&self, table: &str, v: u64) -> bool {
        self.contains(table, v) || self.fences.get(table).is_some_and(|&f| v <= f)
    }
}

/// What a scope's tables had recorded when a snapshot was about to be opened (see
/// [`SequencedXids::capture`]).
struct Captured {
    anchor: Option<u64>,
    tables: Vec<TableXids>,
    /// The highest overflow fence on the scope's tables, if any is set.
    fence: Option<u64>,
}

impl Captured {
    /// Check `gate`: fenced (an overflow gave up transactions on a scope table, and `gate`'s `xmin`
    /// is not past them), else the captured transactions it excludes, else settled.
    fn check(&self, gate: &SnapshotGate) -> Check {
        if let Some(fence) = self.fence
            && unwrap_xid(self.anchor, gate.xmin as u32) <= fence
        {
            return Check::Fenced(fence);
        }
        match self.unsettled(gate) {
            None => Check::Settled,
            Some(pending) => Check::Pending(pending),
        }
    }

    /// Captured transactions that `gate` does not see (unwrapped, sorted), or `None` if it sees them
    /// all. Looks only at `gate`'s `xip` and at `[xmax, highest recorded]`.
    ///
    /// Everything the request paired with the snapshot was sequenced, hence noted, before the
    /// capture. A transaction that was recorded then but pruned before the capture was already
    /// visible, so a snapshot opened after the capture sees it.
    fn unsettled(&self, gate: &SnapshotGate) -> Option<Vec<u64>> {
        if self.tables.is_empty() {
            return None;
        }
        let f = fences_at(self.anchor, gate);
        let mut out = Vec::new();
        for t in &self.tables {
            t.unsettled(f.xmax, &f.xip, &mut out);
        }
        if out.is_empty() {
            return None;
        }
        out.sort_unstable();
        out.dedup();
        Some(out)
    }
}

// ---- configuration ------------------------------------------------------------------------------

/// The settle knobs, resolved once at boot into [`super::BackfillConfig`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettleConfig {
    /// `CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS`: the whole budget of one settle wait, the
    /// pool wait for the retake included.
    pub timeout_ms: u64,
    /// `CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS`: concurrent request-driven settle waits per
    /// pool. `0` = a quarter of the pool (at least 1).
    pub max_waiters: usize,
    /// `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS`: the record's bound, in transactions of dense
    /// backlog (it is enforced on 1024-xid chunks, so memory stays ≈ this / 8 bytes).
    pub max_xids: u64,
    /// `CIRCUITS_SNAPSHOT_SETTLE_POLL_MS`: the poller's tick while the record is non-empty
    /// and nobody is waiting. With waiters it polls from 1 ms, backing off to 32 ms.
    pub poll_ms: u64,
}

impl Default for SettleConfig {
    fn default() -> Self {
        SettleConfig { timeout_ms: 10_000, max_waiters: 0, max_xids: DEFAULT_SETTLE_MAX_XIDS, poll_ms: 100 }
    }
}

// ---- metrics ------------------------------------------------------------------------------------

/// Upper bounds (ms) of the settle-duration histogram buckets; the last bucket is unbounded.
const HIST_BOUNDS_MS: [u64; 14] = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000];

/// Process-wide settle counters (every pool), surfaced on `GET /replication/lsn` as `settle`.
struct SettleMetrics {
    /// Snapshots checked.
    checks: AtomicU64,
    /// Snapshots that excluded a sequenced transaction, waited, and were retaken.
    retakes: AtomicU64,
    /// Waits that ran out of budget (poller not seeing the transactions, or no connection for the
    /// retake in time).
    timeouts: AtomicU64,
    /// Waits refused at admission (too many concurrent waiters).
    rejections: AtomicU64,
    /// Snapshots currently waiting (released connection, waiting on the poller or the pool).
    waiting: AtomicU64,
    poller_ticks: AtomicU64,
    poller_failures: AtomicU64,
    /// Recorded xids the poller dropped as not transactions of this cluster (past `xmax`, neither
    /// running nor known to it).
    forgotten: AtomicU64,
    /// Recorded xids given up at the bound: their settle is no longer guaranteed.
    dropped: AtomicU64,
    /// Times the bound was hit.
    bound_hits: AtomicU64,
    /// Snapshots refused because a table they read was fenced by an overflow (xmin not past it).
    overflow_refusals: AtomicU64,
    /// Settle-duration histogram of every wait (whatever its outcome).
    hist: [AtomicU64; HIST_BOUNDS_MS.len() + 1],
    hist_sum_us: AtomicU64,
    hist_max_us: AtomicU64,
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);

static METRICS: SettleMetrics = SettleMetrics {
    checks: ZERO,
    retakes: ZERO,
    timeouts: ZERO,
    rejections: ZERO,
    waiting: ZERO,
    poller_ticks: ZERO,
    poller_failures: ZERO,
    forgotten: ZERO,
    dropped: ZERO,
    bound_hits: ZERO,
    overflow_refusals: ZERO,
    hist: [ZERO; HIST_BOUNDS_MS.len() + 1],
    hist_sum_us: ZERO,
    hist_max_us: ZERO,
};

fn record_wait(d: Duration) {
    let ms = d.as_millis() as u64;
    let i = HIST_BOUNDS_MS.iter().position(|&b| ms <= b).unwrap_or(HIST_BOUNDS_MS.len());
    METRICS.hist[i].fetch_add(1, Ordering::Relaxed);
    let us = d.as_micros() as u64;
    METRICS.hist_sum_us.fetch_add(us, Ordering::Relaxed);
    METRICS.hist_max_us.fetch_max(us, Ordering::Relaxed);
}

/// Snapshots currently waiting for sequenced transactions to become visible (process-wide) —
/// `visibilityWaits` on `GET /replication/lsn`.
pub fn settle_waits_active() -> u64 {
    METRICS.waiting.load(Ordering::Relaxed)
}

/// The `settle` object of `GET /replication/lsn`: the record's size across `sets`, and the
/// process-wide counters and wait-duration distribution.
pub(super) fn stats_json<'a>(sets: impl Iterator<Item = &'a SequencedXids>) -> serde_json::Value {
    let (mut len, mut peak, mut chunks, mut tables, mut fenced) = (0u64, 0u64, 0usize, 0usize, 0usize);
    for s in sets {
        let r = s.record.lock().unwrap();
        len += r.len;
        peak += r.peak_len;
        chunks += r.chunks;
        tables += r.tables.len();
        fenced += r.fences.len();
    }
    let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
    let counts: Vec<u64> = METRICS.hist.iter().map(l).collect();
    let n: u64 = counts.iter().sum();
    let quantile = |q: f64| -> Option<u64> {
        if n == 0 {
            return None;
        }
        let rank = ((q * n as f64).ceil() as u64).max(1);
        let mut acc = 0;
        for (i, c) in counts.iter().enumerate() {
            acc += c;
            if acc >= rank {
                // A bucket's upper bound; the open-ended last bucket reports the observed max.
                return Some(HIST_BOUNDS_MS.get(i).copied().unwrap_or(l(&METRICS.hist_max_us) / 1000));
            }
        }
        None
    };
    let mut buckets = serde_json::Map::new();
    for (i, c) in counts.iter().enumerate() {
        let name = HIST_BOUNDS_MS.get(i).map_or("inf".to_string(), |b| b.to_string());
        buckets.insert(format!("le_{name}ms"), (*c).into());
    }
    serde_json::json!({
        "sequencedXids": len,
        "sequencedXidsPeak": peak,
        "tables": tables,
        "recordBytes": chunks as u64 * CHUNK_BYTES,
        "xidsDropped": l(&METRICS.dropped),
        "boundHits": l(&METRICS.bound_hits),
        "fencedTables": fenced,
        "overflowRefusals": l(&METRICS.overflow_refusals),
        "forgotten": l(&METRICS.forgotten),
        "pollerTicks": l(&METRICS.poller_ticks),
        "pollerFailures": l(&METRICS.poller_failures),
        "checks": l(&METRICS.checks),
        "retakes": l(&METRICS.retakes),
        "timeouts": l(&METRICS.timeouts),
        "rejections": l(&METRICS.rejections),
        "waiting": l(&METRICS.waiting),
        "waitMs": {
            "count": n,
            "sumMs": l(&METRICS.hist_sum_us) / 1000,
            "maxMs": l(&METRICS.hist_max_us) / 1000,
            "p50Ms": quantile(0.50),
            "p90Ms": quantile(0.90),
            "p99Ms": quantile(0.99),
            "buckets": buckets,
        },
    })
}

// ---- the settle set + its poller ----------------------------------------------------------------

/// Transactions the sequencer has fanned out that no snapshot has yet been seen to contain, per
/// table, plus the one poller that learns when they become visible. One per [`super::Pool`].
///
/// **Memory.** Per-table sparse bitmaps of 1024-xid chunks, bounded by
/// `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS` (in chunks: ≈ that many transactions of dense
/// backlog, ≈ that / 8 bytes). Normally the record is a chunk or two — the poller forgets everything
/// a fresh snapshot shows visible every tick — so the bound only matters when the poller cannot run
/// for a long time (see [`SequencedXids::enforce_bound`]).
///
/// **Poller.** A single task per pool on its own dedicated connection (never a pooled one, so a
/// starved pool cannot starve it): while the record is non-empty it takes a fresh
/// `pg_current_snapshot()` every tick — every 1–32 ms while a snapshot is waiting on it — forgets
/// every recorded xid that snapshot shows visible, and wakes the waiters. It is not
/// correctness-critical: if it cannot connect or query, waiters simply run out of budget (a
/// retryable 503), and the record grows until the bound. It is started on demand and restarted on
/// demand if it has ended (a runtime that went away).
pub struct SequencedXids {
    record: Mutex<Record>,
    /// The database the poller reads snapshots from (`None` for a set no pool owns — unit tests,
    /// where nothing ever polls and waiters time out).
    url: Option<String>,
    cfg: SettleConfig,
    /// Bound on the record, in chunks.
    cap_chunks: usize,
    poller: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Kicks the poller: a first note into an empty record, or a new waiter.
    wake: tokio::sync::Notify,
    /// Bumped by the poller after every prune; waiters re-check on each bump.
    generation: tokio::sync::watch::Sender<u64>,
    /// Snapshots currently waiting on the poller (it polls fast while any are).
    waiters: AtomicUsize,
    /// Admission for request-driven waits.
    admission: tokio::sync::Semaphore,
}

impl Default for SequencedXids {
    fn default() -> Self {
        SequencedXids::new(None, SettleConfig::default(), 20)
    }
}

impl SequencedXids {
    /// A set whose poller reads snapshots from `url` (`None`: never polled). `pool_size` sizes the
    /// default waiter cap.
    pub(super) fn new(url: Option<String>, cfg: SettleConfig, pool_size: usize) -> Self {
        let max_waiters = if cfg.max_waiters == 0 { (pool_size / 4).max(1) } else { cfg.max_waiters };
        SequencedXids {
            record: Mutex::new(Record::default()),
            url,
            cap_chunks: (cfg.max_xids.clamp(CHUNK_BITS, MAX_SETTLE_MAX_XIDS) / CHUNK_BITS) as usize,
            cfg,
            poller: Mutex::new(None),
            wake: tokio::sync::Notify::new(),
            generation: tokio::sync::watch::channel(0).0,
            waiters: AtomicUsize::new(0),
            admission: tokio::sync::Semaphore::new(max_waiters),
        }
    }

    /// Record that transaction `xid` (as pgoutput carries it; only its low 32 bits matter) touching
    /// `tables` is being sequenced. Call BEFORE any of its envelopes can reach a shape stream or a
    /// feed. Idempotent: a re-delivered transaction is recorded once.
    pub fn note<'t>(self: &Arc<Self>, xid: u64, tables: impl IntoIterator<Item = &'t str>) {
        let was_empty;
        {
            let mut guard = self.record.lock().unwrap();
            let r: &mut Record = &mut guard;
            was_empty = r.len == 0;
            let v = r.unwrap(xid as u32);
            r.anchor = Some(r.anchor.map_or(v, |a| a.max(v)));
            let mut previous: Option<&str> = None;
            for table in tables {
                if previous == Some(table) {
                    continue; // a transaction's envelopes are usually one table in a row
                }
                previous = Some(table);
                if !r.tables.contains_key(table) {
                    r.tables.insert(table.into(), TableXids::default());
                }
                let (newly, added) = r.tables.get_mut(table).expect("inserted above").insert(v);
                r.chunks += added;
                if newly {
                    r.len += 1;
                }
            }
            r.peak_len = r.peak_len.max(r.len);
            if r.chunks > self.cap_chunks {
                self.enforce_bound(r);
            }
        }
        if was_empty {
            self.ensure_poller();
            self.wake.notify_one();
        }
    }

    /// The record has hit its bound: give up the OLDEST chunks (across tables) until it fits, and
    /// leave an overflow fence on each table a chunk was taken from: the highest xid given up.
    ///
    /// A given-up transaction can no longer be checked for, so a snapshot of its table cannot be
    /// shown to include it — unless the snapshot's `xmin` is past the fence, which proves every xid
    /// up to the fence has ended (and a sequenced one committed, so it is visible). So while a table
    /// is fenced, a snapshot that reads it is served only if its `xmin` is past the fence, and is
    /// otherwise refused with a retryable 503 at once; the poller clears the fence once one of its
    /// snapshots has `xmin` past it. A waiter's transactions under a fence stay unresolved, so the
    /// bound never satisfies a wait either.
    ///
    /// The degrade is therefore availability, never correctness, and only for the affected tables:
    /// accepting such a snapshot would silently lose a transaction that is already behind the
    /// request's pairing point (the lost-change bug the record exists to prevent); waiting on `xmin`
    /// for every table would let one long-open transaction stall every snapshot. The oldest chunks
    /// go because the newest are the transactions racing a snapshot right now, while the oldest have
    /// been invisible for as long as the bound can hold. Reaching the bound takes a poller outage
    /// spanning at least `CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS` (≥ 2^20) transactions; each
    /// drop is counted (`xidsDropped`), fenced tables are reported (`fencedTables`), and it is logged.
    fn enforce_bound(&self, r: &mut Record) {
        let mut dropped = 0u64;
        while r.chunks > self.cap_chunks {
            let Some(oldest) = r
                .tables
                .iter()
                .filter_map(|(name, t)| t.chunks.front().map(|c| (c.key, name.clone())))
                .min_by_key(|(key, _)| *key)
                .map(|(_, name)| name)
            else {
                break;
            };
            let t = r.tables.get_mut(&oldest).expect("present");
            let chunk = t.chunks.pop_front().expect("non-empty");
            t.len -= u64::from(chunk.len);
            if t.len == 0 {
                r.tables.remove(&oldest);
            }
            if let Some(high) = chunk.max() {
                let fence = r.fences.entry(oldest).or_insert(high);
                *fence = (*fence).max(high);
            }
            r.chunks -= 1;
            r.len -= u64::from(chunk.len);
            dropped += u64::from(chunk.len);
        }
        METRICS.dropped.fetch_add(dropped, Ordering::Relaxed);
        METRICS.bound_hits.fetch_add(1, Ordering::Relaxed);
        r.dropped_since_log += dropped;
        let due = r.last_drop_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(10));
        if due {
            tracing::warn!(
                dropped = r.dropped_since_log,
                cap_xids = self.cap_chunks as u64 * CHUNK_BITS,
                fenced_tables = r.fences.len(),
                "settle record hit its bound (CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS): the oldest \
                 sequenced transactions were given up, and snapshots of their tables answer 503 until \
                 xmin passes them; is the visibility poller failing, or a synchronous standby stalled?"
            );
            r.last_drop_log = Some(Instant::now());
            r.dropped_since_log = 0;
        }
    }

    /// A copy of what is recorded on `scope`'s tables, taken BEFORE a snapshot is opened and checked
    /// against afterwards ([`Captured::unsettled`]). Checking the live record instead would lose a
    /// transaction the poller prunes between the snapshot and the check: it became visible after the
    /// snapshot was taken, so the snapshot still excludes it, yet it would no longer be recorded.
    ///
    /// Normally the record is empty (nothing is copied) or a chunk or two per table.
    ///
    /// The overflow fences are captured with it: a fence the poller clears after the snapshot was
    /// opened says nothing about that snapshot.
    fn capture(&self, scope: &SettleScope) -> Captured {
        let r = self.record.lock().unwrap();
        let mut tables = Vec::new();
        if r.len > 0 {
            for table in &scope.tables {
                if let Some(t) = r.tables.get(table.as_str()) {
                    tables.push(t.clone());
                }
            }
        }
        let mut fence = None;
        if !r.fences.is_empty() {
            for table in &scope.tables {
                if let Some(&f) = r.fences.get(table.as_str()) {
                    fence = Some(fence.map_or(f, |g: u64| g.max(f)));
                }
            }
        }
        Captured { anchor: r.anchor, tables, fence }
    }

    /// Forget everything `gate` shows visible, on every table (the poller's prune), and clear every
    /// overflow fence its `xmin` is past.
    fn prune(&self, gate: &SnapshotGate) {
        let mut r = self.record.lock().unwrap();
        if r.len == 0 && r.fences.is_empty() {
            return;
        }
        let f = r.fences(gate);
        if !r.fences.is_empty() {
            r.fences.retain(|_, fence| f.xmin <= *fence);
        }
        let (mut forgotten, mut removed) = (0u64, 0usize);
        r.tables.retain(|_, t| {
            let (n, c) = t.prune(f.xmax, &f.xip);
            forgotten += n;
            removed += c;
            t.len > 0
        });
        r.len -= forgotten;
        r.chunks -= removed;
    }

    /// Recorded xids (unwrapped, every table) at or after `gate`'s `xmax`.
    fn ahead_of(&self, gate: &SnapshotGate) -> Vec<u64> {
        let r = self.record.lock().unwrap();
        let xmax = r.unwrap(gate.xmax as u32);
        let mut out = Vec::new();
        for t in r.tables.values() {
            t.unsettled(xmax, &[], &mut out);
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Forget `xids` (unwrapped) on every table.
    fn forget(&self, xids: &[u64]) {
        let mut r = self.record.lock().unwrap();
        let (mut n, mut removed) = (0u64, 0usize);
        for t in r.tables.values_mut() {
            for &v in xids {
                let (was, c) = t.remove(v);
                n += u64::from(was);
                removed += c;
            }
        }
        r.tables.retain(|_, t| t.len > 0);
        r.len -= n;
        r.chunks -= removed;
    }

    /// How many sequenced transactions (per table) are not yet known visible.
    pub fn len(&self) -> usize {
        self.record.lock().unwrap().len as usize
    }

    /// Is every sequenced transaction known visible?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Nothing for the poller to learn: every sequenced transaction is known visible and no table is
    /// fenced by an overflow.
    fn is_quiet(&self) -> bool {
        let r = self.record.lock().unwrap();
        r.len == 0 && r.fences.is_empty()
    }

    /// Start the poller if needed and make it tick now (a fenced table's fence is cleared only by a
    /// poll that sees `xmin` past it).
    fn kick(self: &Arc<Self>) {
        self.ensure_poller();
        self.wake.notify_one();
    }

    /// Start the poller if it is not running (never started, or its runtime went away). A set with
    /// no URL, or a caller outside a Tokio runtime, has none.
    fn ensure_poller(self: &Arc<Self>) {
        let Some(url) = self.url.clone() else { return };
        let mut slot = self.poller.lock().unwrap();
        if slot.as_ref().is_some_and(|h| !h.is_finished()) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else { return };
        *slot = Some(rt.spawn(poll_visibility(self.clone(), url)));
    }

    /// Wait until none of `pending` (unwrapped) is recorded on `scope`'s tables any more — the
    /// poller saw it visible or forgot it as foreign — or until `deadline`. A transaction the bound
    /// gave up is not resolved by that: it stays pending while an overflow fence covers it, until the
    /// poller sees `xmin` past the fence.
    /// Never touches a pooled connection. A poller that cannot run does not wedge this: the deadline
    /// does.
    async fn wait_until_settled(
        self: &Arc<Self>,
        scope: &SettleScope,
        mut pending: Vec<u64>,
        deadline: Instant,
    ) -> std::result::Result<(), SnapshotUnsettled> {
        struct Waiter<'a>(&'a AtomicUsize);
        impl Drop for Waiter<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let _waiter = Waiter(&self.waiters);
        // Subscribe BEFORE the first check, so a prune that lands between the check and the wait
        // still wakes us.
        let mut rx = self.generation.subscribe();
        self.ensure_poller();
        self.wake.notify_one();
        loop {
            {
                let r = self.record.lock().unwrap();
                pending.retain(|&v| scope.tables.iter().any(|t| r.unresolved(t, v)));
            }
            if pending.is_empty() {
                return Ok(());
            }
            match tokio::time::timeout_at(deadline.into(), rx.changed()).await {
                Ok(Ok(())) => {}
                // Unreachable (the sender lives in `self`); fail rather than spin.
                Ok(Err(_)) | Err(_) => {
                    return Err(SnapshotUnsettled {
                        cause: UnsettledCause::TimedOut,
                        pending: pending.iter().map(|&v| v as u32).collect(),
                    });
                }
            }
        }
    }
}

/// The poller loop (see [`SequencedXids`]). Runs for the life of its runtime; every set it serves is
/// a pool's, which lives as long as the process.
async fn poll_visibility(set: Arc<SequencedXids>, url: String) {
    const FAST_START: Duration = Duration::from_millis(1);
    const FAST_MAX: Duration = Duration::from_millis(32);
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const BACKOFF_MAX: Duration = Duration::from_secs(2);
    let mut client: Option<tokio_postgres::Client> = None;
    let mut fast = FAST_START;
    let mut backoff = Duration::from_millis(50);
    // Recorded xids that were past `xmax` on the previous tick too: candidates for the foreign check.
    let mut prev_ahead: HashSet<u64> = HashSet::new();
    loop {
        let idle = set.waiters.load(Ordering::Acquire) == 0;
        if idle && set.is_quiet() {
            // Nothing to learn: park until a first note or a waiter.
            set.wake.notified().await;
            fast = FAST_START;
            continue;
        }
        let tick = if idle { Duration::from_millis(set.cfg.poll_ms.max(1)) } else { fast };
        tokio::select! {
            _ = tokio::time::sleep(tick) => {}
            _ = set.wake.notified() => fast = FAST_START,
        }
        if set.waiters.load(Ordering::Acquire) > 0 {
            fast = (fast * 2).min(FAST_MAX);
        }
        if client.as_ref().is_none_or(|c| c.is_closed()) {
            match tokio::time::timeout(QUERY_TIMEOUT, super::connect(&url)).await {
                Ok(Ok(c)) => client = Some(c),
                Ok(Err(e)) => {
                    poller_failed(&format!("{e:#}"), &mut backoff, BACKOFF_MAX).await;
                    continue;
                }
                Err(_) => {
                    poller_failed("connect timed out", &mut backoff, BACKOFF_MAX).await;
                    continue;
                }
            }
        }
        let polled = snapshot_text(client.as_ref().expect("connected above"), QUERY_TIMEOUT).await;
        let text = match polled {
            Ok(text) => text,
            Err(error) => {
                client = None;
                poller_failed(&error, &mut backoff, BACKOFF_MAX).await;
                continue;
            }
        };
        let c = client.as_ref().expect("connected above");
        backoff = Duration::from_millis(50);
        let gate = SnapshotGate::parse(&text, "0/0");
        set.prune(&gate);
        METRICS.poller_ticks.fetch_add(1, Ordering::Relaxed);
        // A recorded xid past this snapshot's xmax on two ticks running is either still in the
        // ProcArray (held — keep waiting for it) or not a transaction of this cluster at all (the
        // change log carried it from before an epoch reset or a restore), which would otherwise be
        // waited for until every such wait timed out. `pg_xact_status` tells them apart without
        // needing to see other roles' backends: `in progress` keeps it; committed/aborted/unknown
        // (it ended — so it is visible now) or "in the future" (foreign) forgets it.
        let ahead = set.ahead_of(&gate);
        if ahead.is_empty() {
            prev_ahead.clear();
        } else {
            let persisted: Vec<u64> = ahead.iter().copied().filter(|v| prev_ahead.contains(v)).take(32).collect();
            prev_ahead = ahead.into_iter().collect();
            if !persisted.is_empty()
                && let Some(xmax8) = text.split(':').nth(1).and_then(|s| s.trim().parse::<u64>().ok())
            {
                let xmax_v = set.record.lock().unwrap().unwrap(xmax8 as u32);
                let mut gone = Vec::new();
                for v in persisted {
                    let full = xmax8.wrapping_add(v.wrapping_sub(xmax_v));
                    let full = full.to_string();
                    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&full];
                    let status = c.query_one("select pg_xact_status($1::text::xid8)", &params);
                    match tokio::time::timeout(QUERY_TIMEOUT, status).await {
                        Ok(Ok(row)) => {
                            if row.get::<_, Option<String>>(0).as_deref() != Some("in progress") {
                                gone.push(v);
                            }
                        }
                        // 22023: "transaction ID … is in the future" — not one of this cluster's.
                        Ok(Err(e)) if e.code().map(|c| c.code()) == Some("22023") => gone.push(v),
                        // Anything else (a lost connection, a timeout): decide next tick.
                        _ => break,
                    }
                }
                if !gone.is_empty() {
                    tracing::warn!(
                        xids = ?gone.iter().map(|&v| v as u32).collect::<Vec<_>>(),
                        "sequenced transaction(s) past the snapshot horizon are not in progress; forgetting them"
                    );
                    METRICS.forgotten.fetch_add(gone.len() as u64, Ordering::Relaxed);
                    set.forget(&gone);
                    prev_ahead.retain(|v| !gone.contains(v));
                }
            }
        }
        set.generation.send_modify(|g| *g = g.wrapping_add(1));
    }
}

/// One `pg_current_snapshot()`, bounded: a hung server must not hang the poller.
async fn snapshot_text(c: &tokio_postgres::Client, limit: Duration) -> std::result::Result<String, String> {
    match tokio::time::timeout(limit, c.query_one("select pg_current_snapshot()::text", &[])).await {
        Ok(Ok(row)) => Ok(row.get(0)),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Err(_) => Err("snapshot query timed out".to_string()),
    }
}

async fn poller_failed(error: &str, backoff: &mut Duration, max: Duration) {
    METRICS.poller_failures.fetch_add(1, Ordering::Relaxed);
    tracing::debug!("settle visibility poller: {error}; retrying in {backoff:?}");
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(max);
}

// ---- scope, errors, and the settled snapshot ----------------------------------------------------

/// Which recorded transactions a snapshot must contain, and whether its wait is admission-capped.
///
/// The tables are every table the read depends on: the table read, plus every table a subquery in
/// its `WHERE` reads. A transaction that touched none of them cannot change what the read returns,
/// so whether the snapshot sees it is irrelevant — a synchronous standby holding writes to one table
/// never delays reads of another.
#[derive(Clone, Debug)]
pub struct SettleScope {
    tables: Vec<String>,
    capped: bool,
}

impl SettleScope {
    /// A read a client request is waiting on (subset query, shape create): admission-capped.
    pub fn request(table: &TableRef) -> Self {
        SettleScope { tables: vec![table.as_str().to_string()], capped: true }
    }

    /// An engine-internal read (a membership query-back, the counts seed at boot): never refused
    /// at admission — refusing it would only turn into flip retries and, past those, a degraded
    /// engine — but still bounded by the settle timeout.
    pub fn internal(table: &TableRef) -> Self {
        SettleScope { tables: vec![table.as_str().to_string()], capped: false }
    }

    /// Also settle against `tables` (a subquery's inner tables).
    pub fn with<'a>(mut self, tables: impl IntoIterator<Item = &'a TableRef>) -> Self {
        for t in tables {
            if !self.tables.iter().any(|x| x == t.as_str()) {
                self.tables.push(t.as_str().to_string());
            }
        }
        self
    }
}

/// Why a snapshot could not be settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsettledCause {
    /// The settle budget ran out (the transactions stayed invisible, the poller could not see them
    /// in time, or no pooled connection came back for the retake in time).
    TimedOut,
    /// Too many snapshots were already waiting (`CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS`).
    Rejected,
    /// A shared shape's creator failed this way; a joiner reports the same.
    Shared,
    /// The settle record overflowed its bound and gave up sequenced transactions on a table the read
    /// depends on; the snapshot's `xmin` is not past them, so it cannot be shown to include them.
    Overflowed,
}

/// A snapshot could not be settled: a transaction the engine has already sequenced is still not
/// visible to new snapshots (typically a synchronous standby that has not acknowledged it).
/// Retryable — the HTTP layer answers 503 with `Retry-After`, typed through every create path.
#[derive(Clone, Debug)]
pub struct SnapshotUnsettled {
    pub cause: UnsettledCause,
    /// Sequenced transactions (32-bit xids) the snapshot did not contain.
    pub pending: Vec<u32>,
}

impl std::fmt::Display for SnapshotUnsettled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = &self.pending[..self.pending.len().min(8)];
        match self.cause {
            UnsettledCause::TimedOut => write!(
                f,
                "snapshot not settled within CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS: {} committed transaction(s) \
                 the engine already sequenced are not yet visible to new snapshots (first xids {first:?}); a \
                 synchronous standby may be holding them. Retry.",
                self.pending.len()
            ),
            UnsettledCause::Rejected => write!(
                f,
                "snapshot not settled: {} committed transaction(s) the engine already sequenced are not yet visible \
                 to new snapshots (first xids {first:?}) and too many requests are already waiting for them \
                 (CIRCUITS_SNAPSHOT_SETTLE_MAX_WAITERS). Retry.",
                self.pending.len()
            ),
            UnsettledCause::Overflowed => write!(
                f,
                "snapshot not settled: the settle record overflowed CIRCUITS_SNAPSHOT_SETTLE_MAX_XIDS and gave \
                 up committed transaction(s) on a table this read depends on (up to xid {first:?}); the table is \
                 served again once no transaction up to there is still running. Is the visibility poller failing, \
                 or a synchronous standby stalled? Retry."
            ),
            UnsettledCause::Shared => f.write_str(
                "the shared shape's creator could not settle its snapshot (a committed transaction is not yet \
                 visible to new snapshots). Retry.",
            ),
        }
    }
}

impl std::error::Error for SnapshotUnsettled {}

/// Counted in `visibilityWaits` for exactly as long as it lives — a dropped request future included.
struct Waiting;

impl Waiting {
    fn start() -> Self {
        METRICS.waiting.fetch_add(1, Ordering::Relaxed);
        Waiting
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        METRICS.waiting.fetch_sub(1, Ordering::Relaxed);
    }
}

/// `BEGIN … REPEATABLE READ` + fences, with the checkout's transaction flag kept right on failure.
async fn open_tracked(client: &PooledClient, statement_timeout_ms: u64, what: &str) -> Result<SnapshotFences> {
    client.transaction_started();
    match open_snapshot(client, statement_timeout_ms, what).await {
        Ok(f) => Ok(f),
        Err(error) => {
            if client.batch_execute("ROLLBACK").await.is_ok() {
                client.transaction_finished();
            }
            Err(error)
        }
    }
}

/// The outcome of checking a snapshot against the record of sequenced transactions.
#[derive(Debug, PartialEq, Eq)]
enum Check {
    /// The snapshot contains every sequenced transaction its read depends on.
    Settled,
    /// Sequenced transactions (unwrapped, sorted) the snapshot excludes.
    Pending(Vec<u64>),
    /// The bound gave up transactions on a table the read depends on, up to this xid (unwrapped),
    /// and the snapshot's `xmin` is not past it: it cannot be shown to include them.
    Fenced(u64),
}

/// What a snapshot is checked by: its visibility fences.
trait HasGate {
    fn gate(&self) -> &SnapshotGate;
}

impl HasGate for SnapshotFences {
    fn gate(&self) -> &SnapshotGate {
        &self.gate
    }
}

impl HasGate for SnapshotGate {
    fn gate(&self) -> &SnapshotGate {
        self
    }
}

/// Open a snapshot with `open` and check it against the sequenced transactions on `scope`'s tables
/// as they were recorded BEFORE it was opened (see [`SequencedXids::capture`]).
async fn open_checked<T: HasGate, Fut: std::future::Future<Output = Result<T>>>(
    seen: &SequencedXids,
    scope: &SettleScope,
    open: impl FnOnce() -> Fut,
) -> Result<(T, Check)> {
    let captured = seen.capture(scope);
    let opened = open().await?;
    let check = captured.check(opened.gate());
    Ok((opened, check))
}

/// Open the `REPEATABLE READ READ ONLY` transaction a backfill or query-back reads, and return its
/// fences with the transaction still open. The snapshot is **settled** against what the sequencer
/// has already fanned out on `scope`'s tables ([`SequencedXids`]): if it excludes such a
/// transaction, it is rolled back, the pooled connection is RELEASED, the call waits (without a
/// connection) until the poller has seen everything it excluded visible, takes a connection again
/// and opens one more snapshot, which is then settled by construction:
///
/// Anything the caller has already paired with this snapshot — the `BeginShape` point of a backfill,
/// the feed offset a subset client HEADed before asking for its page — was produced from what the
/// sequencer had sequenced by then, and all of that was noted before the record was captured for
/// the first check (the capture is taken before the snapshot is opened, so the poller pruning a
/// transaction in between cannot hide it from the check). A transaction
/// the second snapshot excludes was excluded by the first as well (visibility only grows), so it was
/// in the first check's set, and the call waited until it was visible. The second snapshot is
/// therefore not re-checked: re-checking would chase transactions sequenced after the pairing point,
/// which the live tail carries anyway, and under synchronous replication it could chase for ever.
///
/// The whole wait — the poller and the pool wait for the retake — is bounded by
/// `CIRCUITS_SNAPSHOT_SETTLE_TIMEOUT_MS`; a request-driven wait past the admission cap is
/// refused at once. Both are a retryable [`SnapshotUnsettled`].
///
/// On success the caller owns the open transaction (it must `COMMIT`/`ROLLBACK` and call
/// `transaction_finished`).
pub(super) async fn begin_settled_snapshot(
    client: &mut PooledClient,
    scope: &SettleScope,
    statement_timeout_ms: u64,
    what: &str,
) -> Result<SnapshotFences> {
    let started = Instant::now();
    let seen = client.inner.sequenced.clone();
    let (fences, check) = open_checked(&seen, scope, || open_tracked(client, statement_timeout_ms, what)).await?;
    METRICS.checks.fetch_add(1, Ordering::Relaxed);
    let pending = match check {
        Check::Settled => return Ok(fences),
        Check::Pending(pending) => pending,
        Check::Fenced(fence) => {
            if client.batch_execute("ROLLBACK").await.is_ok() {
                client.transaction_finished();
            } else {
                bail!("{what}: rolling back a fenced snapshot failed");
            }
            METRICS.overflow_refusals.fetch_add(1, Ordering::Relaxed);
            // Let the poller clear the fence as soon as `xmin` has passed it, for the retry.
            seen.kick();
            return Err(anyhow::Error::new(SnapshotUnsettled {
                cause: UnsettledCause::Overflowed,
                pending: vec![fence as u32],
            }));
        }
    };
    if client.batch_execute("ROLLBACK").await.is_ok() {
        client.transaction_finished();
    } else {
        bail!("{what}: rolling back an unsettled snapshot failed");
    }
    let _admitted = if scope.capped {
        match seen.admission.try_acquire() {
            Ok(permit) => Some(permit),
            Err(_) => {
                METRICS.rejections.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::Error::new(SnapshotUnsettled {
                    cause: UnsettledCause::Rejected,
                    pending: pending.iter().map(|&v| v as u32).collect(),
                }));
            }
        }
    } else {
        None
    };
    let _waiting = Waiting::start();
    let deadline = started + Duration::from_millis(seen.cfg.timeout_ms);
    // Give the connection back while waiting: a stalled standby must not pin the pool.
    client.release();
    let waited = seen.wait_until_settled(scope, pending.clone(), deadline).await;
    if let Err(unsettled) = waited {
        METRICS.timeouts.fetch_add(1, Ordering::Relaxed);
        record_wait(started.elapsed());
        return Err(anyhow::Error::new(unsettled));
    }
    // The retake's pool wait counts against the same budget.
    match tokio::time::timeout_at(deadline.into(), client.reacquire()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            record_wait(started.elapsed());
            return Err(e);
        }
        Err(_) => {
            METRICS.timeouts.fetch_add(1, Ordering::Relaxed);
            record_wait(started.elapsed());
            return Err(anyhow::Error::new(SnapshotUnsettled {
                cause: UnsettledCause::TimedOut,
                pending: pending.iter().map(|&v| v as u32).collect(),
            }));
        }
    }
    let fences = open_tracked(client, statement_timeout_ms, what).await?;
    METRICS.retakes.fetch_add(1, Ordering::Relaxed);
    record_wait(started.elapsed());
    tracing::debug!(
        what,
        xids = ?pending.iter().map(|&v| v as u32).collect::<Vec<_>>(),
        waited_ms = started.elapsed().as_millis() as u64,
        "snapshot excluded already-sequenced transactions; retaking it after they became visible"
    );
    Ok(fences)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items() -> SettleScope {
        SettleScope::request(&TableRef::parse("public.items").unwrap())
    }

    fn set_with(cfg: SettleConfig) -> Arc<SequencedXids> {
        Arc::new(SequencedXids::new(None, cfg, 20))
    }

    fn gate(s: &str) -> SnapshotGate {
        SnapshotGate::parse(s, "0/1")
    }

    impl SequencedXids {
        /// Capture and check at once (no snapshot is opened in between).
        fn unsettled(&self, scope: &SettleScope, gate: &SnapshotGate) -> Option<Vec<u64>> {
            self.capture(scope).unsettled(gate)
        }
    }

    /// The check reports exactly the recorded transactions a snapshot excludes — those in its xip,
    /// and those at or past its xmax — and a prune forgets the ones it includes (visibility is
    /// permanent). A transaction is recorded once however often it is noted.
    #[test]
    fn reports_what_a_snapshot_excludes_and_prunes_what_it_includes() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(100, ["public.items"]);
        seen.note(100, ["public.items", "public.items"]);
        seen.note(101, ["public.items"]);
        seen.note(4_294_967_396, ["public.items"]); // 2^32 + 100: the same 32-bit xid as the first
        assert_eq!(seen.len(), 2);
        // 100 still in progress (a commit held after its WAL was flushed); 101 visible.
        let g = gate("100:102:100");
        assert_eq!(
            seen.unsettled(&items(), &g).map(|v| v.iter().map(|&x| x as u32).collect::<Vec<_>>()),
            Some(vec![100])
        );
        seen.prune(&g);
        assert_eq!(seen.len(), 1, "101 was seen visible and forgotten");
        // At/after xmax (e.g. the highest xid, still in SyncRep): excluded too.
        seen.note(102, ["public.items"]);
        let u = seen.unsettled(&items(), &g).unwrap();
        assert_eq!(u.iter().map(|&x| x as u32).collect::<Vec<_>>(), vec![100, 102]);
        let later = gate("103:103:");
        assert!(seen.unsettled(&items(), &later).is_none(), "both visible now");
        seen.prune(&later);
        assert!(seen.is_empty());
    }

    /// An open transaction the engine never sequenced — a writer pinning `xmin` for as long as it
    /// likes — never blocks a settle and never keeps anything recorded: only xip entries that were
    /// sequenced, and sequenced xids at/after xmax, count.
    #[test]
    fn a_long_open_unsequenced_writer_never_blocks_settling() {
        let seen = Arc::new(SequencedXids::default());
        // The pin (xid 50) is open throughout and never sequenced; 51..=5000 commit and are.
        for x in 51..=5_000u64 {
            seen.note(x, ["public.items"]);
        }
        let g = gate("50:5001:50");
        assert!(seen.unsettled(&items(), &g).is_none(), "the pin is in xip but was never sequenced");
        seen.prune(&g);
        assert!(seen.is_empty(), "everything but the pin was visible, although xmin stayed at the pin");
    }

    /// Only the tables a read depends on are consulted: a held transaction on one table never delays
    /// a snapshot of another, but does a snapshot whose subquery reads it.
    #[test]
    fn settling_is_scoped_to_the_tables_a_read_depends_on() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(200, ["public.items"]);
        let g = gate("200:201:200");
        let other = TableRef::parse("public.other").unwrap();
        assert!(seen.unsettled(&SettleScope::request(&other), &g).is_none());
        let items_ref = TableRef::parse("public.items").unwrap();
        assert!(seen.unsettled(&SettleScope::request(&other).with([&items_ref]), &g).is_some());
    }

    /// The record is correct across the 2^32 xid wrap: xids from just before and just after the
    /// boundary are ordered as PostgreSQL orders them (modulo 2^32), and a snapshot straddling the
    /// boundary reports and prunes exactly the right ones. 2^32 is a chunk boundary, so the wrap also
    /// crosses chunks.
    #[test]
    fn the_record_is_wraparound_safe() {
        let seen = Arc::new(SequencedXids::default());
        let before: Vec<u64> = (4_294_967_290..=4_294_967_295).collect();
        let after: Vec<u64> = vec![3, 4, 5, 6]; // the next epoch (0..2 are never assigned)
        for &x in before.iter().chain(&after) {
            seen.note(x, ["public.items"]);
        }
        assert_eq!(seen.len(), 10);
        // xmin 2^32-6, xmax 2^32+6 (masked 6), in progress {2^32-1, 2^32+4}.
        let g = SnapshotGate::parse("4294967290:4294967302:4294967295,4294967300", "0/1");
        let u: Vec<u32> = seen.unsettled(&items(), &g).unwrap().iter().map(|&v| v as u32).collect();
        assert_eq!(u, vec![4_294_967_295, 4, 6], "in xip before and after the wrap, and at xmax");
        seen.prune(&g);
        assert_eq!(seen.len(), 3);
        // A next-epoch xid far below the anchor's low bits is still AFTER, not before.
        seen.note(7, ["public.items"]);
        assert!(seen.unsettled(&items(), &SnapshotGate::parse("4294967303:4294967303:", "0/1")).is_some());
        let done = SnapshotGate::parse("4294967304:4294967304:", "0/1");
        seen.prune(&done);
        assert!(seen.is_empty());
    }

    /// T is sequenced (noted) before the request's pairing point, and the request's snapshot S is
    /// taken while T is still invisible. If T becomes visible and the poller prunes it between S being
    /// opened and S being checked, the check must still see that S excludes T: it is made against
    /// the record as it stood BEFORE S was opened, not the live one.
    #[tokio::test]
    async fn a_prune_between_opening_and_checking_a_snapshot_does_not_settle_it() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(600, ["public.items"]);
        let (_, check) = open_checked(&seen, &items(), || async {
            let s = gate("600:601:600"); // S: T (600) still in progress
            seen.prune(&gate("601:601:")); // T visible now; the poller forgets it
            Ok(s)
        })
        .await
        .unwrap();
        let Check::Pending(pending) = check else { panic!("S excludes T but was accepted as settled") };
        assert_eq!(pending.iter().map(|&v| v as u32).collect::<Vec<_>>(), vec![600]);
    }

    /// Past its bound the record gives up its OLDEST transactions, and fences their table: a
    /// snapshot of it that excludes one — or cannot be shown not to (its xmin is not past the fence)
    /// — is refused, never accepted as settled. Memory stays at the bound however long the poller
    /// cannot run, and a table nothing was given up on is unaffected.
    #[test]
    fn the_bound_fences_the_tables_it_gave_up_transactions_on() {
        let seen = set_with(SettleConfig { max_xids: 2 * CHUNK_BITS, ..SettleConfig::default() });
        let dropped_before = METRICS.dropped.load(Ordering::Relaxed);
        // A held transaction, then three chunks' worth of transactions no poller ever prunes.
        seen.note(10_000, ["public.items"]);
        for x in 10_001..10_001 + 3 * CHUNK_BITS {
            seen.note(x, ["public.items"]);
        }
        let last = 10_001 + 3 * CHUNK_BITS;
        seen.note(last, ["public.other"]); // newest: the bound takes items' chunks, never this one
        let r = seen.record.lock().unwrap();
        assert!(r.chunks <= 2, "held at the bound: {} chunks", r.chunks);
        assert!(r.fences.contains_key("public.items"));
        drop(r);
        assert!(METRICS.dropped.load(Ordering::Relaxed) > dropped_before, "the drop is counted");
        let v = |x: u64| seen.record.lock().unwrap().unwrap(x as u32);
        // A snapshot that excludes the given-up 10_000 (xmin at it) is fenced, not settled.
        let fenced = seen.capture(&items()).check(&gate("10000:10000:"));
        assert!(matches!(fenced, Check::Fenced(f) if f >= v(10_000)), "{fenced:?}");
        // So is one whose xmin sits anywhere at or below the fence, whatever else it sees.
        let fence = seen.record.lock().unwrap().fences["public.items"];
        let below = gate(&format!("{0}:{0}:", fence as u32));
        assert!(matches!(seen.capture(&items()).check(&below), Check::Fenced(_)));
        // A snapshot whose xmin is past the fence has seen every given-up xid end: only what is still
        // recorded is checked.
        let past = gate(&format!("{0}:{0}:", fence as u32 + 1));
        assert!(!matches!(seen.capture(&items()).check(&past), Check::Fenced(_)));
        // A table the bound took nothing from — and a subquery scope reading it — is served as before.
        let other = SettleScope::request(&TableRef::parse("public.other").unwrap());
        assert!(!r_fenced(&seen, "public.other"));
        assert_eq!(seen.capture(&other).check(&gate("10000:10000:")), Check::Pending(vec![v(last)]));
        let after = gate(&format!("{0}:{0}:", last + 1));
        assert_eq!(seen.capture(&other).check(&after), Check::Settled);
        let joined = other.with([&TableRef::parse("public.items").unwrap()]);
        assert!(matches!(seen.capture(&joined).check(&gate("10000:10000:")), Check::Fenced(_)));
        // The poller clears the fence once a snapshot's xmin is past it — not before.
        seen.prune(&below);
        assert!(seen.record.lock().unwrap().fences.contains_key("public.items"), "xmin not past the fence");
        seen.prune(&past);
        assert!(seen.record.lock().unwrap().fences.is_empty(), "cleared once xmin passed it");
        assert!(!matches!(seen.capture(&items()).check(&gate("10000:10000:")), Check::Fenced(_)));
    }

    fn r_fenced(seen: &SequencedXids, table: &str) -> bool {
        seen.record.lock().unwrap().fences.contains_key(table)
    }

    /// A transaction a waiter is waiting for that the bound then gives up does NOT satisfy the wait:
    /// it stays pending under the overflow fence until the poller sees xmin past the fence.
    #[tokio::test]
    async fn eviction_never_satisfies_a_waiter() {
        let seen = set_with(SettleConfig { max_xids: 2 * CHUNK_BITS, ..SettleConfig::default() });
        seen.note(20_000, ["public.items"]);
        let pending = seen.unsettled(&items(), &gate("20000:20001:20000")).unwrap();
        let s = seen.clone();
        let evictor = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            for x in 20_001..20_001 + 3 * CHUNK_BITS {
                s.note(x, ["public.items"]); // overflows: 20_000's chunk is given up
            }
            s.generation.send_modify(|g| *g += 1);
        });
        let started = Instant::now();
        let err = seen
            .wait_until_settled(&items(), pending.clone(), started + Duration::from_millis(300))
            .await
            .expect_err("a given-up transaction is not a visible one");
        assert_eq!(err.cause, UnsettledCause::TimedOut);
        evictor.await.unwrap();
        assert!(!seen.record.lock().unwrap().contains("public.items", pending[0]), "it was evicted");
        // Once the poller sees xmin past the fence, the same wait settles.
        let fence = seen.record.lock().unwrap().fences["public.items"];
        seen.prune(&gate(&format!("{0}:{0}:", fence as u32 + 1)));
        seen.wait_until_settled(&items(), pending, Instant::now() + Duration::from_secs(5)).await.expect("settles");
    }

    /// A sparse backlog — a few held transactions among millions that became visible — costs a chunk
    /// per held transaction, not the span between them, so it never nears the bound.
    #[test]
    fn a_sparse_backlog_costs_chunks_not_span() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(1_000, ["public.items"]);
        for x in 1_001..200_000u64 {
            seen.note(x, ["public.items"]);
            if x % 5_000 == 0 {
                seen.prune(&SnapshotGate::parse(&format!("1000:{}:1000", x + 1), "0/1"));
            }
        }
        seen.prune(&SnapshotGate::parse("1000:200001:1000", "0/1"));
        assert_eq!(seen.len(), 1);
        assert_eq!(seen.record.lock().unwrap().chunks, 1);
    }

    /// A waiter whose poller cannot run (here: none at all) is not wedged: it ends at its deadline
    /// with a retryable error, and it never held a pooled connection while it waited.
    #[tokio::test]
    async fn a_failed_poller_does_not_wedge_a_waiter() {
        let seen = Arc::new(SequencedXids::new(
            Some("postgres://postgres@127.0.0.1:1/unreachable".into()),
            SettleConfig::default(),
            20,
        ));
        seen.note(300, ["public.items"]);
        let pending = seen.unsettled(&items(), &gate("300:301:300")).unwrap();
        let started = Instant::now();
        let err = seen
            .wait_until_settled(&items(), pending, started + Duration::from_millis(300))
            .await
            .expect_err("nothing ever sees 300 visible");
        assert_eq!(err.cause, UnsettledCause::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2), "bounded by the deadline: {:?}", started.elapsed());
        assert!(METRICS.poller_failures.load(Ordering::Relaxed) > 0, "the poller ran and failed");
        assert_eq!(seen.waiters.load(Ordering::Acquire), 0, "the waiter is uncounted on the way out");
    }

    /// A waiter wakes as soon as its transactions are gone from the record (here pruned by hand, as
    /// the poller would), without waiting out its deadline.
    #[tokio::test]
    async fn a_waiter_wakes_when_its_transactions_are_seen_visible() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(400, ["public.items"]);
        let pending = seen.unsettled(&items(), &gate("400:401:400")).unwrap();
        let s = seen.clone();
        let pruner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            s.prune(&gate("401:401:"));
            s.generation.send_modify(|g| *g += 1);
        });
        let started = Instant::now();
        seen.wait_until_settled(&items(), pending, started + Duration::from_secs(10)).await.expect("settles");
        assert!(started.elapsed() < Duration::from_secs(5));
        pruner.await.unwrap();
    }

    /// Admission: past the cap a request-driven wait is refused at once; an internal one is not
    /// subject to the cap.
    #[test]
    fn the_waiter_cap_admits_a_quarter_of_the_pool_by_default() {
        let seen = SequencedXids::new(None, SettleConfig::default(), 20);
        let held: Vec<_> = (0..5).map(|_| seen.admission.try_acquire().expect("within the cap")).collect();
        assert!(seen.admission.try_acquire().is_err(), "the sixth waiter is refused");
        drop(held);
        let tiny = SequencedXids::new(None, SettleConfig::default(), 2);
        assert!(tiny.admission.try_acquire().is_ok(), "never below one");
        let explicit = SequencedXids::new(None, SettleConfig { max_waiters: 2, ..SettleConfig::default() }, 20);
        let _a = explicit.admission.try_acquire().unwrap();
        let _b = explicit.admission.try_acquire().unwrap();
        assert!(explicit.admission.try_acquire().is_err());
    }

    /// Foreign xids — a far-future one from before an epoch reset — are still recorded and reported
    /// (the poller's `pg_xact_status` check is what forgets them), and `forget` removes exactly them.
    #[test]
    fn forget_removes_exactly_the_named_xids() {
        let seen = Arc::new(SequencedXids::default());
        seen.note(500, ["public.items", "public.other"]);
        seen.note(501, ["public.items"]);
        let v500 = seen.record.lock().unwrap().unwrap(500);
        seen.forget(&[v500]);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen.record.lock().unwrap().tables.len(), 1, "an emptied table is dropped");
    }
}
