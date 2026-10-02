//! Shape retention: the three-tier lifecycle (active / dormant / evicted) and its layered,
//! dormant-only eviction policy.
//!
//! Replaces delete-on-refcount-0 as the primary lifecycle (a retained shape is not kept actively
//! maintained for ever):
//!
//! - **Active** — maintained live by a tailer. Refcount-0 / client disconnect does NOT deactivate;
//!   brief reconnects stay warm and rejoin the same stream.
//! - **Dormant** — after sitting idle (no engine-visible reads and refcount 0) for
//!   [`RetentionConfig::idle_timeout`]: the tailer's routing state for the shape is dropped, while
//!   the durable stream and the shape record are retained at zero engine cost. Any touch
//!   reactivates by replaying the global change log from the captured resume **position** —
//!   `(segment, offset)`, following rotation pointers across segments (ADR-0006) — with no Postgres
//!   backfill (see `Engine::ensure_active`). While dormant a shape PINS its resume segment against
//!   deletion; one that would pin it past `CIRCUITS_CHANGES_RETAIN_SECS` is evicted
//!   ([`EvictReason::ChangeLogRetention`]) so the segment can go.
//! - **Evicted** — stream and record deleted; a returning client gets `404` and recreates.
//!
//! Eviction is **layered** and applies to dormant shapes only (active shapes are never evicted),
//! least-recently-read first:
//! 1. **Dormancy TTL** (hygiene): dormant longer than [`RetentionConfig::dormant_ttl`] → evict.
//!    Reaps dead shapes even with no resource pressure.
//! 2. **`max_shapes` count cap** (engine cost bound): when the total shape count exceeds
//!    [`RetentionConfig::max_shapes`], evict least-recently-read dormant shapes until under.
//! 3. **Disk budget** (hard backstop): when the tracked shape-stream bytes exceed
//!    [`RetentionConfig::disk_budget_bytes`], evict least-recently-read dormant shapes until
//!    under. Byte accounting is engine-side ([`crate::ds::DsClient`] counts what it appends) —
//!    the durable-streams server exposes no per-stream sizes yet — so it undercounts streams
//!    written before the current process started.
//!
//! If a cap/budget is exceeded but nothing dormant is left to evict, the sweep logs loudly and
//! bumps a metric instead of evicting active shapes.
//!
//! Subquery and aggregate shapes are exempt from dormancy (their engine state — inner-set
//! arrangements, running folds — cannot be rebuilt from a change-log replay alone), so they stay
//! active while retained. So that they cannot leak forever once unsubscribed, the TTL layer evicts
//! them **straight from active** (full teardown) after the same total grace an eligible shape gets
//! (idle timeout + dormancy TTL); like any evicted shape, a returning client recreates them.
//!
//! This module holds the configuration, the lifecycle state machine types, and the **pure** sweep
//! planner ([`plan_sweep`]); `crate::engine` owns the state and executes plans. Persistence of the
//! lifecycle (catalog, `last_read` flushes, restart recovery) is the follow-up catalog work (GH
//! issue #8). Dormancy IS durable: the engine's `meta/catalog` records `Dormant`/`Reactivated`
//! events (with the resume position + snapshot gate), so a restart restores dormant shapes as
//! dormant — no re-registration, no backfill. Only the in-memory clocks reset (dormancy age
//! restarts at boot, so the TTL is conservative across restarts).

use std::time::{Duration, Instant};

use crate::changelog::LogPosition;
use crate::heap_size::HeapSize;
use crate::pg::SnapshotGate;

/// A retained shape lost an input its replay needs and was durably retired. Readers must create
/// a fresh subscription instead of retrying a handle that can never be brought up to date.
#[derive(Debug, Clone)]
pub struct ShapeReplayRetired {
    pub shape: String,
    pub stream: String,
}

impl std::fmt::Display for ShapeReplayRetired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shape '{}' was retired after losing stream '{}'; recreate the subscription", self.shape, self.stream)
    }
}

impl std::error::Error for ShapeReplayRetired {}

/// Replay could not finish, but loss was not confirmed. The retained shape and its resume state
/// stay available for another attempt.
#[derive(Debug, Clone)]
pub struct ReactivationUnavailable {
    pub shape: String,
    pub cause: String,
}

impl std::fmt::Display for ReactivationUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shape '{}' reactivation is unavailable; retry the read: {}", self.shape, self.cause)
    }
}

impl std::error::Error for ReactivationUnavailable {}

/// The one replay result shared by every concurrent touch. Terminal loss is distinct from a
/// failed attempt whose retained stream and resume position are still usable.
#[derive(Debug, Clone)]
pub enum ReactivationOutcome {
    Active,
    Retired { stream: String },
    Retry { cause: String },
}

/// Retention tuning, read from the environment once at engine construction.
///
/// | Env var | Default | Meaning |
/// |---|---|---|
/// | `CIRCUITS_SHAPE_IDLE_SECS` | `1800` (30 min) | Idle time (no reads, refcount 0) before an active shape goes dormant. `0` disables dormancy. |
/// | `CIRCUITS_SHAPE_DORMANT_TTL_SECS` | `604800` (7 days) | Time a shape may stay dormant before it is evicted. `0` disables the TTL layer. |
/// | `CIRCUITS_MAX_SHAPES` | `10000` | Total shape-count cap; over it, least-recently-read dormant shapes are evicted. `0` = unlimited. |
/// | `CIRCUITS_SHAPE_DISK_BUDGET_MB` | `0` (disabled) | Cap on tracked shape-stream bytes; over it, least-recently-read dormant shapes are evicted. |
/// | `CIRCUITS_RETENTION_SWEEP_SECS` | `60` | Sweep interval of the background retention task. |
#[derive(Clone, Debug)]
pub struct RetentionConfig {
    /// Active → dormant idle threshold (`CIRCUITS_SHAPE_IDLE_SECS`, default 30 min; 0 = never).
    pub idle_timeout: Duration,
    /// Dormant → evicted hygiene TTL (`CIRCUITS_SHAPE_DORMANT_TTL_SECS`, default 7 days; 0 = never).
    pub dormant_ttl: Duration,
    /// Total shape-count cap (`CIRCUITS_MAX_SHAPES`, default 10000; 0 = unlimited).
    pub max_shapes: usize,
    /// Shape-stream disk budget in bytes (`CIRCUITS_SHAPE_DISK_BUDGET_MB`, default 0 = disabled).
    pub disk_budget_bytes: u64,
    /// Background sweep interval (`CIRCUITS_RETENTION_SWEEP_SECS`, default 60s).
    pub sweep_interval: Duration,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(default)
}

impl Default for RetentionConfig {
    fn default() -> Self {
        RetentionConfig {
            idle_timeout: Duration::from_secs(1800),
            dormant_ttl: Duration::from_secs(7 * 24 * 3600),
            max_shapes: 10_000,
            disk_budget_bytes: 0,
            sweep_interval: Duration::from_secs(60),
        }
    }
}

impl RetentionConfig {
    pub fn from_env() -> Self {
        let d = RetentionConfig::default();
        RetentionConfig {
            idle_timeout: Duration::from_secs(env_u64("CIRCUITS_SHAPE_IDLE_SECS", d.idle_timeout.as_secs())),
            dormant_ttl: Duration::from_secs(env_u64("CIRCUITS_SHAPE_DORMANT_TTL_SECS", d.dormant_ttl.as_secs())),
            max_shapes: env_u64("CIRCUITS_MAX_SHAPES", d.max_shapes as u64) as usize,
            disk_budget_bytes: env_u64("CIRCUITS_SHAPE_DISK_BUDGET_MB", 0).saturating_mul(1024 * 1024),
            sweep_interval: Duration::from_secs(env_u64("CIRCUITS_RETENTION_SWEEP_SECS", 60).max(1)),
        }
    }
}

/// Where a shape is in the lifecycle. Held per shape id by the engine (`Engine::lives`).
pub enum LifeState {
    /// Maintained live by its tailer.
    Active,
    /// The active → dormant transition is in flight (the tailer is unregistering the shape and
    /// capturing the resume state). A touch waits for it to finish, then reactivates.
    Deactivating { done: tokio::sync::watch::Receiver<bool> },
    /// Engine state dropped; stream + record retained. `resume` is the change-log position
    /// (segment + offset — the log is segmented, ADR-0006) up to which the shape's stream is
    /// complete; `gate` is the shape's original backfill-snapshot fence (still needed if the shape
    /// went dormant with pre-backfill changes in flight). While dormant the shape **pins** its
    /// resume segment: the segment is not deleted until the shape is reactivated or evicted.
    Dormant { since: Instant, resume: LogPosition, gate: SnapshotGate },
    /// A touch is replaying the change log to bring the shape back. Concurrent touches await
    /// the same outcome, including whether failure permanently retired the shape or is retryable.
    /// `resume` is the position the replay is running FROM: it keeps pinning its change-log segment
    /// for the whole replay, so the sweeper cannot delete the segment out from under a
    /// reactivation that outlives one sweep tick (ADR-0006).
    Reactivating { done: tokio::sync::watch::Receiver<Option<ReactivationOutcome>>, resume: LogPosition },
}

impl HeapSize for LifeState {
    /// Only `Dormant`'s `resume` position (its offset `String`) and `gate` own heap; the
    /// `watch::Receiver` variants are channel handles (shared with the sender side), not uniquely
    /// owned data.
    fn heap_bytes(&self) -> usize {
        match self {
            LifeState::Dormant { since: _, resume, gate } => resume.heap_bytes() + gate.heap_bytes(),
            LifeState::Reactivating { done: _, resume } => resume.heap_bytes(),
            LifeState::Active | LifeState::Deactivating { .. } => 0,
        }
    }
}

/// Per-shape lifecycle record.
pub struct ShapeLife {
    /// Last engine-visible read/touch (shape create/join, stream read, rows/log fold). Drives both the idle timer and the LRU eviction order. Direct
    /// durable-streams reads bypass the engine and are NOT observed — but such readers hold a
    /// subscription (refcount ≥ 1), which also blocks dormancy.
    pub last_read: Instant,
    pub state: LifeState,
}

impl HeapSize for ShapeLife {
    /// `last_read` (`Instant`) is inline (no heap).
    fn heap_bytes(&self) -> usize {
        self.state.heap_bytes()
    }
}

impl ShapeLife {
    pub fn active() -> Self {
        ShapeLife { last_read: Instant::now(), state: LifeState::Active }
    }
}

/// One shape's sweep-relevant status, snapshotted by the engine for [`plan_sweep`].
pub struct SweepShape {
    pub id: String,
    /// Live subscriptions (shared-feed refcount; 0 for unshared shapes).
    pub refcount: usize,
    /// Time since the last engine-visible read.
    pub idle: Duration,
    /// `Some(time dormant)` iff the shape is dormant; `None` while active/transitioning.
    pub dormant_for: Option<Duration>,
    /// True while a deactivation or reactivation is in flight — the sweep leaves it alone.
    pub in_transition: bool,
    /// Eligible for dormancy at all (plain row shapes; subquery + aggregate shapes are not).
    pub dormancy_eligible: bool,
    /// Tracked bytes appended to the shape's stream (engine-side accounting).
    pub stream_bytes: u64,
}

/// Did an eviction actually happen?
///
/// The change log's sweeper needs the difference (ADR-0006). It evicts a dormant shape in order to
/// **unpin** the segment the shape resumes from, and then deletes that segment; a "nothing to do"
/// answer — the shape was touched between planning and eviction and is now reactivating, or it was
/// re-subscribed — must not be read as "the pin is released", or the segment would be deleted out
/// from under a live replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evicted {
    /// The shape is gone (or was already gone): nothing it held is held any more.
    Yes,
    /// Nothing was done — the shape is mid-transition, or has been touched/re-subscribed since the
    /// sweep's snapshot. Everything it pinned, it still pins.
    Skipped,
}

/// Why a shape is being evicted (for logs/metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictReason {
    DormantTtl,
    MaxShapes,
    DiskBudget,
    /// The shape's dormant resume position sits in a change-log segment that was rotated out more
    /// than `CIRCUITS_CHANGES_RETAIN_SECS` ago (ADR-0006). Evicting it is what unpins the
    /// segment so it can be deleted — the change log must not be held hostage by a shape nobody has
    /// touched in a week.
    ChangeLogRetention,
}

impl EvictReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictReason::DormantTtl => "dormant-ttl",
            EvictReason::MaxShapes => "max-shapes",
            EvictReason::DiskBudget => "disk-budget",
            EvictReason::ChangeLogRetention => "change-log-retention",
        }
    }
}

/// What one retention sweep should do. Deactivations and evictions are executed by the engine;
/// the `over_*` flags mean a cap/budget is exceeded with nothing dormant left to evict — surfaced
/// loudly (log + metric), never resolved by evicting active shapes.
#[derive(Default)]
pub struct SweepPlan {
    pub deactivate: Vec<String>,
    pub evict: Vec<(String, EvictReason)>,
    pub over_capacity: bool,
    pub over_budget: bool,
}

/// Decide one sweep's actions from a status snapshot. Pure — all engine state is passed in — so
/// the layered policy is unit-testable without tailers or storage.
pub fn plan_sweep(cfg: &RetentionConfig, shapes: &[SweepShape]) -> SweepPlan {
    let mut plan = SweepPlan::default();

    // Tier 0 — dormancy: idle, unsubscribed, eligible, settled shapes go dormant.
    if !cfg.idle_timeout.is_zero() {
        for s in shapes {
            if s.dormancy_eligible
                && !s.in_transition
                && s.dormant_for.is_none()
                && s.refcount == 0
                && s.idle >= cfg.idle_timeout
            {
                plan.deactivate.push(s.id.clone());
            }
        }
    }

    let mut evicted: std::collections::HashSet<&str> = std::collections::HashSet::new();

    // Tier 1 — dormancy TTL (hygiene, independent of pressure).
    if !cfg.dormant_ttl.is_zero() {
        for s in shapes {
            if s.dormant_for.is_some_and(|d| d >= cfg.dormant_ttl) {
                plan.evict.push((s.id.clone(), EvictReason::DormantTtl));
                evicted.insert(&s.id);
            }
        }
        // Shapes that cannot park (subquery / aggregate — their state is not rebuildable from a
        // bounded replay) would otherwise be immortal once unsubscribed: evict them straight from
        // active after the same total grace an eligible shape gets (idle timeout + dormancy TTL).
        // They are recreatable — a returning client gets 404 and recreates.
        if !cfg.idle_timeout.is_zero() {
            for s in shapes {
                if !s.dormancy_eligible
                    && !s.in_transition
                    && s.refcount == 0
                    && s.idle >= cfg.idle_timeout + cfg.dormant_ttl
                {
                    plan.evict.push((s.id.clone(), EvictReason::DormantTtl));
                    evicted.insert(&s.id);
                }
            }
        }
    }

    // Dormant shapes still standing after tier 1, least-recently-read first (largest idle first).
    let mut lru: Vec<&SweepShape> =
        shapes.iter().filter(|s| s.dormant_for.is_some() && !evicted.contains(s.id.as_str())).collect();
    lru.sort_by_key(|s| std::cmp::Reverse(s.idle));
    let mut lru = lru.into_iter();

    // Tier 2 — max_shapes count cap.
    if cfg.max_shapes > 0 {
        let mut count = shapes.len() - evicted.len();
        while count > cfg.max_shapes {
            match lru.next() {
                Some(s) => {
                    plan.evict.push((s.id.clone(), EvictReason::MaxShapes));
                    evicted.insert(&s.id);
                    count -= 1;
                }
                None => {
                    plan.over_capacity = true;
                    break;
                }
            }
        }
    }

    // Tier 3 — disk budget over the tracked shape-stream bytes.
    if cfg.disk_budget_bytes > 0 {
        let mut total: u64 = shapes.iter().filter(|s| !evicted.contains(s.id.as_str())).map(|s| s.stream_bytes).sum();
        while total > cfg.disk_budget_bytes {
            match lru.next() {
                Some(s) => {
                    plan.evict.push((s.id.clone(), EvictReason::DiskBudget));
                    evicted.insert(&s.id);
                    total = total.saturating_sub(s.stream_bytes);
                }
                None => {
                    plan.over_budget = true;
                    break;
                }
            }
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RetentionConfig {
        RetentionConfig {
            idle_timeout: Duration::from_secs(1800),
            dormant_ttl: Duration::from_secs(7 * 24 * 3600),
            max_shapes: 0,
            disk_budget_bytes: 0,
            sweep_interval: Duration::from_secs(60),
        }
    }

    fn shape(id: &str) -> SweepShape {
        SweepShape {
            id: id.into(),
            refcount: 0,
            idle: Duration::ZERO,
            dormant_for: None,
            in_transition: false,
            dormancy_eligible: true,
            stream_bytes: 0,
        }
    }

    fn dormant(id: &str, idle_secs: u64, dormant_secs: u64) -> SweepShape {
        SweepShape {
            idle: Duration::from_secs(idle_secs),
            dormant_for: Some(Duration::from_secs(dormant_secs)),
            ..shape(id)
        }
    }

    #[test]
    fn idle_unsubscribed_shapes_go_dormant() {
        let shapes = vec![
            SweepShape { idle: Duration::from_secs(3600), ..shape("s1") }, // idle past the timeout
            SweepShape { idle: Duration::from_secs(60), ..shape("s2") },   // recently read
            SweepShape { idle: Duration::from_secs(3600), refcount: 2, ..shape("s3") }, // subscribed
            SweepShape { idle: Duration::from_secs(3600), dormancy_eligible: false, ..shape("s4") }, // aggregate/subquery
            SweepShape { idle: Duration::from_secs(3600), in_transition: true, ..shape("s5") },      // mid-transition
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.deactivate, vec!["s1"]);
        assert!(plan.evict.is_empty());
    }

    #[test]
    fn zero_idle_timeout_disables_dormancy() {
        let c = RetentionConfig { idle_timeout: Duration::ZERO, ..cfg() };
        let shapes = vec![SweepShape { idle: Duration::from_secs(1 << 20), ..shape("s1") }];
        assert!(plan_sweep(&c, &shapes).deactivate.is_empty());
    }

    #[test]
    fn dormancy_ttl_reaps_old_dormant_shapes() {
        let shapes = vec![
            dormant("s1", 100, 8 * 24 * 3600), // dormant past the TTL
            dormant("s2", 100, 3600),          // dormant, young
            SweepShape { idle: Duration::from_secs(9 * 24 * 3600), ..shape("s3") }, // active (long idle but never dormanted — e.g. still subscribed elsewhere)
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.evict, vec![("s1".to_string(), EvictReason::DormantTtl)]);
    }

    #[test]
    fn non_parkable_shapes_are_evicted_from_active_after_the_full_grace() {
        let grace = 1800 + 7 * 24 * 3600; // idle_timeout + dormant_ttl
        let shapes = vec![
            // An aggregate/subquery shape (not dormancy-eligible), unsubscribed and idle past the
            // full grace window → evicted straight from active.
            SweepShape { idle: Duration::from_secs(grace), dormancy_eligible: false, ..shape("agg-old") },
            // Same but still subscribed → protected.
            SweepShape { idle: Duration::from_secs(grace), dormancy_eligible: false, refcount: 1, ..shape("agg-held") },
            // Same but within the grace window → kept.
            SweepShape { idle: Duration::from_secs(grace - 1), dormancy_eligible: false, ..shape("agg-young") },
        ];
        let plan = plan_sweep(&cfg(), &shapes);
        assert_eq!(plan.evict, vec![("agg-old".to_string(), EvictReason::DormantTtl)]);
        assert!(plan.deactivate.is_empty(), "non-parkable shapes never deactivate");
    }

    #[test]
    fn max_shapes_evicts_least_recently_read_dormant_first() {
        let c = RetentionConfig { max_shapes: 2, ..cfg() };
        let shapes = vec![
            shape("active"),
            dormant("cold", 5000, 60), // least recently read → goes first
            dormant("warm", 100, 60),
        ];
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("cold".to_string(), EvictReason::MaxShapes)]);
        assert!(!plan.over_capacity);
    }

    #[test]
    fn max_shapes_never_evicts_active_shapes() {
        let c = RetentionConfig { max_shapes: 1, ..cfg() };
        let shapes = vec![shape("a1"), shape("a2"), shape("a3")];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty());
        assert!(plan.over_capacity, "over the cap with nothing dormant must be surfaced, not resolved");
    }

    #[test]
    fn disk_budget_evicts_lru_dormant_until_under() {
        let c = RetentionConfig { disk_budget_bytes: 100, ..cfg() };
        let shapes = vec![
            SweepShape { stream_bytes: 80, ..shape("active") },
            SweepShape { stream_bytes: 30, ..dormant("cold", 5000, 60) },
            SweepShape { stream_bytes: 30, ..dormant("warm", 100, 60) },
        ];
        // 140 tracked > 100: evicting "cold" (LRU) brings it to 110; still over, evict "warm" → 80.
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(
            plan.evict,
            vec![("cold".to_string(), EvictReason::DiskBudget), ("warm".to_string(), EvictReason::DiskBudget)]
        );
        assert!(!plan.over_budget);
    }

    #[test]
    fn disk_budget_over_with_all_active_flags_loudly() {
        let c = RetentionConfig { disk_budget_bytes: 10, ..cfg() };
        let shapes = vec![SweepShape { stream_bytes: 100, ..shape("a") }];
        let plan = plan_sweep(&c, &shapes);
        assert!(plan.evict.is_empty());
        assert!(plan.over_budget);
    }

    #[test]
    fn ttl_evictions_count_toward_the_cap_and_budget() {
        // s1 falls to the TTL; that alone brings the count under max_shapes, so no cap eviction.
        let c = RetentionConfig { max_shapes: 1, ..cfg() };
        let shapes = vec![shape("active"), dormant("old", 5000, 8 * 24 * 3600)];
        let plan = plan_sweep(&c, &shapes);
        assert_eq!(plan.evict, vec![("old".to_string(), EvictReason::DormantTtl)]);
        assert!(!plan.over_capacity);
    }

    #[test]
    fn config_defaults_are_sensible() {
        let d = RetentionConfig::default();
        assert_eq!(d.idle_timeout, Duration::from_secs(1800));
        assert_eq!(d.dormant_ttl, Duration::from_secs(604800));
        assert_eq!(d.max_shapes, 10_000);
        assert_eq!(d.disk_budget_bytes, 0);
        assert_eq!(d.sweep_interval, Duration::from_secs(60));
    }
}
