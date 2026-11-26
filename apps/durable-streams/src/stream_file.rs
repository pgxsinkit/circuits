//! Bounded open data files.
//!
//! Every stream has one data file. The server used to hold each one open for
//! the life of the process, so the open-file count grew with the number of
//! streams ever touched and, in production, reached the container's `nofile`
//! limit after 15 days: WAL checkpoints then failed with `Too many open files`
//! and cold-stream requests returned 500.
//!
//! A [`StreamFile`] is the stream's handle to its data file. It keeps the file
//! open while it is in use and lets the process-wide [`Pool`] close it when:
//!
//! - it has been idle for the idle timeout ([`Pool::close_idle`], run by a
//!   background task), or
//! - the number of open data files reaches the pool's budget, in which case the
//!   least recently used ones are closed before another is opened (LRU).
//!
//! Closing only drops the handle's own `Arc<File>`. Anyone who already resolved
//! the file (an in-flight read, a checkpoint barrier, an appender mid-write)
//! keeps a valid descriptor until it drops its clone, and a file that someone
//! else still holds is never counted as closable. The next [`StreamFile::get`]
//! reopens the path `O_APPEND`, exactly how the file was opened at creation.
//!
//! Durability is unchanged: closing a descriptor does not discard page-cache
//! data, the WAL keeps every acked record until a checkpoint barrier has made
//! the stream files durable (`syncfs` on the stream lanes on Linux, so no
//! per-stream descriptor is involved; `fdatasync` on the same inode through a
//! reopened descriptor elsewhere), and a failed reopen fails the checkpoint
//! before anything is recycled.
//!
//! Reopening by path is only correct while the path names this handle's inode.
//! The one place that replaces a live data file, compaction, [`StreamFile::pin`]s
//! the old handle before renaming the new file over the path, so a reader that
//! captured the old `(file, file_base)` pair keeps reading the old inode.
//!
//! No global lock sits on the read or append path: [`StreamFile::get`] takes
//! the handle's own `RwLock` for reading and refreshes a timestamp. The pool's
//! registry (a sharded map of open handles) is touched only when a file is
//! opened or closed.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Process-wide accounting and eviction for open stream data files.
pub struct Pool {
    /// Data files currently held open by a handle.
    open: AtomicUsize,
    /// Open data files at which the least recently used are closed before
    /// another opens. `usize::MAX` = unbounded (the default until configured).
    max_open: AtomicUsize,
    next_key: AtomicU64,
    /// Every handle whose file is open, so eviction is O(open), not O(streams).
    registry: DashMap<u64, Weak<StreamFile>>,
    /// One LRU pass at a time; concurrent openers skip rather than queue.
    evicting: AtomicBool,
    epoch: Instant,
}

impl Pool {
    pub fn new() -> Self {
        Self {
            open: AtomicUsize::new(0),
            max_open: AtomicUsize::new(usize::MAX),
            next_key: AtomicU64::new(0),
            registry: DashMap::new(),
            evicting: AtomicBool::new(false),
            epoch: Instant::now(),
        }
    }

    /// The pool every stream handle in this process uses.
    pub fn global() -> &'static Pool {
        static POOL: OnceLock<Pool> = OnceLock::new();
        POOL.get_or_init(Pool::new)
    }

    pub fn set_max_open(&self, max_open: usize) {
        self.max_open.store(max_open.max(1), Ordering::Relaxed);
    }

    pub fn max_open(&self) -> usize {
        self.max_open.load(Ordering::Relaxed)
    }

    /// Data files currently held open by a handle.
    pub fn open_count(&self) -> usize {
        self.open.load(Ordering::Relaxed)
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Snapshot the open handles. Collects weak references first and upgrades
    /// them only after the map iteration ends: dropping an upgraded last
    /// reference inside the iteration would run `StreamFile::drop`, which
    /// removes from the shard being iterated.
    fn open_handles(&self) -> Vec<Arc<StreamFile>> {
        let weak: Vec<Weak<StreamFile>> = self.registry.iter().map(|e| e.value().clone()).collect();
        weak.into_iter().filter_map(|w| w.upgrade()).collect()
    }

    /// Close every open file that has been idle for at least `idle` and is not
    /// in use elsewhere. Returns how many were closed.
    pub fn close_idle(&self, idle: Duration) -> usize {
        let Some(cutoff) = self.now_ms().checked_sub(idle.as_millis() as u64) else {
            return 0;
        };
        self.open_handles().into_iter().filter(|h| h.close_if(Some(cutoff))).count()
    }

    /// Close least-recently-used files until `open ≤ target`. Returns how many
    /// were closed.
    fn close_lru(&self, target: usize) -> usize {
        let mut handles = self.open_handles();
        handles.sort_by_key(|h| h.last_used_ms.load(Ordering::Relaxed));
        let mut closed = 0;
        for h in handles {
            if self.open_count() <= target {
                break;
            }
            if h.close_if(None) {
                closed += 1;
            }
        }
        closed
    }

    /// Called before opening a file: at the budget, close the least recently
    /// used tenth so the next opens do not each pay for a pass.
    fn make_room(&self) {
        let max = self.max_open();
        if self.open_count() < max {
            return;
        }
        if self.evicting.swap(true, Ordering::Acquire) {
            return;
        }
        self.close_lru(max - max / 10 - 1);
        self.evicting.store(false, Ordering::Release);
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::new()
    }
}

/// A stream's handle to its data file, opened on demand. See the module docs.
pub struct StreamFile {
    pool: &'static Pool,
    key: u64,
    me: Weak<StreamFile>,
    path: PathBuf,
    slot: RwLock<Option<Arc<File>>>,
    last_used_ms: AtomicU64,
    /// Never closed by the pool: set on a handle about to be replaced by a file
    /// renamed over its path (compaction), where a reopen would read the wrong
    /// inode.
    pinned: AtomicBool,
}

impl StreamFile {
    /// A handle over a file the caller just opened (`read` + `append`).
    pub fn from_open(path: PathBuf, file: File) -> Arc<Self> {
        Self::from_open_in(Pool::global(), path, file)
    }

    /// A handle that opens `path` on first use.
    pub fn lazy(path: PathBuf) -> Arc<Self> {
        Self::lazy_in(Pool::global(), path)
    }

    pub fn from_open_in(pool: &'static Pool, path: PathBuf, file: File) -> Arc<Self> {
        let handle = Self::lazy_in(pool, path);
        {
            let mut slot = handle.slot.write().unwrap();
            pool.make_room();
            handle.register(&mut slot, Arc::new(file));
        }
        handle
    }

    pub fn lazy_in(pool: &'static Pool, path: PathBuf) -> Arc<Self> {
        let key = pool.next_key.fetch_add(1, Ordering::Relaxed);
        Arc::new_cyclic(|me| StreamFile {
            pool,
            key,
            me: me.clone(),
            path,
            slot: RwLock::new(None),
            last_used_ms: AtomicU64::new(pool.now_ms()),
            pinned: AtomicBool::new(false),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether this handle currently holds its file open.
    pub fn is_open(&self) -> bool {
        self.slot.read().unwrap().is_some()
    }

    /// The open file, reopening it if the pool closed it. Callers drop the
    /// returned `Arc` when done; holding it keeps the descriptor open.
    pub fn get(&self) -> io::Result<Arc<File>> {
        self.touch();
        if let Some(file) = self.slot.read().unwrap().as_ref() {
            return Ok(Arc::clone(file));
        }
        self.pool.make_room();
        let mut slot = self.slot.write().unwrap();
        if let Some(file) = slot.as_ref() {
            return Ok(Arc::clone(file));
        }
        let file = match open_data_file(&self.path) {
            Ok(file) => file,
            Err(e) if is_fd_exhaustion(&e) => {
                // Another opener raced past the budget, or the budget is set
                // above what the process can hold: free some and retry once.
                let open = self.pool.open_count();
                self.pool.close_lru(open - open / 10);
                open_data_file(&self.path)?
            }
            Err(e) => return Err(e),
        };
        let file = Arc::new(file);
        self.register(&mut slot, Arc::clone(&file));
        Ok(file)
    }

    fn register(&self, slot: &mut Option<Arc<File>>, file: Arc<File>) {
        *slot = Some(file);
        self.pool.open.fetch_add(1, Ordering::Relaxed);
        self.pool.registry.insert(self.key, self.me.clone());
    }

    /// `O_APPEND` write of the whole buffer.
    pub fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        use std::io::Write;
        (&*self.get()?).write_all(buf)
    }

    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.get()?.set_len(len)
    }

    /// Keep this handle's current inode open for the handle's lifetime. Call
    /// before renaming another file over [`Self::path`].
    pub fn pin(&self) -> io::Result<()> {
        // Hold this descriptor while acquiring the slot lock. Publishing the pin under that
        // lock keeps an eviction that began earlier from closing it after we return.
        let file = self.get()?;
        let _slot = self.slot.write().unwrap();
        self.pinned.store(true, Ordering::Relaxed);
        drop(file);
        Ok(())
    }

    fn touch(&self) {
        let now = self.pool.now_ms();
        // Skip the store when nothing changed meaningfully, so concurrent
        // readers of one hot stream do not bounce its cache line.
        if now.saturating_sub(self.last_used_ms.load(Ordering::Relaxed)) >= 10 {
            self.last_used_ms.store(now, Ordering::Relaxed);
        }
    }

    /// Close the file if it is open, unpinned, held by nobody else and (when
    /// `idle_cutoff_ms` is given) last used at or before the cutoff.
    fn close_if(&self, idle_cutoff_ms: Option<u64>) -> bool {
        // Never wait: the holder is opening or closing it right now.
        let Ok(mut slot) = self.slot.try_write() else {
            return false;
        };
        if self.pinned.load(Ordering::Relaxed) {
            return false;
        }
        let Some(file) = slot.as_ref() else {
            return false;
        };
        if Arc::strong_count(file) > 1 {
            return false;
        }
        if idle_cutoff_ms.is_some_and(|cutoff| self.last_used_ms.load(Ordering::Relaxed) > cutoff) {
            return false;
        }
        *slot = None;
        self.pool.open.fetch_sub(1, Ordering::Relaxed);
        self.pool.registry.remove(&self.key);
        true
    }
}

impl Drop for StreamFile {
    fn drop(&mut self) {
        if self.slot.get_mut().map(|slot| slot.is_some()).unwrap_or(false) {
            self.pool.open.fetch_sub(1, Ordering::Relaxed);
            self.pool.registry.remove(&self.key);
        }
    }
}

impl std::fmt::Debug for StreamFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamFile").field("path", &self.path).field("open", &self.is_open()).finish()
    }
}

fn open_data_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).append(true).open(path)
}

fn is_fd_exhaustion(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
}

/// Run the pool's background work for the life of the runtime: close files idle
/// for `idle`, and every `stats_every` print one `ds_fd_stats` JSON line to
/// stderr (and record the same values as telemetry gauges). The line is what
/// the deployment's log-based alarm reads, so its shape is a contract:
/// `{"event":"ds_fd_stats","open_fds":…,"fd_limit":…,"open_stream_files":…,"max_open_stream_files":…}`.
pub fn spawn_maintenance(idle: Duration, stats_every: Option<Duration>) {
    let pool = Pool::global();
    let sweep_every = (idle / 4).clamp(Duration::from_secs(1), Duration::from_secs(15));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _ = tokio::task::spawn_blocking(move || pool.close_idle(idle)).await;
        }
    });
    if let Some(every) = stats_every {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                // Listing /proc/self/fd is O(open fds): keep it off the workers.
                let (open_fds, fd_limit) = tokio::task::spawn_blocking(process_fds).await.unwrap_or((None, None));
                let (open, max) = (pool.open_count() as u64, pool.max_open() as u64);
                eprintln!("{}", fd_stats_line(open_fds, fd_limit, open, max));
                crate::telemetry::record_fd_stats(open_fds, fd_limit, open, max);
            }
        });
    }
}

fn fd_stats_line(
    open_fds: Option<u64>,
    fd_limit: Option<u64>,
    open_stream_files: u64,
    max_open_stream_files: u64,
) -> String {
    serde_json::json!({
        "event": "ds_fd_stats",
        "open_fds": open_fds,
        "fd_limit": fd_limit,
        "open_stream_files": open_stream_files,
        "max_open_stream_files": max_open_stream_files,
    })
    .to_string()
}

/// The default open-stream-file budget: half the descriptor limit, leaving the
/// other half for connections, WAL segments, sealed chunks and sidecar writes.
pub fn default_max_open(fd_limit: Option<u64>) -> usize {
    match fd_limit {
        Some(limit) => ((limit / 2) as usize).max(64),
        None => 32_768,
    }
}

/// The process's open descriptors and its `RLIMIT_NOFILE` soft limit.
pub fn process_fds() -> (Option<u64>, Option<u64>) {
    #[cfg(target_os = "linux")]
    let dir = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let dir = "/dev/fd";
    // The directory handle itself shows up as one entry.
    let open = std::fs::read_dir(dir).ok().map(|entries| entries.count().saturating_sub(1) as u64);
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: getrlimit only writes the struct it is given.
    let limit =
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 { Some(limit.rlim_cur) } else { None };
    (open, limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;

    fn leaked_pool(max_open: usize) -> &'static Pool {
        let pool = Box::leak(Box::new(Pool::new()));
        pool.set_max_open(max_open);
        pool
    }

    fn data_files(tag: &str, n: usize) -> (PathBuf, Vec<PathBuf>) {
        let dir = std::env::temp_dir().join(format!(
            "ds-stream-file-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = (0..n)
            .map(|i| {
                let p = dir.join(format!("s{i}"));
                std::fs::write(&p, format!("stream-{i}")).unwrap();
                p
            })
            .collect();
        (dir, paths)
    }

    fn read_all(file: &File) -> String {
        let len = file.metadata().unwrap().len() as usize;
        let mut buf = vec![0; len];
        file.read_exact_at(&mut buf, 0).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn open_files_stay_within_the_budget_and_every_stream_still_reads() {
        let pool = leaked_pool(8);
        let (dir, paths) = data_files("budget", 100);
        let handles: Vec<_> =
            paths.iter().map(|p| StreamFile::from_open_in(pool, p.clone(), open_data_file(p).unwrap())).collect();
        assert!(pool.open_count() <= 8, "open {}", pool.open_count());
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(read_all(&h.get().unwrap()), format!("stream-{i}"));
            assert!(pool.open_count() <= 8, "open {}", pool.open_count());
        }
        drop(handles);
        assert_eq!(pool.open_count(), 0);
        assert!(pool.registry.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn appends_through_a_reopened_file_land_at_the_end() {
        let pool = leaked_pool(1);
        let (dir, paths) = data_files("append", 2);
        let a = StreamFile::lazy_in(pool, paths[0].clone());
        let b = StreamFile::lazy_in(pool, paths[1].clone());
        a.write_all(b"+a1").unwrap();
        b.write_all(b"+b1").unwrap(); // closes a
        assert!(!a.is_open());
        a.write_all(b"+a2").unwrap();
        assert_eq!(read_all(&a.get().unwrap()), "stream-0+a1+a2");
        assert_eq!(read_all(&b.get().unwrap()), "stream-1+b1");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_file_in_use_is_never_closed() {
        let pool = leaked_pool(usize::MAX);
        let (dir, paths) = data_files("in-use", 1);
        let h = StreamFile::lazy_in(pool, paths[0].clone());
        let held = h.get().unwrap();
        assert_eq!(pool.close_idle(Duration::ZERO), 0);
        assert!(h.is_open());
        drop(held);
        assert_eq!(pool.close_idle(Duration::ZERO), 1);
        assert!(!h.is_open());
        assert_eq!(pool.open_count(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn only_idle_files_close_on_the_idle_pass() {
        let pool = leaked_pool(usize::MAX);
        let (dir, paths) = data_files("idle", 2);
        let old = StreamFile::lazy_in(pool, paths[0].clone());
        let fresh = StreamFile::lazy_in(pool, paths[1].clone());
        old.get().unwrap();
        std::thread::sleep(Duration::from_millis(60));
        fresh.get().unwrap();
        assert_eq!(pool.close_idle(Duration::from_millis(40)), 1);
        assert!(!old.is_open());
        assert!(fresh.is_open());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Compaction renames a new file over the live path. A reader holding the
    /// old handle must keep reading the old inode, not reopen the new one.
    #[test]
    fn a_pinned_handle_keeps_its_inode_across_a_rename_over_its_path() {
        let pool = leaked_pool(1);
        let (dir, paths) = data_files("pin", 2);
        let live = StreamFile::lazy_in(pool, paths[0].clone());
        live.pin().unwrap();
        let tmp = dir.join("compact.tmp");
        std::fs::write(&tmp, "residual").unwrap();
        std::fs::rename(&tmp, &paths[0]).unwrap();
        // Budget pressure must not close the pinned handle.
        let other = StreamFile::lazy_in(pool, paths[1].clone());
        other.get().unwrap();
        assert!(live.is_open());
        assert_eq!(read_all(&live.get().unwrap()), "stream-0");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn fd_stats_line_is_the_shape_the_alarm_reads() {
        let line = fd_stats_line(Some(52_000), Some(65_536), 120, 32_768);
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["event"], "ds_fd_stats");
        assert_eq!(v["open_fds"], 52_000);
        assert_eq!(v["fd_limit"], 65_536);
        assert_eq!(v["open_stream_files"], 120);
        assert_eq!(v["max_open_stream_files"], 32_768);
        assert!(!line.contains('\n'));
    }

    #[test]
    fn default_budget_is_half_the_descriptor_limit() {
        assert_eq!(default_max_open(Some(65_536)), 32_768);
        assert_eq!(default_max_open(Some(100)), 64);
    }

    #[test]
    fn process_fds_reports_open_and_limit() {
        let (open, limit) = process_fds();
        assert!(open.unwrap() > 0);
        assert!(limit.unwrap() >= open.unwrap());
    }
}
