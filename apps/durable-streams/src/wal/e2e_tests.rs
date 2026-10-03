//! End-to-end durability + sharding integration tests (design spec §13).
//! **The integration gate**: where the framing, committers, sharded `WalSet`,
//! checkpoint, and per-shard recovery built+unit-tested in Tasks 1–9 are
//! exercised together over the REAL append/recover/read flow — the genuine HTTP
//! handler path (`handlers::handle` → `handle_create` / `handle_append_inner`
//! → `write_wire` → `maybe_sync_on_ack` → WAL `reserve_and_stage`/`wait_durable`),
//! a simulated crash (drop the store + committers WITHOUT a graceful shutdown),
//! and the full startup recovery sequence (`WalSet::open` → sidecar pass →
//! `wal::recovery::recover` → `reset_after_recovery` → re-attach + `spawn_committers`).
//!
//! Unlike the unit tests, these never hand-build a WAL segment or call a
//! `wal::*` helper directly to *produce* the durable state: every acked record
//! gets durable through the same code an HTTP `POST`/`PUT` runs in production.
//! That is the point — to catch bugs that only appear when the pieces are wired
//! together (paths that pass in isolation but not end-to-end).

use std::io;
use std::sync::Arc;

use bytes::Bytes;

use crate::api::{Method, Req};
use crate::handlers;
use crate::handlers::test_support::{temp_dir, DurabilityGuard};
use crate::store::Store;
use crate::tier::TierConfig;
use crate::wal::shard::CommitterHandle;
use crate::wal::walset::WalSet;

/// A booted WAL-mode server harness: the store with its WAL attached and the
/// per-shard committers running on their dedicated OS threads. Committer
/// [`CommitterHandle`]s are held so a test can stop + join them to simulate a
/// crash. (Every record a crash test relies on is already acked — hence durable —
/// before the stop, so the final drain a graceful stop performs is a no-op for
/// those records; the on-disk state matches an abrupt crash.)
struct Harness {
    store: Arc<Store>,
    walset: Arc<WalSet>,
    committers: Vec<CommitterHandle>,
}

impl Harness {
    /// Replicate the **exact** `main.rs` WAL startup sequence (spec §9) for the
    /// data dir at `dir` with `shards` shards (or the persisted N if it already
    /// exists). `default_n` is the would-be `available_parallelism` — used only
    /// to seed a fresh dir, and chosen DIFFERENT from `shards` in the N-stability
    /// test to prove routing ignores it.
    ///
    /// Order (load-bearing, spec §9): `WalSet::open` (non-destructive) → build
    /// `Store` (runs the sidecar identity pass) → `wal::recovery::recover` (replay
    /// the durable WAL into the per-stream files + fsync) → `reset_after_recovery`
    /// (wipe the old WAL) → attach + `spawn_committers`.
    fn boot(dir: &std::path::Path, shards: Option<usize>, default_n: usize) -> io::Result<Harness> {
        Harness::boot_with_segment_size(dir, shards, default_n, crate::wal::segment::SEGMENT_BYTES)
    }

    /// [`Harness::boot`] with an explicit WAL segment size, so a test can force
    /// segment rolls/recycles cheaply (multi-segment recovery coverage).
    fn boot_with_segment_size(
        dir: &std::path::Path,
        shards: Option<usize>,
        default_n: usize,
        segment_size: u64,
    ) -> io::Result<Harness> {
        let walset = WalSet::open_with_segment_size(dir, shards, default_n, segment_size)?;
        let store = Arc::new(Store::new_with_tier(dir.to_path_buf(), TierConfig::default())?);
        crate::wal::recovery::recover(&store, &walset)?;
        walset.reset_after_recovery()?;
        store.wal.set(Arc::clone(&walset)).unwrap_or_else(|_| panic!("WAL already attached"));
        // Spawn committers ourselves (not `walset.spawn_committers()`) so we keep
        // the handles and can stop them to simulate a crash.
        let mut committers = Vec::new();
        for shard in walset.shards() {
            committers.push(shard.spawn_committer());
        }
        Ok(Harness { store, walset, committers })
    }

    /// Stop + join every committer thread (so no further `durable_lsn` advance can
    /// race the test's subsequent file surgery). All records the caller cares
    /// about are already acked/durable, so each committer's final drain is a
    /// no-op for them.
    fn stop_committers(&mut self) {
        for h in self.committers.drain(..) {
            h.stop();
        }
    }

    /// Simulate a crash: stop the committers (so no further `durable_lsn`
    /// advance) and drop the store + WalSet WITHOUT a graceful drain/shutdown.
    /// The data dir on disk is left exactly as the live process left it.
    fn crash(mut self) {
        self.stop_committers();
        drop(self.store);
        drop(self.walset);
    }
}

/// Build a `PUT` (create) request for `path` with `content_type` and an optional
/// body. Extra headers (e.g. fork headers) are appended verbatim.
fn put_req(path: &str, content_type: &str, body: &[u8], extra: &[(&str, &str)]) -> Req {
    let mut headers = vec![("content-type".to_string(), content_type.to_string())];
    for (k, v) in extra {
        headers.push((k.to_string(), v.to_string()));
    }
    Req { method: Method::Put, path: path.to_string(), query: None, headers, body: Bytes::copy_from_slice(body) }
}

/// Build a `POST` (append) request for `path`.
fn post_req(path: &str, content_type: &str, body: &[u8]) -> Req {
    Req {
        method: Method::Post,
        path: path.to_string(),
        query: None,
        headers: vec![("content-type".to_string(), content_type.to_string())],
        body: Bytes::copy_from_slice(body),
    }
}

fn producer_post(path: &str, body: &[u8], epoch: u64, seq: u64, close: bool) -> Req {
    let mut request = post_req(path, OCTET, body);
    request.headers.extend([
        ("producer-id".into(), "producer".into()),
        ("producer-epoch".into(), epoch.to_string()),
        ("producer-seq".into(), seq.to_string()),
        ("stream-seq".into(), format!("{epoch:04}-{seq:04}")),
    ]);
    if close {
        request.headers.push(("stream-closed".into(), "true".into()));
    }
    request
}

/// Create a stream over the REAL HTTP path; assert a 2xx.
async fn create_stream(store: &Arc<Store>, path: &str, content_type: &str) {
    let resp = handlers::handle(Arc::clone(store), put_req(path, content_type, b"", &[])).await;
    assert!((200..300).contains(&resp.status), "create {path} expected 2xx, got {}", resp.status);
}

/// Append one record over the REAL HTTP path; assert a 2xx ack (which, in WAL
/// mode, means the record's lsn is durable — its prefix is on disk + fdatasync'd;
/// in `memory` mode it means the page-cache write completed — no WAL, no fsync).
async fn append_acked(store: &Arc<Store>, path: &str, content_type: &str, body: &[u8]) {
    let resp = handlers::handle(Arc::clone(store), post_req(path, content_type, body)).await;
    assert!((200..300).contains(&resp.status), "append to {path} expected 2xx ack, got {}", resp.status);
}

/// The data-file path for a stream by name (the read surface; spec §8). Resolves
/// the live `StreamState.file_path` so we read exactly what `sendfile` would.
fn stream_file_bytes(store: &Arc<Store>, path: &str) -> Vec<u8> {
    let st = store.get(path).unwrap_or_else(|| panic!("stream {path} not found"));
    std::fs::read(&st.file_path).unwrap()
}

/// The shard index a stream name routes to, after creating it (so we can assert
/// two streams land on different shards). Uses the live store + walset routing.
fn shard_index_of(store: &Arc<Store>, walset: &Arc<WalSet>, path: &str) -> usize {
    let st = store.get(path).unwrap();
    let target = walset.shard_for(st.id);
    walset.shards().iter().position(|s| Arc::ptr_eq(s, target)).unwrap()
}

const OCTET: &str = "application/octet-stream";
const JSON: &str = "application/json";

/// Format a byte offset as the wire `Stream-Fork-Offset` value
/// (`<16-digit seq>_<16-digit byte-offset>`; the seq part is ignored for byte
/// resolution by `parse_offset`).
fn fork_offset(bytes: u64) -> String {
    format!("{:016}_{:016}", 0, bytes)
}

// ===========================================================================
// (1) NO-LOSS across ≥2 shards (spec §13 "No-loss", §14 criterion 1)
// ===========================================================================

/// Create several streams (chosen so they hash to ≥2 different shards), append
/// K records to each over the REAL handler path, confirm each append is ACKED
/// (durable in the WAL), then simulate a crash (abort committers, drop store +
/// WalSet, keep the data dir) and reopen with the full startup recovery
/// sequence. Every acked record must recover BYTE-IDENTICAL; an un-acked tail
/// (staged but its committer never advanced `durable_lsn`) must be ABSENT.
#[tokio::test]
async fn e2e_no_loss_two_shards_acked_records_survive_crash() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("noloss");

    // 4 shards so a handful of streams reliably spreads across ≥2 of them.
    let h = Harness::boot(dir.path(), Some(4), 4).unwrap();

    // Create enough streams that at least two distinct shards are touched.
    let names = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    for n in names {
        create_stream(&h.store, n, OCTET).await;
    }
    let mut used_shards = std::collections::BTreeSet::new();
    for n in names {
        used_shards.insert(shard_index_of(&h.store, &h.walset, n));
    }
    assert!(used_shards.len() >= 2, "test needs streams on ≥2 shards; got shards {used_shards:?}");

    // Append K records to each stream; build the expected per-stream byte image.
    const K: usize = 5;
    let mut expected: std::collections::HashMap<&str, Vec<u8>> = std::collections::HashMap::new();
    for n in names {
        let buf = expected.entry(n).or_default();
        for i in 0..K {
            let rec = format!("{n}-record-{i:03}|").into_bytes();
            append_acked(&h.store, n, OCTET, &rec).await;
            buf.extend_from_slice(&rec);
        }
    }

    // CRASH: abort committers + drop everything, no graceful shutdown.
    h.crash();

    // REOPEN with the full startup recovery sequence.
    let h2 = Harness::boot(dir.path(), None, 4).unwrap();

    // Every acked record survived byte-identical on each stream's read surface.
    for n in names {
        let got = stream_file_bytes(&h2.store, n);
        assert_eq!(got, expected[n], "stream {n}: all {K} acked records recover byte-identical after crash");
        let st = h2.store.get(n).unwrap();
        assert_eq!(st.tail().bytes, expected[n].len() as u64, "stream {n}: recovered tail == total acked bytes");
    }
    h2.crash();
}

/// No-loss boundary: an un-acked tail is ABSENT after recovery. A record can
/// only ack (return 2xx) AFTER its WAL record is whole + fdatasync'd — so the
/// honest model of a "staged but un-acked" record is one whose WAL bytes are
/// TORN (a partial write a crash left mid-record, before the committer's
/// fdatasync could make a whole record durable) while its data DID reach the
/// per-stream file's page cache (`write_wire` runs upstream of the ack gate). On
/// a real crash that record never acked; recovery stops at the first torn WAL
/// record and truncates the file tail back to the durable frontier → the
/// un-acked bytes are gone, the genuinely-acked prefix is fully restored.
#[tokio::test]
async fn e2e_no_loss_unacked_tail_is_absent_after_crash() {
    let _guard = DurabilityGuard::wal();
    use crate::wal::codec::HEADER_LEN;

    let dir = temp_dir("noloss-unacked");

    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "s", OCTET).await;

    // Two genuinely-acked appends through the real path (durable in the WAL).
    append_acked(&h.store, "s", OCTET, b"acked-one|").await;
    append_acked(&h.store, "s", OCTET, b"acked-two|").await;
    let acked: &[u8] = b"acked-one|acked-two|";

    // The un-acked tail: its data reached the per-stream FILE page cache (as
    // `write_wire` would), but its WAL record is TORN. Abort the committer so no
    // further durable_lsn advance, then:
    //   (a) append the un-acked bytes to the per-stream file (page-cache write).
    //   (b) plant a TORN partial WAL record right after the two whole durable
    //       records: only a few header bytes, never a full framed record.
    h.stop_committers();
    let st = h.store.get("s").unwrap();
    let unacked: &[u8] = b"UNACKED-NEVER-DURABLE|";
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&st.file_path).unwrap();
        f.write_all(unacked).unwrap(); // (a) page-cache tail past the durable frontier
        f.sync_all().unwrap();
    }
    // (b) The two durable records occupy `2*HEADER_LEN + len("acked-one|") +
    //     len("acked-two|")` bytes at the start of the active segment `1.wal`.
    //     Overwrite the bytes just past them with a partial (torn) header so the
    //     decoder ends the durable log there.
    let seg_path = crate::wal::segment::seg_path(&dir.path().join("wal").join("0"), 1);
    let durable_wal_len = 2 * HEADER_LEN + b"acked-one|".len() + b"acked-two|".len();
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&seg_path).unwrap();
        f.seek(SeekFrom::Start(durable_wal_len as u64)).unwrap();
        // A few non-zero bytes that cannot decode as a whole record (header CRC
        // will not validate / payload short) → torn end-of-log.
        f.write_all(&[0xAB, 0xCD, 0xEF, 0x01, 0x02]).unwrap();
        f.sync_all().unwrap();
    }

    // Crash + reopen.
    drop(st);
    let store = h.store;
    let walset = h.walset;
    drop(store);
    drop(walset);

    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    let got = stream_file_bytes(&h2.store, "s");
    assert_eq!(got, acked, "only the two ACKED records recover; the torn, un-acked tail is truncated away");
    h2.crash();
}

// ===========================================================================
// (2) NO TORN RECORD incl. JSON (spec §13 "No torn record", §14 criterion 2)
// ===========================================================================

/// Post-checkpoint torn case: a per-stream JSON file carries a torn trailing
/// record (a partial JSON value appended to the page cache but never acked into
/// the WAL). The durable WAL covers only whole records. After the real startup
/// recovery the file must end on a whole-record boundary and read back as only
/// whole, valid JSON values.
#[tokio::test]
async fn e2e_no_torn_json_tail_repaired_to_whole_records() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("torn-json");

    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "j", JSON).await;

    // Two whole JSON records through the real path. The wire encoding appends a
    // trailing ',' to each value (handlers::encode_wire), so the file is
    // `{"a":1},{"b":2},`.
    append_acked(&h.store, "j", JSON, br#"{"a":1}"#).await;
    append_acked(&h.store, "j", JSON, br#"{"b":2}"#).await;
    let durable = stream_file_bytes(&h.store, "j");
    assert_eq!(durable, br#"{"a":1},{"b":2},"#, "two whole JSON wire records acked");

    // Now simulate a page-cache write that reached the FILE but never the durable
    // WAL: append a TORN trailing JSON value straight to the per-stream file
    // (bypassing the WAL), then crash. This is exactly the C1 scenario `fast`
    // could not fix: a half-written `{"c":` with no durable record boundary.
    let st = h.store.get("j").unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&st.file_path).unwrap();
        f.write_all(br#"{"c":"#).unwrap(); // torn, un-acked JSON tail
        f.sync_all().unwrap();
    }
    drop(st);
    h.crash();

    // Reopen: recovery must truncate the torn tail back to the durable frontier.
    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    let got = stream_file_bytes(&h2.store, "j");
    assert_eq!(got, br#"{"a":1},{"b":2},"#, "recovery repaired the file to whole records; torn `{{\"c\":` discarded");
    // Every record reads back as valid JSON (the wire is value,value, — split on
    // the trailing commas and parse each).
    let text = String::from_utf8(got.clone()).unwrap();
    for v in text.trim_end_matches(',').split("},") {
        let val = if v.ends_with('}') { v.to_string() } else { format!("{v}}}") };
        serde_json::from_str::<serde_json::Value>(&val)
            .unwrap_or_else(|e| panic!("recovered record {val:?} is not valid JSON: {e}"));
    }
    h2.crash();
}

/// The CRITICAL Task-7 case: the torn tail when the last DURABLE record is
/// ≤ `checkpoint_lsn`. Append+ack two records (durable in the WAL), drive a real
/// checkpoint so `checkpoint_lsn` covers BOTH, then add a torn page-cache tail
/// and crash. A `checkpoint_lsn`-bounded replay would see no post-checkpoint
/// record for the stream → leave the torn tail. The shipped replay-from-oldest
/// must still compute the frontier and truncate the torn tail.
#[tokio::test]
async fn e2e_no_torn_tail_when_last_durable_record_below_checkpoint() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("torn-below-ckpt");

    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "s", OCTET).await;
    append_acked(&h.store, "s", OCTET, b"rec-one|").await;
    append_acked(&h.store, "s", OCTET, b"rec-two|").await;
    let durable: &[u8] = b"rec-one|rec-two|";

    // Drive a REAL checkpoint: fdatasync the touched per-stream file + persist
    // checkpoint_lsn covering both acked records. (Single shard.)
    let ckpt = h.walset.shards()[0].checkpoint().await.unwrap();
    assert!(ckpt >= 2, "checkpoint_lsn covers both acked records (got {ckpt})");

    // Torn page-cache tail past the durable+checkpointed frontier, then crash.
    let st = h.store.get("s").unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&st.file_path).unwrap();
        f.write_all(b"TORN-TAIL-PAST-CHECKPOINT").unwrap();
        f.sync_all().unwrap();
    }
    drop(st);
    h.crash();

    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    let got = stream_file_bytes(&h2.store, "s");
    assert_eq!(
        got, durable,
        "torn tail truncated even though the last durable record is ≤ checkpoint_lsn (Task-7 critical)"
    );
    h2.crash();
}

// ===========================================================================
// (3) SHARDING — parallel recovery + below-file_base skip (spec §13 "Sharding")
// ===========================================================================

/// Streams hashing to different shards recover in parallel correctly (every
/// stream's records restored from its own shard's WAL, in one `recover` call
/// that spawns a thread per shard). Also asserts the live routing actually
/// spread the streams across ≥2 shards (else the test is vacuous).
#[tokio::test]
async fn e2e_sharding_parallel_recovery_across_shards() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("sharding");

    let h = Harness::boot(dir.path(), Some(4), 4).unwrap();
    // Many streams to guarantee a multi-shard spread.
    let names: Vec<String> = (0..12).map(|i| format!("stream-{i:02}")).collect();
    for n in &names {
        create_stream(&h.store, n, OCTET).await;
    }
    let shards: std::collections::BTreeSet<usize> =
        names.iter().map(|n| shard_index_of(&h.store, &h.walset, n)).collect();
    assert!(shards.len() >= 2, "streams must span ≥2 shards; got {shards:?}");

    let mut expected: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for n in &names {
        let mut buf = Vec::new();
        for i in 0..3 {
            let rec = format!("{n}#{i}|").into_bytes();
            append_acked(&h.store, n, OCTET, &rec).await;
            buf.extend_from_slice(&rec);
        }
        expected.insert(n.clone(), buf);
    }
    h.crash();

    // One `recover` call inside boot replays all shards in parallel.
    let h2 = Harness::boot(dir.path(), None, 4).unwrap();
    for n in &names {
        assert_eq!(
            stream_file_bytes(&h2.store, n),
            expected[n],
            "stream {n} (its own shard) recovers all records after parallel recovery"
        );
    }
    h2.crash();
}

/// A record with `stream_offset < file_base` (a forked/compacted stream's
/// already-sealed prefix) is SKIPPED on replay — no out-of-range write. We build
/// this end-to-end: fork a stream at a non-zero offset over the REAL handler path
/// (so `file_base > 0`), append+ack a record to the fork, then ALSO stage a WAL
/// record below the fork's `file_base` directly into its shard (the kind of
/// record a pre-fork compaction left behind). After crash+recovery the fork's
/// file holds only the in-range record; the below-frontier record is not applied.
#[tokio::test]
async fn e2e_sharding_below_file_base_record_skipped() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("below-base");

    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    // Parent with content so a fork can diverge at offset > 0.
    create_stream(&h.store, "parent", OCTET).await;
    append_acked(&h.store, "parent", OCTET, b"0123456789").await; // tail = 10
                                                                  // Fork at offset 5 over the real path → the fork's file_base == 5.
    let resp = handlers::handle(
        Arc::clone(&h.store),
        put_req("child", OCTET, b"", &[("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(5))]),
    )
    .await;
    assert!((200..300).contains(&resp.status), "fork create got {}", resp.status);
    let child = h.store.get("child").unwrap();
    let file_base = child.shared.read().unwrap().file_base;
    assert_eq!(file_base, 5, "forked stream file_base = fork offset");

    // Append one in-range record to the fork via the real path (stream_offset =
    // file_base = 5, file position 0).
    append_acked(&h.store, "child", OCTET, b"FORKDATA").await;

    // Stage a WAL record for the fork BELOW its file_base (stream_offset 2 < 5):
    // the frontier-skip case. It must be skipped on replay (re-applying would be
    // an out-of-range / double-apply; those bytes live in the parent/sealed
    // prefix). Stop the committers first so we don't perturb the acked frontier.
    h.stop_committers();
    let shard = h.walset.shard_for(child.id);
    shard
        .reserve_and_stage(
            crate::wal::codec::RecordKind::Append,
            child.id,
            2, // < file_base (5) → must be skipped
            b"BELOW-FRONTIER",
        )
        .unwrap();
    // Re-run a committer briefly so this staged record DOES become durable in the
    // WAL (proving the skip is in recovery's replay, not just an un-acked drop).
    let c = shard.spawn_committer();
    // Wait until durable so the record is genuinely in the WAL's durable range.
    shard.wait_durable(shard.tail_lsn()).await;
    c.stop();

    drop(child);
    let store = h.store;
    let walset = h.walset;
    drop(store);
    drop(walset);

    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    // The fork's file holds ONLY the in-range record (8 bytes "FORKDATA"). The
    // below-frontier record was skipped (not written out of range at a negative
    // / wrapped position).
    let got = stream_file_bytes(&h2.store, "child");
    assert_eq!(got, b"FORKDATA", "below-file_base WAL record skipped on replay; only the in-range record applied");
    let child2 = h2.store.get("child").unwrap();
    assert_eq!(child2.tail().bytes, file_base + 8, "fork tail = file_base + in-range bytes (no out-of-range write)");
    h2.crash();
}

// ===========================================================================
// (4) N-STABILITY (spec §13 "N-stability", §5)
// ===========================================================================

/// Reopen the same data dir with a DIFFERENT `available_parallelism`/default_n →
/// every stream still resolves to its PERSISTED shard (routing uses the persisted
/// N + stream_id, never the per-boot core count). Also asserts the lib-level
/// guard: `WalSet::open` with `--wal-shards` ≠ the persisted N is an error
/// (the `is_err()` the brief calls out — main.rs maps it to exit 2).
#[tokio::test]
async fn e2e_n_stability_shard_resolution_ignores_core_count() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("n-stability");

    // Persist N = 4, seeded with default_n = 4.
    let h = Harness::boot(dir.path(), Some(4), 4).unwrap();
    let names: Vec<String> = (0..10).map(|i| format!("s-{i}")).collect();
    for n in &names {
        create_stream(&h.store, n, OCTET).await;
        append_acked(&h.store, n, OCTET, b"x").await;
    }
    // Record each stream's (id, shard index) under the persisted-4 routing.
    let mut want: Vec<(u64, usize)> = Vec::new();
    for n in &names {
        let st = h.store.get(n).unwrap();
        want.push((st.id, shard_index_of(&h.store, &h.walset, n)));
    }
    h.crash();

    // Reopen with a DIFFERENT default_n (16) — a machine with more cores. The
    // persisted N (4) must win, so every stream resolves to the SAME shard.
    let h2 = Harness::boot(dir.path(), None, 16).unwrap();
    assert_eq!(h2.walset.shards().len(), 4, "persisted N (4) used, not default_n (16)");
    for (id, expect_idx) in &want {
        let target = h2.walset.shard_for(*id);
        let got_idx = h2.walset.shards().iter().position(|s| Arc::ptr_eq(s, target)).unwrap();
        assert_eq!(got_idx, *expect_idx, "stream id {id} resolves to its persisted shard");
    }

    // Lib-level guard (maps to exit 2 in main.rs): a requested N ≠ persisted is
    // rejected.
    assert!(
        WalSet::open(dir.path(), Some(8), 8).is_err(),
        "--wal-shards 8 ≠ persisted 4 is rejected (exit 2 at the binary level)"
    );
    assert!(WalSet::open(dir.path(), Some(4), 99).is_ok(), "a matching --wal-shards is accepted");
    h2.crash();
}

// ===========================================================================
// (5) CHECKPOINT NON-BLOCKING (spec §13 "Checkpoint non-blocking", §7)
// ===========================================================================

/// With the checkpoint NEVER run, appends keep acking over the real handler path
/// and the WAL `size_bytes` grows (does not shrink — a lagging checkpoint only
/// delays WAL recycling, it never backpressures the ack path).
#[tokio::test]
async fn e2e_checkpoint_non_blocking_appends_ack_and_wal_grows() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("ckpt-nonblock");

    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "s", OCTET).await;

    let size0: u64 = h.walset.shards().iter().map(|s| s.wal_size_bytes()).sum();

    // Many acked appends through the real path, with NO checkpoint ever driven.
    // Each `append_acked` only returns 2xx after `wait_durable` — so the ack path
    // is provably gated on the committer's durable_lsn, not on checkpoint.
    for i in 0..32u64 {
        let rec = format!("payload-{i:04}|").into_bytes();
        tokio::time::timeout(std::time::Duration::from_secs(5), append_acked(&h.store, "s", OCTET, &rec))
            .await
            .expect("appends ack with NO checkpoint having run (non-blocking)");
    }

    let size1: u64 = h.walset.shards().iter().map(|s| s.wal_size_bytes()).sum();
    assert!(size1 >= size0, "WAL size_bytes does not shrink without a checkpoint (got {size0} → {size1})");
    // And the on-disk segment is retained (not recycled, since no checkpoint ran).
    assert!(h.walset.shards()[0].wal_segments() >= 1, "WAL segment retained without a checkpoint");

    h.crash();
}

// ===========================================================================
// (6) CARRIED Task-5 nit: drive the FULL HTTP handler path for a FORKED stream
//     (file_base > 0) and assert the recovered data is correct (spec §13/§9).
// ===========================================================================

/// Closes the Task-5 gap: the forked-offset durability test drove the
/// `write_wire` + `maybe_sync_on_ack` helpers directly. This drives the genuine
/// HTTP path end-to-end — `PUT` a fork (file_base > 0), `POST` records to it,
/// then crash + recover — and asserts the recovered fork data is byte-correct.
/// This exercises the LOGICAL `stream_offset = file_base + file-relative` mapping
/// the handler computes and recovery's `file_pos = stream_offset − file_base`
/// inversion, over the real wire.
#[tokio::test]
async fn e2e_forked_stream_full_http_path_recovers_correctly() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("forked-http");

    let h = Harness::boot(dir.path(), Some(2), 2).unwrap();

    // Parent with 10 bytes, fork at offset 7 → fork file_base = 7.
    create_stream(&h.store, "p", OCTET).await;
    append_acked(&h.store, "p", OCTET, b"ABCDEFGHIJ").await;
    let resp = handlers::handle(
        Arc::clone(&h.store),
        put_req("f", OCTET, b"", &[("stream-forked-from", "p"), ("stream-fork-offset", &fork_offset(7))]),
    )
    .await;
    assert!((200..300).contains(&resp.status), "fork create got {}", resp.status);
    let child = h.store.get("f").unwrap();
    assert_eq!(child.shared.read().unwrap().file_base, 7, "fork file_base = 7");
    drop(child);

    // Append records to the fork through the REAL POST path. The handler computes
    // stream_offset = file_base(7) + file-relative pre-offset, and stages THAT
    // logical offset into the WAL; recovery inverts it back to file pos.
    append_acked(&h.store, "f", OCTET, b"forkrec1|").await;
    append_acked(&h.store, "f", OCTET, b"forkrec2|").await;
    let expected: &[u8] = b"forkrec1|forkrec2|"; // file-relative bytes (file pos 0..)
    assert_eq!(stream_file_bytes(&h.store, "f"), expected, "fork file before crash");

    h.crash();

    // Reopen: recovery must place the fork's WAL payloads at file pos
    // (stream_offset − file_base), reconstructing exactly the fork-relative bytes.
    let h2 = Harness::boot(dir.path(), None, 2).unwrap();
    let got = stream_file_bytes(&h2.store, "f");
    assert_eq!(got, expected, "forked stream (file_base > 0) recovers byte-correct over the full HTTP path");
    let child2 = h2.store.get("f").unwrap();
    assert_eq!(child2.tail().bytes, 7 + expected.len() as u64, "fork tail = file_base + recovered bytes");
    h2.crash();
}

// ===========================================================================
// (7) COMPAT: strict-created data dir reopens WAL-only without data loss
// ===========================================================================

/// A data dir with per-stream files but NO `wal/` subtree (as a `strict`-era
/// deployment would have) reopens WAL-only without losing data. The WAL recovery
/// replays an empty WAL (no records → no truncations) and leaves the per-stream
/// files untouched. This is the characterization test that must PASS before the
/// WAL is made unconditional in main.rs.
#[tokio::test]
async fn strict_created_dir_reopens_wal_only_without_data_loss() {
    let _guard = DurabilityGuard::wal();
    use std::io::Write;
    let dir = temp_dir("strict-compat");

    // --- Phase 1: write data directly to per-stream files (no WAL), simulating
    // a strict-mode deployment. We bypass the HTTP handler and write directly
    // via the appender so no WAL is touched.
    {
        let store = Arc::new(
            crate::store::Store::new_with_tier(dir.path().to_path_buf(), crate::tier::TierConfig::default()).unwrap(),
        );
        let st = {
            let s = Arc::clone(&store);
            tokio::task::spawn_blocking(move || {
                s.create(
                    "s/keep",
                    crate::store::StreamConfig {
                        content_type: "application/octet-stream".into(),
                        ttl_seconds: None,
                        expires_at: None,
                        expires_at_raw: None,
                        create_closed: false,
                        forked_from: None,
                        fork_offset_raw: None,
                        fork_sub_offset: None,
                    },
                    None,
                    0,
                )
            })
            .await
            .unwrap()
            .unwrap()
        };
        let st = match st {
            crate::store::CreateResult::Created(s) => s,
            _ => panic!("create failed in Phase 1"),
        };
        // Append bytes directly to the per-stream file (no WAL).
        {
            let mut ap = st.appender.lock().await;
            (&*ap.file).write_all(b"hello-world").unwrap();
            ap.written += 11;
            let mut s = st.shared.write().unwrap();
            s.tail = s.file_base + ap.written;
        }
        // Persist the tail to the sidecar so recovery sees it on reopen.
        let st2 = Arc::clone(&st);
        tokio::task::spawn_blocking(move || crate::store::write_meta_sync(&st2, true)).await.unwrap().unwrap();
        // A strict-era server predates the `durable_tail` sidecar proof — strip
        // the field the CURRENT writer emitted so the sidecar is byte-faithful
        // to what an old deployment left behind (recovery must fall back to
        // trusting the file size for such sidecars).
        let meta_path = crate::store::meta_path(&st.file_path);
        let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        v.as_object_mut().unwrap().remove("durable_tail");
        std::fs::write(&meta_path, serde_json::to_vec(&v).unwrap()).unwrap();
    }
    // Remove any wal/ subtree — a strict-era dir has none.
    std::fs::remove_dir_all(dir.path().join("wal")).ok();

    // --- Phase 2: reopen WAL-only (the exact main.rs startup sequence).
    let store = Arc::new(
        crate::store::Store::new_with_tier(dir.path().to_path_buf(), crate::tier::TierConfig::default()).unwrap(),
    );
    let walset = crate::wal::walset::WalSet::open(dir.path(), None, 1).unwrap();
    // An empty WAL replay must NOT truncate pre-existing per-stream data.
    crate::wal::recovery::recover(&store, &walset).unwrap();
    walset.reset_after_recovery().unwrap();
    store.wal.set(Arc::clone(&walset)).unwrap_or_else(|_| panic!("wal already set"));

    let st = store.get("s/keep").expect("stream must survive WAL-only reopen");
    let got = std::fs::read(&st.file_path).unwrap();
    assert_eq!(got, b"hello-world", "pre-WAL data must survive a WAL-only reopen without loss");
    assert_eq!(st.tail().bytes, 11, "recovered tail must equal the bytes written in Phase 1");
}

// ===========================================================================
// (7b) MULTI-SEGMENT recovery: boot must not clobber sealed/recycled segments
// ===========================================================================

/// Acked records that live in WAL segments AFTER the first survive a crash.
///
/// With a small segment size, enough acked appends roll the WAL: `1.wal` is
/// SEALED (truncated to its exactly-packed length + fsync'd) and later records
/// land in `<n>.wal`. A crash + reboot must replay ALL retained segments.
///
/// Regression (sim seed 89837): `Shard::open` re-preallocated `1.wal` to full
/// segment size, so the sealed segment grew a zero tail; replay read that tail
/// as `Incomplete` (= end of the durable log) and silently dropped every
/// record in later segments — then `reconcile_tail` TRUNCATED the per-stream
/// files back to the stale frontier. Acked-data loss on the recovery path.
#[tokio::test]
async fn e2e_multi_segment_acked_records_after_first_seal_survive_crash() {
    let _guard = DurabilityGuard::wal();
    const SEG: u64 = 4096;
    let dir = temp_dir("multi-seg");

    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, SEG).unwrap();
    create_stream(&h.store, "s", OCTET).await;

    // Append acked records until the shard has rolled at least once (≥2
    // on-disk segments), then a few more so the post-roll segment holds data.
    let mut expected = Vec::new();
    let mut i = 0usize;
    while h.walset.shards()[0].wal_segments() < 2 || i < 40 {
        let rec = format!("multi-seg-record-{i:04}|").into_bytes();
        append_acked(&h.store, "s", OCTET, &rec).await;
        expected.extend_from_slice(&rec);
        i += 1;
        assert!(i < 10_000, "never rolled a segment; check SEG/record sizing");
    }
    assert!(h.walset.shards()[0].wal_segments() >= 2, "test needs ≥2 retained segments");

    h.crash();

    let h2 = Harness::boot_with_segment_size(dir.path(), None, 1, SEG).unwrap();
    let got = stream_file_bytes(&h2.store, "s");
    assert_eq!(
        got.len(),
        expected.len(),
        "every acked record recovers across ALL retained segments (lost {} bytes)",
        expected.len().saturating_sub(got.len())
    );
    assert_eq!(got, expected, "recovered bytes byte-identical across segment seams");
    h2.crash();
}

/// Acked records recover when `1.wal` was RECYCLED (checkpoint deleted it and
/// the oldest retained segment starts at lsn > 1).
///
/// Regression (same root cause, worse case): `Shard::open` unconditionally
/// created a fresh, all-zero `1.wal`. Replay walked segments in start-lsn
/// order, began with the spurious zero-filled `1.wal`, decoded `Incomplete` at
/// offset 0, and treated that as the end of the durable log — replaying
/// NOTHING. Every acked record after the last checkpoint was truncated away.
#[tokio::test]
async fn e2e_recycled_first_segment_acked_records_survive_crash() {
    let _guard = DurabilityGuard::wal();
    const SEG: u64 = 4096;
    let dir = temp_dir("recycled-first");

    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, SEG).unwrap();
    create_stream(&h.store, "s", OCTET).await;

    // Phase 1: roll past 1.wal, then checkpoint → sealed segments fully below
    // the floor (including 1.wal) are recycled (deleted).
    let mut expected = Vec::new();
    let mut i = 0usize;
    while h.walset.shards()[0].wal_segments() < 3 {
        let rec = format!("pre-ckpt-{i:04}|").into_bytes();
        append_acked(&h.store, "s", OCTET, &rec).await;
        expected.extend_from_slice(&rec);
        i += 1;
        assert!(i < 10_000, "never rolled; check SEG/record sizing");
    }
    h.walset.shards()[0].checkpoint().await.unwrap();
    assert!(
        !dir.path().join("wal").join("0").join("1.wal").exists(),
        "checkpoint recycled 1.wal (else this test is vacuous)"
    );

    // Phase 2: more ACKED records after the checkpoint (they live only in the
    // WAL + page cache; the checkpoint that would fsync them never runs).
    for j in 0..25usize {
        let rec = format!("post-ckpt-{j:04}|").into_bytes();
        append_acked(&h.store, "s", OCTET, &rec).await;
        expected.extend_from_slice(&rec);
    }

    h.crash();

    let h2 = Harness::boot_with_segment_size(dir.path(), None, 1, SEG).unwrap();
    let got = stream_file_bytes(&h2.store, "s");
    assert_eq!(
        got.len(),
        expected.len(),
        "post-checkpoint acked records recover from the retained (recycle-survivor) segments"
    );
    assert_eq!(got, expected, "recovered bytes byte-identical");
    h2.crash();
}

/// Recovery-hardening: a stage failure must leave NO trace — the 500'd bytes
/// must not be resurrected by later successful appends, neither live nor
/// across a crash/recovery. (Uses the shard's test-only write-failure
/// injection, which surfaces exactly like a production stage error.)
#[tokio::test]
async fn e2e_stage_failure_rolls_back_data_write() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("stage-rollback");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "rb", OCTET).await;

    let mut expected = Vec::new();
    append_acked(&h.store, "rb", OCTET, b"first|").await;
    expected.extend_from_slice(b"first|");

    // Inject: the next WAL stage write fails -> the append must 500 and the
    // data-file write must be rolled back.
    h.walset.shards()[0].fail_next_write();
    let resp = handlers::handle(Arc::clone(&h.store), post_req("rb", OCTET, b"LOST-must-not-resurrect|")).await;
    assert_eq!(resp.status, 500, "injected stage failure must 500");

    // A later append succeeds; the failed bytes must NOT appear before it.
    append_acked(&h.store, "rb", OCTET, b"second|").await;
    expected.extend_from_slice(b"second|");

    let live = stream_file_bytes(&h.store, "rb");
    assert_eq!(live, expected, "500'd bytes must not persist in the live file");

    h.crash();
    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    let got = stream_file_bytes(&h2.store, "rb");
    assert_eq!(got, expected, "500'd bytes must not resurrect across recovery");
    h2.crash();
}

/// Release the synchronous WAL stage hook even if a fixture assertion panics.
/// The hook has its own timeout as a second bound on test-runtime shutdown.
struct StagePause(std::sync::mpsc::Sender<()>);

impl Drop for StagePause {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

async fn stream_frontier_image(store: &Arc<Store>, name: &str) -> serde_json::Value {
    let stream = store.get(name).unwrap();
    let written = stream.appender.lock().await.written;
    let (tail, durable_tail) = {
        let shared = stream.shared.read().unwrap();
        (shared.tail, shared.durable_tail)
    };
    let mut offsets = Vec::new();
    for method in [Method::Get, Method::Head] {
        let request = Req {
            method,
            path: name.into(),
            query: Some(format!("offset={}", fork_offset(0))),
            headers: Vec::new(),
            body: Bytes::new(),
        };
        let response = handlers::handle(store.clone(), request).await;
        offsets.push(serde_json::json!({
            "status": response.status,
            "next_offset": response.headers.iter().find(|(key, _)| key.eq_ignore_ascii_case("stream-next-offset")).map(|(_, value)| value),
            "declared_body_bytes": response.body.len(),
        }));
    }
    serde_json::json!({
        "physical_bytes": stream_file_bytes(store, name),
        "writer_tail": tail,
        "durable_tail": durable_tail,
        "appender_written": written,
        "reads": offsets,
    })
}

/// A checkpoint must not certify bytes that a real POST will truncate after
/// WAL staging rejects it. Pause only at the existing pre-stage seam: the real
/// handler has written its body, advanced its tail and registered dirty work.
async fn checkpoint_does_not_certify_rejected_write(initial_put: bool) {
    let dir = temp_dir("checkpoint-rejected-append-tail");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "other", OCTET).await;
    if !initial_put {
        create_stream(&h.store, "stream", OCTET).await;
        append_acked(&h.store, "stream", OCTET, b"baseline|").await;
    }
    let shard = h.walset.shards()[0].clone();
    shard.checkpoint().await.unwrap();
    let baseline = if initial_put { None } else { Some(stream_frontier_image(&h.store, "stream").await) };
    let baseline_bytes = if initial_put { b"".as_slice() } else { b"baseline|".as_slice() };
    let baseline_tail = baseline_bytes.len() as u64;

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    shard.set_on_stage_hook(Box::new(move |id| {
        let entered = entered_tx.lock().unwrap().take();
        if let Some(entered) = entered {
            let _ = entered.send(id);
            let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
        }
    }));
    let pause = StagePause(release_tx);
    let request =
        if initial_put { put_req("stream", OCTET, &[b'x'; 512], &[]) } else { post_req("stream", OCTET, &[b'x'; 512]) };
    let append = tokio::spawn(handlers::handle(h.store.clone(), request));
    let stream_id = tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    let attempted_tail = h.store.get("stream").unwrap().shared.read().unwrap().tail;
    let (capture_tx, capture_rx) = tokio::sync::oneshot::channel();
    let capture_tx = std::sync::Mutex::new(Some(capture_tx));
    shard.set_on_checkpoint_capture_hook(Box::new(move |id| {
        if id == stream_id {
            if let Some(captured) = capture_tx.lock().unwrap().take() {
                let _ = captured.send(());
            }
        }
    }));
    let checkpoint_shard = shard.clone();
    let checkpoint = tokio::spawn(async move { checkpoint_shard.checkpoint().await });
    tokio::time::timeout(std::time::Duration::from_secs(2), capture_rx).await.unwrap().unwrap();
    // A blocked capture owns no shard-wide staging/dirty lock. A different
    // stream on this same shard must still stage, become durable and ACK.
    let other = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handlers::handle(h.store.clone(), post_req("other", OCTET, b"other|")),
    )
    .await;
    let blocked = !checkpoint.is_finished();
    // Always unblock and join the rejected POST before asserting or dropping
    // the runtime. Its 512-byte body cannot fit a 256-byte WAL segment.
    drop(pause);
    let rejected = tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), checkpoint).await.unwrap().unwrap().unwrap();
    let checkpoint_tail = shard.read_durable_tails().unwrap().get(&stream_id).copied();
    shard.set_on_stage_hook(Box::new(|_| {}));
    shard.set_on_checkpoint_capture_hook(Box::new(|_| {}));
    let rolled_back = stream_frontier_image(&h.store, "stream").await;
    let baseline = baseline.unwrap_or_else(|| rolled_back.clone());
    drop(shard);
    h.crash();

    // Exercise the entire production recovery sequence, its read offsets and
    // its next writer, then reboot again. No tail or proof is manufactured.
    let reopened = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered = stream_frontier_image(&reopened.store, "stream").await;
    let following = handlers::handle(reopened.store.clone(), post_req("stream", OCTET, b"next|")).await;
    let after_following = stream_frontier_image(&reopened.store, "stream").await;
    reopened.crash();
    let repeated = match Harness::boot_with_segment_size(dir.path(), None, 1, 256) {
        Ok(rebooted) => {
            let image = stream_frontier_image(&rebooted.store, "stream").await;
            rebooted.crash();
            image
        }
        Err(error) => serde_json::json!({ "recovery_error": error.to_string() }),
    };
    assert_eq!(rejected.status, 500, "the oversized real append must be rejected");
    assert_eq!(attempted_tail, baseline_tail + 512, "the fixture must cross the tentative file/tail window");
    assert!(blocked, "checkpoint must exclude the tentative write/stage/rollback window");
    assert_eq!(other.expect("other stream must ACK while capture is blocked").status, 204);
    assert_eq!(rolled_back["physical_bytes"], serde_json::json!(baseline_bytes));
    assert_eq!(rolled_back["writer_tail"], baseline_tail);
    assert_eq!(rolled_back["durable_tail"], baseline_tail);
    assert_eq!(rolled_back["appender_written"], baseline_tail);
    assert_eq!(rolled_back, baseline, "stage failure must restore live bytes, offsets and appender position");
    assert_eq!(checkpoint_tail, Some(baseline_tail), "checkpoint must certify only the acknowledged prefix");
    assert_eq!(recovered, baseline, "recovered offsets and appender position must agree with physical bytes");
    assert_eq!(following.status, 204);
    let mut expected = baseline_bytes.to_vec();
    expected.extend_from_slice(b"next|");
    assert_eq!(after_following["physical_bytes"], serde_json::json!(expected));
    assert_eq!(after_following["writer_tail"], baseline_tail + 5);
    assert_eq!(after_following["durable_tail"], baseline_tail + 5);
    assert_eq!(after_following["appender_written"], baseline_tail + 5);
    assert_eq!(repeated, after_following, "a second reboot must preserve the same exact bytes and offsets");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_checkpoint_tail_does_not_certify_rejected_append() {
    let _guard = DurabilityGuard::wal();
    checkpoint_does_not_certify_rejected_write(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_checkpoint_tail_does_not_certify_rejected_initial_put() {
    let _guard = DurabilityGuard::wal();
    checkpoint_does_not_certify_rejected_write(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_checkpoint_tail_covers_durable_append_before_reader_publication() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("checkpoint-reader-publication-lag");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    create_stream(&h.store, "other", OCTET).await;
    let stream = h.store.get("stream").unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    stream.set_append_commit_hook(Box::new(move |tail| {
        if tail == 6 {
            let entered = entered_tx.lock().unwrap().take();
            if let Some(entered) = entered {
                let _ = entered.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let pause = StagePause(release_tx);
    let first = tokio::spawn(handlers::handle(h.store.clone(), post_req("stream", OCTET, b"first|")));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    let reader_tail = stream.tail().bytes;
    // Force the paused append's segment to be sealed, so this checkpoint must
    // persist its prefix even when its only WAL copy is subsequently recycled.
    let other = handlers::handle(h.store.clone(), post_req("other", OCTET, &[b'o'; 200])).await;
    // The existing completion seam runs AFTER wait_durable returned. Its
    // callback has not exposed bytes yet; recycling must still preserve them.
    let floor = h.walset.shards()[0].checkpoint().await.unwrap();
    let proof = h.walset.shards()[0].read_durable_tails().unwrap().get(&stream.id).copied();
    let recycled = !h.walset.shards()[0].dir().join("1.wal").exists();
    // The later callback wins publication order and extends the same stream.
    let second = handlers::handle(h.store.clone(), post_req("stream", OCTET, b"second|")).await;
    drop(pause);
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), first).await.unwrap().unwrap();
    stream.set_append_commit_hook(Box::new(|_| {}));
    let live = stream_frontier_image(&h.store, "stream").await;
    drop(stream);
    h.crash();
    let reopened = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered = stream_frontier_image(&reopened.store, "stream").await;
    reopened.crash();
    let repeated = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered_again = stream_frontier_image(&repeated.store, "stream").await;
    repeated.crash();
    assert_eq!(reader_tail, 0, "the fixture must stop before reader publication");
    assert_eq!(other.status, 204);
    assert!(recycled, "the WAL segment holding the paused append must actually be recycled");
    assert!(floor > 0, "the append must already be WAL-durable");
    assert_eq!(proof, Some(6), "checkpoint must not substitute the lagging reader frontier");
    assert_eq!((first.status, second.status), (204, 204));
    assert_eq!(live["physical_bytes"], serde_json::json!(b"first|second|"));
    assert_eq!(live["writer_tail"], 13);
    assert_eq!(live["durable_tail"], 13);
    assert_eq!(live["appender_written"], 13);
    assert_eq!((recovered, recovered_again), (live.clone(), live));
}

/// Compaction holds the async appender while awaiting filesystem work from
/// this pool. Checkpoint must never wait for that appender on a blocking worker.
#[test]
fn e2e_checkpoint_tail_capture_progresses_with_one_blocking_worker() {
    let _guard = DurabilityGuard::wal();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = temp_dir("checkpoint-single-blocking-worker");
        let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
        create_stream(&h.store, "stream", OCTET).await;
        append_acked(&h.store, "stream", OCTET, b"baseline|").await;
        let stream = h.store.get("stream").unwrap();
        let appender = stream.appender.lock().await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = std::sync::Mutex::new(Some(entered_tx));
        h.walset.shards()[0].set_on_checkpoint_capture_hook(Box::new(move |_| {
            if let Some(entered) = entered_tx.lock().unwrap().take() {
                let _ = entered.send(());
            }
        }));
        let shard = h.walset.shards()[0].clone();
        let checkpoint = tokio::spawn(async move { shard.checkpoint().await });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
        // Queue real file IO behind checkpoint's sole blocking worker, as a
        // compaction does while retaining its appender guard. Collect timeout
        // before releasing the guard; always release it before any assertion.
        let file = stream.file_path.clone();
        let mut filesystem = tokio::task::spawn_blocking(move || std::fs::metadata(file).map(|meta| meta.len()));
        let work = tokio::time::timeout(std::time::Duration::from_secs(2), &mut filesystem).await;
        drop(appender);
        let progressed = work.is_ok();
        let bytes = match work {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => filesystem.await.unwrap().unwrap(),
        };
        checkpoint.await.unwrap().unwrap();
        h.walset.shards()[0].set_on_checkpoint_capture_hook(Box::new(|_| {}));
        let proof = h.walset.shards()[0].read_durable_tails().unwrap().get(&stream.id).copied();
        drop(stream);
        h.crash();
        assert!(progressed, "checkpoint must leave the sole blocking worker available for appender-owned IO");
        assert_eq!(bytes, 9);
        assert_eq!(proof, Some(9));
    });
}

fn wal_file_image(dir: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

/// Defense for an image an older writer may have corrupted. The false tail
/// below is deliberately synthetic; the real race is reproduced separately.
#[tokio::test]
async fn e2e_recovery_refuses_false_checkpoint_tail_without_mutating_its_stream() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("recovery-false-checkpoint-tail");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    append_acked(&h.store, "stream", OCTET, b"baseline|").await;
    create_stream(&h.store, "control", OCTET).await;
    append_acked(&h.store, "control", OCTET, &[b'c'; 200]).await;
    let stream = h.store.get("stream").unwrap();
    let file = stream.file_path.clone();
    let meta = crate::store::meta_path(&file);
    let id = stream.id;
    let shard = h.walset.shards()[0].clone();
    shard.checkpoint().await.unwrap();
    let shard_dir = shard.dir().to_path_buf();
    let tails = shard_dir.join("tails");
    assert!(!shard_dir.join("1.wal").exists(), "the baseline WAL must have been recycled");
    let control_id = h.store.get("control").unwrap().id;
    drop(stream);
    drop(shard);
    h.crash();
    std::fs::write(&tails, format!("{id} 521\n{control_id} 200\n")).unwrap();
    let before = (
        std::fs::read(&file).unwrap(),
        std::fs::read(&meta).unwrap(),
        std::fs::read(&tails).unwrap(),
        wal_file_image(&shard_dir),
    );
    let mut outcomes = Vec::new();
    for _ in 0..3 {
        let outcome = match Harness::boot_with_segment_size(dir.path(), None, 1, 256) {
            Ok(booted) => {
                let tail = booted.store.get("stream").unwrap().tail().bytes;
                booted.crash();
                (None, format!("boot incorrectly published tail {tail}"))
            }
            Err(error) => (Some(error.kind()), error.to_string()),
        };
        outcomes.push(outcome);
    }
    let after = (
        std::fs::read(&file).unwrap(),
        std::fs::read(&meta).unwrap(),
        std::fs::read(&tails).ok(),
        wal_file_image(&shard_dir),
    );
    assert!(
        outcomes.iter().all(|(kind, reason)| *kind == Some(io::ErrorKind::InvalidData) && reason.contains("missing durable bytes")),
        "unsupported proof must refuse every boot with a named production error: {outcomes:?}"
    );
    assert_eq!(
        after,
        (before.0, before.1, Some(before.2), before.3),
        "refused boots must preserve the failing stream, proof and retained WAL"
    );
}

async fn recovery_refuses_wal_hole(later_record: bool) {
    let dir = temp_dir("recovery-wal-hole");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    append_acked(&h.store, "stream", OCTET, b"baseline|").await;
    // Keep a valid earlier record for the later-hole variant. For the first
    // record variant, a legitimate complete boot resets only the old WAL.
    let h = if later_record {
        h
    } else {
        h.crash();
        Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap()
    };
    let stream = h.store.get("stream").unwrap();
    let file = stream.file_path.clone();
    let meta = crate::store::meta_path(&file);
    let shard = h.walset.shards()[0].clone();
    // Deliberately synthetic WAL record: model a writer restored at a phantom
    // offset. Use the real codec/stage/committer, but never call this an HTTP
    // race reproduction or manufacture physical zero-filled bytes ourselves.
    let lsn = shard.reserve_and_stage(crate::wal::codec::RecordKind::Append, stream.id, 521, b"next|").unwrap();
    shard.wait_durable(lsn).await;
    let shard_dir = shard.dir().to_path_buf();
    drop(stream);
    drop(shard);
    h.crash();
    let before = (std::fs::read(&file).unwrap(), std::fs::read(&meta).unwrap(), wal_file_image(&shard_dir));
    let mut outcomes = Vec::new();
    for _ in 0..3 {
        let outcome = match Harness::boot_with_segment_size(dir.path(), None, 1, 256) {
            Ok(booted) => {
                let tail = booted.store.get("stream").unwrap().tail().bytes;
                booted.crash();
                (None, format!("boot incorrectly published tail {tail}"))
            }
            Err(error) => (Some(error.kind()), error.to_string()),
        };
        let refused = outcome.0.is_some();
        outcomes.push(outcome);
        if !refused {
            break;
        }
    }
    let after = (std::fs::read(&file).unwrap(), std::fs::read(&meta).unwrap(), wal_file_image(&shard_dir));
    assert!(
        after == before,
        "a WAL hole must refuse BEFORE mutation: physical bytes {}→{}, metadata preserved={}, WAL preserved={}, outcomes={outcomes:?}",
        before.0.len(), after.0.len(), before.1 == after.1, before.2 == after.2,
    );
    assert!(
        outcomes.len() == 3
            && outcomes
                .iter()
                .all(|(kind, reason)| *kind == Some(io::ErrorKind::InvalidData) && reason.contains("WAL replay hole")),
        "every attempt must return a named production error: {outcomes:?}"
    );
}

#[tokio::test]
async fn e2e_recovery_refuses_first_wal_hole_before_mutating_its_stream() {
    let _guard = DurabilityGuard::wal();
    recovery_refuses_wal_hole(false).await;
}

#[tokio::test]
async fn e2e_recovery_refuses_later_wal_hole_before_mutating_its_stream() {
    let _guard = DurabilityGuard::wal();
    recovery_refuses_wal_hole(true).await;
}

#[tokio::test]
async fn e2e_recovery_retained_wal_repairs_short_file_with_overlap_and_adjacent_extensions() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("recovery-short-file-repair");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    for bytes in [b"baseline|".as_slice(), b"next|", b"later|"] {
        append_acked(&h.store, "stream", OCTET, bytes).await;
    }
    h.walset.shards()[0].checkpoint().await.unwrap();
    let file = h.store.get("stream").unwrap().file_path.clone();
    h.crash();
    // Simulate a short data file; retained valid records overlap the remaining
    // prefix, then extend it contiguously twice, despite its higher proof.
    std::fs::OpenOptions::new().write(true).open(file).unwrap().set_len(5).unwrap();
    let reopened = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered = stream_frontier_image(&reopened.store, "stream").await;
    reopened.crash();
    let repeated = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered_again = stream_frontier_image(&repeated.store, "stream").await;
    repeated.crash();
    assert_eq!(recovered["physical_bytes"], serde_json::json!(b"baseline|next|later|"));
    assert_eq!(recovered["writer_tail"], 20);
    assert_eq!(recovered["durable_tail"], 20);
    assert_eq!(recovered["appender_written"], 20);
    assert_eq!(recovered_again, recovered);
}

struct TailCacheGuard(usize);

impl TailCacheGuard {
    fn enabled() -> Self {
        let guard = Self(crate::store::tail_cache_bytes());
        crate::store::set_tail_cache_bytes(64);
        guard
    }
}

impl Drop for TailCacheGuard {
    fn drop(&mut self) {
        crate::store::set_tail_cache_bytes(self.0);
    }
}

async fn inline_sse_source(store: &Arc<Store>, path: &str, offset: &str) -> Box<dyn crate::api::EventSource> {
    let response = handlers::handle(
        store.clone(),
        Req {
            method: Method::Get,
            path: path.into(),
            query: Some(format!("offset={offset}&live=sse")),
            headers: Vec::new(),
            body: Bytes::new(),
        },
    )
    .await;
    assert_eq!(response.status, 200);
    let crate::api::Body::Sse(source) = response.body else {
        panic!("expected real SSE body");
    };
    #[cfg(target_os = "linux")]
    assert!(source.reactor_reg().is_none(), "the fixture must exercise the actual inline fork path");
    source
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_tail_publication_reordering_delivers_inline_sse_and_waiting_long_poll() {
    let _guard = DurabilityGuard::wal();
    let _cache = TailCacheGuard::enabled();
    let dir = temp_dir("tail-publication-reorder");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "parent", "text/plain").await;
    let fork = put_req(
        "stream",
        "text/plain",
        b"",
        &[("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(0))],
    );
    assert_eq!(handlers::handle(h.store.clone(), fork).await.status, 201);
    let stream = h.store.get("stream").unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    stream.set_tail_publish_hook(Box::new(move |tail| {
        if tail == 6 {
            let entered = entered_tx.lock().unwrap().take();
            if let Some(entered) = entered {
                let _ = entered.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let pause = StagePause(release_tx);
    let first = tokio::spawn(handlers::handle(h.store.clone(), post_req("stream", "text/plain", b"first|")));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();

    // Register a real caught-up long-poll while the shared durable tail is 6,
    // but the delayed append has not notified its watch. Leave it unpolled
    // during the writes, modelling a connection task delayed by other work.
    let request = Req {
        method: Method::Get,
        path: "stream".into(),
        query: Some("offset=now&live=long-poll".into()),
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let mut poll = Box::pin(handlers::handle(h.store.clone(), request));
    let waiting =
        std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(poll.as_mut(), cx).is_pending()))
            .await;
    let registered = stream.tail_tx.receiver_count();
    let second = handlers::handle(h.store.clone(), post_req("stream", "text/plain", b"second|")).await;
    let mut close = post_req("stream", "text/plain", b"");
    close.headers.push(("stream-closed".into(), "true".into()));
    let close = handlers::handle(h.store.clone(), close).await;
    let before_release = *stream.tail_tx.borrow();
    // Release and join every writer before assertions and consumer timeouts.
    drop(pause);
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), first).await.unwrap().unwrap();
    stream.set_tail_publish_hook(Box::new(|_| {}));
    let authoritative = stream.tail();
    let watch = *stream.tail_tx.borrow();
    let cached = stream.tail_chunk_slice(6, 13);
    let polled = tokio::time::timeout(std::time::Duration::from_millis(100), &mut poll).await;
    let long_poll = polled.as_ref().ok().map(|response| {
        serde_json::json!({
            "status": response.status,
            "next_offset": response.headers.iter().find(|(key, _)| key.eq_ignore_ascii_case("stream-next-offset")).map(|(_, value)| value),
            "closed": response.headers.iter().find(|(key, _)| key.eq_ignore_ascii_case("stream-closed")).map(|(_, value)| value),
            "body_bytes": response.body.len(),
        })
    });
    let long_poll_body = polled.as_ref().ok().and_then(|response| match &response.body {
        crate::api::Body::Full(bytes) => Some(bytes.clone()),
        _ => None,
    });
    drop(poll);

    // A fork forces the actual inline SSE path on Linux as well as elsewhere.
    // Opening AFTER the stale watch replacement must still deliver all bytes
    // and durable EOF, then terminate without waiting for another append.
    let response = handlers::handle(
        h.store.clone(),
        Req {
            method: Method::Get,
            path: "stream".into(),
            query: Some(format!("offset={}&live=sse", fork_offset(0))),
            headers: Vec::new(),
            body: Bytes::new(),
        },
    )
    .await;
    let status = response.status;
    let crate::api::Body::Sse(mut source) = response.body else {
        panic!("expected real inline SSE body");
    };
    #[cfg(target_os = "linux")]
    let inline = source.reactor_reg().is_none();
    #[cfg(not(target_os = "linux"))]
    let inline = true;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), source.next_chunk()).await.unwrap().unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    let ended =
        matches!(tokio::time::timeout(std::time::Duration::from_millis(100), source.next_chunk()).await, Ok(None));
    drop(source);
    drop(stream);
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let recovered_bytes = stream_file_bytes(&reopened.store, "stream");
    let recovered_tail = reopened.store.get("stream").unwrap().tail();
    reopened.crash();

    assert!(waiting && registered > 0, "the real long-poll must be waiting before later publications");
    assert_eq!((first.status, second.status, close.status, status), (204, 204, 204, 200));
    assert_eq!(before_release, crate::store::Tail { bytes: 13, closed: true });
    assert_eq!(authoritative, crate::store::Tail { bytes: 13, closed: true });
    assert!(inline);
    assert!(
        frame.contains("data:first|second|") && frame.contains("\"streamClosed\":true") && ended && long_poll.is_some(),
        "real consumers lost published bytes/EOF: SSE={frame:?}, terminated={ended}, long_poll={long_poll:?}, shared={authoritative:?}, watch={watch:?}"
    );
    assert_eq!(watch, authoritative, "tail notifications must not regress behind committed bytes or closure");
    assert_eq!(cached, Some(Bytes::from_static(b"second|")), "a stale callback must preserve the newer resident chunk");
    let long_poll = long_poll.unwrap();
    assert_eq!(long_poll["status"], 200);
    assert_eq!(long_poll["body_bytes"], 7);
    assert_eq!(long_poll["closed"], "true");
    assert_eq!(long_poll["next_offset"], fork_offset(13));
    assert_eq!(long_poll_body, Some(Bytes::from_static(b"second|")));
    assert_eq!(recovered_bytes, b"first|second|");
    assert_eq!(recovered_tail, authoritative);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_tail_publication_close_before_delayed_callback_finishes_waiting_and_now_sse() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("tail-publication-close-first");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "parent", "text/plain").await;
    create_stream(&h.store, "other", OCTET).await;
    assert_eq!(
        handlers::handle(
            h.store.clone(),
            put_req(
                "stream",
                "text/plain",
                b"",
                &[("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(0)),]
            )
        )
        .await
        .status,
        201
    );
    let mut waiting = inline_sse_source(&h.store, "stream", &fork_offset(0)).await;
    let initial = waiting.next_chunk().await.unwrap();
    let stream = h.store.get("stream").unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    stream.set_tail_publish_hook(Box::new(move |tail| {
        if tail == 6 {
            let entered = entered_tx.lock().unwrap().take();
            if let Some(entered) = entered {
                let _ = entered.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let pause = StagePause(release_tx);
    let append = tokio::spawn(handlers::handle(h.store.clone(), post_req("stream", "text/plain", b"first|")));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    let other = handlers::handle(h.store.clone(), post_req("other", OCTET, b"other|")).await;
    let mut close = post_req("stream", "text/plain", b"");
    close.headers.push(("stream-closed".into(), "true".into()));
    let close = handlers::handle(h.store.clone(), close).await;
    let committed = *stream.tail_tx.borrow();
    drop(pause);
    let append = tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap();
    stream.set_tail_publish_hook(Box::new(|_| {}));
    let watch = *stream.tail_tx.borrow();
    let frame = String::from_utf8(waiting.next_chunk().await.unwrap().to_vec()).unwrap();
    let ended =
        matches!(tokio::time::timeout(std::time::Duration::from_millis(100), waiting.next_chunk()).await, Ok(None));
    let mut now = inline_sse_source(&h.store, "stream", "now").await;
    let now_frame = String::from_utf8(now.next_chunk().await.unwrap().to_vec()).unwrap();
    let now_ended =
        matches!(tokio::time::timeout(std::time::Duration::from_millis(100), now.next_chunk()).await, Ok(None));
    drop(waiting);
    drop(now);
    drop(stream);
    h.crash();
    assert!(String::from_utf8(initial.to_vec()).unwrap().contains("\"upToDate\":true"));
    assert_eq!((append.status, close.status, other.status), (204, 204, 204));
    assert_eq!(committed, crate::store::Tail { bytes: 6, closed: true });
    assert_eq!(watch, committed, "a delayed equal/open callback must preserve EOF");
    assert!(frame.contains("data:first|") && frame.contains("\"streamClosed\":true") && ended, "waiting SSE={frame:?}");
    assert!(
        now_frame.contains(&fork_offset(6)) && now_frame.contains("\"streamClosed\":true") && now_ended,
        "now SSE={now_frame:?}"
    );
}

#[tokio::test]
async fn e2e_tail_publication_cache_before_wake_and_initial_closed_put_in_both_modes() {
    for wal in [false, true] {
        let _guard = if wal { DurabilityGuard::wal() } else { DurabilityGuard::memory() };
        let _cache = TailCacheGuard::enabled();
        let dir = temp_dir("tail-publication-both-modes");
        let harness = if wal { Some(Harness::boot(dir.path(), Some(1), 1).unwrap()) } else { None };
        let store = harness.as_ref().map(|h| h.store.clone()).unwrap_or_else(|| {
            Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap())
        });
        create_stream(&store, "parent", "text/plain").await;
        let created = handlers::handle(
            store.clone(),
            put_req(
                "closed",
                "text/plain",
                b"closed-body|",
                &[("stream-closed", "true"), ("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(0))],
            ),
        )
        .await;
        let closed = store.get("closed").unwrap();
        assert_eq!(created.status, 201);
        assert_eq!(closed.tail_tx.receiver_count(), 0, "publication must retain state without a subscriber");
        assert_eq!(closed.tail(), crate::store::Tail { bytes: 12, closed: true });
        assert_eq!(*closed.tail_tx.borrow(), closed.tail());
        assert_eq!(closed.tail_chunk_slice(0, 12), Some(Bytes::from_static(b"closed-body|")));
        let mut source = inline_sse_source(&store, "closed", &fork_offset(0)).await;
        let frame = String::from_utf8(source.next_chunk().await.unwrap().to_vec()).unwrap();
        assert!(frame.contains("data:closed-body|") && frame.contains("\"streamClosed\":true"));
        assert!(source.next_chunk().await.is_none());
        drop(source);

        create_stream(&store, "hot", "text/plain").await;
        append_acked(&store, "hot", "text/plain", b"first|").await;
        let hot = store.get("hot").unwrap();
        let mut poll = Box::pin(handlers::handle(
            store.clone(),
            Req {
                method: Method::Get,
                path: "hot".into(),
                query: Some(format!("offset={}&live=long-poll", fork_offset(6))),
                headers: Vec::new(),
                body: Bytes::new(),
            },
        ));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                std::future::Future::poll(poll.as_mut(), cx).is_pending()
            ))
            .await
        );
        let append = handlers::handle(store.clone(), post_req("hot", "text/plain", b"second|")).await;
        let polled = tokio::time::timeout(std::time::Duration::from_secs(2), &mut poll).await.unwrap();
        drop(poll);
        assert_eq!(append.status, 204);
        assert_eq!(polled.status, 200);
        let crate::api::Body::Full(body) = polled.body else {
            panic!("woken long-poll must use the already-installed resident chunk");
        };
        assert_eq!(body, Bytes::from_static(b"second|"));
        let mut close = post_req("hot", "text/plain", b"");
        close.headers.push(("stream-closed".into(), "true".into()));
        assert_eq!(handlers::handle(store.clone(), close).await.status, 204);
        assert_eq!(*hot.tail_tx.borrow(), crate::store::Tail { bytes: 13, closed: true });
        assert_eq!(
            hot.tail_chunk_slice(6, 13),
            Some(Bytes::from_static(b"second|")),
            "close preserves the newest cache"
        );
        drop(hot);
        drop(closed);
        drop(store);
        let rebooted = if let Some(harness) = harness {
            harness.crash();
            Some(Harness::boot(dir.path(), None, 1).unwrap())
        } else {
            None
        };
        let store = rebooted.as_ref().map(|h| h.store.clone()).unwrap_or_else(|| {
            Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap())
        });
        assert_eq!(stream_file_bytes(&store, "closed"), b"closed-body|");
        assert_eq!(store.get("closed").unwrap().tail(), crate::store::Tail { bytes: 12, closed: true });
        assert_eq!(stream_file_bytes(&store, "hot"), b"first|second|");
        drop(store);
        if let Some(rebooted) = rebooted {
            rebooted.crash();
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn e2e_tail_publication_reactor_delivers_data_and_eof_without_watch_receivers() {
    use tokio::io::AsyncReadExt;

    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("tail-publication-reactor");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "stream", "text/plain").await;
    let response = handlers::handle(
        h.store.clone(),
        Req {
            method: Method::Get,
            path: "stream".into(),
            query: Some(format!("offset={}&live=sse", fork_offset(0))),
            headers: Vec::new(),
            body: Bytes::new(),
        },
    )
    .await;
    let crate::api::Body::Sse(source) = response.body else {
        panic!("expected reactor-eligible SSE body");
    };
    let registration = source.reactor_reg().expect("root live stream must be reactor-eligible");
    drop(source);
    let stream = h.store.get("stream").unwrap();
    assert_eq!(stream.tail_tx.receiver_count(), 0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    // This is the real reactor handoff and raw chunked body over TCP. No global
    // shutdown: EOF must close this subscriber and release its permit itself.
    crate::sse_reactor::register(server, Vec::new(), registration, permit);
    let mut bytes = Vec::new();
    let initial =
        tokio::time::timeout(std::time::Duration::from_secs(2), client.read_buf(&mut bytes)).await.unwrap().unwrap();
    assert!(initial > 0, "the subscriber must be seated and emit its initial control before the append");
    let append = handlers::handle(h.store.clone(), post_req("stream", "text/plain", b"reactor|")).await;
    let mut close = post_req("stream", "text/plain", b"");
    close.headers.push(("stream-closed".into(), "true".into()));
    let close = handlers::handle(h.store.clone(), close).await;
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), client.read_to_end(&mut bytes)).await;
    let receiver_count = stream.tail_tx.receiver_count();
    let tail = *stream.tail_tx.borrow();
    drop(client);
    // Socket EOF precedes unlinking the subscriber and dropping its permit.
    // Wait for that cleanup explicitly before checking permit availability.
    let released = tokio::time::timeout(std::time::Duration::from_secs(2), semaphore.clone().acquire_owned()).await;
    let release_ok = matches!(&released, Ok(Ok(_)));
    if let Ok(Ok(permit)) = released {
        drop(permit);
    }
    drop(listener);
    drop(stream);
    h.crash();
    assert!(matches!(read, Ok(Ok(_))), "the actual reactor must finish the TCP body successfully at durable EOF");
    assert!(release_ok, "EOF must finish releasing the reactor subscriber permit");
    assert_eq!((append.status, close.status, receiver_count), (204, 204, 0));
    assert_eq!(tail, crate::store::Tail { bytes: 8, closed: true });
    let wire = String::from_utf8(bytes).unwrap();
    assert!(
        wire.contains("data:reactor|") && wire.contains("\"streamClosed\":true") && wire.ends_with("0\r\n\r\n"),
        "reactor body={wire:?}"
    );
    assert_eq!(semaphore.available_permits(), 1, "EOF must release the reactor subscriber");
}

fn append_meta_image(meta: &crate::store::Meta) -> serde_json::Value {
    serde_json::json!({
        "closed": meta.closed,
        "closed_by": meta.closed_by,
        "producers": meta.producers,
        "last_seq_header": meta.last_seq_header,
        "durable_tail": meta.durable_tail,
    })
}

fn append_shared_image(stream: &crate::store::StreamState) -> serde_json::Value {
    let shared = stream.shared.read().unwrap();
    serde_json::json!({
        "closed": shared.closed,
        "closed_by": shared.closed_by,
        "producers": shared.producers.iter().map(|(id, entry)| (id, entry.writer)).collect::<std::collections::HashMap<_, _>>(),
        "last_seq_header": shared.last_seq_header,
        "durable_tail": shared.durable_tail,
    })
}

/// Dirty work from an acknowledged append or a TTL read must never persist
/// writer state from a later POST rejected before WAL staging. Checkpoint's
/// tail capture is deliberately completed before the failed append begins:
/// this fixture isolates its sidecar write from the separate tail-map proof.
async fn general_metadata_does_not_persist_failed_append(checkpoint: bool) {
    let dir = temp_dir(if checkpoint { "general-meta-checkpoint" } else { "general-meta-sweep" });
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    let create = put_req("stream", OCTET, b"", &[("stream-ttl", "60")]);
    assert_eq!(handlers::handle(h.store.clone(), create).await.status, 201);
    let stream = h.store.get("stream").unwrap();
    let sidecar = crate::store::meta_path(&stream.file_path);
    let read_meta = || serde_json::from_slice::<crate::store::Meta>(&std::fs::read(&sidecar).unwrap()).unwrap();
    let producer_request = |sequence: u64, body: &[u8], close: bool| {
        let mut request = post_req("stream", OCTET, body);
        request.headers.extend([
            ("producer-id".into(), "producer".into()),
            ("producer-epoch".into(), "1".into()),
            ("producer-seq".into(), sequence.to_string()),
            ("stream-seq".into(), format!("{sequence:04}")),
        ]);
        if close {
            request.headers.push(("stream-closed".into(), "true".into()));
        }
        request
    };
    assert_eq!(handlers::handle(h.store.clone(), producer_request(0, b"baseline|", false)).await.status, 200);
    h.walset.shards()[0].checkpoint().await.unwrap();
    assert!(!stream.meta_dirty.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(append_meta_image(&read_meta()), append_shared_image(&stream));

    let mut expected_bytes = b"baseline|".to_vec();
    let failed_sequence = if checkpoint {
        // The previous acknowledged producer update, not fabricated shared
        // state, provides checkpoint's pending metadata work.
        assert_eq!(handlers::handle(h.store.clone(), producer_request(1, b"prior|", false)).await.status, 200);
        expected_bytes.extend_from_slice(b"prior|");
        assert!(stream.meta_dirty.load(std::sync::atomic::Ordering::Acquire));
        2
    } else {
        // GET is the real sliding-TTL renewal/queue route. HEAD and ordinary
        // get() intentionally do not supply dirty metadata work.
        assert!(h.store.get_for_read("stream").is_some());
        assert!(stream.meta_dirty.load(std::sync::atomic::Ordering::Acquire));
        1
    };
    let committed = append_shared_image(&stream);

    let mut checkpoint_pause = None;
    let mut checkpoint_task = None;
    if checkpoint {
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
        let captured_tx = std::sync::Mutex::new(Some(captured_tx));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        h.walset.shards()[0].set_on_checkpoint_tails_hook(Box::new(move || {
            if let Some(captured) = captured_tx.lock().unwrap().take() {
                let _ = captured.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }));
        checkpoint_pause = Some(StagePause(release_tx));
        let shard = h.walset.shards()[0].clone();
        checkpoint_task = Some(tokio::spawn(async move { shard.checkpoint().await }));
        tokio::time::timeout(std::time::Duration::from_secs(2), captured_rx).await.unwrap().unwrap();
    }

    let stream_id = stream.id;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    h.walset.shards()[0].set_on_stage_hook(Box::new(move |id| {
        if id == stream_id {
            if let Some(entered) = entered_tx.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let pause = StagePause(release_tx);
    let append = tokio::spawn(handlers::handle(h.store.clone(), producer_request(failed_sequence, &[b'x'; 512], true)));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    let speculative = append_shared_image(&stream);
    let speculative_close_visible = stream.shared.read().unwrap().closed_durable;
    drop(checkpoint_pause);
    let crossed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let swept = if let Some(task) = checkpoint_task.as_mut() {
            task.await.unwrap().unwrap();
            None
        } else {
            let store = h.store.clone();
            Some(tokio::task::spawn_blocking(move || store.sweep_meta_once()).await.unwrap())
        };
        (append_meta_image(&read_meta()), swept)
    })
    .await;
    // Release and join the real POST before asserting metadata images or
    // dropping the runtime. Its body cannot fit a 256-byte WAL segment.
    drop(pause);
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap();
    if crossed.is_err() {
        if let Some(task) = checkpoint_task.as_mut() {
            task.await.unwrap().unwrap();
        }
    }
    h.walset.shards()[0].set_on_stage_hook(Box::new(|_| {}));
    h.walset.shards()[0].set_on_checkpoint_tails_hook(Box::new(|| {}));
    let rolled_back = append_shared_image(&stream);
    let live_bytes = stream_file_bytes(&h.store, "stream");
    let (persisted, swept) = crossed.expect("metadata writer must finish while the failed append is paused");
    drop(stream);
    h.crash();

    // Run the complete production recovery sequence before comparing the
    // captured sidecar, live rollback and reopened state.
    let reopened = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered = append_meta_image(&read_meta());
    let reopened_stream = reopened.store.get("stream").unwrap();
    let reopened_state = append_shared_image(&reopened_stream);
    let recovered_bytes = stream_file_bytes(&reopened.store, "stream");
    drop(reopened_stream);
    reopened.crash();
    assert_eq!(response.status, 500, "oversized append must fail staging");
    if !checkpoint {
        assert_eq!(swept, Some(1), "the legitimate TTL renewal must supply one sweep write");
    }
    assert_eq!(speculative["closed"], true, "the pause must cross the speculative close window");
    assert!(!speculative_close_visible, "the failed close was never reader-visible");
    assert_ne!(speculative["producers"], committed["producers"], "the pause must cross a producer update");
    assert_ne!(speculative["last_seq_header"], committed["last_seq_header"], "the pause must cross a writer update");
    assert_eq!(rolled_back, committed, "the failed append restores the live metadata");
    assert_eq!(live_bytes, expected_bytes, "stage failure removes its attempted bytes before restart");
    assert_eq!(recovered_bytes, expected_bytes, "no bytes from the failed append recover");
    assert_eq!(
        (persisted, recovered, reopened_state),
        (committed.clone(), committed.clone(), committed),
        "the general metadata write and reopened stream must contain only committed append state"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_general_meta_sweep_does_not_persist_failed_append() {
    let _guard = DurabilityGuard::wal();
    general_metadata_does_not_persist_failed_append(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_general_meta_checkpoint_does_not_persist_failed_append() {
    let _guard = DurabilityGuard::wal();
    general_metadata_does_not_persist_failed_append(true).await;
}

async fn duplicate_waiting_for_durability(close: bool, committed_retry: bool) {
    let dir = temp_dir("duplicate-before-durability");
    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    let request = |seq: u64, close: bool| {
        let mut request = post_req("stream", OCTET, b"record|");
        request.headers.extend([
            ("producer-id".into(), "producer".into()),
            ("producer-epoch".into(), "1".into()),
            ("producer-seq".into(), seq.to_string()),
        ]);
        if close {
            request.headers.push(("stream-closed".into(), "true".into()));
        }
        request
    };
    assert_eq!(handlers::handle(h.store.clone(), request(0, false)).await.status, 200);
    h.stop_committers();
    let append = tokio::spawn(handlers::handle(h.store.clone(), request(1, close)));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while h.walset.shards()[0].waiter_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let duplicate = handlers::handle(h.store.clone(), request(if committed_retry { 0 } else { 1 }, close)).await;
    let speculative = h.store.get("stream").unwrap().tail();
    h.committers.push(h.walset.shards()[0].spawn_committer());
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap();
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let bytes = stream_file_bytes(&reopened.store, "stream");
    reopened.crash();
    assert_eq!(response.status, 200);
    assert_eq!(speculative.bytes, 7);
    assert!(!speculative.closed, "pending close must not be visible");
    assert_eq!(bytes, b"record|record|");
    if committed_retry {
        assert_eq!(duplicate.status, 204);
        assert_eq!(
            duplicate
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("producer-seq"))
                .map(|(_, value)| value.as_str()),
            Some("0"),
            "duplicate replies report the committed sequence"
        );
    } else {
        assert_eq!(duplicate.status, 503, "a duplicate must retry until its bytes and any closure are durable");
    }
}

#[tokio::test]
async fn e2e_duplicate_does_not_ack_pending_append() {
    let _guard = DurabilityGuard::wal();
    duplicate_waiting_for_durability(false, false).await;
}

#[tokio::test]
async fn e2e_duplicate_does_not_ack_pending_close() {
    let _guard = DurabilityGuard::wal();
    duplicate_waiting_for_durability(true, false).await;
}

#[tokio::test]
async fn e2e_duplicate_does_not_report_pending_producer_sequence() {
    let _guard = DurabilityGuard::wal();
    duplicate_waiting_for_durability(false, true).await;
}

async fn cancelled_staged_request_finishes_publication(initial_put: bool, close_body: bool, immediate_close: bool) {
    let dir = temp_dir("cancelled-staged-publication");
    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    if !initial_put {
        create_stream(&h.store, "stream", OCTET).await;
    }
    h.stop_committers();
    let request = if initial_put {
        put_req("stream", OCTET, b"body|", &[])
    } else {
        producer_post("stream", b"body|", 1, 0, close_body)
    };
    let append = tokio::spawn(handlers::handle(h.store.clone(), request));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while h.walset.shards()[0].waiter_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let stream = h.store.get("stream").unwrap();
    append.abort();
    let cancelled = append.await;
    let retained_admission = stream.admitted_operations();
    let close_request = || {
        let mut close = post_req("stream", OCTET, b"");
        close.headers.push(("stream-closed".into(), "true".into()));
        close
    };
    // Keep the committer stopped: this request cannot depend on whether the
    // detached continuation has already been polled by this runtime.
    let premature_close = if immediate_close {
        let response = handlers::handle(h.store.clone(), close_request()).await;
        Some((response.status, stream.tail()))
    } else {
        None
    };
    h.committers.push(h.walset.shards()[0].spawn_committer());
    let publication = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let committed = initial_put || stream.shared.read().unwrap().producers["producer"].committed.is_some();
            if stream.tail().bytes == 5 && committed && stream.admitted_operations() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let retry = if initial_put {
        put_req("stream", OCTET, b"", &[])
    } else {
        producer_post("stream", b"body|", 1, 0, close_body)
    };
    let duplicate = handlers::handle(h.store.clone(), retry).await;
    let close = handlers::handle(h.store.clone(), close_request()).await;
    let live_tail = stream.tail();
    drop(stream);
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let bytes = stream_file_bytes(&reopened.store, "stream");
    let tail = reopened.store.get("stream").unwrap().tail();
    reopened.crash();
    assert!(matches!(cancelled, Err(error) if error.is_cancelled()));
    assert_eq!(
        (retained_admission, publication.is_ok(), duplicate.status, close.status),
        (
            1,
            true,
            if initial_put {
                200
            } else if immediate_close {
                409
            } else {
                204
            },
            204
        ),
        "staged ownership must survive cancellation and finish publication, metadata and later requests"
    );
    if immediate_close {
        assert_eq!(premature_close, Some((503, crate::store::Tail { bytes: 0, closed: false })));
    }
    assert_eq!(live_tail, crate::store::Tail { bytes: 5, closed: true });
    assert_eq!(bytes, b"body|");
    assert_eq!(tail, live_tail);
}

#[tokio::test]
async fn e2e_cancelled_staged_append_finishes_publication_and_metadata() {
    let _guard = DurabilityGuard::wal();
    cancelled_staged_request_finishes_publication(false, false, false).await;
}

#[tokio::test]
async fn e2e_cancelled_initial_put_body_finishes_publication() {
    let _guard = DurabilityGuard::wal();
    cancelled_staged_request_finishes_publication(true, false, false).await;
}

#[tokio::test]
async fn e2e_cancelled_staged_close_body_finishes_original_candidate() {
    let _guard = DurabilityGuard::wal();
    cancelled_staged_request_finishes_publication(false, true, false).await;
}

#[tokio::test]
async fn e2e_cancelled_staged_append_allows_close_retry_after_immediate_503() {
    let _guard = DurabilityGuard::wal();
    cancelled_staged_request_finishes_publication(false, false, true).await;
}

#[tokio::test]
async fn e2e_close_only_waits_for_preceding_append_durability() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("close-only-pending-body");
    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    h.stop_committers();
    let append = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", b"body|", 1, 0, false)));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while h.walset.shards()[0].waiter_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let close = || {
        let mut close = post_req("stream", OCTET, b"");
        close.headers.push(("stream-closed".into(), "true".into()));
        close
    };
    let pending = handlers::handle(h.store.clone(), close()).await;
    let before_durability = h.store.get("stream").unwrap().tail();
    h.committers.push(h.walset.shards()[0].spawn_committer());
    assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap().status, 200);
    let completed = handlers::handle(h.store.clone(), close()).await;
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let bytes = stream_file_bytes(&reopened.store, "stream");
    let tail = reopened.store.get("stream").unwrap().tail();
    reopened.crash();
    assert_eq!(pending.status, 503, "close-only cannot commit EOF ahead of preceding bytes");
    assert_eq!(before_durability, crate::store::Tail { bytes: 0, closed: false });
    assert_eq!(completed.status, 204, "matching close retries finish once bytes are durable");
    assert_eq!(bytes, b"body|");
    assert_eq!(tail, crate::store::Tail { bytes: 5, closed: true });
}

#[tokio::test]
async fn e2e_failed_close_metadata_retries_original_candidate() {
    let _guard = DurabilityGuard::wal();
    for after_rename in [false, true] {
        let dir = temp_dir("close-meta-failure-retry");
        let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
        assert_eq!(
            handlers::handle(h.store.clone(), put_req("stream", OCTET, b"", &[("stream-ttl", "60")])).await.status,
            201
        );
        let stream = h.store.get("stream").unwrap();
        let read_meta = || {
            serde_json::from_slice::<crate::store::Meta>(
                &std::fs::read(crate::store::meta_path(&stream.file_path)).unwrap(),
            )
            .unwrap()
        };
        stream.fail_close_meta_once(after_rename);
        let failed = handlers::handle(h.store.clone(), producer_post("stream", b"body|", 1, 0, true)).await;
        let uncertain_meta = read_meta();
        let uncommitted_tail = stream.tail();
        assert!(h.store.get_for_read("stream").is_some());
        let swept = tokio::task::spawn_blocking({
            let store = h.store.clone();
            move || store.sweep_meta_once()
        })
        .await
        .unwrap();
        let swept_meta = read_meta();
        let mismatch = handlers::handle(h.store.clone(), producer_post("stream", b"body|", 1, 1, true)).await;
        let mut unowned = post_req("stream", OCTET, b"different|");
        unowned.headers.push(("stream-closed".into(), "true".into()));
        let unowned = handlers::handle(h.store.clone(), unowned).await;
        let retry = || {
            let mut retry = producer_post("stream", b"body-must-not-be-written-twice|", 1, 0, true);
            retry.headers.iter_mut().find(|(key, _)| key == "stream-seq").unwrap().1 = "9999".into();
            retry
        };
        let producerless_retry = || {
            let mut retry = post_req("stream", OCTET, b"");
            retry.headers.push(("stream-closed".into(), "true".into()));
            retry
        };
        let (first, second) = tokio::join!(
            handlers::handle(h.store.clone(), retry()),
            handlers::handle(h.store.clone(), producerless_retry())
        );
        let committed_meta = read_meta();
        let committed_tail = stream.tail();
        drop(stream);
        h.crash();
        let reopened = Harness::boot(dir.path(), None, 1).unwrap();
        let bytes = stream_file_bytes(&reopened.store, "stream");
        let tail = reopened.store.get("stream").unwrap().tail();
        reopened.crash();
        assert_eq!(failed.status, 500);
        assert_eq!(
            uncertain_meta.closed, after_rename,
            "post-rename failure leaves an explicitly uncertain disk image"
        );
        assert_eq!(uncommitted_tail, crate::store::Tail { bytes: 5, closed: false });
        assert_eq!(swept, 1);
        assert!(!swept_meta.closed, "general writers must not commit a failed close candidate");
        assert!(swept_meta.producers.is_empty());
        assert!(swept_meta.last_seq_header.is_none());
        assert_eq!(mismatch.status, 409);
        assert_eq!(unowned.status, 409);
        assert_eq!((first.status, second.status), (204, 204));
        assert!(committed_meta.closed);
        assert_eq!(
            committed_meta.closed_by,
            Some(("producer".into(), 1, 0)),
            "producerless retry preserves original close ownership"
        );
        assert_eq!(
            committed_meta.last_seq_header.as_deref(),
            Some("0001-0000"),
            "retry preserves the accepted writer sequence"
        );
        assert_eq!(committed_meta.producers["producer"].last_seq, 0);
        assert_eq!(committed_tail, crate::store::Tail { bytes: 5, closed: true });
        assert_eq!(bytes, b"body|", "retry never appends the candidate's body again");
        assert_eq!(tail, committed_tail);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_committed_metadata_does_not_regress_with_callback_order() {
    let _guard = DurabilityGuard::wal();
    for next_epoch in [1, 2] {
        let dir = temp_dir("append-commit-order");
        let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
        create_stream(&h.store, "stream", OCTET).await;
        let stream = h.store.get("stream").unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = std::sync::Mutex::new(Some(entered_tx));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        stream.set_append_commit_hook(Box::new(move |tail| {
            if tail == 6 {
                if let Some(entered) = entered_tx.lock().unwrap().take() {
                    let _ = entered.send(());
                    let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
                }
            }
        }));
        let pause = StagePause(release_tx);
        let first = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", b"first|", 1, 0, false)));
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
        let next_seq = u64::from(next_epoch == 1);
        let second =
            handlers::handle(h.store.clone(), producer_post("stream", b"second|", next_epoch, next_seq, false)).await;
        drop(pause);
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), first).await.unwrap().unwrap();
        stream.set_append_commit_hook(Box::new(|_| {}));
        h.walset.shards()[0].checkpoint().await.unwrap();
        let meta: crate::store::Meta =
            serde_json::from_slice(&std::fs::read(crate::store::meta_path(&stream.file_path)).unwrap()).unwrap();
        let committed = stream.shared.read().unwrap().producers["producer"].committed.unwrap();
        drop(stream);
        h.crash();
        let reopened = Harness::boot(dir.path(), None, 1).unwrap();
        let recovered =
            reopened.store.get("stream").unwrap().shared.read().unwrap().producers["producer"].committed.unwrap();
        let bytes = stream_file_bytes(&reopened.store, "stream");
        reopened.crash();
        assert_eq!((first.status, second.status), (200, 200));
        assert_eq!((committed.epoch, committed.last_seq), (next_epoch, next_seq));
        assert_eq!((meta.producers["producer"].epoch, meta.producers["producer"].last_seq), (next_epoch, next_seq));
        assert_eq!((recovered.epoch, recovered.last_seq), (next_epoch, next_seq));
        assert_eq!(meta.last_seq_header, Some(format!("{next_epoch:04}-{next_seq:04}")));
        assert_eq!(bytes, b"first|second|");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn e2e_stage_rollback_preserves_an_earlier_commit_promotion() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("rollback-preserves-commit");
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    let stream = h.store.get("stream").unwrap();
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let committed_tx = std::sync::Mutex::new(Some(committed_tx));
    let (first_release_tx, first_release_rx) = std::sync::mpsc::channel();
    let first_release_rx = std::sync::Mutex::new(first_release_rx);
    stream.set_append_commit_hook(Box::new(move |tail| {
        if tail == 6 {
            if let Some(entered) = committed_tx.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = first_release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let first_pause = StagePause(first_release_tx);
    let first = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", b"first|", 1, 0, false)));
    tokio::time::timeout(std::time::Duration::from_secs(2), committed_rx).await.unwrap().unwrap();
    let (staged_tx, staged_rx) = tokio::sync::oneshot::channel();
    let staged_tx = std::sync::Mutex::new(Some(staged_tx));
    let (failed_release_tx, failed_release_rx) = std::sync::mpsc::channel();
    let failed_release_rx = std::sync::Mutex::new(failed_release_rx);
    let stream_id = stream.id;
    h.walset.shards()[0].set_on_stage_hook(Box::new(move |id| {
        if id == stream_id {
            if let Some(entered) = staged_tx.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = failed_release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let failed_pause = StagePause(failed_release_tx);
    let failed = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", &[b'x'; 512], 1, 1, true)));
    tokio::time::timeout(std::time::Duration::from_secs(2), staged_rx).await.unwrap().unwrap();
    drop(first_pause);
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), first).await.unwrap().unwrap();
    let promoted = stream.shared.read().unwrap().producers["producer"].committed.unwrap();
    crate::store::write_meta_sync(&stream, true).unwrap();
    drop(failed_pause);
    let failed = tokio::time::timeout(std::time::Duration::from_secs(2), failed).await.unwrap().unwrap();
    stream.set_append_commit_hook(Box::new(|_| {}));
    h.walset.shards()[0].set_on_stage_hook(Box::new(|_| {}));
    let rolled_back = stream.shared.read().unwrap().producers["producer"].committed.unwrap();
    drop(stream);
    h.crash();
    let reopened = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered =
        reopened.store.get("stream").unwrap().shared.read().unwrap().producers["producer"].committed.unwrap();
    let bytes = stream_file_bytes(&reopened.store, "stream");
    let closed = reopened.store.get("stream").unwrap().tail().closed;
    reopened.crash();
    assert_eq!(first.status, 200);
    assert_eq!(failed.status, 500);
    for state in [promoted, rolled_back, recovered] {
        assert_eq!((state.epoch, state.last_seq), (1, 0), "rollback must retain earlier request-owned promotion");
    }
    assert_eq!(bytes, b"first|");
    assert!(!closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_close_worker_publishes_after_http_waiter_cancellation() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("owned-close-publication");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    let stream = h.store.get("stream").unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    stream.set_close_meta_hook(Box::new(move || {
        if let Some(entered) = entered_tx.lock().unwrap().take() {
            let _ = entered.send(());
            let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
        }
    }));
    let pause = StagePause(release_tx);
    let append = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", b"body|", 1, 0, true)));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    append.abort();
    let cancelled = append.await;
    let before_publication = stream.tail();
    let mut sweep = tokio::task::spawn_blocking({
        let stream = stream.clone();
        move || crate::store::write_meta_sync(&stream, true)
    });
    let writer_blocked = tokio::time::timeout(std::time::Duration::from_millis(30), &mut sweep).await.is_err();
    drop(pause);
    let swept = sweep.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !stream.tail().closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let committed_tail = stream.tail();
    let meta: crate::store::Meta =
        serde_json::from_slice(&std::fs::read(crate::store::meta_path(&stream.file_path)).unwrap()).unwrap();
    stream.set_close_meta_hook(Box::new(|| {}));
    drop(stream);
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let tail = reopened.store.get("stream").unwrap().tail();
    let bytes = stream_file_bytes(&reopened.store, "stream");
    reopened.crash();
    assert!(matches!(cancelled, Err(error) if error.is_cancelled()));
    assert_eq!(before_publication, crate::store::Tail { bytes: 5, closed: false });
    assert!(writer_blocked, "general metadata capture waits for the owned close's publication");
    swept.unwrap();
    assert!(meta.closed, "a later general writer cannot reopen a committed close");
    assert_eq!(meta.producers["producer"].last_seq, 0);
    assert_eq!(committed_tail, crate::store::Tail { bytes: 5, closed: true });
    assert_eq!(tail, committed_tail);
    assert_eq!(bytes, b"body|");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_cancelled_close_worker_retains_admission_until_delete_can_finish() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("owned-close-delete-admission");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "stream", OCTET).await;
    let stream = h.store.get("stream").unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    stream.set_close_meta_hook(Box::new(move || {
        if let Some(entered) = entered_tx.lock().unwrap().take() {
            let _ = entered.send(());
            let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
        }
    }));
    let pause = StagePause(release_tx);
    let append = tokio::spawn(handlers::handle(h.store.clone(), producer_post("stream", b"body|", 1, 0, true)));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    append.abort();
    let cancelled = append.await;
    let owned_admission = stream.admitted_operations();
    let mut deletion = tokio::spawn(handlers::handle(
        h.store.clone(),
        Req { method: Method::Delete, path: "stream".into(), query: None, headers: vec![], body: Bytes::new() },
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !stream.is_retiring() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let delete_blocked = tokio::time::timeout(std::time::Duration::from_millis(30), &mut deletion).await.is_err();
    let files_retained = stream.file_path.exists() && crate::store::meta_path(&stream.file_path).exists();
    drop(pause);
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), deletion).await.unwrap().unwrap();
    let admission_released = stream.admitted_operations();
    let files_absent = !stream.file_path.exists() && !crate::store::meta_path(&stream.file_path).exists();
    stream.set_close_meta_hook(Box::new(|| {}));
    drop(stream);
    h.crash();
    let reopened = Harness::boot(dir.path(), None, 1).unwrap();
    let remains_absent = reopened.store.get("stream").is_none();
    reopened.crash();
    assert!(matches!(cancelled, Err(error) if error.is_cancelled()));
    assert_eq!(owned_admission, 1, "owned worker retains the original admission after HTTP cancellation");
    assert!(delete_blocked, "DELETE drains the owned worker without unlinking its sidecar");
    assert!(files_retained);
    assert_eq!(response.status, 204);
    assert_eq!(admission_released, 0);
    assert!(files_absent);
    assert!(remains_absent);
}

#[tokio::test]
async fn e2e_committed_metadata_memory_mode_and_put_body() {
    let _guard = DurabilityGuard::memory();
    let dir = temp_dir("memory-committed-metadata");
    let store = Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap());
    assert_eq!(handlers::handle(store.clone(), put_req("stream", OCTET, b"initial|", &[])).await.status, 201);
    assert_eq!(handlers::handle(store.clone(), producer_post("stream", b"open|", 1, 0, false)).await.status, 200);
    assert_eq!(store.sweep_meta_once(), 1);
    let stream = store.get("stream").unwrap();
    let swept: crate::store::Meta =
        serde_json::from_slice(&std::fs::read(crate::store::meta_path(&stream.file_path)).unwrap()).unwrap();
    assert_eq!(handlers::handle(store.clone(), producer_post("stream", b"close|", 1, 1, true)).await.status, 200);
    let mut closed_put = put_req("closed-put", OCTET, b"closed-body|", &[]);
    closed_put.headers.push(("stream-closed".into(), "true".into()));
    assert_eq!(handlers::handle(store.clone(), closed_put).await.status, 201);
    drop(stream);
    drop(store);
    let reopened = Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap();
    let stream = reopened.get("stream").unwrap();
    let committed = stream.shared.read().unwrap().producers["producer"].committed.unwrap();
    assert!(!swept.closed);
    assert_eq!(swept.producers["producer"].last_seq, 0);
    assert_eq!(swept.last_seq_header.as_deref(), Some("0001-0000"));
    assert_eq!(std::fs::read(&stream.file_path).unwrap(), b"initial|open|close|");
    assert!(stream.tail().closed);
    assert_eq!((committed.epoch, committed.last_seq), (1, 1));
    let closed_put = reopened.get("closed-put").unwrap();
    assert!(closed_put.tail().closed);
    assert_eq!(std::fs::read(&closed_put.file_path).unwrap(), b"closed-body|");
}

/// Cross a real fork-reference update with an append that has changed its
/// speculative writer state but has not staged any WAL record. Its 512-byte
/// body cannot fit the fixture's 256-byte segment, so releasing the hook
/// deterministically produces a 500 and rolls back the live append.
async fn fork_reference_does_not_persist_inflight_append(release_child: bool) {
    let dir = temp_dir(if release_child { "fork-release-inflight" } else { "fork-reserve-inflight" });
    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, 256).unwrap();
    create_stream(&h.store, "parent", OCTET).await;
    let parent = h.store.get("parent").unwrap();
    let sidecar = crate::store::meta_path(&parent.file_path);
    let read_meta = || serde_json::from_slice::<crate::store::Meta>(&std::fs::read(&sidecar).unwrap()).unwrap();
    let committed = append_meta_image(&read_meta());
    let fork_request =
        || put_req("child", OCTET, b"", &[("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(0))]);
    if release_child {
        assert_eq!(handlers::handle(h.store.clone(), fork_request()).await.status, 201);
        assert_eq!(read_meta().ref_count, 1);
    }

    let parent_id = parent.id;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = std::sync::Mutex::new(Some(entered_tx));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    h.walset.shards()[0].set_on_stage_hook(Box::new(move |id| {
        if id == parent_id {
            if let Some(entered) = entered_tx.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = release_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }));
    let pause = StagePause(release_tx);
    let mut request = post_req("parent", OCTET, &[b'x'; 512]);
    request.headers.extend([
        ("producer-id".into(), "failed-producer".into()),
        ("producer-epoch".into(), "1".into()),
        ("producer-seq".into(), "0".into()),
        ("stream-seq".into(), "0001".into()),
        ("stream-closed".into(), "true".into()),
    ]);
    let append = tokio::spawn(handlers::handle(h.store.clone(), request));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
    {
        let shared = parent.shared.read().unwrap();
        assert!(shared.closed, "the hook must cross the speculative close window");
        assert!(!shared.closed_durable);
        assert_eq!(shared.durable_tail, 0);
        assert!(shared.producers.contains_key("failed-producer"));
    }

    let crossed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        if release_child {
            let deletion =
                Req { method: Method::Delete, path: "child".into(), query: None, headers: vec![], body: Bytes::new() };
            assert_eq!(handlers::handle(h.store.clone(), deletion).await.status, 204);
            // DELETE owns removal, while parent-reference persistence is an
            // asynchronous follow-up. Wait for its actual disk publication.
            while read_meta().ref_count != 0 {
                tokio::task::yield_now().await;
            }
        } else {
            assert_eq!(handlers::handle(h.store.clone(), fork_request()).await.status, 201);
            assert_eq!(read_meta().ref_count, 1);
        }
        append_meta_image(&read_meta())
    })
    .await;
    drop(pause);
    assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap().status, 500);
    h.walset.shards()[0].set_on_stage_hook(Box::new(|_| {}));
    {
        let shared = parent.shared.read().unwrap();
        assert!(!shared.closed, "failed append rolls back its live close");
        assert!(!shared.closed_durable);
        assert!(shared.producers.is_empty(), "failed append rolls back live producer dedupe");
        assert!(shared.last_seq_header.is_none(), "failed append rolls back live writer sequence");
    }
    assert_eq!(stream_file_bytes(&h.store, "parent"), b"", "failed bytes are removed before restart");
    let persisted = crossed.expect("fork-reference metadata update must finish while the append is paused");
    drop(parent);
    h.crash();

    let restored = Harness::boot_with_segment_size(dir.path(), None, 1, 256).unwrap();
    let recovered = append_meta_image(&read_meta());
    assert_eq!(stream_file_bytes(&restored.store, "parent"), b"", "failed bytes do not recover from WAL");
    let reopened_parent = restored.store.get("parent").unwrap();
    let reopened = reopened_parent.shared.read().unwrap();
    let reopened_state = serde_json::json!({
        "closed": reopened.closed,
        "closed_by": reopened.closed_by,
        "producers": reopened.producers.iter().map(|(id, entry)| (id, entry.writer)).collect::<std::collections::HashMap<_, _>>(),
        "last_seq_header": reopened.last_seq_header,
        "durable_tail": reopened.durable_tail,
    });
    drop(reopened);
    drop(reopened_parent);
    restored.crash();
    assert_eq!(
        (persisted, recovered, reopened_state),
        (committed.clone(), committed.clone(), committed),
        "the reference write, recovered sidecar, and reopened stream must preserve committed append metadata"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_fork_reservation_does_not_persist_an_inflight_append_snapshot() {
    let _guard = DurabilityGuard::wal();
    fork_reference_does_not_persist_inflight_append(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_parent_refcount_release_does_not_persist_an_inflight_append_snapshot() {
    let _guard = DurabilityGuard::wal();
    fork_reference_does_not_persist_inflight_append(true).await;
}

async fn fork_reference_preserves_metadata_while_append_waits_for_durability(release_child: bool) {
    let dir = temp_dir(if release_child { "fork-release-wal-wait" } else { "fork-reserve-wal-wait" });
    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "parent", OCTET).await;
    let prefix = b"acked-prefix|";
    append_acked(&h.store, "parent", OCTET, prefix).await;
    let parent = h.store.get("parent").unwrap();
    let sidecar = crate::store::meta_path(&parent.file_path);
    let fork_request = || {
        put_req(
            "child",
            OCTET,
            b"",
            &[("stream-forked-from", "parent"), ("stream-fork-offset", &fork_offset(prefix.len() as u64))],
        )
    };
    if release_child {
        assert_eq!(handlers::handle(h.store.clone(), fork_request()).await.status, 201);
    }
    crate::store::write_meta_sync(&parent, true).unwrap();
    let read_meta = || serde_json::from_slice::<crate::store::Meta>(&std::fs::read(&sidecar).unwrap()).unwrap();
    let committed = append_meta_image(&read_meta());
    h.stop_committers();

    let mut request = post_req("parent", OCTET, b"acked-after-wait|");
    request.headers.extend([
        ("producer-id".into(), "waiting-producer".into()),
        ("producer-epoch".into(), "1".into()),
        ("producer-seq".into(), "0".into()),
        ("stream-seq".into(), "0002".into()),
        ("stream-closed".into(), "true".into()),
    ]);
    let append = tokio::spawn(handlers::handle(h.store.clone(), request));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while h.walset.shards()[0].waiter_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!append.is_finished(), "staged append must wait for its WAL barrier");
    assert!(!parent.tail().closed);
    assert_eq!(parent.tail().bytes, prefix.len() as u64);
    let crossed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        if release_child {
            let response = handlers::handle(
                h.store.clone(),
                Req { method: Method::Delete, path: "child".into(), query: None, headers: vec![], body: Bytes::new() },
            )
            .await;
            assert_eq!(response.status, 204);
            while read_meta().ref_count != 0 {
                tokio::task::yield_now().await;
            }
        } else {
            assert_eq!(handlers::handle(h.store.clone(), fork_request()).await.status, 201);
        }
        append_meta_image(&read_meta())
    })
    .await;
    // Finish the real WAL barrier and acknowledged append before crashing.
    // This fixture tests premature metadata, not a synthetic WAL power cut.
    h.committers.push(h.walset.shards()[0].spawn_committer());
    assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(2), append).await.unwrap().unwrap().status, 200);
    let crossed = crossed.expect("fork update must finish before the WAL barrier");
    assert_eq!(crossed, committed, "staged producer/close state stays out of a reference-only write");
    drop(parent);
    h.crash();

    let restored = Harness::boot(dir.path(), None, 1).unwrap();
    assert_eq!(stream_file_bytes(&restored.store, "parent"), b"acked-prefix|acked-after-wait|");
    assert!(restored.store.get("parent").unwrap().tail().closed, "the acknowledged close now recovers");
    if release_child {
        assert!(restored.store.get("child").is_none());
    } else {
        let child = restored.store.get("child").unwrap();
        let mut slices = Vec::new();
        crate::store::resolve_range(&child, 0, prefix.len() as u64, &mut slices);
        let segments = crate::store::into_local_segments(slices).unwrap_or_else(|_| panic!("local inherited prefix"));
        assert_eq!(
            crate::store::materialize_segments(&segments).as_ref(),
            prefix,
            "surviving fork retains its committed prefix"
        );
    }
    restored.crash();
}

#[tokio::test]
async fn e2e_fork_reservation_preserves_metadata_before_durable_append_publication() {
    let _guard = DurabilityGuard::wal();
    fork_reference_preserves_metadata_while_append_waits_for_durability(false).await;
}

#[tokio::test]
async fn e2e_parent_refcount_release_preserves_metadata_before_durable_append_publication() {
    let _guard = DurabilityGuard::wal();
    fork_reference_preserves_metadata_while_append_waits_for_durability(true).await;
}

/// Recovery-hardening: an unparsable sidecar must QUARANTINE the stream (skip
/// + keep the data file + park the sidecar as .meta.corrupt), never delete the
/// data file — a torn sidecar next to real data is a torn write, not garbage.
#[tokio::test]
async fn e2e_corrupt_sidecar_quarantines_instead_of_deleting() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("sidecar-quarantine");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "q", OCTET).await;
    append_acked(&h.store, "q", OCTET, b"precious|").await;
    let data_path = h.store.get("q").unwrap().file_path.clone();
    let meta_path = std::path::PathBuf::from(format!("{}.meta", data_path.display()));
    h.crash();

    // Tear the sidecar (simulates a crash-torn rename target).
    std::fs::write(&meta_path, b"{ this is not json").unwrap();

    for _ in 0..3 {
        assert!(Harness::boot(dir.path(), None, 1).is_err(), "unresolved WAL identity refuses boot");
        assert!(data_path.exists(), "data file must NOT be deleted");
        assert!(meta_path.with_extension("meta.corrupt").exists(), "sidecar parked as .meta.corrupt for repair");
        assert_eq!(std::fs::read(&data_path).unwrap(), b"precious|", "repeated boots preserve data");
    }
}

#[tokio::test]
async fn e2e_delete_drains_append_durability_and_fences_new_appends() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("delete-in-flight-append");
    let wal = WalSet::open(dir.path(), Some(1), 1).unwrap();
    let store = Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap());
    store.wal.set(wal.clone()).ok();
    create_stream(&store, "s", OCTET).await;
    let st = store.get("s").unwrap();
    let append = tokio::spawn(handlers::handle(store.clone(), post_req("s", OCTET, b"pending")));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while st.shared.read().unwrap().tail == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(st.appender.try_lock().is_ok(), "WAL wait happens off the appender mutex");
    let delete = tokio::spawn(handlers::handle(
        store.clone(),
        Req { method: Method::Delete, path: "s".into(), query: None, headers: vec![], body: Bytes::new() },
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !st.is_retiring() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!delete.is_finished(), "DELETE must wait for the admitted append's durability and publication");
    assert!(st.file_path.exists(), "pending append still owns its file");
    let rejected = handlers::handle(store.clone(), post_req("s", OCTET, b"late")).await;
    assert_eq!(rejected.status, 410, "retirement fence rejects new writes");
    let committer = wal.shards()[0].spawn_committer();
    assert_eq!(append.await.unwrap().status, 204);
    assert_eq!(delete.await.unwrap().status, 204);
    committer.stop();
    assert!(!st.file_path.exists());
    assert_eq!(st.tail().bytes, b"pending".len() as u64, "admitted append published before deletion");
    drop(store);
    drop(wal);
    let boot = Harness::boot(dir.path(), None, 1).unwrap();
    assert!(boot.store.get("s").is_none(), "WAL recovery never resurrects acknowledged deletion");
    boot.crash();
}

#[tokio::test]
async fn e2e_failed_delete_partial_unlink_retries_and_stays_gone() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("delete-partial-unlink");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "s", OCTET).await;
    append_acked(&h.store, "s", OCTET, b"owned-by-stream").await;
    let st = h.store.get("s").unwrap();
    let sidecar = crate::store::meta_path(&st.file_path);
    let saved_data = st.file_path.with_extension("saved");
    std::fs::rename(&st.file_path, &saved_data).unwrap();
    std::fs::create_dir(&st.file_path).unwrap();
    let delete_req =
        || Req { method: Method::Delete, path: "s".into(), query: None, headers: vec![], body: Bytes::new() };
    assert_eq!(
        handlers::handle(h.store.clone(), delete_req()).await.status,
        500,
        "real data unlink failure must fail DELETE"
    );
    assert!(!sidecar.exists(), "failure followed a successful sidecar unlink");
    assert!(h.store.streams.contains_key("s"), "partially removed identity retained for retry");
    assert_eq!(handlers::handle(h.store.clone(), post_req("s", OCTET, b"late")).await.status, 410);
    assert_eq!(handlers::handle(h.store.clone(), put_req("s", OCTET, b"", &[])).await.status, 409);
    crate::store::write_meta_sync(&st, true).unwrap();
    assert!(!sidecar.exists(), "delayed sidecar writes are fenced during failed deletion");
    std::fs::remove_dir(&st.file_path).unwrap();
    std::fs::rename(saved_data, &st.file_path).unwrap();
    assert_eq!(
        handlers::handle(h.store.clone(), delete_req()).await.status,
        204,
        "retry tolerates already missing sidecar"
    );
    h.crash();
    for _ in 0..3 {
        let boot = Harness::boot(dir.path(), None, 1).unwrap();
        assert!(boot.store.get("s").is_none(), "successful retry survives every restart");
        boot.crash();
    }
}

#[test]
fn e2e_delete_does_not_starve_close_with_one_blocking_worker() {
    let _guard = DurabilityGuard::wal();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = temp_dir("delete-close-one-worker");
        let wal = WalSet::open(dir.path(), Some(1), 1).unwrap();
        let store = Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap());
        store.wal.set(wal.clone()).ok();
        create_stream(&store, "s", OCTET).await;
        let st = store.get("s").unwrap();
        let mut request = post_req("s", OCTET, b"final-data");
        request.headers.push(("stream-closed".into(), "true".into()));
        let mut close = tokio::spawn(handlers::handle(store.clone(), request));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !st.shared.read().unwrap().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut delete = tokio::spawn(handlers::handle(
            store.clone(),
            Req { method: Method::Delete, path: "s".into(), query: None, headers: vec![], body: Bytes::new() },
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !st.is_retiring() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let committer = wal.shards()[0].spawn_committer();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            assert_eq!((&mut close).await.unwrap().status, 204);
            assert_eq!((&mut delete).await.unwrap().status, 204);
        })
        .await;
        if completed.is_err() {
            // Drop the admitted close guard even on regression, so the runtime
            // itself can shut down instead of hanging its blocking workers.
            close.abort();
            delete.abort();
            let _ = close.await;
            let _ = delete.await;
        }
        committer.stop();
        assert!(completed.is_ok(), "DELETE must leave a blocking worker available for close metadata");
    });
}

#[tokio::test]
async fn e2e_corrupt_fork_parent_preserves_descendant_wal() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("quarantine-fork-descendant");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "parent", OCTET).await;
    append_acked(&h.store, "parent", OCTET, b"parent-prefix").await;
    h.crash();
    // Reboot removes the parent's WAL/checkpoint evidence after persisting its
    // durable tail; only the subsequently created child's WAL remains.
    let h = Harness::boot(dir.path(), None, 1).unwrap();
    let parent = h.store.get("parent").unwrap();
    let parent_meta = crate::store::meta_path(&parent.file_path);
    let offset = crate::store::format_offset(parent.tail().bytes);
    let response = handlers::handle(
        h.store.clone(),
        put_req("child", OCTET, b"", &[("stream-forked-from", "parent"), ("stream-fork-offset", &offset)]),
    )
    .await;
    assert_eq!(response.status, 201);
    append_acked(&h.store, "child", OCTET, b"child-owned-data").await;
    let repaired_meta = std::fs::read(&parent_meta).unwrap();
    let child_file = h.store.get("child").unwrap().file_path.clone();
    h.crash();
    std::fs::write(&parent_meta, b"{torn").unwrap();
    let wal_path = dir.path().join("wal/0/1.wal");
    let retained_wal = std::fs::read(&wal_path).unwrap();
    for _ in 0..3 {
        assert!(Harness::boot(dir.path(), None, 1).is_err(), "unresolved ancestry cannot license descendant WAL reset");
        assert_eq!(std::fs::read(&wal_path).unwrap(), retained_wal);
        assert_eq!(std::fs::read(&child_file).unwrap(), b"child-owned-data");
    }
    std::fs::rename(parent_meta.with_extension("meta.corrupt"), &parent_meta).unwrap();
    std::fs::write(parent_meta, repaired_meta).unwrap();
    let restored = Harness::boot(dir.path(), None, 1).unwrap();
    assert!(restored.store.get("child").is_some());
    assert_eq!(std::fs::read(child_file).unwrap(), b"child-owned-data");
    restored.crash();
}

#[tokio::test]
async fn e2e_retirement_forgets_tails_only_after_success_and_preserves_survivor() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("retirement-tail-pruning");
    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "retired", OCTET).await;
    create_stream(&h.store, "survivor", OCTET).await;
    append_acked(&h.store, "retired", OCTET, b"retired-bytes").await;
    append_acked(&h.store, "survivor", OCTET, b"surviving-bytes").await;
    let retired = h.store.get("retired").unwrap();
    let shard = h.walset.shard_for(retired.id);
    shard.checkpoint().await.unwrap();
    let sidecar = crate::store::meta_path(&retired.file_path);
    let saved = sidecar.with_extension("meta.saved");
    std::fs::rename(&sidecar, &saved).unwrap();
    std::fs::create_dir(&sidecar).unwrap();
    assert!(h.store.delete_durable(&retired).await.is_err(), "real unlink refusal fails retirement");
    assert!(h.store.streams.contains_key("retired"), "failed removal retains the retryable identity");
    assert!(*retired.deletion_watch().borrow(), "failed hard deletion remains honestly terminal-fenced");
    assert!(!retired.wal_retired(), "a pending fence cannot prune durability proof");
    shard.checkpoint().await.unwrap();
    assert_eq!(shard.read_durable_tails().unwrap().get(&retired.id), Some(&(b"retired-bytes".len() as u64)));
    std::fs::remove_dir(&sidecar).unwrap();
    std::fs::rename(saved, &sidecar).unwrap();
    h.store.delete_durable(&retired).await.unwrap();
    assert!(retired.wal_retired());
    assert!(
        shard.read_durable_tails().unwrap().contains_key(&retired.id),
        "disk pruning may lag a successful DELETE until checkpoint"
    );
    h.crash();
    // Crash before pruning: stale deleted-id proof is harmless, and the live
    // stream's strengthened proof survives recovery and the WAL reset.
    let h = Harness::boot(dir.path(), None, 1).unwrap();
    assert!(h.store.get("retired").is_none());
    assert_eq!(stream_file_bytes(&h.store, "survivor"), b"surviving-bytes");
    create_stream(&h.store, "retired-again", OCTET).await;
    append_acked(&h.store, "retired-again", OCTET, b"more").await;
    append_acked(&h.store, "survivor", OCTET, b"-still-live").await;
    let retired = h.store.get("retired-again").unwrap();
    let survivor = h.store.get("survivor").unwrap();
    let shard = h.walset.shard_for(retired.id);
    shard.checkpoint().await.unwrap();
    h.store.delete_durable(&retired).await.unwrap();
    shard.checkpoint().await.unwrap();
    let tails = shard.read_durable_tails().unwrap();
    assert!(!tails.contains_key(&retired.id), "idle checkpoint persists deletion pruning");
    assert_eq!(tails.get(&survivor.id), Some(&(b"surviving-bytes-still-live".len() as u64)));
    h.crash();
    let restored = Harness::boot(dir.path(), None, 1).unwrap();
    assert_eq!(stream_file_bytes(&restored.store, "survivor"), b"surviving-bytes-still-live");
    assert!(restored.store.get("retired-again").is_none());
    restored.crash();
}

/// Recovery-hardening: on an initialized store, a stream lane whose dir is
/// empty and unmarked (= its device mount is missing) must REFUSE to boot —
/// continuing would drop the lane's streams and let the WAL reset destroy
/// their acked records.
#[tokio::test]
async fn e2e_missing_lane_mount_refuses_boot() {
    let _guard = DurabilityGuard::wal();
    crate::store::set_stream_lanes(3);
    let dir = temp_dir("lane-mount-guard");
    {
        let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
        create_stream(&h.store, "lm", OCTET).await;
        append_acked(&h.store, "lm", OCTET, b"x|").await;
        h.crash();
    }
    // Simulate a missing mount: replace lane 1's dir with a fresh empty dir.
    let lane1 = dir.path().join("streams").join("1");
    std::fs::remove_dir_all(&lane1).unwrap();
    std::fs::create_dir_all(&lane1).unwrap();

    let err = Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default())
        .err()
        .expect("boot must refuse when a lane mount is missing");
    assert!(err.to_string().contains("mount"), "error should name the missing mount: {err}");
    crate::store::set_stream_lanes(1);
}

/// `--stream-lanes N`: stream data files hash across `streams/<0..N>/` subdirs
/// (one per device in the intended deployment — the ~1M-stream writeback-wall
/// fix). Crash recovery must find every file in its lane dir, and the
/// checkpoint's per-lane syncfs must preserve durability-before-recycle exactly
/// as the single-lane layout does. Guarded by DurabilityGuard (serialized) since
/// stream-lanes is process-global state; reset to 1 before releasing the guard.
#[tokio::test]
async fn e2e_stream_lanes_recover_acked_records() {
    let _guard = DurabilityGuard::wal();
    crate::store::set_stream_lanes(3);
    const SEG: u64 = 4096;
    let dir = temp_dir("stream-lanes");

    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, SEG).unwrap();
    // Enough streams that the FNV lane hash populates more than one lane.
    let names: Vec<String> = (0..12).map(|i| format!("lane-s{i}")).collect();
    for n in &names {
        create_stream(&h.store, n, OCTET).await;
    }
    let mut expected: std::collections::HashMap<String, Vec<u8>> = Default::default();
    for round in 0..40usize {
        for n in &names {
            let rec = format!("{n}-r{round:03}|").into_bytes();
            append_acked(&h.store, n, OCTET, &rec).await;
            expected.entry(n.clone()).or_default().extend_from_slice(&rec);
        }
    }
    // Checkpoint (per-lane syncfs + recycle), then more acked appends on top.
    h.walset.shards()[0].checkpoint().await.unwrap();
    for n in &names {
        let rec = format!("{n}-post|").into_bytes();
        append_acked(&h.store, n, OCTET, &rec).await;
        expected.entry(n.clone()).or_default().extend_from_slice(&rec);
    }

    h.crash();

    let h2 = Harness::boot_with_segment_size(dir.path(), None, 1, SEG).unwrap();
    // Layout sanity: files actually spread across lane subdirs.
    let lanes_used = (0..3)
        .filter(|l| {
            std::fs::read_dir(dir.path().join("streams").join(l.to_string()))
                .map(|d| d.flatten().next().is_some())
                .unwrap_or(false)
        })
        .count();
    assert!(lanes_used >= 2, "expected streams spread over lanes, got {lanes_used}");
    for n in &names {
        let got = stream_file_bytes(&h2.store, n);
        assert_eq!(&got, expected.get(n).unwrap(), "stream {n} recovers byte-identical across lanes");
    }
    h2.crash();
    // Layout-mismatch guard: reopening this 3-lane dir with a different lane
    // count must be REFUSED (persisted `.lanes` marker) — a silent mismatch
    // would make every existing stream invisible.
    crate::store::set_stream_lanes(2);
    let err = Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default())
        .err()
        .expect("opening a 3-lane layout with --stream-lanes 2 must fail");
    assert!(err.to_string().contains("stream-lanes"), "mismatch error should name the knob: {err}");
    crate::store::set_stream_lanes(1);
}

/// Cardinality-cliff #1: with `--wal-checkpoint-syncfs on`, the checkpoint makes
/// touched per-stream files durable via ONE `syncfs()` barrier instead of the
/// per-stream `fdatasync` loop. This must preserve the durability-before-recycle
/// guarantee: after a checkpoint recycles the WAL, acked records (both those below
/// the checkpoint floor, made durable by `syncfs`, and those appended after) must
/// still recover byte-identically. On Linux this exercises the real `syncfs` path;
/// on other targets the code falls back to the per-stream loop (still correct).
#[tokio::test]
async fn e2e_checkpoint_syncfs_recovers_acked_records() {
    let _guard = DurabilityGuard::wal();
    const SEG: u64 = 4096;
    let dir = temp_dir("syncfs-ckpt");

    let h = Harness::boot_with_segment_size(dir.path(), Some(1), 1, SEG).unwrap();
    create_stream(&h.store, "s", OCTET).await;

    let mut expected = Vec::new();
    // Enough records + small segments to force at least one roll, so the checkpoint
    // actually recycles a fully-below-floor segment (relying on the syncfs'd file).
    for i in 0..400usize {
        let rec = format!("syncfs-{i:04}|").into_bytes();
        append_acked(&h.store, "s", OCTET, &rec).await;
        expected.extend_from_slice(&rec);
    }
    // Force the checkpoint → syncfs barrier → recycle.
    h.walset.shards()[0].checkpoint().await.unwrap();
    // More acked appends after the checkpoint (live segment).
    for i in 400..500usize {
        let rec = format!("syncfs-{i:04}|").into_bytes();
        append_acked(&h.store, "s", OCTET, &rec).await;
        expected.extend_from_slice(&rec);
    }

    h.crash();

    let h2 = Harness::boot_with_segment_size(dir.path(), None, 1, SEG).unwrap();
    let got = stream_file_bytes(&h2.store, "s");
    assert_eq!(
        got, expected,
        "syncfs-checkpoint acked records recover byte-identical (durability-before-recycle held)"
    );
    h2.crash();
}

// ===========================================================================
// (7c) WAL-QUIET stream: torn unacked tail truncated via the sidecar proof
// ===========================================================================

/// A stream with NO durable WAL record and NO checkpoint `tails` entry (created
/// after the last checkpoint; its only append was in-flight at the crash) must
/// still have its torn, never-acked page-cache tail truncated on recovery.
///
/// Regression (sim seed 20230): with the WAL bytes for the in-flight append
/// torn by power loss and its data-file bytes partially persisted, recovery had
/// NO truncation proof for the stream — the sidecar pass trusted
/// `tail = file size` and exposed the torn fragment to readers (the exact C1
/// shape the WAL exists to prevent). The sidecar now persists a `durable_tail`
/// proof (fsynced at create/close, refreshed at checkpoint + recovery), and
/// recovery seeds every stream's frontier from it.
#[tokio::test]
async fn e2e_wal_quiet_stream_torn_unacked_tail_truncated() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("quiet-torn");

    let mut h = Harness::boot(dir.path(), Some(1), 1).unwrap();

    // An earlier checkpointed stream so the shard's tails file is non-empty
    // (proves the fix is not just "empty tails == reconcile everything").
    create_stream(&h.store, "older", OCTET).await;
    append_acked(&h.store, "older", OCTET, b"older-rec|").await;
    h.walset.shards()[0].checkpoint().await.unwrap();

    // The WAL-quiet stream: created AFTER the checkpoint, never acked an append.
    create_stream(&h.store, "fresh", OCTET).await;

    // Its only append is in-flight at the crash: bytes reached the data file's
    // page cache and the WAL staging buffer, but the committer never fsync'd
    // (stop it first), so no ack was ever released.
    h.stop_committers();
    let st = h.store.get("fresh").unwrap();
    let torn: &[u8] = b"TORN-IN-FLIGHT-NEVER-ACKED";
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&st.file_path).unwrap();
        f.write_all(torn).unwrap();
        f.sync_all().unwrap(); // even fully-persisted: still un-acked, must go
    }
    let shard = h.walset.shard_for(st.id).clone();
    shard.reserve_and_stage(crate::wal::codec::RecordKind::Append, st.id, 0, torn).unwrap();
    // Power loss tears the staged (never-fdatasync'd) WAL record: zero it out.
    // Everything at/above this record was never covered by an ack.
    {
        use std::io::{Seek, SeekFrom, Write};
        let seg = crate::wal::segment::seg_path(&dir.path().join("wal").join("0"), 1);
        let len = std::fs::metadata(&seg).unwrap().len();
        let mut f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
        // The quiet stream's record is the LAST staged record; zeroing the whole
        // segment suffix past the durable prefix models its loss. Find the
        // offset by decoding up to the first record for `st.id`.
        let bytes = std::fs::read(&seg).unwrap();
        let mut off = 0usize;
        while let crate::wal::codec::Decoded::Record { stream_id, total, .. } =
            crate::wal::codec::decode_at(&bytes, off)
        {
            if stream_id == st.id {
                break;
            }
            off += total;
        }
        f.seek(SeekFrom::Start(off as u64)).unwrap();
        f.write_all(&vec![0u8; (len as usize) - off]).unwrap();
        f.sync_all().unwrap();
    }

    drop(st);
    let store = h.store;
    let walset = h.walset;
    drop(store);
    drop(walset);

    // Reopen: recovery must truncate the torn tail even though the stream has
    // zero surviving WAL records and no tails entry — the sidecar's durable_tail
    // proof (0, persisted at create) is the seed.
    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    let got = stream_file_bytes(&h2.store, "fresh");
    assert_eq!(got, b"", "torn un-acked tail truncated on a WAL-quiet stream (sidecar durable_tail proof)");
    let st2 = h2.store.get("fresh").unwrap();
    assert_eq!(st2.tail().bytes, 0, "tail reconciled to the durable frontier (0)");
    // The checkpointed stream is untouched.
    assert_eq!(stream_file_bytes(&h2.store, "older"), b"older-rec|");
    h2.crash();
}

// ===========================================================================
// (7d) DELETE ack durability: an acked DELETE survives a crash
// ===========================================================================

/// The 204 for DELETE is a durability promise. Regression (sim seed 20387):
/// `handle_delete` acked while the file + sidecar unlinks ran on a DETACHED
/// blocking task — a crash right after the ack (before the task ran) left both
/// files on disk and the stream RESURRECTED with all its data on reboot. The
/// unlinks (+ parent-dir fsync) are now awaited before the 204.
#[tokio::test]
async fn e2e_acked_delete_is_durable_no_resurrection_after_crash() {
    let _guard = DurabilityGuard::wal();
    let dir = temp_dir("delete-durable");

    let h = Harness::boot(dir.path(), Some(1), 1).unwrap();
    create_stream(&h.store, "victim", OCTET).await;
    append_acked(&h.store, "victim", OCTET, b"doomed-data|").await;
    let file_path = h.store.get("victim").unwrap().file_path.clone();
    let meta = crate::store::meta_path(&file_path);

    let resp = handlers::handle(
        Arc::clone(&h.store),
        Req { method: Method::Delete, path: "victim".into(), query: None, headers: vec![], body: Bytes::new() },
    )
    .await;
    assert_eq!(resp.status, 204, "delete acked");
    // The ack IS the durability point: both on-disk artifacts are already gone
    // when the response returns (not on some detached task's schedule).
    assert!(!file_path.exists(), "data file removed before the DELETE ack");
    assert!(!meta.exists(), "meta sidecar removed before the DELETE ack");

    // Crash + reboot: the stream must not resurrect.
    h.crash();
    let h2 = Harness::boot(dir.path(), None, 1).unwrap();
    assert!(h2.store.get("victim").is_none(), "acked-deleted stream must not resurrect after a crash");
    h2.crash();
}

// ===========================================================================
// (8) MEMORY-MODE sidecar recovery (no WAL)
// ===========================================================================

/// In `memory` mode there is no WAL: appends write to the per-stream file
/// (buffered) and ack on the page-cache write. On restart the server rebuilds
/// state from the per-stream files + `.meta` sidecars (the existing sidecar
/// pass that also runs in `wal` mode). This test confirms that a memory-mode
/// server's data is present after a simulated restart — the Store reopen runs
/// the sidecar pass and the stream is fully accessible.
///
/// This is host-runnable: it exercises the plain file I/O path (no splice, no
/// Linux-only syscalls). The `DurabilityGuard::memory()` acquires the
/// serialization mutex so this test cannot race the durability-mode global
/// with other e2e tests.
#[tokio::test]
async fn memory_mode_data_survives_restart_via_sidecar() {
    let _guard = DurabilityGuard::memory();
    let dir = temp_dir("mem-sidecar");

    // Phase 1: create + append in memory mode (no WAL attached).
    {
        let store = Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap());
        // Do NOT attach a WalSet — memory mode has no WAL.
        create_stream(&store, "m/keep", OCTET).await;
        append_acked(&store, "m/keep", OCTET, b"survive-me").await;
        // `store` drops here without a WAL shutdown — simulates a restart.
    }

    // Phase 2: reopen — the sidecar pass rebuilds from the per-stream file +
    // `.meta`; no WAL to replay.
    let store2 = Arc::new(Store::new_with_tier(dir.path().to_path_buf(), TierConfig::default()).unwrap());
    let st = store2.get("m/keep").expect("stream recovered from sidecar");
    let got = std::fs::read(&st.file_path).unwrap();
    assert_eq!(got, b"survive-me", "memory-mode data survives restart via sidecar pass");
    assert_eq!(st.tail().bytes, b"survive-me".len() as u64, "recovered tail == appended bytes");
}
