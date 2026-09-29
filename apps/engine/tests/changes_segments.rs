//! The segmented change log's storage-facing behaviour (ADR-0006), driven against a fake
//! durable-streams server: the **boot walk-forward** over segments a crashed predecessor closed
//! without recording, and the sequencer's refusal to skip ahead when a closed segment has no
//! successor.
//!
//! The walk is the repair path for the one crash window rotation has: `ensure_stream(n+1)` →
//! append the pointer to `n` → close `n` → record `ChangesRotated { n+1 }`. A process that dies
//! after the close and before the record boots with a stale idea of the current segment, and must
//! discover the truth from storage rather than append to a stream that can never accept an append
//! again.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use circuits_engine::changelog::{
    ChangeLogConfig, ChangeLogWriter, ChangesState, LogPosition, SegmentPin, next_segment_for_reader,
    plan_segment_deletion, resolve_current, rotation_envelope, segment_path, should_rotate,
};
use circuits_engine::ds::DsClient;

/// A durable-streams stub whose only interesting state is which streams exist and which are closed.
#[derive(Clone, Default)]
struct FakeDs {
    present: Arc<Mutex<HashSet<String>>>,
    closed: Arc<Mutex<HashSet<String>>>,
    appended: Arc<Mutex<Vec<(String, String)>>>,
}

impl FakeDs {
    /// Storage as a process that crashed mid-rotation would leave it: `changes/0..=n` all exist,
    /// `changes/0..n` are closed.
    fn rotated_through(n: u32) -> FakeDs {
        let ds = FakeDs::default();
        for i in 0..=n {
            ds.present.lock().unwrap().insert(segment_path(i));
            if i < n {
                ds.closed.lock().unwrap().insert(segment_path(i));
            }
        }
        ds
    }
}

async fn ds_handler(State(ds): State<FakeDs>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    match *req.method() {
        Method::PUT => {
            ds.present.lock().unwrap().insert(path);
            StatusCode::OK.into_response()
        }
        Method::HEAD => {
            if !ds.present.lock().unwrap().contains(&path) {
                return StatusCode::NOT_FOUND.into_response();
            }
            if ds.closed.lock().unwrap().contains(&path) {
                return ([("stream-next-offset", "tip"), ("stream-closed", "true")]).into_response();
            }
            ([("stream-next-offset", "tip")]).into_response()
        }
        Method::POST => {
            if !ds.present.lock().unwrap().contains(&path) {
                return StatusCode::NOT_FOUND.into_response();
            }
            if ds.closed.lock().unwrap().contains(&path) {
                return (StatusCode::CONFLICT, [("stream-closed", "true"), ("stream-next-offset", "tip")])
                    .into_response();
            }
            // `Stream-Closed: true` with no body is a close, not an append.
            if req.headers().get("stream-closed").is_some() {
                ds.closed.lock().unwrap().insert(path);
                return StatusCode::NO_CONTENT.into_response();
            }
            ds.appended.lock().unwrap().push((path, "append".to_string()));
            ([("stream-next-offset", "0000000000000000_0000000000000100")]).into_response()
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        Method::DELETE => {
            ds.present.lock().unwrap().remove(&path);
            StatusCode::OK.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn fake_ds() -> (DsClient, FakeDs) {
    fake_ds_with(FakeDs::default()).await
}

async fn fake_ds_with(state: FakeDs) -> (DsClient, FakeDs) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(ds_handler).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (DsClient::new(format!("http://{address}")), state)
}

/// A first boot has nothing in storage: the walk stops at the segment it was asked about (the
/// caller then creates it), and does not invent rotations.
#[tokio::test]
async fn a_first_boot_resolves_to_segment_zero() {
    let (ds, _state) = fake_ds().await;
    assert_eq!(resolve_current(&ds, 0, false).await.unwrap(), 0);
}

/// The opposite reading of the same storage state: the catalog RECORDED that this segment began,
/// so its absence means later rotations whose records were lost plus a sweep that deleted it.
/// Recreating it empty would park the sequencer on a stream nothing writes to — refuse instead.
#[tokio::test]
async fn a_recorded_segment_that_storage_does_not_have_refuses_the_boot() {
    let (ds, _state) = fake_ds().await;
    let err = resolve_current(&ds, 3, true).await.expect_err("a recorded segment cannot just be missing");
    let msg = format!("{err:#}");
    assert!(msg.contains("changes/3"), "{msg}");
    assert!(msg.contains("Refusing to boot"), "{msg}");
}

/// A segment that exists and is open is the current one, whatever came before it.
#[tokio::test]
async fn an_open_segment_is_the_current_one() {
    let (ds, _state) = fake_ds_with(FakeDs::rotated_through(2)).await;
    assert_eq!(resolve_current(&ds, 2, true).await.unwrap(), 2);
}

/// The repair the walk exists for: the catalog's answer is 0, but storage says 0 and 1 are closed.
/// The engine must land on 2 — the only segment it may append to — rather than trust the catalog.
#[tokio::test]
async fn the_walk_steps_over_every_segment_a_crash_left_closed() {
    let (ds, _state) = fake_ds_with(FakeDs::rotated_through(3)).await;
    assert_eq!(resolve_current(&ds, 0, true).await.unwrap(), 3);
    assert_eq!(resolve_current(&ds, 1, true).await.unwrap(), 3, "starting further along lands in the same place");
}

/// A READER must never take the writer's walk. Where the writer wants the one segment it may append
/// to (the first OPEN one), a reader wants the very next one — everything in between is a span of
/// changes it has not read. With 0, 1 and 2 all closed, the reader visits 1, then 2, then 3.
#[tokio::test]
async fn a_reader_steps_one_segment_at_a_time_over_a_run_of_closed_ones() {
    let (ds, _state) = fake_ds_with(FakeDs::rotated_through(3)).await;
    assert_eq!(next_segment_for_reader(&ds, 0).await.unwrap(), 1);
    assert_eq!(next_segment_for_reader(&ds, 1).await.unwrap(), 2);
    assert_eq!(next_segment_for_reader(&ds, 2).await.unwrap(), 3);
    // The writer, from the same place, skips straight to the open one — which is exactly why a
    // reader may not use it.
    assert_eq!(resolve_current(&ds, 0, true).await.unwrap(), 3);
}

/// The reader's step refuses the same impossible state the walk does, rather than hunting forward.
#[tokio::test]
async fn a_reader_refuses_to_step_onto_a_segment_that_does_not_exist() {
    let state = FakeDs::default();
    state.present.lock().unwrap().insert(segment_path(0));
    state.closed.lock().unwrap().insert(segment_path(0));
    let (ds, _state) = fake_ds_with(state).await;
    let err = next_segment_for_reader(&ds, 0).await.expect_err("nothing to step to");
    assert!(format!("{err:#}").contains("Refusing to skip ahead"), "{err:#}");
}

/// A closed segment with no successor cannot have been produced by the engine (rotation creates
/// the successor BEFORE it closes the predecessor), so the walk refuses it loudly instead of
/// skipping to some later segment and silently dropping whatever is in between.
#[tokio::test]
async fn a_closed_segment_with_no_successor_is_refused_not_skipped() {
    let state = FakeDs::default();
    state.present.lock().unwrap().insert(segment_path(0));
    state.closed.lock().unwrap().insert(segment_path(0));
    let (ds, _state) = fake_ds_with(state).await;
    let err = resolve_current(&ds, 0, true).await.expect_err("this storage state is not reachable");
    let msg = format!("{err:#}");
    assert!(msg.contains("changes/1"), "the refusal names the successor it looked for: {msg}");
    assert!(msg.contains("Refusing to skip ahead"), "{msg}");
}

/// The writer never appends to a closed segment: told (by a 409) that the segment it believed was
/// current is closed, it walks forward and lands the commit on the open one.
#[tokio::test]
async fn the_writer_routes_around_a_segment_closed_under_it() {
    let (ds, state) = fake_ds_with(FakeDs::rotated_through(2)).await;
    let rotations = Arc::new(Mutex::new(Vec::new()));
    let seen = rotations.clone();
    let writer = ChangeLogWriter::new(
        ds,
        Arc::new(ChangesState::default()), // believes segment 0 is current
        ChangeLogConfig { segment_bytes: 0, segment_age: std::time::Duration::ZERO, retain: std::time::Duration::ZERO },
        Arc::new(move |segment, _at| seen.lock().unwrap().push(segment)),
    );
    writer.append_commit(&[rotation_envelope(99)]).await.unwrap();
    assert_eq!(writer.state().current(), 2, "the writer discovered the real current segment");
    assert_eq!(
        state.appended.lock().unwrap().iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
        vec![segment_path(2)],
        "and the commit landed there, not on a closed segment"
    );
    assert_eq!(
        *rotations.lock().unwrap(),
        vec![1, 2],
        "the rotations the crashed predecessor never recorded are written now"
    );
}

/// The whole rotation, end to end against storage: the successor exists, the predecessor carries
/// the pointer and is closed, and the writer reports it so the catalog can record it.
#[tokio::test]
async fn rotating_creates_the_successor_points_at_it_and_closes_the_predecessor() {
    let state = FakeDs::default();
    state.present.lock().unwrap().insert(segment_path(0));
    let (ds, state) = fake_ds_with(state).await;
    let rotations = Arc::new(Mutex::new(Vec::new()));
    let seen = rotations.clone();
    let writer = ChangeLogWriter::new(
        ds,
        Arc::new(ChangesState::default()),
        // One byte is enough: the fake reports a 256-byte tail, so the first commit trips it.
        ChangeLogConfig { segment_bytes: 1, segment_age: std::time::Duration::ZERO, retain: std::time::Duration::ZERO },
        Arc::new(move |segment, _at| seen.lock().unwrap().push(segment)),
    );
    writer.append_commit(&[rotation_envelope(99)]).await.unwrap();
    writer.maybe_rotate().await;

    assert_eq!(writer.state().current(), 1);
    assert!(state.present.lock().unwrap().contains(&segment_path(1)), "the successor exists");
    assert!(state.closed.lock().unwrap().contains(&segment_path(0)), "the predecessor is closed");
    assert_eq!(*rotations.lock().unwrap(), vec![1], "the rotation is reported for the catalog");
    // Two appends to segment 0: the commit, then the rotation pointer as its final item.
    assert_eq!(state.appended.lock().unwrap().len(), 2);
    assert!(state.appended.lock().unwrap().iter().all(|(p, _)| p == &segment_path(0)));
}

/// An untouched segment is never rotated: an age-based policy on an idle engine would otherwise
/// mint an empty segment per interval, forever.
#[tokio::test]
async fn an_empty_segment_is_never_rotated() {
    let state = FakeDs::default();
    state.present.lock().unwrap().insert(segment_path(0));
    let (ds, state) = fake_ds_with(state).await;
    let writer = ChangeLogWriter::new(
        ds,
        Arc::new(ChangesState::default()),
        ChangeLogConfig { segment_bytes: 1, segment_age: std::time::Duration::ZERO, retain: std::time::Duration::ZERO },
        Arc::new(|_, _| {}),
    );
    // The policy would fire on any non-zero size...
    assert!(should_rotate(writer.config(), 1, std::time::Duration::ZERO));
    // ...but nothing has been appended, so there is nothing to release.
    writer.maybe_rotate().await;
    writer.force_rotate().await;
    assert_eq!(writer.state().current(), 0);
    assert!(!state.present.lock().unwrap().contains(&segment_path(1)));
}

/// The deletion floor is the checkpoint that is DURABLE, not the one the sequencer holds in memory
/// (ADR-0006). The sequencer publishes a crossing the instant it happens, but the catalog append
/// that makes it survive a restart is asynchronous — so during that window the floor must still be
/// the older, durable position, or a crash would leave a boot resuming inside a deleted segment.
///
/// Inducing the real window in a live engine means stalling the catalog writer, so this asserts the
/// floor computation directly (stated as such): the same inputs, one floor from the in-memory
/// position and one from the durable one, must disagree in exactly the dangerous way.
#[test]
fn the_deletion_floor_must_be_the_durable_checkpoint_not_the_in_memory_one() {
    let segments: std::collections::BTreeMap<u32, u64> = [(2, 200), (3, 300), (4, 400)].into_iter().collect();
    let in_memory = LogPosition::start_of(4); // the sequencer has just crossed into 4
    let durable = LogPosition { segment: 3, offset: "0000000000000000_0000000000000009".into() };

    // What the in-memory position would license: segment 3 deleted...
    let optimistic = plan_segment_deletion(&segments, 4, in_memory.segment, &[], Duration::from_secs(60), 1000);
    assert_eq!(optimistic.delete, vec![2, 3]);

    // ...while a restart at this instant would resume at `durable`, INSIDE segment 3.
    let safe = plan_segment_deletion(&segments, 4, durable.segment, &[], Duration::from_secs(60), 1000);
    assert_eq!(safe.delete, vec![2], "the segment the durable checkpoint still names is kept");
    assert!(durable < in_memory, "the durable checkpoint always trails the in-memory one");
}

/// A pin the sweeper failed to release must not license deleting its segment. The executor
/// re-plans after the evictions with the retain window OFF, so every pin that still exists counts.
#[test]
fn a_skipped_eviction_leaves_its_segment_pinned_on_the_second_pass() {
    let segments: std::collections::BTreeMap<u32, u64> = [(0, 100), (1, 200), (2, 300)].into_iter().collect();
    let stale = vec![SegmentPin { shape_id: "s1".into(), segment: 0, evictable: true }];
    // Pass 1 (real retain window): the pin is stale, so evict it and delete what that unpins.
    let first = plan_segment_deletion(&segments, 2, 2, &stale, Duration::from_secs(1), 100_000);
    assert_eq!(first.evict, vec!["s1".to_string()]);
    assert_eq!(first.delete, vec![0, 1]);
    // The eviction was a no-op (the shape was touched and is reactivating). Pass 2, retain OFF:
    let still_pinned = vec![SegmentPin { shape_id: "s1".into(), segment: 0, evictable: false }];
    let second = plan_segment_deletion(&segments, 2, 2, &still_pinned, Duration::ZERO, 100_000);
    assert!(second.evict.is_empty());
    assert!(second.delete.is_empty(), "nothing is deleted while the pin stands");
}
