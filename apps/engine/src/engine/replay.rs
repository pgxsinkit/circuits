//! Admission and fixed horizons for dormant-shape replay. Queued wakes retain their original
//! history pin but register no pending buffer until a scan permit is available.

use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayConfig {
    pub concurrency: usize,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self { concurrency: 4 }
    }
}

impl ReplayConfig {
    pub fn resolve(env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let concurrency = match env("CIRCUITS_REPLAY_CONCURRENCY").filter(|value| !value.trim().is_empty()) {
            None => Self::default().concurrency,
            Some(value) => value.trim().parse::<usize>().context("CIRCUITS_REPLAY_CONCURRENCY must be an integer")?,
        };
        if concurrency == 0 || concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("CIRCUITS_REPLAY_CONCURRENCY must be between 1 and {}", tokio::sync::Semaphore::MAX_PERMITS);
        }
        Ok(Self { concurrency })
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct ReplayStats {
    pub active: u64,
    pub queued: u64,
    pub peak: u64,
    pub pages: u64,
    pub bytes: u64,
}

pub struct ReplayControls {
    slots: Arc<tokio::sync::Semaphore>,
    active: AtomicU64,
    queued: AtomicU64,
    peak: AtomicU64,
    pages: AtomicU64,
    bytes: AtomicU64,
}

impl ReplayControls {
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(config.concurrency)),
            active: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            pages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> ReplayStats {
        ReplayStats {
            active: self.active.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            peak: self.peak.load(Ordering::Relaxed),
            pages: self.pages.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        attempt: &ReplayAttempt,
        shutdown: &crate::shutdown::ShutdownToken,
    ) -> Result<ReplayPermit> {
        self.queued.fetch_add(1, Ordering::Relaxed);
        let waiting = QueuedReplay(self.clone());
        let permit = attempt.run(shutdown, self.slots.clone().acquire_owned()).await?;
        drop(waiting);
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        Ok(ReplayPermit { controls: self.clone(), _permit: permit })
    }
}

struct QueuedReplay(Arc<ReplayControls>);

impl Drop for QueuedReplay {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) struct ReplayPermit {
    controls: Arc<ReplayControls>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ReplayPermit {
    fn record_page(&self, bytes: u64) {
        self.controls.pages.fetch_add(1, Ordering::Relaxed);
        self.controls.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl Drop for ReplayPermit {
    fn drop(&mut self) {
        self.controls.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Identity belongs to the detached owner, never to an individual HTTP request. Purge cancels
/// this token and queued sequencer commands must check it before installing state.
#[derive(Clone)]
pub struct ReplayAttempt {
    id: u64,
    cancelled: Arc<tokio::sync::watch::Sender<bool>>,
}

impl ReplayAttempt {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self { id: NEXT_ID.fetch_add(1, Ordering::Relaxed), cancelled: Arc::new(tokio::sync::watch::channel(false).0) }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.send_replace(true);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    pub(crate) async fn cancelled(&self) {
        let mut rx = self.cancelled.subscribe();
        let _ = rx.wait_for(|cancelled| *cancelled).await;
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            bail!("shape replay attempt {} was cancelled", self.id);
        }
        Ok(())
    }

    pub(crate) async fn run<T, E>(
        &self,
        shutdown: &crate::shutdown::ShutdownToken,
        work: impl Future<Output = std::result::Result<T, E>>,
    ) -> Result<T>
    where
        E: Into<anyhow::Error>,
    {
        self.check()?;
        tokio::select! {
            biased;
            _ = self.cancelled() => bail!("shape replay attempt {} was cancelled", self.id),
            _ = shutdown.wait() => bail!("shape replay stopped for process shutdown"),
            result = work => result.map_err(Into::into),
        }
    }
}

/// Replay only bytes consumed before pending registration. Incomplete transaction prefixes are
/// safe here: plain shapes emit absolute rows, while the live sequencer still holds the complete
/// transaction and delivers it to pending buffering when its end marker arrives.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_changes_until(
    ds: &DsClient,
    ts: &TableSchema,
    table: &TableRef,
    pred: &CompiledPredicate,
    out_cols: Option<&Arc<Vec<usize>>>,
    gate: &crate::pg::SnapshotGate,
    source_floor: Option<crate::ds::SourcePosition>,
    stream_path: &str,
    from: &LogPosition,
    until: &LogPosition,
    library_mode: bool,
    shutdown: &crate::shutdown::ShutdownToken,
    attempt: &ReplayAttempt,
    permit: &ReplayPermit,
) -> Result<u64> {
    let mut pos = from.clone();
    let end_bytes = crate::ds::replay_offset_bytes(&until.offset)?;
    if pos.segment > until.segment
        || (pos.segment == until.segment && crate::ds::replay_offset_bytes(&pos.offset)? > end_bytes)
    {
        bail!("shape replay start {from} is past admission horizon {until}");
    }
    let mut rotate_to = None;
    let mut emitted = 0;
    let mut stale_schema_reported = HashSet::new();
    let mut source_highwater = source_floor;
    loop {
        attempt.check()?;
        if pos.segment == until.segment && crate::ds::replay_offset_bytes(&pos.offset)? == end_bytes {
            break;
        }
        let page_end = (pos.segment == until.segment).then_some(until.offset.as_str());
        let read = attempt
            .run(shutdown, ds.read_until(&pos.path(), &pos.offset, page_end))
            .await
            .with_context(|| format!("replaying change log from {pos} through {until}"))?;
        permit.record_page(read.bytes);
        let rr = read.page;
        if let Some(next) = crate::changelog::rotation_target_in(&rr.envelopes) {
            rotate_to = Some(next);
        }
        let delivered = rr.envelopes.len();
        let mut outs = Vec::new();
        for env in &rr.envelopes {
            if env.type_ != table.as_str() {
                continue;
            }
            if !library_mode {
                let origin = crate::ds::SourcePosition::from_envelope(env)?;
                if source_highwater.is_some_and(|floor| origin <= floor) {
                    continue;
                }
                source_highwater = Some(origin);
            }
            if !schema_describes(ts, env) {
                metrics().sequencer_stale_schema_skipped.fetch_add(1, Ordering::Relaxed);
                if let Some(stamp) = &env.headers.schema
                    && stale_schema_reported.insert(stamp.clone())
                {
                    tracing::warn!("shape replay on '{table}': skipping changes decoded under retired schema {stamp}");
                }
                continue;
            }
            let (delta, txid, lsn) = apply_envelope(ts, env).with_context(|| {
                format!(
                    "replaying change log at {pos} for '{table}': current-schema change '{}' cannot be applied",
                    env.key
                )
            })?;
            let absolute = library_mode && needs_absolute_emission(env);
            if delta.is_empty() && !absolute {
                continue;
            }
            let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
            let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
            if gate.should_skip(lsn_u64, xid) {
                continue;
            }
            let group_start = outs.len();
            if absolute {
                let held = delta.iter().find(|Tup2(_, w)| *w > 0).map(|Tup2(r, _)| r).filter(|r| pred.matches(r));
                if let Some(env) = absolute_envelope(ts, &env.key, held, txid, lsn, out_cols.map(|c| c.as_slice())) {
                    outs.push(env);
                }
            } else {
                let matched = eval_standalone(pred, &delta);
                outs.extend(translate_output(ts, matched, txid, lsn, out_cols.map(|c| c.as_slice())));
            }
            if !library_mode {
                super::output::stamp_plain_effect(env, &mut outs[group_start..])?;
            }
        }
        if !outs.is_empty() {
            emitted += outs.len() as u64;
            let append = async {
                if library_mode {
                    ds.append_retrying(stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown).await
                } else {
                    ds.append_plain_retrying(stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown).await
                }
            };
            attempt.run(shutdown, append).await.context("append replay to retained stream")?;
        }
        let advanced = rr.next_offset.as_deref().is_some_and(|next| next != pos.offset);
        if let Some(next) = rr.next_offset {
            pos.offset = next;
        }
        if pos.segment == until.segment && crate::ds::replay_offset_bytes(&pos.offset)? == end_bytes {
            break;
        }
        if rr.closed && (delivered == 0 || !advanced) {
            let next = match rotate_to.take() {
                Some(next) => next,
                None => attempt.run(shutdown, crate::changelog::next_segment_for_reader(ds, pos.segment)).await?,
            };
            if pos.segment.checked_add(1) != Some(next) || next > until.segment {
                bail!("replay rotation from {pos} would skip the admission horizon {until}");
            }
            pos = LogPosition::start_of(next);
        } else if !advanced || (rr.up_to_date && !rr.closed) {
            bail!("change log stopped at {pos} before admission horizon {until}");
        }
    }
    // A zero-byte replay still relies on retained history. A closed segment promises that its
    // exact successor exists, even if the close occurred after admission. Validate that promise
    // without scanning beyond the fixed horizon.
    let endpoint_path = until.path();
    let head = attempt
        .run(shutdown, ds.head(&endpoint_path))
        .await?
        .ok_or_else(|| anyhow::Error::new(crate::ds::StreamGone { path: endpoint_path, status: 404 }))?;
    let tail = crate::ds::replay_offset_bytes(
        head.next_offset.as_deref().context("replay endpoint HEAD omitted its next offset")?,
    )?;
    if tail < end_bytes {
        bail!("retained change-log tail is before replay admission horizon {until}");
    }
    if head.closed {
        until.segment.checked_add(1).context("closed replay endpoint has no possible successor")?;
        attempt.run(shutdown, crate::changelog::next_segment_for_reader(ds, until.segment)).await?;
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct ReplayStore {
        segments: Arc<std::sync::Mutex<HashMap<u32, (String, bool)>>>,
        appended: Arc<std::sync::Mutex<Vec<Envelope>>>,
        reads: Arc<AtomicU64>,
        sequence: Arc<std::sync::Mutex<Option<String>>>,
    }

    struct ReplayServer {
        store: ReplayStore,
        ds: DsClient,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for ReplayServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl ReplayStore {
        fn append(&self, segment: u32, env: Envelope, closed: bool) -> LogPosition {
            let mut segments = self.segments.lock().unwrap();
            let entry = segments.entry(segment).or_default();
            entry.0.push_str(&serde_json::to_string(&env).unwrap());
            entry.0.push(',');
            entry.1 = closed;
            LogPosition { segment, offset: format!("0000000000000000_{:016}", entry.0.len()) }
        }
    }

    async fn replay_server() -> ReplayServer {
        use axum::extract::{Request, State};
        use axum::http::{Method, StatusCode};
        use axum::response::{IntoResponse, Response};

        async fn handler(State(store): State<ReplayStore>, req: Request) -> Response {
            let path = req.uri().path().trim_start_matches('/').to_string();
            if *req.method() == Method::HEAD && path.starts_with("shape/") {
                let mut res = StatusCode::OK.into_response();
                res.headers_mut().insert("stream-seq-guard", "durable-v1".parse().unwrap());
                if let Some(sequence) = store.sequence.lock().unwrap().as_ref() {
                    res.headers_mut().insert("stream-seq", sequence.parse().unwrap());
                }
                return res;
            }
            if *req.method() == Method::HEAD && path.starts_with("changes/") {
                let segment = path.strip_prefix("changes/").unwrap().parse::<u32>().unwrap();
                let segments = store.segments.lock().unwrap();
                return match segments.get(&segment) {
                    Some((wire, closed)) => (
                        [
                            ("stream-next-offset", format!("0000000000000000_{:016}", wire.len())),
                            ("stream-closed", closed.to_string()),
                        ],
                        "",
                    )
                        .into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                };
            }
            if *req.method() == Method::GET {
                store.reads.fetch_add(1, Ordering::Relaxed);
                let segment = path.strip_prefix("changes/").unwrap().parse::<u32>().unwrap();
                let at = req.uri().query().unwrap().split('&').find_map(|pair| pair.strip_prefix("offset=")).unwrap();
                let start = crate::ds::replay_offset_bytes(at).unwrap() as usize;
                let segments = store.segments.lock().unwrap();
                let (wire, closed) = &segments[&segment];
                let body =
                    if start == wire.len() { "[]".to_string() } else { format!("[{}]", &wire[start..wire.len() - 1]) };
                return (
                    [
                        ("stream-next-offset", format!("0000000000000000_{:016}", wire.len())),
                        ("stream-up-to-date", "true".to_string()),
                        ("stream-closed", closed.to_string()),
                    ],
                    body,
                )
                    .into_response();
            }
            if *req.method() == Method::POST {
                let sequence = req.headers().get("stream-seq").map(|h| h.to_str().unwrap().to_string());
                if let Some(desired) = &sequence {
                    let expected: Option<String> =
                        serde_json::from_str(req.headers()["stream-expected-seq"].to_str().unwrap()).unwrap();
                    assert_eq!(expected, *store.sequence.lock().unwrap());
                    *store.sequence.lock().unwrap() = Some(desired.clone());
                }
                let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
                store.appended.lock().unwrap().extend(serde_json::from_slice::<Vec<Envelope>>(&body).unwrap());
                if let Some(sequence) = sequence {
                    return ([("stream-seq-guard", "durable-v1".to_string()), ("stream-seq", sequence)], "")
                        .into_response();
                }
            }
            StatusCode::OK.into_response()
        }
        let store = ReplayStore {
            segments: Arc::new(std::sync::Mutex::new(HashMap::new())),
            appended: Arc::new(std::sync::Mutex::new(Vec::new())),
            reads: Arc::new(AtomicU64::new(0)),
            sequence: Arc::new(std::sync::Mutex::new(None)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ds = DsClient::new(format!("http://{}", listener.local_addr().unwrap()));
        let router = axum::Router::new().fallback(axum::routing::any(handler)).with_state(store.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        ReplayServer { store, ds, task }
    }

    fn items() -> TableSchema {
        let def: crate::schema::TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": { "type": "int" }, "n": { "type": "int" } }, "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def(&TableRef::parse("items").unwrap(), &def).unwrap()
    }

    fn change(n: i32, last: bool) -> Envelope {
        Envelope {
            type_: "public.items".to_string(),
            key: "1".to_string(),
            value: Some(serde_json::json!({ "id": 1, "n": n })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".to_string(),
                txid: Some("7".to_string()),
                offset: None,
                lsn: Some("0/10".to_string()),
                seq: Some(n as u64),
                last: Some(last),
                schema: None,
            },
        }
    }

    async fn run_replay(server: &ReplayServer, from: &LogPosition, until: &LogPosition) -> u64 {
        let ts = items();
        let pred = CompiledPredicate::compile_opt(None, &ts).unwrap();
        let controls = Arc::new(ReplayControls::new(ReplayConfig { concurrency: 1 }));
        let attempt = ReplayAttempt::new();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let permit = controls.acquire(&attempt, &shutdown).await.unwrap();
        replay_changes_until(
            &server.ds,
            &ts,
            &ts.table,
            &pred,
            None,
            &crate::pg::SnapshotGate::passthrough(),
            None,
            "shape/s1",
            from,
            until,
            true,
            &shutdown,
            &attempt,
            &permit,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn pg_dormant_replay_deduplicates_source_effects_across_raw_pages() {
        let server = replay_server().await;
        server.store.append(0, change(2, true), true);
        server.store.append(1, change(3, true), false);
        server.store.append(1, change(2, true), false);
        server.store.append(1, change(3, true), false);
        let until = server.store.append(1, change(4, true), false);
        let ts = items();
        let pred = CompiledPredicate::compile_opt(None, &ts).unwrap();
        let controls = Arc::new(ReplayControls::new(ReplayConfig { concurrency: 1 }));
        let attempt = ReplayAttempt::new();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let permit = controls.acquire(&attempt, &shutdown).await.unwrap();
        let result = replay_changes_until(
            &server.ds,
            &ts,
            &ts.table,
            &pred,
            None,
            &crate::pg::SnapshotGate::passthrough(),
            None,
            "shape/s1",
            &LogPosition::start(),
            &until,
            false,
            &shutdown,
            &attempt,
            &permit,
        )
        .await;
        assert_eq!(result.unwrap(), 3);
        assert!(server.store.reads.load(Ordering::Relaxed) >= 3, "the duplicate source prefix crossed real read pages");
        let wire = server.store.appended.lock().unwrap();
        assert_eq!(
            wire.iter().map(|env| env.value.as_ref().unwrap()["n"].as_i64().unwrap()).collect::<Vec<_>>(),
            [2, 3, 4]
        );
        assert_eq!(wire.iter().map(|env| env.headers.seq.unwrap()).collect::<Vec<_>>(), [2, 3, 4]);
    }

    #[tokio::test]
    async fn fixed_replay_horizon_excludes_later_same_key_writes_across_repeated_wakes() {
        let server = replay_server().await;
        let first = server.store.append(0, change(1, true), false);
        let mut tail = first.clone();
        for n in 2..=20 {
            tail = server.store.append(0, change(n, true), false);
        }
        assert_eq!(run_replay(&server, &LogPosition::start(), &first).await, 1);
        assert_eq!(server.store.appended.lock().unwrap().last().unwrap().value.as_ref().unwrap()["n"], 1);
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 1);
        server.store.append(0, change(21, true), false);
        assert_eq!(run_replay(&server, &first, &tail).await, 19);
        assert_eq!(server.store.appended.lock().unwrap().last().unwrap().value.as_ref().unwrap()["n"], 20);
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 2);
        assert_eq!(run_replay(&server, &tail, &tail).await, 0);
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn fixed_replay_crosses_closed_segments_and_accepts_a_held_transaction_prefix() {
        let server = replay_server().await;
        server.store.append(0, change(1, true), false);
        server.store.append(0, crate::changelog::rotation_envelope(1), true);
        let until = server.store.append(1, change(2, false), false);
        server.store.append(1, change(3, true), false);
        assert_eq!(run_replay(&server, &LogPosition::start(), &until).await, 2);
        let appended = server.store.appended.lock().unwrap();
        assert_eq!(appended.len(), 2);
        assert_eq!(appended.last().unwrap().value.as_ref().unwrap()["n"], 2);
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn zero_byte_replay_validates_missing_current_and_closed_tail_successor() {
        let server = replay_server().await;
        let until = server.store.append(0, change(1, true), true);
        let ts = items();
        let pred = CompiledPredicate::compile_opt(None, &ts).unwrap();
        let controls = Arc::new(ReplayControls::new(ReplayConfig { concurrency: 1 }));
        let attempt = ReplayAttempt::new();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let permit = controls.acquire(&attempt, &shutdown).await.unwrap();
        let gate = crate::pg::SnapshotGate::passthrough();
        let run = || {
            replay_changes_until(
                &server.ds, &ts, &ts.table, &pred, None, &gate, None, "shape/s1", &until, &until, true, &shutdown,
                &attempt, &permit,
            )
        };
        let error = run().await.expect_err("closed-tail replay requires the existing successor");
        assert_eq!(crate::ds::stream_gone(&error).unwrap().path, "changes/1");
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 0);
        server.store.segments.lock().unwrap().remove(&0);
        let error = run().await.expect_err("zero-byte replay still requires its own retained segment");
        assert_eq!(crate::ds::stream_gone(&error).unwrap().path, "changes/0");
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn closed_replay_endpoint_requires_successor_even_after_admission_tail_grows() {
        let server = replay_server().await;
        let until = server.store.append(0, change(1, true), false);
        server.store.append(0, crate::changelog::rotation_envelope(1), true);
        let ts = items();
        let pred = CompiledPredicate::compile_opt(None, &ts).unwrap();
        let controls = Arc::new(ReplayControls::new(ReplayConfig { concurrency: 1 }));
        let attempt = ReplayAttempt::new();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let permit = controls.acquire(&attempt, &shutdown).await.unwrap();
        let error = replay_changes_until(
            &server.ds,
            &ts,
            &ts.table,
            &pred,
            None,
            &crate::pg::SnapshotGate::passthrough(),
            None,
            "shape/s1",
            &LogPosition::start(),
            &until,
            true,
            &shutdown,
            &attempt,
            &permit,
        )
        .await
        .expect_err("closure promises an existing exact successor");
        assert_eq!(crate::ds::stream_gone(&error).unwrap().path, "changes/1");
        assert_eq!(server.store.reads.load(Ordering::Relaxed), 1);
        assert_eq!(server.store.appended.lock().unwrap().len(), 1, "no scan past the captured horizon");
    }

    #[test]
    fn replay_config_requires_nonzero_bounded_concurrency() {
        assert_eq!(ReplayConfig::resolve(|_| None).unwrap(), ReplayConfig::default());
        for bad in ["0", "-1", "bad", "18446744073709551615"] {
            assert!(ReplayConfig::resolve(|_| Some(bad.to_string())).is_err());
        }
        assert_eq!(ReplayConfig::resolve(|_| Some("1".to_string())).unwrap().concurrency, 1);
        assert_eq!(ReplayConfig::resolve(|_| Some(" 1 ".to_string())).unwrap().concurrency, 1);
        assert_eq!(ReplayConfig::resolve(|_| Some(" ".to_string())).unwrap(), ReplayConfig::default());
    }

    #[tokio::test]
    async fn replay_admission_is_bounded_and_cancelled_waiters_release_accounting() {
        let controls = Arc::new(ReplayControls::new(ReplayConfig { concurrency: 1 }));
        let shutdown = crate::shutdown::ShutdownToken::new();
        let first = ReplayAttempt::new();
        let permit = controls.acquire(&first, &shutdown).await.unwrap();
        let second = ReplayAttempt::new();
        let waiting = tokio::spawn({
            let controls = controls.clone();
            let second = second.clone();
            let shutdown = shutdown.clone();
            async move { controls.acquire(&second, &shutdown).await }
        });
        while controls.stats().queued != 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(controls.stats().active, 1);
        second.cancel();
        assert!(waiting.await.unwrap().is_err());
        assert_eq!(controls.stats().queued, 0);
        permit.record_page(123);
        assert_eq!(controls.stats().pages, 1);
        assert_eq!(controls.stats().bytes, 123);
        drop(permit);
        assert_eq!(controls.stats().active, 0);
        assert_eq!(controls.stats().peak, 1);
        let third = ReplayAttempt::new();
        drop(controls.acquire(&third, &shutdown).await.unwrap());
        assert_eq!(controls.stats().active, 0);
    }

    #[tokio::test]
    async fn replay_cancel_and_shutdown_interrupt_inflight_work() {
        let attempt = ReplayAttempt::new();
        let shutdown = crate::shutdown::ShutdownToken::new();
        let task = tokio::spawn({
            let attempt = attempt.clone();
            let shutdown = shutdown.clone();
            async move { attempt.run(&shutdown, std::future::pending::<Result<()>>()).await }
        });
        attempt.cancel();
        assert!(task.await.unwrap().is_err());
        assert!(!shutdown.is_shutting_down());
        let attempt = ReplayAttempt::new();
        shutdown.begin();
        assert!(attempt.run(&shutdown, std::future::pending::<Result<()>>()).await.is_err());
    }
}
