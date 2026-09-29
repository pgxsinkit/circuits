//! A shape that goes **dormant while a transaction is being held** must park at the PINNED
//! change-log position, not at the sequencer's read cursor (ADR-0003).
//!
//! The read cursor can already be past a chunk of a transaction the sequencer has not applied yet.
//! Parking there would make the shape resume *after* rows it never saw, and its reactivation replay
//! would silently start beyond them — the shape would be permanently missing that transaction.
//! Parking at the pin re-reads the completed run instead, which is idempotent: the replay appends
//! absolute per-pk rows.
//!
//! Its own test binary because it sets retention env vars, which are process-global and read once at
//! `Engine::new`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use circuits_engine::ds::DsClient;
use circuits_engine::engine::Engine;
use circuits_engine::schema::Schema;
use circuits_engine::table_ref::TableRef;

type Page = (String, String);

#[derive(Clone, Default)]
struct FakeLog {
    pages: Arc<Mutex<HashMap<String, Page>>>,
    catalog: Arc<Mutex<Vec<serde_json::Value>>>,
}

fn env_json(txid: u32, lsn: &str, key: &str, seq: u32, last: bool) -> String {
    let marker = if last { r#","last":true"# } else { "" };
    format!(
        r#"{{"type":"public.t","key":"{key}","value":{{"id":"{key}"}},"headers":{{"operation":"insert","txid":"{txid}","lsn":"{lsn}","seq":{seq}{marker}}}}}"#
    )
}

async fn ds_handler(State(log): State<FakeLog>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    match *req.method() {
        Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
        Method::HEAD => ([("stream-next-offset", "tip")]).into_response(),
        Method::POST => {
            if req.headers().get("stream-closed").is_some() || path.starts_with("changes") {
                return StatusCode::OK.into_response();
            }
            let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            if path.starts_with("meta")
                && let Ok(events) = serde_json::from_slice::<Vec<serde_json::Value>>(&body)
            {
                log.catalog.lock().unwrap().extend(events);
            }
            StatusCode::OK.into_response()
        }
        Method::GET if path.starts_with("changes") => {
            let at = query.split('&').find_map(|kv| kv.strip_prefix("offset=")).unwrap_or("-1").to_string();
            let page = log.pages.lock().unwrap().get(&at).cloned();
            match page {
                Some((next, body)) => ([("stream-next-offset", next.as_str())], body).into_response(),
                None => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    (StatusCode::NO_CONTENT, [("stream-next-offset", at.as_str())]).into_response()
                }
            }
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[tokio::test]
async fn a_shape_parked_while_a_transaction_is_held_resumes_at_the_pinned_position() {
    // Second-scale retention so the idle sweep actually fires. Set before `Engine::new`, which is
    // where `RetentionConfig::from_env` reads them; this binary runs exactly one test, so nothing
    // else observes the mutation.
    // SAFETY: single-threaded point in a single-test binary, before any engine or sweeper exists.
    unsafe {
        std::env::set_var("CIRCUITS_SHAPE_IDLE_SECS", "1");
        std::env::set_var("CIRCUITS_RETENTION_SWEEP_SECS", "1");
        std::env::set_var("CIRCUITS_SHAPE_DORMANT_TTL_SECS", "3600");
    }

    let state = FakeLog::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(ds_handler).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // One complete transaction, then the first chunk of a large one — the read cursor moves to
    // "01" while the sequencer is still holding B, so the pin stays at "-1".
    state.pages.lock().unwrap().insert(
        "-1".to_string(),
        (
            "01".to_string(),
            format!("[{},{}]", env_json(500, "0/50", "a1", 0, true), env_json(501, "0/51", "b1", 0, false)),
        ),
    );

    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    let t = TableRef::parse("t").unwrap();
    let shape = engine.create_shape(&t, None, None, false, false).await.unwrap();

    // The sequencer has applied A and is holding B: publication is pinned behind the read cursor.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if engine.table_offset(&t).await.is_some_and(|p| p.offset == "-1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "pinned while B is held");

    // Let it go idle so the retention sweep parks it.
    engine.release_shape(&shape.id).await;
    let dormant = {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let found = state
                .catalog
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.get("t").and_then(|t| t.as_str()) == Some("dormant"))
                .cloned();
            if let Some(ev) = found {
                break ev;
            }
            assert!(std::time::Instant::now() < deadline, "timed out waiting for the shape to go dormant");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    assert_eq!(dormant["id"].as_str(), Some(shape.id.as_str()));
    assert_eq!(
        dormant["resume"]["offset"].as_str(),
        Some("-1"),
        "the shape parked at the PINNED position, not the read cursor (which is already at '01', \
         past a chunk of a transaction it has never seen): {dormant}"
    );
}
