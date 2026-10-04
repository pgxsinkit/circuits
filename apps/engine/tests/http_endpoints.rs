//! Integration tests for the engine router: the liveness/readiness split (`/health` vs `/ready`:
//! state machine + exact body + status codes + cache headers), the boot and degradation gates on
//! the shape routes, and route registration.
//! The router is driven in-process via `Service::oneshot`; no Postgres or durable-streams server is
//! needed (the health phase is set at Engine construction).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use circuits_engine::ds::DsClient;
use circuits_engine::engine::Engine;
use circuits_engine::http::router;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tower::ServiceExt; // for `oneshot`
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

#[derive(Clone, Default)]
struct CapturedWarnings(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

impl<S: tracing::Subscriber> Layer<S> for CapturedWarnings {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

async fn captured_request(
    engine: Engine,
    method: &str,
    route: &str,
    body: String,
) -> (axum::response::Response, Vec<BTreeMap<String, String>>) {
    let warnings = CapturedWarnings::default();
    let subscriber = Registry::default().with(warnings.clone());
    let request = Request::builder()
        .method(method)
        .uri(route)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = router(engine).oneshot(request).with_subscriber(subscriber).await.unwrap();
    let events = warnings.0.lock().unwrap().clone();
    (response, events)
}

#[tokio::test]
async fn refused_shape_create_logs_one_warning_with_request_context() {
    let (response, warnings) = captured_request(
        library_engine(),
        "POST",
        "/shapes",
        r#"{"table":"nope","where":{"col":"id","op":"eq","value":"private-literal"}}"#.to_owned(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body_string(response).await, r#"{"error":"unknown table 'public.nope'"}"#);
    assert_eq!(warnings.len(), 1, "every refused create must produce one WARN: {warnings:?}");
    let warning = &warnings[0];
    assert_eq!(warning["operation"], "create_shape");
    assert_eq!(warning["route"], "/shapes");
    assert_eq!(warning["status"], "400");
    assert_eq!(warning["table"], "\"public.nope\"");
    assert_eq!(warning["error"], "\"unknown table 'public.nope'\"");
    assert_eq!(warning["predicate"], "root=leaf nodes=1 depth=1 truncated=false");
    assert!(!format!("{warnings:?}").contains("private-literal"));
}

const CREATE_ROUTES: &[(&str, &str)] = &[("/shapes", "create_shape"), ("/aggregate", "create_aggregate")];

#[tokio::test]
async fn both_create_routes_log_validation_boot_and_degradation_refusals() {
    for &(route, operation) in CREATE_ROUTES {
        for case in ["unknown_table", "subscription", "booting", "degraded"] {
            let engine = if case == "booting" || case == "subscription" {
                Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into())
            } else {
                library_engine()
            };
            if case == "degraded" {
                engine.force_degraded();
            }
            let mut body = serde_json::json!({"table": "nope", "fn": "count"});
            if case == "subscription" {
                body["subscription"] = serde_json::json!("private-subscription\n");
            }
            let (response, warnings) = captured_request(engine, "POST", route, body.to_string()).await;
            let (status, error, retry_after) = match case {
                "unknown_table" => (400, "unknown table 'public.nope'", None),
                "subscription" => (400, "subscription must not contain control characters", None),
                "booting" => {
                    (503, "engine is still booting (the durable shape catalog is not restored yet); retry", Some("1"))
                }
                _ => (503, "degraded: subquery membership effects were lost; restart required", None),
            };
            assert_eq!(response.status().as_u16(), status, "{route} {case}");
            assert_eq!(response.headers().get("retry-after").map(|v| v.to_str().unwrap()), retry_after);
            assert_eq!(body_string(response).await, serde_json::json!({"error": error}).to_string());
            assert_eq!(warnings.len(), 1, "{route} {case}: {warnings:?}");
            let warning = &warnings[0];
            assert_eq!(warning["operation"], operation);
            assert_eq!(warning["route"], route);
            assert_eq!(warning["status"], status.to_string());
            assert_eq!(warning["error"], format!("{error:?}"));
            assert_eq!(warning["table"], "\"public.nope\"");
            assert_eq!(warning["predicate"], "root=all nodes=0 depth=0 truncated=false");
            assert!(!format!("{warnings:?}").contains("private-subscription"));
        }
    }
}

#[tokio::test]
async fn create_extractor_refusals_log_category_without_inferred_context() {
    for &(route, operation) in CREATE_ROUTES {
        for (body, status, category, diagnostic) in [
            (
                r#"{"table":"private-table","fn":"count","where":"#,
                400,
                "json_syntax",
                "Failed to parse the request body as JSON",
            ),
            (r#"{"table":"a.b.c","fn":"count"}"#, 422, "json_data", "contains a '.'"),
            (
                r#"{"table":"private-table","fn":"count","where":{"invalid":"private-literal"}}"#,
                422,
                "json_data",
                "did not match any variant",
            ),
        ] {
            let (response, warnings) = captured_request(library_engine(), "POST", route, body.to_owned()).await;
            assert_eq!(response.status().as_u16(), status);
            assert!(response.headers().get("retry-after").is_none());
            assert_eq!(response.headers()["content-type"], "text/plain; charset=utf-8");
            assert!(body_string(response).await.contains(diagnostic));
            assert_eq!(warnings.len(), 1, "{route}: {warnings:?}");
            let warning = &warnings[0];
            assert_eq!(warning["operation"], operation);
            assert_eq!(warning["route"], route);
            assert_eq!(warning["status"], status.to_string());
            assert_eq!(warning["rejection"], category);
            assert_eq!(warning["context"], "unavailable");
            assert!(!warning.contains_key("table"));
            assert!(!warning.contains_key("predicate"));
            assert!(!format!("{warnings:?}").contains("private-"));
        }
    }
}

#[tokio::test]
async fn refusal_context_is_bounded_and_control_characters_are_escaped() {
    let table = format!("先\n{}", "界".repeat(2000));
    let body = serde_json::json!({"table": table, "fn": "count"});
    for &(route, _) in CREATE_ROUTES {
        let (response, warnings) = captured_request(library_engine(), "POST", route, body.to_string()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // Bounds apply to logs only: the complete diagnostic is still returned to the caller.
        assert_eq!(
            body_string(response).await,
            serde_json::json!({"error": format!("unknown table 'public.{table}'")}).to_string()
        );
        assert_eq!(warnings.len(), 1);
        let warning = &warnings[0];
        let expected_table = format!("public.先\n{}…", "界".repeat(246));
        assert_eq!(warning["table"], format!("{expected_table:?}"));
        assert_eq!(warning["error"].chars().count(), 1027); // 1024 capped chars + debug quotes + escaped newline
        assert!(warning["error"].ends_with("…\""));
        assert!(!warning["table"].contains('\n'));
        assert!(!warning["error"].contains('\n'));
    }
}

#[tokio::test]
async fn unrelated_refusals_and_probes_do_not_gain_create_warnings() {
    for (method, route, body, status) in [
        ("GET", "/health", "", StatusCode::OK),
        ("GET", "/shapes/missing", "", StatusCode::NOT_FOUND),
        ("POST", "/query", r#"{"table":"nope"}"#, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        let (response, warnings) = captured_request(library_engine(), method, route, body.to_owned()).await;
        assert_eq!(response.status(), status);
        assert!(warnings.is_empty(), "{method} {route}: {warnings:?}");
    }
}

fn library_engine() -> Engine {
    Engine::new(DsClient::new("http://127.0.0.1:1"))
}

async fn body_string(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// `GET /ready` is the probe a load balancer gates on: 200 only when the engine is actually able
/// to serve. Library mode has nothing to wait for, so it is ready from construction.
#[tokio::test]
async fn ready_is_200_active_in_library_mode() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

/// Postgres mode before `setup_postgres`: NOT ready (503 `waiting`).
#[tokio::test]
async fn ready_is_503_waiting_before_postgres_is_up() {
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    assert_eq!(engine.readiness_status(), "waiting");
    let res = router(engine).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

/// A degraded engine is live but not ready — and `/health` must NOT go with it, or a kubelet would
/// restart a pod whose problem a restart is the documented fix for only when an operator says so.
#[tokio::test]
async fn degraded_is_not_ready_but_is_still_live() {
    let engine = library_engine();
    engine.force_degraded();
    assert_eq!(engine.readiness_status(), "degraded");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"degraded"}"#);

    let res = router(engine).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "liveness must not follow readiness");
    assert_eq!(body_string(res).await, "ok");
}

/// The first thing a `SIGTERM` does: `/ready` turns 503 `shutting_down` so a load balancer drains
/// the pod BEFORE anything is wound down. Liveness is untouched — the process is still perfectly
/// able to answer what it already accepted.
#[tokio::test]
async fn shutdown_makes_ready_503_before_anything_else_changes() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    assert_eq!(engine.readiness_status(), "shutting_down");
    assert_eq!(engine.health_status(), "active", "shutdown must not rewrite the boot phase");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"shutting_down"}"#);

    let res = router(engine).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn legacy_health_still_ok() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_string(res).await, "ok");
}

/// `DELETE /table/{table}/rows` is registered and validates its input: an unknown table is a 400
/// with an `error` body (not a 404/405, which would mean the route or method is missing).
#[tokio::test]
async fn delete_table_rows_rejects_unknown_table() {
    let res = router(library_engine())
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/table/nope/rows")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keys":[{"id":1}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(body_string(res).await.contains("unknown table"));
}

const INTROSPECTION_ROUTES: &[&str] = &["/trace", "/graph", "/graph/node", "/state", "/state/node"];

/// `CIRCUITS_TRACE=0` (introspection off) removes the visualizer/introspection surface entirely
/// — the routes are never registered, so `/trace` can never gain a subscriber and the hot path
/// keeps its zero-subscriber fast path. Everything else keeps serving.
#[tokio::test]
async fn introspection_disabled_unregisters_viz_routes() {
    use circuits_engine::http::router_with_introspection;
    for route in INTROSPECTION_ROUTES {
        let res = router_with_introspection(library_engine(), false)
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "{route} should be unregistered");
    }
    // The rest of the surface is untouched.
    let res = router_with_introspection(library_engine(), false)
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Default (`router`, introspection on): the same routes respond (200, or 400 for the two that
/// require a query param — anything but 404 proves registration).
#[tokio::test]
async fn introspection_enabled_by_default() {
    for route in INTROSPECTION_ROUTES {
        let res = router(library_engine())
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::NOT_FOUND, "{route} should be registered");
    }
}

/// A degraded engine has lost membership effects it cannot re-derive, so every route that would
/// answer WITH membership must refuse (503 + the typed error body) rather than serve what the
/// engine knows is wrong — while the observability surface stays up, because that is what an
/// operator needs to see the failure and decide to restart.
#[tokio::test]
async fn degraded_refuses_the_membership_routes_and_keeps_observability_up() {
    let engine = library_engine();
    engine.force_degraded();
    let call = async |method: &str, uri: &str, body: &'static str| {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router(engine.clone()).oneshot(req).await.unwrap()
    };

    for (method, uri, body) in [
        ("POST", "/shapes", r#"{"table":"t"}"#),
        ("POST", "/aggregate", r#"{"table":"t","fn":"count"}"#),
        ("POST", "/query", r#"{"table":"t"}"#),
        ("GET", "/shapes/s1", ""),
        ("GET", "/shapes/s1/rows", ""),
        ("GET", "/shapes/s1/log", ""),
    ] {
        let res = call(method, uri, body).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{method} {uri} must refuse");
        assert_eq!(
            body_string(res).await,
            r#"{"error":"degraded: subquery membership effects were lost; restart required"}"#,
            "{method} {uri} body"
        );
    }

    // The barrier endpoint still answers so the held `pendingFlips` and the `flipFailures` count
    // are readable.
    let res = call("GET", "/replication/lsn", "").await;
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
    assert_eq!(v["flipFailures"], 1);

    for uri in ["/metrics", "/memory", "/subqueries", "/graph", "/state", "/health"] {
        assert_eq!(call("GET", uri, "").await.status(), StatusCode::OK, "{uri} must stay up");
    }
}

/// Postgres mode before the boot resolves (ADR-0009): every route that would create, join,
/// reactivate, release or purge a shape — or report on one the catalog restore has not installed
/// yet — answers 503 with `Retry-After`, never the 404/400/500 a not-yet-restored registry would
/// otherwise produce. A create here would spawn a sequencer that reads and checkpoints past the
/// backlog before the restore registers its shapes, or mint an id a catalog record still owns.
/// Library mode has no boot and is unaffected.
#[tokio::test]
async fn a_booting_engine_refuses_shape_mutations_with_retry_after() {
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    let call = async |engine: &Engine, method: &str, uri: &str, body: &'static str| {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router(engine.clone()).oneshot(req).await.unwrap()
    };

    for (method, uri, body) in [
        ("POST", "/shapes", r#"{"table":"items"}"#),
        ("POST", "/aggregate", r#"{"table":"items","fn":"count"}"#),
        ("POST", "/query", r#"{"table":"items"}"#),
        ("GET", "/shapes/s1", ""),
        ("GET", "/shapes/s1/rows", ""),
        ("GET", "/shapes/s1/log", ""),
        ("DELETE", "/shapes/s1", ""),
        ("DELETE", "/shapes/s1?purge=true", ""),
    ] {
        let res = call(&engine, method, uri, body).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{method} {uri} must wait for the boot");
        assert_eq!(res.headers().get("retry-after").map(|v| v.to_str().unwrap()), Some("1"), "{method} {uri}");
        assert!(body_string(res).await.contains("still booting"), "{method} {uri} names the reason");
    }
    assert!(engine.ensure_booted().is_err());

    // `POST /schema` is not a boot-time question at all in Postgres mode: the schema is Postgres's.
    // Refused with 409 whatever the boot phase, and without touching anything.
    let res = call(
        &engine,
        "POST",
        "/schema",
        r#"{"schema":{"tables":{"items":{"columns":{"id":{"type":"int"}},"primaryKey":"id"}}}}"#,
    )
    .await;
    assert_eq!(res.status(), StatusCode::CONFLICT);
    assert!(body_string(res).await.contains("Postgres mode"));

    // Library mode: no boot to wait for, so the same create gets past the gate (and fails on its own
    // terms — no such table — rather than being told to come back).
    let res = call(&library_engine(), "POST", "/shapes", r#"{"table":"items"}"#).await;
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(library_engine().ensure_booted().is_ok());
}
