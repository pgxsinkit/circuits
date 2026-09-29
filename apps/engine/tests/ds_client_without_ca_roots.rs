//! The log-server client builds, and talks plain HTTP, on a host with no CA certificates.
//!
//! The engine speaks plain HTTP to the log server, but its reqwest carries rustls (dbsp's storage
//! crate enables it), and since reqwest 0.13 rustls loads the system's CA roots when a client is
//! built and refuses to build with none: `reqwest::Client::new()` panicked, and the engine image is
//! Debian slim without `ca-certificates`, so that was a crash at boot. rustls finds the system roots
//! through `SSL_CERT_FILE` / `SSL_CERT_DIR` when they are set, so pointing both at nothing is a host
//! without a CA bundle. This file holds one test, because it changes the process environment.

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use circuits_engine::ds::DsClient;

#[tokio::test]
async fn the_log_server_client_works_without_system_ca_roots() {
    let nowhere = std::env::temp_dir().join(format!("circuits-no-ca-roots-{}", std::process::id()));
    assert!(!nowhere.exists(), "{} must not exist", nowhere.display());
    // SAFETY: this test binary has this one test, and nothing in it has started a thread that
    // reads the environment yet.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", nowhere.join("roots.pem"));
        std::env::set_var("SSL_CERT_DIR", &nowhere);
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(|| async { StatusCode::OK.into_response() });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Built with `reqwest::Client::new()`, this panicked: "No CA certificates were loaded from the
    // system".
    let client = DsClient::new(format!("http://{address}"));
    client.ensure_stream("shape/s1").await.expect("a plain-HTTP PUT needs no CA root");
}
