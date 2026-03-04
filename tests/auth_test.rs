//! Tests for auth enforcement.
//!
//! When auth is enabled, unauthenticated requests should be rejected with a
//! non-200 status (401/403). Auth endpoints (/v3/auth/*) must remain
//! accessible so clients can authenticate.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;

use barkeeper::api::gateway;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::hub::WatchHub;

fn b64(s: &str) -> String {
    B64.encode(s.as_bytes())
}

/// Spin up a minimal barkeeper instance with auth support and return the HTTP
/// gateway address.
async fn start_test_instance() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();

    let config = RaftConfig {
        node_id: 1,
        data_dir: dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let store = Arc::new(
        KvStore::open(dir.path().join("kv.redb")).expect("open KvStore"),
    );

    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(Arc::clone(&store), apply_rx).await;

    let raft_handle = spawn_raft_node(config, apply_tx).await;

    let lease_manager = Arc::new(LeaseManager::new());
    let cluster_runtime = rebar_core::runtime::Runtime::new(1);
    let cluster_manager = spawn_cluster_actor(&cluster_runtime, 1).await;
    cluster_manager
        .add_initial_member(1, "test-node".to_string(), vec![], vec![])
        .await;

    let watch_hub = Arc::new(WatchHub::with_store(Arc::clone(&store)));
    let auth_runtime = rebar_core::runtime::Runtime::new(1);
    let auth_manager = spawn_auth_actor(&auth_runtime).await;

    let app = gateway::create_router(
        raft_handle.clone(),
        Arc::clone(&store),
        Arc::clone(&watch_hub),
        Arc::clone(&lease_manager),
        cluster_manager,
        1,
        1,
        Arc::clone(&raft_handle.current_term),
        auth_manager,
        Arc::new(std::sync::Mutex::new(vec![])),
    );

    let http_port = portpicker::pick_unused_port().expect("no free port");
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .expect("bind HTTP");
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    // Wait for single-node Raft election.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    (bound_addr, dir)
}

/// When auth is enabled, unauthenticated requests should be rejected.
#[tokio::test]
async fn test_auth_enabled_rejects_unauthenticated() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Create root user.
    let resp = client
        .post(format!("http://{}/v3/auth/user/add", addr))
        .body(r#"{"name":"root","password":"rootpass"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "user/add should succeed before auth is enabled"
    );

    // Create root role.
    let resp = client
        .post(format!("http://{}/v3/auth/role/add", addr))
        .body(r#"{"name":"root"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "role/add should succeed");

    // Grant root role to root user.
    let resp = client
        .post(format!("http://{}/v3/auth/user/grant", addr))
        .body(r#"{"user":"root","role":"root"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "user/grant should succeed");

    // Enable auth.
    let resp = client
        .post(format!("http://{}/v3/auth/enable", addr))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "auth/enable should succeed");

    // Unauthenticated put should fail.
    let resp = client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(
            r#"{{"key":"{}","value":"{}"}}"#,
            b64("secret"),
            b64("val")
        ))
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        200,
        "unauthenticated request should be rejected when auth is enabled"
    );

    // Authenticate and get token.
    let auth_resp: Value = client
        .post(format!("http://{}/v3/auth/authenticate", addr))
        .body(r#"{"name":"root","password":"rootpass"}"#)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = auth_resp["token"]
        .as_str()
        .expect("should get auth token");

    // Authenticated put should succeed.
    let resp = client
        .post(format!("http://{}/v3/kv/put", addr))
        .header("Authorization", token)
        .body(format!(
            r#"{{"key":"{}","value":"{}"}}"#,
            b64("secret"),
            b64("val")
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "authenticated request should succeed");
}
