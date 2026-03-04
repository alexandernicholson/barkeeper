//! Integration tests verifying etcd v3 HTTP gateway API compatibility.
//!
//! Each test starts a full barkeeper instance (Raft + KV store + HTTP gateway)
//! on random ports and exercises the JSON endpoints that mirror etcd's
//! grpc-gateway surface.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use barkeeper::api::gateway;
use barkeeper::cluster::manager::ClusterManager;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};

/// Spin up a minimal barkeeper instance and return the HTTP gateway address.
///
/// Instead of starting the full `BarkeepServer` (which also binds a gRPC
/// server and blocks), we manually wire up only the components needed for
/// HTTP gateway testing: KvStore, Raft node, state machine, and the Axum
/// router. This avoids fighting the blocking `tonic::Server::serve`.
async fn start_test_instance() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();

    let config = RaftConfig {
        node_id: 1,
        data_dir: dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    // Open the KV store.
    let store = Arc::new(
        KvStore::open(dir.path().join("kv.redb")).expect("open KvStore"),
    );

    // Create the apply channel and spawn the state machine.
    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(Arc::clone(&store), apply_rx).await;

    // Spawn the Raft node (single-node, becomes leader after election timeout).
    let raft_handle = spawn_raft_node(config, apply_tx, None).await;

    // Create managers for lease and cluster.
    let lease_manager = Arc::new(LeaseManager::new());
    let cluster_manager = Arc::new(ClusterManager::new(1));
    cluster_manager
        .add_initial_member(1, String::new(), vec![], vec![])
        .await;

    let cluster_id = 1u64;
    let member_id = 1u64;

    // Build the HTTP gateway router.
    let app = gateway::create_router(
        raft_handle,
        Arc::clone(&store),
        Arc::clone(&lease_manager),
        Arc::clone(&cluster_manager),
        cluster_id,
        member_id,
    );

    // Bind to a random port.
    let http_port = portpicker::pick_unused_port().expect("no free port");
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .expect("bind HTTP");
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    // Wait for the single-node Raft to elect itself leader.
    // The election timeout range is 150-300ms; give it a comfortable margin.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    (bound_addr, dir)
}

fn b64(s: &str) -> String {
    B64.encode(s.as_bytes())
}

fn b64_decode(s: &str) -> String {
    String::from_utf8(B64.decode(s).unwrap()).unwrap()
}

// ── KV: Put + Range ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_put_and_range() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // PUT key=hello value=world
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("hello"),
            "value": b64("world"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "put failed: {}", resp.status());
    let body: Value = resp.json().await.unwrap();
    // Verify the response has the expected structure.
    assert!(body["header"].is_object(), "put response should have header");

    // RANGE key=hello
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("hello"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "range failed: {}", resp.status());
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().expect("kvs should be an array");
    assert_eq!(kvs.len(), 1);
    assert_eq!(
        b64_decode(kvs[0]["value"].as_str().unwrap()),
        "world"
    );
    assert_eq!(
        b64_decode(kvs[0]["key"].as_str().unwrap()),
        "hello"
    );
}

// ── KV: Delete ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_delete_range() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // PUT a key.
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("delme"),
            "value": b64("gone"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // DELETE the key.
    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({
            "key": b64("delme"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "delete failed: {}", resp.status());
    let body: Value = resp.json().await.unwrap();
    assert!(body["header"].is_object(), "delete response should have header");

    // Verify the key is gone.
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("delme"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().expect("kvs should be an array");
    assert_eq!(kvs.len(), 0, "key should be deleted");
}

// ── KV: Range with multiple keys ────────────────────────────────────────────

#[tokio::test]
async fn test_http_range_multiple_keys() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Put several keys with a common prefix.
    for key in &["key/a", "key/b", "key/c"] {
        let resp = client
            .post(format!("{}/v3/kv/put", base))
            .json(&json!({
                "key": b64(key),
                "value": b64(&format!("val-{}", key)),
            }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Range with range_end to get all keys starting with "key/".
    // etcd convention: range_end is the key with the last byte incremented.
    // "key/" -> range_end "key0" (0x2F + 1 = 0x30 = '0')
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("key/"),
            "range_end": b64("key0"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().expect("kvs should be an array");
    assert_eq!(kvs.len(), 3, "should return all 3 prefixed keys");
    assert_eq!(body["count"].as_i64().unwrap(), 3);
}

// ── KV: Range with limit ────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_range_with_limit() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    for i in 0..5 {
        let resp = client
            .post(format!("{}/v3/kv/put", base))
            .json(&json!({
                "key": b64(&format!("lim/{}", i)),
                "value": b64(&format!("v{}", i)),
            }))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Range with limit=2
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("lim/"),
            "range_end": b64("lim0"),
            "limit": 2,
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 2, "limit should cap results at 2");
    assert!(body["more"].as_bool().unwrap(), "more should be true");
}

// ── Lease: Grant and List ───────────────────────────────────────────────────

#[tokio::test]
async fn test_http_lease_grant_and_list() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Grant a lease with TTL=60.
    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({ "TTL": 60 }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "grant failed: {}", resp.status());
    let body: Value = resp.json().await.unwrap();
    let lease_id = body["ID"].as_i64().unwrap();
    assert!(lease_id > 0, "lease ID should be positive");
    assert_eq!(body["TTL"].as_i64().unwrap(), 60);

    // List leases.
    let resp = client
        .post(format!("{}/v3/lease/leases", base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    let leases = body["leases"].as_array().unwrap();
    assert!(
        leases.iter().any(|l| l["ID"].as_i64().unwrap() == lease_id),
        "granted lease should appear in list"
    );
}

// ── Lease: Grant with specific ID ───────────────────────────────────────────

#[tokio::test]
async fn test_http_lease_grant_specific_id() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Grant a lease with a specific ID.
    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({ "TTL": 30, "ID": 42 }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ID"].as_i64().unwrap(), 42);
    assert_eq!(body["TTL"].as_i64().unwrap(), 30);
}

// ── Lease: Revoke ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_lease_revoke() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Grant, then revoke.
    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({ "TTL": 10 }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let lease_id = body["ID"].as_i64().unwrap();

    let resp = client
        .post(format!("{}/v3/lease/revoke", base))
        .json(&json!({ "ID": lease_id }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "revoke failed");

    // Revoking again should return 404.
    let resp = client
        .post(format!("{}/v3/lease/revoke", base))
        .json(&json!({ "ID": lease_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404, "revoking non-existent lease should 404");
}

// ── Lease: TimeToLive ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_lease_timetolive() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Grant a lease with TTL=120.
    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({ "TTL": 120 }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let lease_id = body["ID"].as_i64().unwrap();

    // Query time-to-live.
    let resp = client
        .post(format!("{}/v3/lease/timetolive", base))
        .json(&json!({ "ID": lease_id }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ID"].as_i64().unwrap(), lease_id);
    assert_eq!(body["grantedTTL"].as_i64().unwrap(), 120);
    // TTL should be close to 120 since we just granted it.
    let ttl = body["TTL"].as_i64().unwrap();
    assert!(ttl > 100, "TTL should still be high, got {}", ttl);
}

// ── Cluster: Member List ────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_cluster_member_list() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/cluster/member/list", base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    let members = body["members"].as_array().unwrap();
    assert_eq!(members.len(), 1, "single-node cluster should have 1 member");
    assert_eq!(members[0]["ID"].as_u64().unwrap(), 1);
}

// ── Maintenance: Status ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_maintenance_status() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/maintenance/status", base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["version"].as_str().unwrap(), "0.1.0");
    assert!(body["header"]["cluster_id"].as_u64().unwrap() > 0);
    assert!(body["header"]["member_id"].as_u64().unwrap() > 0);
    assert_eq!(body["leader"].as_u64().unwrap(), 1);
}

// ── KV: Put then overwrite then read ────────────────────────────────────────

#[tokio::test]
async fn test_http_put_overwrite() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Put v1.
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("counter"),
            "value": b64("1"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Put v2 (overwrite).
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("counter"),
            "value": b64("2"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert!(body["header"].is_object(), "overwrite response should have header");

    // Read should return latest value.
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("counter"),
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 1);
    assert_eq!(b64_decode(kvs[0]["value"].as_str().unwrap()), "2");
}

// ── KV: Range on non-existent key returns empty ─────────────────────────────

#[tokio::test]
async fn test_http_range_nonexistent_key() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("does-not-exist"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 0, "non-existent key should return empty kvs");
    assert_eq!(body["count"].as_i64().unwrap(), 0);
}

// ── KV: Multiple put-delete cycles ──────────────────────────────────────────

#[tokio::test]
async fn test_http_put_delete_put_cycle() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Put
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("cycle"),
            "value": b64("first"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Delete
    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({
            "key": b64("cycle"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Re-put
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({
            "key": b64("cycle"),
            "value": b64("second"),
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Should see "second".
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({
            "key": b64("cycle"),
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 1);
    assert_eq!(b64_decode(kvs[0]["value"].as_str().unwrap()), "second");
}
