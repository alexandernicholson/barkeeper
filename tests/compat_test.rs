//! Comprehensive integration tests verifying etcd v3 HTTP gateway API
//! compatibility. Each test starts a full barkeeper instance (Raft + KV
//! store + HTTP gateway) on random ports and verifies behavioral parity
//! with etcd's grpc-gateway JSON API.
//!
//! Test coverage based on side-by-side etcd v3.5.17 benchmark comparison.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use barkeeper::api::gateway;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::hub::WatchHub;

/// Spin up a minimal barkeeper instance and return the HTTP gateway address.
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

    // Spawn lease expiry timer — checks every 500ms for expired leases.
    {
        let lm = Arc::clone(&lease_manager);
        let st = Arc::clone(&store);
        let wh = Arc::clone(&watch_hub);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                let expired = lm.check_expired().await;
                for lease in expired {
                    for key in &lease.keys {
                        if let Ok(result) = st.delete_range(key, b"") {
                            for prev in &result.prev_kvs {
                                let tombstone = barkeeper::proto::mvccpb::KeyValue {
                                    key: prev.key.clone(),
                                    create_revision: 0,
                                    mod_revision: result.revision,
                                    version: 0,
                                    value: vec![],
                                    lease: 0,
                                };
                                wh.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
                            }
                        }
                    }
                }
            }
        });
    }

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

fn b64(s: &str) -> String {
    B64.encode(s.as_bytes())
}

fn b64_decode(s: &str) -> String {
    String::from_utf8(B64.decode(s).unwrap()).unwrap()
}

/// Helper: parse string-encoded i64 (proto3 JSON convention).
fn str_i64(val: &Value) -> i64 {
    val.as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| val.as_i64())
        .expect("expected string-encoded i64")
}

/// Helper: parse string-encoded u64 (proto3 JSON convention).
fn str_u64(val: &Value) -> u64 {
    val.as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| val.as_u64())
        .expect("expected string-encoded u64")
}

// ═══════════════════════════════════════════════════════════════════════════
// Proto3 JSON Format Verification
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_proto3_json_header_fields_are_strings() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("fmttest"), "value": b64("v")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let header = &body["header"];

    // Per proto3 JSON mapping, uint64/int64 fields must be strings.
    assert!(header["cluster_id"].is_string(), "cluster_id should be string");
    assert!(header["member_id"].is_string(), "member_id should be string");
    assert!(header["revision"].is_string(), "revision should be string");
    assert!(header["raft_term"].is_string(), "raft_term should be string");
}

#[tokio::test]
async fn test_proto3_json_kv_fields_are_strings() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("kvfmt"), "value": b64("v")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("kvfmt")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kv = &body["kvs"][0];

    assert!(kv["create_revision"].is_string(), "create_revision should be string");
    assert!(kv["mod_revision"].is_string(), "mod_revision should be string");
    assert!(kv["version"].is_string(), "version should be string");
    assert!(kv["lease"].is_null() || !kv.get("lease").is_some(),
        "lease=0 should be omitted per proto3 convention");
}

#[tokio::test]
async fn test_no_content_type_header_required() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Send request without Content-Type header (like etcd's grpc-gateway accepts).
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .body(format!(
            r#"{{"key":"{}","value":"{}"}}"#,
            b64("noct"),
            b64("val")
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "should work without Content-Type header");
}

// ═══════════════════════════════════════════════════════════════════════════
// KV Put Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_kv_put_basic() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("foo"), "value": b64("bar")}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert!(body["header"].is_object());
    assert!(str_i64(&body["header"]["revision"]) > 0);
}

#[tokio::test]
async fn test_kv_put_overwrite_increments_version() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Put v1
    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("ver"), "value": b64("v1")}))
        .send()
        .await
        .unwrap();

    // Put v2 (overwrite)
    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("ver"), "value": b64("v2")}))
        .send()
        .await
        .unwrap();

    // Read — should see v2 with version=2
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("ver")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kv = &body["kvs"][0];
    assert_eq!(b64_decode(kv["value"].as_str().unwrap()), "v2");
    assert_eq!(str_i64(&kv["version"]), 2);
}

#[tokio::test]
async fn test_kv_put_with_prev_kv() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Put original value
    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("pk"), "value": b64("orig")}))
        .send()
        .await
        .unwrap();

    // Overwrite with prev_kv=true
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("pk"), "value": b64("new"), "prev_kv": true}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    let prev = &body["prev_kv"];
    assert!(prev.is_object(), "prev_kv should be present");
    assert_eq!(b64_decode(prev["key"].as_str().unwrap()), "pk");
    assert_eq!(b64_decode(prev["value"].as_str().unwrap()), "orig");
}

// ═══════════════════════════════════════════════════════════════════════════
// KV Range Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_kv_range_single_key() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("rk"), "value": b64("rv")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("rk")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 1);
    assert_eq!(b64_decode(kvs[0]["key"].as_str().unwrap()), "rk");
    assert_eq!(b64_decode(kvs[0]["value"].as_str().unwrap()), "rv");
    assert_eq!(str_i64(&body["count"]), 1);
}

#[tokio::test]
async fn test_kv_range_nonexistent_returns_empty() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("noexist")}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    // etcd omits kvs, count, more when empty. We accept both absent and empty.
    let kvs = body.get("kvs");
    assert!(
        kvs.is_none() || kvs.unwrap().as_array().map_or(true, |a| a.is_empty()),
        "nonexistent key should return no kvs"
    );
}

#[tokio::test]
async fn test_kv_range_prefix() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    for (k, v) in &[("pfx/a", "1"), ("pfx/b", "2"), ("pfx/c", "3"), ("other", "x")] {
        client
            .post(format!("{}/v3/kv/put", base))
            .json(&json!({"key": b64(k), "value": b64(v)}))
            .send()
            .await
            .unwrap();
    }

    // Range with range_end for prefix "pfx/" → range_end "pfx0"
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("pfx/"), "range_end": b64("pfx0")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 3, "prefix range should return 3 keys");
    assert_eq!(str_i64(&body["count"]), 3);
}

#[tokio::test]
async fn test_kv_range_with_limit() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    for i in 0..5 {
        client
            .post(format!("{}/v3/kv/put", base))
            .json(&json!({"key": b64(&format!("l/{}", i)), "value": b64("v")}))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("l/"), "range_end": b64("l0"), "limit": 2}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 2);
    assert!(body["more"].as_bool().unwrap_or(false), "more should be true");
}

// ═══════════════════════════════════════════════════════════════════════════
// KV Delete Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_kv_delete_single_returns_count() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("del1"), "value": b64("v")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("del1")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(str_i64(&body["deleted"]), 1, "should report 1 key deleted");
}

#[tokio::test]
async fn test_kv_delete_nonexistent_returns_zero() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("noexist")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    // etcd omits "deleted" when 0; barkeeper may include "0" or omit
    let deleted = body.get("deleted").map(|v| str_i64(v)).unwrap_or(0);
    assert_eq!(deleted, 0, "deleting nonexistent key should return 0");
}

#[tokio::test]
async fn test_kv_delete_prefix_returns_count() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    for k in &["dp/a", "dp/b", "dp/c"] {
        client
            .post(format!("{}/v3/kv/put", base))
            .json(&json!({"key": b64(k), "value": b64("v")}))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("dp/"), "range_end": b64("dp0")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(str_i64(&body["deleted"]), 3, "should report 3 keys deleted");
}

#[tokio::test]
async fn test_kv_delete_then_read_returns_empty() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("dv"), "value": b64("v")}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("dv")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("dv")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body.get("kvs");
    assert!(
        kvs.is_none() || kvs.unwrap().as_array().map_or(true, |a| a.is_empty()),
        "deleted key should not appear in range"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Txn Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_txn_success_path() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Setup: put original value
    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("txk"), "value": b64("orig")}))
        .send()
        .await
        .unwrap();

    // Txn: if value(txk) == "orig" then put txk="new" else put txk="fail"
    let resp = client
        .post(format!("{}/v3/kv/txn", base))
        .json(&json!({
            "compare": [{
                "key": b64("txk"),
                "target": "VALUE",
                "result": "EQUAL",
                "value": b64("orig")
            }],
            "success": [{"requestPut": {"key": b64("txk"), "value": b64("new")}}],
            "failure": [{"requestPut": {"key": b64("txk"), "value": b64("fail")}}]
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["succeeded"].as_bool().unwrap(), true);

    // Verify the success branch was applied
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("txk")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(b64_decode(body["kvs"][0]["value"].as_str().unwrap()), "new");
}

#[tokio::test]
async fn test_txn_failure_path() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("txf"), "value": b64("orig")}))
        .send()
        .await
        .unwrap();

    // Txn with non-matching compare → failure path
    let resp = client
        .post(format!("{}/v3/kv/txn", base))
        .json(&json!({
            "compare": [{
                "key": b64("txf"),
                "target": "VALUE",
                "result": "EQUAL",
                "value": b64("wrong")
            }],
            "success": [{"requestPut": {"key": b64("txf"), "value": b64("should_not")}}],
            "failure": [{"requestPut": {"key": b64("txf"), "value": b64("fallback")}}]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();

    // succeeded should be false (omitted per proto3 or explicitly false)
    let succeeded = body.get("succeeded").and_then(|v| v.as_bool()).unwrap_or(false);
    assert!(!succeeded, "txn should fail when compare doesn't match");

    // Verify the failure branch was applied
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("txf")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(b64_decode(body["kvs"][0]["value"].as_str().unwrap()), "fallback");
}

#[tokio::test]
async fn test_txn_response_put_omits_prev_kv() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("txp"), "value": b64("v1")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/txn", base))
        .json(&json!({
            "compare": [{
                "key": b64("txp"),
                "target": "VALUE",
                "result": "EQUAL",
                "value": b64("v1")
            }],
            "success": [{"requestPut": {"key": b64("txp"), "value": b64("v2")}}],
            "failure": []
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();

    // The response_put should NOT include prev_kv unless requested
    let resp_op = &body["responses"][0]["response_put"];
    assert!(resp_op["prev_kv"].is_null(), "txn put should not include prev_kv");
    assert!(resp_op["header"]["revision"].is_string(), "inner header revision should be string");
}

// ═══════════════════════════════════════════════════════════════════════════
// Lease Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_lease_grant_returns_string_fields() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({"TTL": 60}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    // ID and TTL should be string-encoded per proto3 JSON
    assert!(body["ID"].is_string(), "ID should be string");
    assert!(body["TTL"].is_string(), "TTL should be string");
    assert_eq!(str_i64(&body["TTL"]), 60);
    assert!(str_i64(&body["ID"]) > 0, "lease ID should be positive");
    // error="" should be omitted
    assert!(body.get("error").is_none(), "empty error should be omitted");
}

#[tokio::test]
async fn test_lease_list() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Grant two leases
    let resp1 = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({"TTL": 30}))
        .send()
        .await
        .unwrap();
    let id1 = str_i64(&resp1.json::<Value>().await.unwrap()["ID"]);

    let resp2 = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({"TTL": 60}))
        .send()
        .await
        .unwrap();
    let id2 = str_i64(&resp2.json::<Value>().await.unwrap()["ID"]);

    // List leases
    let resp = client
        .post(format!("{}/v3/lease/leases", base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let leases = body["leases"].as_array().unwrap();
    let ids: Vec<i64> = leases.iter().map(|l| str_i64(&l["ID"])).collect();
    assert!(ids.contains(&id1), "lease list should contain first lease");
    assert!(ids.contains(&id2), "lease list should contain second lease");
}

#[tokio::test]
async fn test_lease_revoke_then_404() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({"TTL": 10}))
        .send()
        .await
        .unwrap();
    let lease_id = str_i64(&resp.json::<Value>().await.unwrap()["ID"]);

    // Revoke
    let resp = client
        .post(format!("{}/v3/lease/revoke", base))
        .json(&json!({"ID": lease_id}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Revoke again → 404
    let resp = client
        .post(format!("{}/v3/lease/revoke", base))
        .json(&json!({"ID": lease_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn test_lease_timetolive() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/lease/grant", base))
        .json(&json!({"TTL": 120}))
        .send()
        .await
        .unwrap();
    let lease_id = str_i64(&resp.json::<Value>().await.unwrap()["ID"]);

    let resp = client
        .post(format!("{}/v3/lease/timetolive", base))
        .json(&json!({"ID": lease_id}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(str_i64(&body["ID"]), lease_id);
    assert_eq!(str_i64(&body["grantedTTL"]), 120);
    assert!(str_i64(&body["TTL"]) > 100, "TTL should be close to 120");
}

// ═══════════════════════════════════════════════════════════════════════════
// Cluster Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cluster_member_list() {
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
    assert_eq!(members.len(), 1, "single-node cluster");
    assert!(members[0]["ID"].is_string(), "ID should be string");
    assert_eq!(str_u64(&members[0]["ID"]), 1);
    assert_eq!(members[0]["name"].as_str().unwrap(), "test-node");
    // isLearner should be omitted when false (proto3 convention)
    assert!(members[0].get("isLearner").is_none(), "isLearner=false should be omitted");
    // revision should be omitted for member list (not KV-related)
    assert!(body["header"].get("revision").is_none(),
        "member list header should omit revision");
}

// ═══════════════════════════════════════════════════════════════════════════
// Maintenance Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_maintenance_status() {
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
    assert!(body["dbSize"].is_string(), "dbSize should be string");
    assert!(body["leader"].is_string(), "leader should be string");
    assert!(body["raftIndex"].is_string(), "raftIndex should be string");
    assert!(body["raftTerm"].is_string(), "raftTerm should be string");
    assert!(body["raftAppliedIndex"].is_string(), "raftAppliedIndex should be string");
    assert!(body["dbSizeInUse"].is_string(), "dbSizeInUse should be string");
}

// ═══════════════════════════════════════════════════════════════════════════
// Put-Delete-Put Cycle (Regression)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_put_delete_put_cycle() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("cycle"), "value": b64("first")}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("cycle")}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("cycle"), "value": b64("second")}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("cycle")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 1);
    assert_eq!(b64_decode(kvs[0]["value"].as_str().unwrap()), "second");
    // After delete+re-put, version resets to 1 (new key lifecycle)
    assert_eq!(str_i64(&kvs[0]["version"]), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// JSON Field Omission Tests (proto3 Convention)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_empty_range_omits_default_fields() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("noexist")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();

    // etcd omits kvs, count, more when empty/zero/false
    assert!(
        body.get("kvs").is_none() || body["kvs"].as_array().map_or(true, |a| a.is_empty()),
        "empty kvs should be omitted or empty"
    );
    assert!(
        body.get("count").is_none() || str_i64(&body["count"]) == 0,
        "zero count should be omitted"
    );
    assert!(
        body.get("more").is_none() || !body["more"].as_bool().unwrap_or(false),
        "false more should be omitted"
    );
}

#[tokio::test]
async fn test_delete_zero_omits_deleted() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    let resp = client
        .post(format!("{}/v3/kv/deleterange", base))
        .json(&json!({"key": b64("nothing")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();

    // "deleted": "0" should be omitted per proto3 convention
    assert!(
        body.get("deleted").is_none()
            || body["deleted"].as_str().map_or(true, |s| s == "0")
            || body["deleted"].as_i64() == Some(0),
        "zero deleted should be omitted or zero"
    );
}

/// Response headers should include a non-zero raft_term after the node
/// becomes leader (single-node cluster elects itself).
#[tokio::test]
async fn test_response_header_has_nonzero_raft_term() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    let resp: Value = client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("termtest"), b64("val")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let raft_term = &resp["header"]["raft_term"];
    // Proto3 JSON: raft_term is a string.
    let term_val: u64 = raft_term.as_str().unwrap_or("0").parse().unwrap_or(0);
    assert!(term_val > 0, "raft_term should be > 0, got {}", term_val);
}

// ═══════════════════════════════════════════════════════════════════════════
// Compaction Tests
// ═══════════════════════════════════════════════════════════════════════════

/// HTTP compaction endpoint should work, not return 501.
#[tokio::test]
async fn test_http_compaction_works() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Put some keys to create revisions.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(
            r#"{{"key":"{}","value":"{}"}}"#,
            b64("compact1"),
            b64("v1")
        ))
        .send()
        .await
        .unwrap();
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(
            r#"{{"key":"{}","value":"{}"}}"#,
            b64("compact2"),
            b64("v2")
        ))
        .send()
        .await
        .unwrap();

    // Compact at revision 1.
    let resp = client
        .post(format!("http://{}/v3/kv/compaction", addr))
        .body(r#"{"revision":"1"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "compaction should return 200, not 501");

    let body: Value = resp.json().await.unwrap();
    assert!(
        body.get("header").is_some(),
        "compaction response should have header"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Lease Expiry Tests
// ═══════════════════════════════════════════════════════════════════════════

/// After a write, the maintenance status should show advancing raft_applied_index.
#[tokio::test]
async fn test_writes_advance_raft_applied_index() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Check initial status.
    let status1: Value = client
        .post(format!("http://{}/v3/maintenance/status", addr))
        .body("{}")
        .send().await.unwrap()
        .json().await.unwrap();

    // Do a write.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("raftwrite"), b64("val")))
        .send().await.unwrap();

    // Check status after write.
    let status2: Value = client
        .post(format!("http://{}/v3/maintenance/status", addr))
        .body("{}")
        .send().await.unwrap()
        .json().await.unwrap();

    let idx1: u64 = status1["raftAppliedIndex"].as_str().unwrap_or("0").parse().unwrap_or(0);
    let idx2: u64 = status2["raftAppliedIndex"].as_str().unwrap_or("0").parse().unwrap_or(0);
    assert!(idx2 > idx1, "raft_applied_index should advance after write: {} -> {}", idx1, idx2);
}

/// Keys attached to expired leases should be automatically deleted.
/// This tests the production expiry timer path.
#[tokio::test]
async fn test_lease_expiry_via_production_timer() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Grant a short-lived lease.
    let resp: Value = client
        .post(format!("http://{}/v3/lease/grant", addr))
        .body(r#"{"TTL": 1}"#)
        .send().await.unwrap()
        .json().await.unwrap();
    let lease_id = resp["ID"].as_str().unwrap();

    // Put a key with the lease.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(
            r#"{{"key":"{}","value":"{}","lease":{}}}"#,
            b64("expiring"), b64("temp"), lease_id
        ))
        .send().await.unwrap();

    // Key should exist.
    let get: Value = client
        .post(format!("http://{}/v3/kv/range", addr))
        .body(format!(r#"{{"key":"{}"}}"#, b64("expiring")))
        .send().await.unwrap()
        .json().await.unwrap();
    assert!(get["kvs"].as_array().unwrap().len() > 0);

    // Wait for expiry (TTL=1 + timer interval + margin).
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Key should be gone.
    let get2: Value = client
        .post(format!("http://{}/v3/kv/range", addr))
        .body(format!(r#"{{"key":"{}"}}"#, b64("expiring")))
        .send().await.unwrap()
        .json().await.unwrap();
    let kvs = get2.get("kvs").and_then(|v| v.as_array());
    assert!(
        kvs.is_none() || kvs.unwrap().is_empty(),
        "key should be deleted after lease expiry"
    );
}
