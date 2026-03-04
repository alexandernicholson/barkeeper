//! Integration test: lease expiry deletes attached keys from KV store.
//! This tests the full path: grant lease → put with lease → wait → key gone.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use barkeeper::api::gateway;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::actor::{spawn_kv_store_actor, KvStoreActorHandle};
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;

fn b64(s: &str) -> String {
    B64.encode(s.as_bytes())
}

async fn start_instance_with_lease_expiry() -> (SocketAddr, KvStoreActorHandle, Arc<LeaseManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        node_id: 1,
        data_dir: dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let kv_store = KvStore::open(dir.path().join("kv.redb")).unwrap();
    let kv_runtime = Runtime::new(1);
    let store = spawn_kv_store_actor(&kv_runtime, kv_store).await;

    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx).await;
    let raft_handle = spawn_raft_node(config, apply_tx).await;

    let lease_manager = Arc::new(LeaseManager::new());
    let watch_runtime = Runtime::new(1);
    let watch_hub = spawn_watch_hub_actor(&watch_runtime, Some(store.clone())).await;
    let cluster_runtime = Runtime::new(1);
    let cluster_manager = spawn_cluster_actor(&cluster_runtime, 1).await;
    cluster_manager.add_initial_member(1, "test".to_string(), vec![], vec![]).await;

    // Spawn a lease expiry timer that checks every 500ms.
    let lm_clone = Arc::clone(&lease_manager);
    let store_clone = store.clone();
    let hub_clone = watch_hub.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(500)).await;
            let expired = lm_clone.check_expired().await;
            for lease in expired {
                for key in &lease.keys {
                    let _ = store_clone.delete_range(key.clone(), vec![]).await;
                    // Notify watchers of deletion.
                    let tombstone = barkeeper::proto::mvccpb::KeyValue {
                        key: key.clone(),
                        create_revision: 0,
                        mod_revision: 0,
                        version: 0,
                        value: vec![],
                        lease: 0,
                    };
                    hub_clone.notify(key.clone(), 1, tombstone, None).await;
                }
            }
        }
    });

    let auth_runtime = Runtime::new(1);
    let auth_manager = spawn_auth_actor(&auth_runtime).await;

    let app = gateway::create_router(
        raft_handle.clone(),
        store.clone(),
        watch_hub.clone(),
        Arc::clone(&lease_manager),
        cluster_manager,
        1, 1,
        Arc::clone(&raft_handle.current_term),
        auth_manager,
        Arc::new(std::sync::Mutex::new(vec![])),
    );

    let port = portpicker::pick_unused_port().unwrap();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    sleep(Duration::from_millis(500)).await;

    (bound, store, lease_manager, dir)
}

#[tokio::test]
async fn test_lease_expiry_deletes_key_via_http() {
    let (addr, _store, _lm, _dir) = start_instance_with_lease_expiry().await;
    let http = format!("http://{}", addr);
    let client = Client::new();

    // Grant a lease with TTL=2.
    let resp: Value = client
        .post(format!("{}/v3/lease/grant", http))
        .body(r#"{"TTL": 2}"#)
        .send().await.unwrap()
        .json().await.unwrap();
    let lease_id = resp["ID"].as_str().unwrap();

    // Put a key with the lease.
    client
        .post(format!("{}/v3/kv/put", http))
        .body(format!(r#"{{"key":"{}","value":"{}","lease":{}}}"#, b64("ephemeral"), b64("temp"), lease_id))
        .send().await.unwrap();

    // Key should exist now.
    let get: Value = client
        .post(format!("{}/v3/kv/range", http))
        .body(format!(r#"{{"key":"{}"}}"#, b64("ephemeral")))
        .send().await.unwrap()
        .json().await.unwrap();
    assert!(get["kvs"].as_array().unwrap().len() > 0, "key should exist before expiry");

    // Wait for lease to expire (TTL=2 + 500ms check interval + margin).
    sleep(Duration::from_secs(3)).await;

    // Key should be gone.
    let get2: Value = client
        .post(format!("{}/v3/kv/range", http))
        .body(format!(r#"{{"key":"{}"}}"#, b64("ephemeral")))
        .send().await.unwrap()
        .json().await.unwrap();
    let kvs = get2.get("kvs").and_then(|v| v.as_array());
    assert!(
        kvs.is_none() || kvs.unwrap().is_empty(),
        "key should be deleted after lease expiry"
    );
}
