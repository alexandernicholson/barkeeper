//! Client-layer tests: exercise every gRPC RPC against a real barkeeper.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

use barkeeper::api::auth_service::AuthService;
use barkeeper::api::cluster_service::ClusterService;
use barkeeper::api::kv_service::KvService;
use barkeeper::api::lease_service::LeaseService;
use barkeeper::api::maintenance_service::MaintenanceService;
use barkeeper::api::watch_service::WatchService;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::apply_broker::ApplyResultBroker;
use barkeeper::kv::apply_notifier::ApplyNotifier;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::write_buffer::WriteBuffer;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::proto::etcdserverpb::auth_server::AuthServer;
use barkeeper::proto::etcdserverpb::cluster_server::ClusterServer;
use barkeeper::proto::etcdserverpb::kv_server::KvServer;
use barkeeper::proto::etcdserverpb::lease_server::LeaseServer;
use barkeeper::proto::etcdserverpb::maintenance_server::MaintenanceServer;
use barkeeper::proto::etcdserverpb::watch_server::WatchServer;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;
use tonic::transport::Server;

use bkctl::client::BkClient;

/// Starts a real barkeeper gRPC instance on a random port.
struct TestServer {
    endpoint: String,
    _dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = RaftConfig {
            node_id: 1,
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let kv_store = Arc::new(KvStore::open(dir.path()).expect("open KvStore"));
        let kv_runtime = Runtime::new(1);
        let store = spawn_kv_store_actor(&kv_runtime, Arc::clone(&kv_store)).await;

        let (apply_tx, apply_rx) = mpsc::channel(256);
        let write_buffer = Arc::new(WriteBuffer::new());
        let revision = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let raft_handle =
            spawn_raft_node(config, apply_tx, revision, Arc::clone(&write_buffer)).await;

        let lease_manager = Arc::new(LeaseManager::new());
        let cluster_runtime = Runtime::new(1);
        let cluster_manager = spawn_cluster_actor(&cluster_runtime, 1).await;
        cluster_manager
            .add_initial_member(1, "test-node".to_string(), vec![], vec![])
            .await;

        let watch_runtime = Runtime::new(1);
        let watch_hub = spawn_watch_hub_actor(&watch_runtime, Some(store.clone())).await;
        let auth_runtime = Runtime::new(1);
        let auth_manager = spawn_auth_actor(&auth_runtime).await;

        let broker = Arc::new(ApplyResultBroker::new());
        let notifier = ApplyNotifier::new(0);

        spawn_state_machine(
            apply_rx,
            Arc::clone(&kv_store),
            watch_hub.clone(),
            Arc::clone(&lease_manager),
            Arc::clone(&broker),
            notifier.clone(),
            Arc::clone(&write_buffer),
        )
        .await;

        let raft_term = Arc::clone(&raft_handle.current_term);

        // Build gRPC services.
        let kv_service = KvService::new(
            store.clone(),
            Arc::clone(&kv_store),
            Arc::clone(&lease_manager),
            1, 1,
            Arc::clone(&raft_term),
            raft_handle.clone(),
            Arc::clone(&broker),
            notifier.clone(),
        );
        let watch_service = WatchService::new(
            watch_hub.clone(),
            store.clone(),
            1, 1,
            Arc::clone(&raft_term),
        );
        let lease_service = LeaseService::new(
            Arc::clone(&lease_manager),
            store.clone(),
            1, 1,
            Arc::clone(&raft_term),
        );
        let cluster_service = ClusterService::new(
            cluster_manager,
            1, 1,
            Arc::clone(&raft_term),
        );
        let maintenance_service = MaintenanceService::new(
            store.clone(),
            Arc::clone(&kv_store),
            1, 1,
            Arc::clone(&raft_term),
            raft_handle.clone(),
        );
        let auth_service = AuthService::new(
            auth_manager,
            1, 1,
            Arc::clone(&raft_term),
        );

        let grpc_port = portpicker::pick_unused_port().expect("no free port");
        let grpc_addr: SocketAddr = ([127, 0, 0, 1], grpc_port).into();

        tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(kv_service))
                .add_service(WatchServer::new(watch_service))
                .add_service(LeaseServer::new(lease_service))
                .add_service(ClusterServer::new(cluster_service))
                .add_service(MaintenanceServer::new(maintenance_service))
                .add_service(AuthServer::new(auth_service))
                .serve(grpc_addr)
                .await
                .expect("gRPC server failed");
        });

        // Wait for Raft election.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Self {
            endpoint: format!("http://{}", grpc_addr),
            _dir: dir,
        }
    }
}

#[tokio::test]
async fn test_put_and_get() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    client.put(b"hello", b"world").await.unwrap();
    let kv = client.get(b"hello").await.unwrap();
    assert!(kv.is_some());
    assert_eq!(kv.unwrap().value, b"world");
}

#[tokio::test]
async fn test_get_missing_key() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    let kv = client.get(b"nonexistent").await.unwrap();
    assert!(kv.is_none());
}

#[tokio::test]
async fn test_delete() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    client.put(b"to-delete", b"val").await.unwrap();
    let deleted = client.delete(b"to-delete").await.unwrap();
    assert_eq!(deleted, 1);
    let kv = client.get(b"to-delete").await.unwrap();
    assert!(kv.is_none());
}

#[tokio::test]
async fn test_get_prefix() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    client.put(b"/app/a", b"1").await.unwrap();
    client.put(b"/app/b", b"2").await.unwrap();
    client.put(b"/other", b"3").await.unwrap();

    let kvs = client.get_prefix(b"/app/").await.unwrap();
    assert_eq!(kvs.len(), 2);
}

#[tokio::test]
async fn test_member_list() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    let members = client.member_list().await.unwrap();
    assert!(!members.is_empty());
    assert_eq!(members[0].name, "test-node");
}

#[tokio::test]
async fn test_status() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    let status = client.status().await.unwrap();
    assert!(status.leader > 0);
}

#[tokio::test]
async fn test_watch_receives_put_event() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();

    let mut watch_rx = client.watch(b"/watched/").await.unwrap();

    // Give the watch stream time to establish.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    client.put(b"/watched/key1", b"val1").await.unwrap();

    let event = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        watch_rx.recv(),
    )
    .await
    .expect("watch timed out")
    .expect("watch channel closed");

    assert_eq!(event.kv.as_ref().unwrap().key, b"/watched/key1");
}
