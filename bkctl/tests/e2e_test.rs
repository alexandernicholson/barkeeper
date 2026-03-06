//! End-to-end tests: start barkeeper, connect BkClient, drive App model through user flows.

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

use bkctl::app::{App, ClusterStatus, EventType, Tab, WatchEvent};
use bkctl::client::BkClient;

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
            watch_hub.clone(), store.clone(), 1, 1, Arc::clone(&raft_term),
        );
        let lease_service = LeaseService::new(
            Arc::clone(&lease_manager), store.clone(), 1, 1, Arc::clone(&raft_term),
        );
        let cluster_service = ClusterService::new(
            cluster_manager, 1, 1, Arc::clone(&raft_term),
        );
        let maintenance_service = MaintenanceService::new(
            store.clone(), 1, 1, Arc::clone(&raft_term), raft_handle.clone(),
        );
        let auth_service = AuthService::new(
            auth_manager, 1, 1, Arc::clone(&raft_term),
        );

        let port = portpicker::pick_unused_port().expect("no free port");
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();

        tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(kv_service))
                .add_service(WatchServer::new(watch_service))
                .add_service(LeaseServer::new(lease_service))
                .add_service(ClusterServer::new(cluster_service))
                .add_service(MaintenanceServer::new(maintenance_service))
                .add_service(AuthServer::new(auth_service))
                .serve(addr)
                .await
                .unwrap();
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Self {
            endpoint: format!("http://{}", addr),
            _dir: dir,
        }
    }
}

#[tokio::test]
async fn test_e2e_put_and_browse() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    // Put some keys via client.
    client.put(b"/test/hello", b"world").await.unwrap();
    client.put(b"/test/foo", b"bar").await.unwrap();

    // Refresh keys as the main loop would.
    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());

    // Should see "test/" prefix at root.
    assert!(app.visible_prefixes().contains(&"test/".to_string()));

    // Descend into test/.
    app.descend("test/");
    assert_eq!(app.current_prefix(), "/test/");

    // Should see leaf keys.
    let keys = app.visible_keys();
    assert!(keys.contains(&"foo".to_string()));
    assert!(keys.contains(&"hello".to_string()));

    // Select "hello" and fetch its value.
    let prefixes = app.visible_prefixes();
    let hello_idx = prefixes.len()
        + keys.iter().position(|k| k == "hello").unwrap();
    while app.selected_index() < hello_idx {
        app.move_down();
    }

    let full_key = app.selected_full_key().unwrap();
    assert_eq!(full_key, "/test/hello");

    let kv = client.get(full_key.as_bytes()).await.unwrap().unwrap();
    app.set_selected_value(Some(kv.value.clone()));
    assert_eq!(app.selected_value(), Some(b"world".as_ref()));
}

#[tokio::test]
async fn test_e2e_delete_key() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    client.put(b"/del/target", b"val").await.unwrap();

    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    app.descend("del/");
    assert!(app.visible_keys().contains(&"target".to_string()));

    client.delete(b"/del/target").await.unwrap();

    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    assert!(!app.visible_keys().contains(&"target".to_string()));
}

#[tokio::test]
async fn test_e2e_watch_receives_events() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    let mut watch_rx = client.watch(b"/watched/").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    client.put(b"/watched/key1", b"val1").await.unwrap();

    let event = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        watch_rx.recv(),
    )
    .await
    .unwrap()
    .unwrap();

    app.push_watch_event(WatchEvent {
        key: event.kv.as_ref().unwrap().key.clone(),
        event_type: if event.r#type == 0 {
            EventType::Put
        } else {
            EventType::Delete
        },
        value_size: event.kv.as_ref().unwrap().value.len(),
    });

    assert_eq!(app.watch_events().len(), 1);
    assert_eq!(app.watch_events()[0].key, b"/watched/key1");
}

#[tokio::test]
async fn test_e2e_cluster_status() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    let members = client.member_list().await.unwrap();
    let status = client.status().await.unwrap();

    app.set_members(
        members
            .iter()
            .map(|m| (m.name.clone(), m.id, m.id == status.leader))
            .collect(),
    );
    app.set_cluster_status(ClusterStatus {
        leader_id: status.leader,
        raft_term: status.raft_term,
        member_count: members.len(),
    });

    assert_eq!(app.members().len(), 1);
    assert_eq!(app.members()[0].name, "test-node");
    assert!(app.members()[0].is_leader);
    assert!(app.cluster_status().unwrap().raft_term > 0);
}

#[tokio::test]
async fn test_e2e_tab_switching_preserves_state() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    app.push_watch_event(WatchEvent {
        key: b"/event".to_vec(),
        event_type: EventType::Put,
        value_size: 10,
    });

    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Keys);

    client.put(b"/x/y", b"z").await.unwrap();
    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    app.descend("x/");

    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Dashboard);
    assert_eq!(app.watch_events().len(), 1);

    app.toggle_tab();
    assert_eq!(app.current_prefix(), "/x/");
}
