# bkctl TUI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build `bkctl`, a standalone Ratatui TUI for exploring barkeeper clusters — browse keys, monitor cluster status, and watch events in real time.

**Architecture:** Separate crate in a Cargo workspace alongside barkeeper. Three layers: gRPC client wrapper, pure state/model, stateless rendering. All layers tested independently, plus end-to-end tests against a real barkeeper instance.

**Tech Stack:** Rust, ratatui + crossterm, tonic gRPC client, tokio, clap

**Design doc:** `docs/plans/2026-03-06-bkctl-tui-design.md`

---

### Task 1: Convert to Cargo Workspace

**Files:**
- Modify: `Cargo.toml` (root — convert to workspace)
- Create: `barkeeper/Cargo.toml` (move existing package here)
- Move: `src/`, `build.rs`, `tests/`, `benches/`, `proto/` into `barkeeper/`

**Step 1: Create workspace Cargo.toml**

Replace root `Cargo.toml` with a workspace definition and move the existing package into a `barkeeper/` subdirectory.

```toml
# Root Cargo.toml (workspace)
[workspace]
members = ["barkeeper", "bkctl"]
resolver = "2"
```

Move all source files:
```bash
cd /home/alexandernicholson/.pxycrab/workspace/barkeeper
mkdir -p barkeeper-pkg
# Move package files into subdirectory
git mv src barkeeper-pkg/
git mv build.rs barkeeper-pkg/
git mv tests barkeeper-pkg/
git mv benches barkeeper-pkg/
git mv proto barkeeper-pkg/
# Move the original Cargo.toml content into the subpackage
cp Cargo.toml barkeeper-pkg/Cargo.toml
# Rename the subdirectory to barkeeper
git mv barkeeper-pkg barkeeper
```

Then rewrite root `Cargo.toml` to the workspace definition above.

The `barkeeper/Cargo.toml` stays exactly as-is (the current package definition) but update the proto paths in `barkeeper/build.rs`:

```rust
// barkeeper/build.rs — no changes needed, proto/ moved alongside it
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &[
                "proto/etcdserverpb/rpc.proto",
                "proto/etcdserverpb/kv.proto",
                "proto/authpb/auth.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}
```

**Step 2: Verify barkeeper still builds**

Run: `cargo build -p barkeeper`
Expected: builds successfully

**Step 3: Run existing tests**

Run: `cargo test -p barkeeper`
Expected: all existing tests pass

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: convert to cargo workspace, move barkeeper into sub-crate"
```

---

### Task 2: Create bkctl Crate Skeleton

**Files:**
- Create: `bkctl/Cargo.toml`
- Create: `bkctl/build.rs`
- Create: `bkctl/src/main.rs`
- Create: `bkctl/src/lib.rs`

**Step 1: Create bkctl/Cargo.toml**

```toml
[package]
name = "bkctl"
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
description = "TUI for exploring barkeeper clusters"

[dependencies]
# TUI
ratatui = "0.29"
crossterm = "0.28"

# gRPC client
tonic = "0.12"
prost = "0.13"

# Async
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"

# CLI
clap = { version = "4", features = ["derive"] }

# Serialization (for pretty-printing JSON values)
serde_json = "1"

[dev-dependencies]
# For TestServer — starts a real barkeeper instance
barkeeper = { path = "../barkeeper" }
tempfile = "3"
portpicker = "0.1"
rebar-core = { git = "https://github.com/alexandernicholson/rebar.git", branch = "main" }

[build-dependencies]
tonic-build = "0.12"
```

**Step 2: Create bkctl/build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate gRPC client stubs from the shared proto definitions.
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "../barkeeper/proto/etcdserverpb/rpc.proto",
                "../barkeeper/proto/etcdserverpb/kv.proto",
                "../barkeeper/proto/authpb/auth.proto",
            ],
            &["../barkeeper/proto/"],
        )?;
    Ok(())
}
```

**Step 3: Create bkctl/src/lib.rs**

```rust
pub mod client;
pub mod app;
pub mod ui;

pub mod proto {
    pub mod mvccpb {
        tonic::include_proto!("mvccpb");
    }
    pub mod etcdserverpb {
        tonic::include_proto!("etcdserverpb");
    }
    pub mod authpb {
        tonic::include_proto!("authpb");
    }
}
```

**Step 4: Create stub modules**

Create `bkctl/src/client.rs`:
```rust
//! gRPC client wrapper for barkeeper.
```

Create `bkctl/src/app.rs`:
```rust
//! TUI application state and transitions.
```

Create `bkctl/src/ui.rs`:
```rust
//! Ratatui rendering functions.
```

**Step 5: Create bkctl/src/main.rs**

```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "bkctl", about = "TUI for exploring barkeeper clusters")]
struct Cli {
    /// Comma-separated gRPC endpoints
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoints: String,

    /// Initial watch/browse prefix
    #[arg(long, default_value = "/")]
    prefix: String,
}

fn main() {
    let _cli = Cli::parse();
    println!("bkctl stub");
}
```

**Step 6: Verify it compiles**

Run: `cargo build -p bkctl`
Expected: compiles, proto stubs generated

**Step 7: Commit**

```bash
git add bkctl/
git commit -m "feat(bkctl): scaffold crate with proto client stubs"
```

---

### Task 3: Implement Client Layer

**Files:**
- Modify: `bkctl/src/client.rs`
- Create: `bkctl/tests/client_test.rs`

**Step 1: Write client tests**

Create `bkctl/tests/client_test.rs`. This file contains the TestServer utility and all client-layer tests.

```rust
//! Client-layer tests: exercise every gRPC RPC against a real barkeeper.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

// Re-use barkeeper internals to spin up a test instance.
use barkeeper::api::gateway;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::apply_broker::ApplyResultBroker;
use barkeeper::kv::apply_notifier::ApplyNotifier;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::write_buffer::WriteBuffer;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;

/// Starts a real barkeeper instance on a random port. Returns the gRPC endpoint URL.
/// The TempDir handle keeps the data directory alive until dropped.
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

        // Start gRPC server using the barkeeper gRPC services directly.
        // We need both HTTP gateway (for compatibility) and gRPC server.
        let grpc_port = portpicker::pick_unused_port().expect("no free port");
        let grpc_addr: SocketAddr = ([127, 0, 0, 1], grpc_port).into();

        // Build the HTTP gateway (which also handles gRPC).
        let app = gateway::create_router(
            raft_handle.clone(),
            store.clone(),
            Arc::clone(&kv_store),
            watch_hub.clone(),
            Arc::clone(&lease_manager),
            cluster_manager,
            1,
            1,
            Arc::clone(&raft_handle.current_term),
            auth_manager,
            Arc::new(std::sync::Mutex::new(vec![])),
            broker,
            notifier,
            write_buffer,
        );

        let listener = tokio::net::TcpListener::bind(grpc_addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server failed");
        });

        // Wait for Raft election.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Self {
            endpoint: format!("http://{}", bound_addr),
            _dir: dir,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

use bkctl::client::BkClient;

#[tokio::test]
async fn test_put_and_get() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    client.put(b"hello", b"world").await.unwrap();
    let kv = client.get(b"hello").await.unwrap();
    assert!(kv.is_some());
    assert_eq!(kv.unwrap().value, b"world");
}

#[tokio::test]
async fn test_get_missing_key() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    let kv = client.get(b"nonexistent").await.unwrap();
    assert!(kv.is_none());
}

#[tokio::test]
async fn test_delete() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    client.put(b"to-delete", b"val").await.unwrap();
    let deleted = client.delete(b"to-delete").await.unwrap();
    assert_eq!(deleted, 1);
    let kv = client.get(b"to-delete").await.unwrap();
    assert!(kv.is_none());
}

#[tokio::test]
async fn test_get_prefix() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    client.put(b"/app/a", b"1").await.unwrap();
    client.put(b"/app/b", b"2").await.unwrap();
    client.put(b"/other", b"3").await.unwrap();

    let kvs = client.get_prefix(b"/app/").await.unwrap();
    assert_eq!(kvs.len(), 2);
}

#[tokio::test]
async fn test_member_list() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    let members = client.member_list().await.unwrap();
    assert!(!members.is_empty());
    assert_eq!(members[0].name, "test-node");
}

#[tokio::test]
async fn test_status() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    let status = client.status().await.unwrap();
    assert!(status.leader > 0);
}

#[tokio::test]
async fn test_watch_receives_put_event() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();

    // Start watching before the put.
    let mut watch_rx = client.watch(b"/watched/").await.unwrap();

    // Give the watch stream time to establish.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Put a key that matches the watch prefix.
    client.put(b"/watched/key1", b"val1").await.unwrap();

    // Should receive the event within a reasonable timeout.
    let event = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        watch_rx.recv(),
    )
    .await
    .expect("watch timed out")
    .expect("watch channel closed");

    assert_eq!(event.kv.as_ref().unwrap().key, b"/watched/key1");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p bkctl --test client_test`
Expected: FAIL — `BkClient` doesn't exist yet

**Step 3: Implement BkClient**

Write `bkctl/src/client.rs`:

```rust
//! Thin async wrapper around tonic gRPC stubs for barkeeper.

use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::proto::etcdserverpb::{
    self,
    kv_client::KvClient,
    watch_client::WatchClient,
    cluster_client::ClusterClient,
    maintenance_client::MaintenanceClient,
    RangeRequest, PutRequest, DeleteRangeRequest,
    WatchRequest, WatchCreateRequest, watch_request,
    MemberListRequest, StatusRequest,
};
use crate::proto::mvccpb::{self, KeyValue, Event};

/// gRPC client for barkeeper.
#[derive(Clone)]
pub struct BkClient {
    kv: KvClient<Channel>,
    watch: WatchClient<Channel>,
    cluster: ClusterClient<Channel>,
    maintenance: MaintenanceClient<Channel>,
}

impl BkClient {
    /// Connect to a barkeeper endpoint.
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let channel = Channel::from_shared(endpoint.to_string())
            .unwrap()
            .connect()
            .await?;
        Ok(Self {
            kv: KvClient::new(channel.clone()),
            watch: WatchClient::new(channel.clone()),
            cluster: ClusterClient::new(channel.clone()),
            maintenance: MaintenanceClient::new(channel),
        })
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), tonic::Status> {
        let mut kv = self.kv.clone();
        kv.put(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    /// Get a single key. Returns None if not found.
    pub async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, tonic::Status> {
        let mut kv = self.kv.clone();
        let resp = kv
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.kvs.into_iter().next())
    }

    /// Get all keys with the given prefix.
    pub async fn get_prefix(&self, prefix: &[u8]) -> Result<Vec<KeyValue>, tonic::Status> {
        let mut kv = self.kv.clone();
        let range_end = prefix_range_end(prefix);
        let resp = kv
            .range(RangeRequest {
                key: prefix.to_vec(),
                range_end,
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.kvs)
    }

    /// Delete a single key. Returns the number of keys deleted.
    pub async fn delete(&self, key: &[u8]) -> Result<i64, tonic::Status> {
        let mut kv = self.kv.clone();
        let resp = kv
            .delete_range(DeleteRangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.deleted)
    }

    /// List cluster members.
    pub async fn member_list(
        &self,
    ) -> Result<Vec<etcdserverpb::Member>, tonic::Status> {
        let mut cluster = self.cluster.clone();
        let resp = cluster
            .member_list(MemberListRequest { linearizable: false })
            .await?
            .into_inner();
        Ok(resp.members)
    }

    /// Get cluster status.
    pub async fn status(&self) -> Result<etcdserverpb::StatusResponse, tonic::Status> {
        let mut maint = self.maintenance.clone();
        let resp = maint
            .status(StatusRequest {})
            .await?
            .into_inner();
        Ok(resp)
    }

    /// Start watching a prefix. Returns a channel that receives events.
    pub async fn watch(
        &self,
        prefix: &[u8],
    ) -> Result<mpsc::UnboundedReceiver<Event>, tonic::Status> {
        let mut watch = self.watch.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        let create_req = WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: prefix.to_vec(),
                    range_end: prefix_range_end(prefix),
                    ..Default::default()
                },
            )),
        };

        let (req_tx, req_rx) = mpsc::channel(1);
        req_tx.send(create_req).await.map_err(|_| {
            tonic::Status::internal("failed to send watch create request")
        })?;

        let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
        let mut resp_stream = watch.watch(req_stream).await?.into_inner();

        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            while let Some(Ok(resp)) = resp_stream.next().await {
                for event in resp.events {
                    if tx.send(event).is_err() {
                        return; // receiver dropped
                    }
                }
            }
        });

        Ok(rx)
    }
}

/// Compute the range end for a prefix scan (increment last byte).
fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // All 0xff — use empty (meaning no upper bound)
    vec![0]
}
```

**Step 4: Run client tests**

Run: `cargo test -p bkctl --test client_test`
Expected: all 7 tests PASS

**Step 5: Commit**

```bash
git add bkctl/src/client.rs bkctl/tests/client_test.rs
git commit -m "feat(bkctl): implement gRPC client layer with tests"
```

---

### Task 4: Implement State/Model Layer

**Files:**
- Modify: `bkctl/src/app.rs`
- Create: `bkctl/tests/app_test.rs`

**Step 1: Write state/model tests**

Create `bkctl/tests/app_test.rs`:

```rust
//! State/model layer tests — pure functions, no I/O, no server.

use bkctl::app::{App, Tab, WatchEvent, EventType};

#[test]
fn test_initial_state() {
    let app = App::new();
    assert_eq!(app.active_tab(), Tab::Dashboard);
    assert_eq!(app.current_prefix(), "/");
    assert!(app.watch_events().is_empty());
}

#[test]
fn test_tab_switching() {
    let mut app = App::new();
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Keys);
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Dashboard);
}

#[test]
fn test_prefix_navigation_descend() {
    let mut app = App::new();
    app.set_keys(vec![
        b"/a/1".to_vec(),
        b"/a/2".to_vec(),
        b"/b/1".to_vec(),
    ]);
    assert_eq!(app.visible_prefixes(), vec!["a/", "b/"]);
    app.descend("a/");
    assert_eq!(app.current_prefix(), "/a/");
    assert_eq!(app.visible_keys(), vec!["1", "2"]);
}

#[test]
fn test_prefix_navigation_ascend() {
    let mut app = App::new();
    app.set_keys(vec![b"/a/b/c".to_vec()]);
    app.descend("a/");
    app.descend("b/");
    assert_eq!(app.current_prefix(), "/a/b/");
    app.ascend();
    assert_eq!(app.current_prefix(), "/a/");
    app.ascend();
    assert_eq!(app.current_prefix(), "/");
}

#[test]
fn test_ascend_at_root_is_noop() {
    let mut app = App::new();
    app.ascend();
    assert_eq!(app.current_prefix(), "/");
}

#[test]
fn test_key_selection() {
    let mut app = App::new();
    app.set_keys(vec![b"/a".to_vec(), b"/b".to_vec(), b"/c".to_vec()]);
    assert_eq!(app.selected_index(), 0);
    app.move_down();
    assert_eq!(app.selected_index(), 1);
    app.move_down();
    assert_eq!(app.selected_index(), 2);
    app.move_down(); // should clamp
    assert_eq!(app.selected_index(), 2);
    app.move_up();
    assert_eq!(app.selected_index(), 1);
}

#[test]
fn test_watch_event_buffer() {
    let mut app = App::new();
    for i in 0..1100 {
        app.push_watch_event(WatchEvent {
            key: format!("/key/{}", i).into_bytes(),
            event_type: EventType::Put,
            value_size: 100,
        });
    }
    // Should cap at 1000.
    assert_eq!(app.watch_events().len(), 1000);
    // Most recent event should be last.
    assert_eq!(
        app.watch_events().last().unwrap().key,
        b"/key/1099"
    );
}

#[test]
fn test_search_filter() {
    let mut app = App::new();
    app.set_keys(vec![
        b"/app/nginx".to_vec(),
        b"/app/redis".to_vec(),
        b"/sys/kube".to_vec(),
    ]);
    app.set_search("nginx");
    let visible = app.filtered_keys();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0], b"/app/nginx");
    app.clear_search();
    assert_eq!(app.filtered_keys().len(), 3);
}

#[test]
fn test_set_members() {
    let mut app = App::new();
    app.set_members(vec![
        ("node-0".to_string(), 1, true),
        ("node-1".to_string(), 2, false),
    ]);
    assert_eq!(app.members().len(), 2);
    assert!(app.members()[0].is_leader);
}

#[test]
fn test_put_dialog() {
    let mut app = App::new();
    assert!(!app.is_dialog_open());
    app.open_put_dialog();
    assert!(app.is_dialog_open());
    app.set_dialog_key("test/key");
    app.set_dialog_value("hello");
    assert_eq!(app.dialog_key(), "test/key");
    assert_eq!(app.dialog_value(), "hello");
    app.close_dialog();
    assert!(!app.is_dialog_open());
}

#[test]
fn test_delete_confirmation() {
    let mut app = App::new();
    app.set_keys(vec![b"/a".to_vec()]);
    app.open_delete_confirm();
    assert!(app.is_delete_confirm_open());
    app.close_dialog();
    assert!(!app.is_delete_confirm_open());
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p bkctl --test app_test`
Expected: FAIL — `App` doesn't exist yet

**Step 3: Implement App state/model**

Write `bkctl/src/app.rs`:

```rust
//! TUI application state and transition functions.
//!
//! All state is plain data. All transitions are pure functions.
//! No I/O, no rendering, no async.

use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Dashboard,
    Keys,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub key: Vec<u8>,
    pub event_type: EventType,
    pub value_size: usize,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub name: String,
    pub id: u64,
    pub is_leader: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub leader_id: u64,
    pub raft_term: u64,
    pub member_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogKind {
    Put,
    DeleteConfirm,
}

const MAX_WATCH_EVENTS: usize = 1000;

pub struct App {
    // Tab state
    active_tab: Tab,

    // Keys tab state
    prefix: String,
    keys: Vec<Vec<u8>>,
    selected_index: usize,
    search: Option<String>,

    // Key detail (value of selected key)
    selected_value: Option<Vec<u8>>,
    selected_key_meta: Option<KeyMeta>,

    // Dashboard state
    members: Vec<MemberInfo>,
    cluster_status: Option<ClusterStatus>,
    watch_events: Vec<WatchEvent>,

    // Dialog state
    dialog: Option<DialogKind>,
    dialog_key: String,
    dialog_value: String,

    // Connection
    pub endpoint: String,
    pub watch_prefix: String,

    // Quit signal
    pub should_quit: bool,
}

#[derive(Debug, Clone)]
pub struct KeyMeta {
    pub revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

impl App {
    pub fn new() -> Self {
        Self {
            active_tab: Tab::Dashboard,
            prefix: "/".to_string(),
            keys: Vec::new(),
            selected_index: 0,
            search: None,
            selected_value: None,
            selected_key_meta: None,
            members: Vec::new(),
            cluster_status: None,
            watch_events: Vec::new(),
            dialog: None,
            dialog_key: String::new(),
            dialog_value: String::new(),
            endpoint: String::new(),
            watch_prefix: "/".to_string(),
            should_quit: false,
        }
    }

    // --- Tab ---

    pub fn active_tab(&self) -> Tab {
        self.active_tab
    }

    pub fn toggle_tab(&mut self) {
        self.active_tab = match self.active_tab {
            Tab::Dashboard => Tab::Keys,
            Tab::Keys => Tab::Dashboard,
        };
    }

    // --- Prefix navigation ---

    pub fn current_prefix(&self) -> &str {
        &self.prefix
    }

    pub fn set_keys(&mut self, keys: Vec<Vec<u8>>) {
        self.keys = keys;
        self.selected_index = 0;
    }

    /// Get visible sub-prefixes under the current prefix.
    pub fn visible_prefixes(&self) -> Vec<String> {
        let prefix = self.prefix.as_bytes();
        let mut prefixes = BTreeSet::new();
        for key in &self.keys {
            if key.starts_with(prefix) {
                let suffix = &key[prefix.len()..];
                if let Some(slash_pos) = suffix.iter().position(|&b| b == b'/') {
                    let sub = &suffix[..=slash_pos];
                    if let Ok(s) = std::str::from_utf8(sub) {
                        prefixes.insert(s.to_string());
                    }
                }
            }
        }
        prefixes.into_iter().collect()
    }

    /// Get visible leaf keys (no further slash) under the current prefix.
    pub fn visible_keys(&self) -> Vec<String> {
        let prefix = self.prefix.as_bytes();
        let mut result = Vec::new();
        for key in &self.keys {
            if key.starts_with(prefix) {
                let suffix = &key[prefix.len()..];
                if !suffix.contains(&b'/') && !suffix.is_empty() {
                    if let Ok(s) = std::str::from_utf8(suffix) {
                        result.push(s.to_string());
                    }
                }
            }
        }
        result
    }

    /// Total visible items (prefixes + keys) in the current view.
    pub fn visible_count(&self) -> usize {
        self.visible_prefixes().len() + self.visible_keys().len()
    }

    pub fn descend(&mut self, sub_prefix: &str) {
        self.prefix.push_str(sub_prefix);
        self.selected_index = 0;
    }

    pub fn ascend(&mut self) {
        if self.prefix == "/" {
            return;
        }
        // Remove trailing slash, then find the previous slash.
        let trimmed = &self.prefix[..self.prefix.len() - 1];
        if let Some(pos) = trimmed.rfind('/') {
            self.prefix = trimmed[..=pos].to_string();
        } else {
            self.prefix = "/".to_string();
        }
        self.selected_index = 0;
    }

    // --- Selection ---

    pub fn selected_index(&self) -> usize {
        self.selected_index
    }

    pub fn move_down(&mut self) {
        let max = self.visible_count().saturating_sub(1);
        if self.selected_index < max {
            self.selected_index += 1;
        }
    }

    pub fn move_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    pub fn selected_value(&self) -> Option<&[u8]> {
        self.selected_value.as_deref()
    }

    pub fn set_selected_value(&mut self, value: Option<Vec<u8>>) {
        self.selected_value = value;
    }

    pub fn selected_key_meta(&self) -> Option<&KeyMeta> {
        self.selected_key_meta.as_ref()
    }

    pub fn set_selected_key_meta(&mut self, meta: Option<KeyMeta>) {
        self.selected_key_meta = meta;
    }

    /// Get the full key path of the currently selected item, if it's a leaf key.
    pub fn selected_full_key(&self) -> Option<String> {
        let prefixes = self.visible_prefixes();
        let keys = self.visible_keys();
        if self.selected_index < prefixes.len() {
            None // selected a prefix, not a leaf
        } else {
            let key_idx = self.selected_index - prefixes.len();
            keys.get(key_idx).map(|k| format!("{}{}", self.prefix, k))
        }
    }

    // --- Watch events ---

    pub fn watch_events(&self) -> &[WatchEvent] {
        &self.watch_events
    }

    pub fn push_watch_event(&mut self, event: WatchEvent) {
        self.watch_events.push(event);
        if self.watch_events.len() > MAX_WATCH_EVENTS {
            let drain = self.watch_events.len() - MAX_WATCH_EVENTS;
            self.watch_events.drain(..drain);
        }
    }

    // --- Members ---

    pub fn members(&self) -> &[MemberInfo] {
        &self.members
    }

    pub fn set_members(&mut self, members: Vec<(String, u64, bool)>) {
        self.members = members
            .into_iter()
            .map(|(name, id, is_leader)| MemberInfo {
                name,
                id,
                is_leader,
            })
            .collect();
    }

    pub fn cluster_status(&self) -> Option<&ClusterStatus> {
        self.cluster_status.as_ref()
    }

    pub fn set_cluster_status(&mut self, status: ClusterStatus) {
        self.cluster_status = Some(status);
    }

    // --- Search ---

    pub fn set_search(&mut self, query: &str) {
        self.search = Some(query.to_string());
    }

    pub fn clear_search(&mut self) {
        self.search = None;
    }

    pub fn search_query(&self) -> Option<&str> {
        self.search.as_deref()
    }

    /// Get keys filtered by current search query (or all if no search).
    pub fn filtered_keys(&self) -> Vec<Vec<u8>> {
        match &self.search {
            None => self.keys.clone(),
            Some(query) => {
                let q = query.as_bytes();
                self.keys
                    .iter()
                    .filter(|k| k.windows(q.len()).any(|w| w == q))
                    .cloned()
                    .collect()
            }
        }
    }

    // --- Dialogs ---

    pub fn is_dialog_open(&self) -> bool {
        self.dialog.is_some()
    }

    pub fn is_delete_confirm_open(&self) -> bool {
        self.dialog == Some(DialogKind::DeleteConfirm)
    }

    pub fn open_put_dialog(&mut self) {
        self.dialog = Some(DialogKind::Put);
        self.dialog_key.clear();
        self.dialog_value.clear();
    }

    pub fn open_delete_confirm(&mut self) {
        self.dialog = Some(DialogKind::DeleteConfirm);
    }

    pub fn close_dialog(&mut self) {
        self.dialog = None;
    }

    pub fn dialog_kind(&self) -> Option<DialogKind> {
        self.dialog
    }

    pub fn dialog_key(&self) -> &str {
        &self.dialog_key
    }

    pub fn dialog_value(&self) -> &str {
        &self.dialog_value
    }

    pub fn set_dialog_key(&mut self, key: &str) {
        self.dialog_key = key.to_string();
    }

    pub fn set_dialog_value(&mut self, value: &str) {
        self.dialog_value = value.to_string();
    }
}
```

**Step 4: Run tests**

Run: `cargo test -p bkctl --test app_test`
Expected: all 11 tests PASS

**Step 5: Commit**

```bash
git add bkctl/src/app.rs bkctl/tests/app_test.rs
git commit -m "feat(bkctl): implement state/model layer with tests"
```

---

### Task 5: Implement Rendering Layer

**Files:**
- Modify: `bkctl/src/ui.rs`
- Create: `bkctl/tests/ui_test.rs`

**Step 1: Write rendering tests**

Create `bkctl/tests/ui_test.rs`:

```rust
//! Rendering layer tests — use TestBackend to verify TUI output.

use ratatui::backend::TestBackend;
use ratatui::Terminal;

use bkctl::app::{App, Tab, MemberInfo, ClusterStatus, WatchEvent, EventType};
use bkctl::ui;

fn buffer_to_string(buf: &ratatui::buffer::Buffer) -> String {
    let mut s = String::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            s.push(buf.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' '));
        }
        s.push('\n');
    }
    s
}

#[test]
fn test_dashboard_renders_members() {
    let mut app = App::new();
    app.set_members(vec![
        ("node-0".to_string(), 1, true),
        ("node-1".to_string(), 2, false),
    ]);
    app.set_cluster_status(ClusterStatus {
        leader_id: 1,
        raft_term: 42,
        member_count: 2,
    });

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal
        .draw(|f| ui::draw(f, &app))
        .unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("node-0"), "should show member name");
    assert!(content.contains("Leader"), "should show leader status");
    assert!(content.contains("node-1"), "should show second member");
}

#[test]
fn test_dashboard_renders_watch_events() {
    let mut app = App::new();
    app.push_watch_event(WatchEvent {
        key: b"/test/key".to_vec(),
        event_type: EventType::Put,
        value_size: 1024,
    });

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("/test/key"), "should show watch event key");
    assert!(content.contains("PUT"), "should show event type");
}

#[test]
fn test_keys_tab_renders_prefixes_and_keys() {
    let mut app = App::new();
    app.toggle_tab(); // Switch to Keys tab
    app.set_keys(vec![
        b"/app/web".to_vec(),
        b"/app/api".to_vec(),
        b"/sys/core".to_vec(),
    ]);

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("app/"), "should show app/ prefix");
    assert!(content.contains("sys/"), "should show sys/ prefix");
}

#[test]
fn test_tab_indicator_shows_active() {
    let mut app = App::new();
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);
    assert!(content.contains("Dashboard"), "should show Dashboard tab");
    assert!(content.contains("Keys"), "should show Keys tab");
}

#[test]
fn test_status_line_shows_endpoint() {
    let mut app = App::new();
    app.endpoint = "http://127.0.0.1:2379".to_string();

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(
        content.contains("127.0.0.1:2379"),
        "should show endpoint in status line"
    );
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p bkctl --test ui_test`
Expected: FAIL — `ui::draw` doesn't exist yet

**Step 3: Implement rendering**

Write `bkctl/src/ui.rs`:

```rust
//! Stateless rendering functions for the TUI.
//!
//! All functions take an immutable `&App` reference and draw to the frame.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Tabs, Wrap,
};
use ratatui::Frame;

use crate::app::{App, DialogKind, EventType, Tab};

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // tab bar
            Constraint::Min(10),   // main content
            Constraint::Length(1), // status line
        ])
        .split(f.area());

    draw_tab_bar(f, app, chunks[0]);

    match app.active_tab() {
        Tab::Dashboard => draw_dashboard(f, app, chunks[1]),
        Tab::Keys => draw_keys(f, app, chunks[1]),
    }

    draw_status_line(f, app, chunks[2]);

    // Draw dialog overlay if open.
    if let Some(kind) = app.dialog_kind() {
        draw_dialog(f, app, kind);
    }
}

fn draw_tab_bar(f: &mut Frame, app: &App, area: Rect) {
    let titles = vec!["Dashboard", "Keys"];
    let selected = match app.active_tab() {
        Tab::Dashboard => 0,
        Tab::Keys => 1,
    };
    let tabs = Tabs::new(titles)
        .select(selected)
        .block(Block::default().borders(Borders::ALL).title("bkctl"))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    f.render_widget(tabs, area);
}

fn draw_dashboard(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // cluster summary
            Constraint::Min(5),    // members table
            Constraint::Min(5),    // watch events
        ])
        .split(area);

    // Cluster summary
    let status_text = if let Some(status) = app.cluster_status() {
        format!(
            " Cluster: {} nodes │ Leader: {} │ Raft term: {}",
            status.member_count, status.leader_id, status.raft_term
        )
    } else {
        " Connecting...".to_string()
    };
    let summary = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(summary, chunks[0]);

    // Members table
    let header = Row::new(vec!["ID", "Name", "Status"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = app
        .members()
        .iter()
        .map(|m| {
            let status = if m.is_leader { "Leader" } else { "Follower" };
            Row::new(vec![
                Cell::from(m.id.to_string()),
                Cell::from(m.name.clone()),
                Cell::from(status),
            ])
        })
        .collect();
    let table = Table::new(rows, [Constraint::Length(6), Constraint::Min(20), Constraint::Length(10)])
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Members"));
    f.render_widget(table, chunks[1]);

    // Watch events
    let items: Vec<ListItem> = app
        .watch_events()
        .iter()
        .rev()
        .take(chunks[2].height.saturating_sub(2) as usize)
        .map(|e| {
            let type_str = match e.event_type {
                EventType::Put => "PUT",
                EventType::Delete => "DEL",
            };
            let key = String::from_utf8_lossy(&e.key);
            let line = format!(" {} {:40} ({}B)", type_str, key, e.value_size);
            ListItem::new(line)
        })
        .collect();
    let events_list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Watch Events [prefix: {}]", app.watch_prefix)),
    );
    f.render_widget(events_list, chunks[2]);
}

fn draw_keys(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // split pane
            Constraint::Length(1), // keybindings bar
        ])
        .split(area);

    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(chunks[0]);

    // Left pane: prefix tree
    let mut items = Vec::new();
    let prefixes = app.visible_prefixes();
    let keys = app.visible_keys();

    for (i, p) in prefixes.iter().enumerate() {
        let style = if i == app.selected_index() {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Cyan)
        };
        items.push(ListItem::new(format!("  {}", p)).style(style));
    }
    for (i, k) in keys.iter().enumerate() {
        let idx = prefixes.len() + i;
        let style = if idx == app.selected_index() {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let marker = if idx == app.selected_index() { "> " } else { "  " };
        items.push(ListItem::new(format!("{}{}", marker, k)).style(style));
    }

    let left = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Path: {}", app.current_prefix())),
    );
    f.render_widget(left, panes[0]);

    // Right pane: value display
    let value_content = if let Some(value) = app.selected_value() {
        // Try pretty-printing as JSON.
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) {
            serde_json::to_string_pretty(&json).unwrap_or_else(|_| {
                String::from_utf8_lossy(value).to_string()
            })
        } else {
            String::from_utf8_lossy(value).to_string()
        }
    } else {
        "Select a key to view its value".to_string()
    };

    let mut right_lines = Vec::new();
    if let Some(key) = app.selected_full_key() {
        right_lines.push(Line::from(vec![
            Span::styled("Key: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&key),
        ]));
    }
    if let Some(meta) = app.selected_key_meta() {
        right_lines.push(Line::from(format!(
            "Rev: {}  Mod: {}  Ver: {}  Lease: {}",
            meta.revision, meta.mod_revision, meta.version, meta.lease
        )));
        right_lines.push(Line::from(""));
    }
    for line in value_content.lines() {
        right_lines.push(Line::from(line.to_string()));
    }

    let right = Paragraph::new(right_lines)
        .block(Block::default().borders(Borders::ALL).title("Value"))
        .wrap(Wrap { trim: false });
    f.render_widget(right, panes[1]);

    // Keybindings bar
    let bindings = Paragraph::new(Line::from(vec![
        Span::styled("[p]", Style::default().fg(Color::Yellow)),
        Span::raw("ut "),
        Span::styled("[d]", Style::default().fg(Color::Yellow)),
        Span::raw("elete "),
        Span::styled("[/]", Style::default().fg(Color::Yellow)),
        Span::raw("search "),
        Span::styled("[r]", Style::default().fg(Color::Yellow)),
        Span::raw("efresh "),
        Span::styled("[q]", Style::default().fg(Color::Yellow)),
        Span::raw("uit"),
    ]));
    f.render_widget(bindings, chunks[1]);
}

fn draw_status_line(f: &mut Frame, app: &App, area: Rect) {
    let text = if app.endpoint.is_empty() {
        " bkctl".to_string()
    } else {
        format!(" bkctl │ {}", app.endpoint)
    };
    let status = Paragraph::new(text).style(
        Style::default()
            .bg(Color::DarkGray)
            .fg(Color::White),
    );
    f.render_widget(status, area);
}

fn draw_dialog(f: &mut Frame, app: &App, kind: DialogKind) {
    let area = centered_rect(60, 20, f.area());

    // Clear the area.
    let clear = Block::default().style(Style::default().bg(Color::Black));
    f.render_widget(clear, area);

    match kind {
        DialogKind::Put => {
            let text = format!(
                "Key:   {}\nValue: {}\n\n[Enter] confirm  [Esc] cancel",
                app.dialog_key(),
                app.dialog_value()
            );
            let dialog = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Put Key"))
                .wrap(Wrap { trim: false });
            f.render_widget(dialog, area);
        }
        DialogKind::DeleteConfirm => {
            let key_name = app.selected_full_key().unwrap_or_default();
            let text = format!(
                "Delete key: {}?\n\n[y] yes  [n/Esc] cancel",
                key_name
            );
            let dialog = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Confirm Delete"))
                .wrap(Wrap { trim: false });
            f.render_widget(dialog, area);
        }
    }
}

/// Helper to create a centered rectangle.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
```

**Step 4: Run rendering tests**

Run: `cargo test -p bkctl --test ui_test`
Expected: all 5 tests PASS

**Step 5: Commit**

```bash
git add bkctl/src/ui.rs bkctl/tests/ui_test.rs
git commit -m "feat(bkctl): implement rendering layer with tests"
```

---

### Task 6: Wire Up Main Binary with Event Loop

**Files:**
- Modify: `bkctl/src/main.rs`
- Modify: `bkctl/src/lib.rs` (add `event` module)
- Create: `bkctl/src/event.rs`

**Step 1: Create event handler**

Write `bkctl/src/event.rs`:

```rust
//! Terminal event handling — maps crossterm events to app state transitions.

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use std::time::Duration;

use crate::app::{App, DialogKind, Tab};

/// Poll for a terminal event with the given timeout.
/// Returns None if no event within the timeout.
pub fn poll_event(timeout: Duration) -> Option<Event> {
    if event::poll(timeout).ok()? {
        event::read().ok()
    } else {
        None
    }
}

/// Handle a key event, mutating app state. Returns true if the event was consumed.
pub fn handle_key_event(app: &mut App, code: KeyCode, modifiers: KeyModifiers) -> bool {
    // Dialog mode takes priority.
    if app.is_dialog_open() {
        return handle_dialog_key(app, code);
    }

    match code {
        KeyCode::Char('q') | KeyCode::Esc => {
            app.should_quit = true;
            true
        }
        KeyCode::Tab => {
            app.toggle_tab();
            true
        }
        KeyCode::Char('j') | KeyCode::Down => {
            app.move_down();
            true
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.move_up();
            true
        }
        KeyCode::Enter => {
            if app.active_tab() == Tab::Keys {
                // If selected item is a prefix, descend.
                let prefixes = app.visible_prefixes();
                if app.selected_index() < prefixes.len() {
                    let p = prefixes[app.selected_index()].clone();
                    app.descend(&p);
                }
                // Otherwise it's a leaf key — triggers value fetch (handled by main loop).
            }
            true
        }
        KeyCode::Backspace => {
            if app.active_tab() == Tab::Keys {
                app.ascend();
            }
            true
        }
        KeyCode::Char('p') => {
            if app.active_tab() == Tab::Keys {
                app.open_put_dialog();
            }
            true
        }
        KeyCode::Char('d') => {
            if app.active_tab() == Tab::Keys {
                app.open_delete_confirm();
            }
            true
        }
        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
            true
        }
        _ => false,
    }
}

fn handle_dialog_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Esc => {
            app.close_dialog();
            true
        }
        KeyCode::Enter => {
            // Confirm action — the main loop handles the actual RPC.
            true
        }
        _ => true, // consume all keys in dialog mode
    }
}
```

**Step 2: Update lib.rs**

Add `pub mod event;` to `bkctl/src/lib.rs`.

**Step 3: Implement main.rs**

Write `bkctl/src/main.rs`:

```rust
use std::io;
use std::time::Duration;

use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use bkctl::app::{App, ClusterStatus, EventType, Tab, WatchEvent};
use bkctl::client::BkClient;
use bkctl::event::handle_key_event;
use bkctl::ui;

#[derive(Parser)]
#[command(name = "bkctl", about = "TUI for exploring barkeeper clusters")]
struct Cli {
    /// Comma-separated gRPC endpoints
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoints: String,

    /// Initial watch/browse prefix
    #[arg(long, default_value = "/")]
    prefix: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Connect to barkeeper.
    let client = BkClient::connect(&cli.endpoints).await?;

    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initialize app state.
    let mut app = App::new();
    app.endpoint = cli.endpoints.clone();
    app.watch_prefix = cli.prefix.clone();

    // Start watch stream.
    let mut watch_rx = client.watch(cli.prefix.as_bytes()).await.ok();

    // Periodic refresh ticker.
    let mut last_refresh = std::time::Instant::now();
    let refresh_interval = Duration::from_secs(2);

    // Initial data load.
    refresh_dashboard(&client, &mut app).await;
    refresh_keys(&client, &mut app).await;

    loop {
        // Draw.
        terminal.draw(|f| ui::draw(f, &app))?;

        // Poll watch events (non-blocking).
        if let Some(ref mut rx) = watch_rx {
            while let Ok(event) = rx.try_recv() {
                let event_type = if event.r#type == 0 {
                    EventType::Put
                } else {
                    EventType::Delete
                };
                let kv = event.kv.as_ref();
                app.push_watch_event(WatchEvent {
                    key: kv.map(|k| k.key.clone()).unwrap_or_default(),
                    event_type,
                    value_size: kv.map(|k| k.value.len()).unwrap_or(0),
                });
            }
        }

        // Periodic refresh.
        if last_refresh.elapsed() >= refresh_interval {
            refresh_dashboard(&client, &mut app).await;
            if app.active_tab() == Tab::Keys {
                refresh_keys(&client, &mut app).await;
            }
            last_refresh = std::time::Instant::now();
        }

        // Handle input.
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                handle_key_event(&mut app, key.code, key.modifiers);

                // Handle actions that need async RPCs.
                match key.code {
                    KeyCode::Enter if app.active_tab() == Tab::Keys => {
                        // Fetch value for selected key.
                        if let Some(full_key) = app.selected_full_key() {
                            if let Ok(Some(kv)) = client.get(full_key.as_bytes()).await {
                                app.set_selected_value(Some(kv.value.clone()));
                                app.set_selected_key_meta(Some(bkctl::app::KeyMeta {
                                    revision: kv.create_revision,
                                    mod_revision: kv.mod_revision,
                                    version: kv.version,
                                    lease: kv.lease,
                                }));
                            }
                        }
                    }
                    KeyCode::Char('r') => {
                        refresh_dashboard(&client, &mut app).await;
                        refresh_keys(&client, &mut app).await;
                    }
                    _ => {}
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

async fn refresh_dashboard(client: &BkClient, app: &mut App) {
    if let Ok(members) = client.member_list().await {
        if let Ok(status) = client.status().await {
            let member_data: Vec<(String, u64, bool)> = members
                .iter()
                .map(|m| (m.name.clone(), m.id, m.id == status.leader))
                .collect();
            app.set_cluster_status(ClusterStatus {
                leader_id: status.leader,
                raft_term: status.raft_term,
                member_count: members.len(),
            });
            app.set_members(member_data);
        }
    }
}

async fn refresh_keys(client: &BkClient, app: &mut App) {
    if let Ok(kvs) = client.get_prefix(app.current_prefix().as_bytes()).await {
        let keys: Vec<Vec<u8>> = kvs.into_iter().map(|kv| kv.key).collect();
        app.set_keys(keys);
    }
}
```

**Step 4: Verify it compiles**

Run: `cargo build -p bkctl`
Expected: compiles successfully

**Step 5: Commit**

```bash
git add bkctl/src/event.rs bkctl/src/main.rs bkctl/src/lib.rs
git commit -m "feat(bkctl): wire up main binary with event loop and keybindings"
```

---

### Task 7: End-to-End Tests

**Files:**
- Create: `bkctl/tests/e2e_test.rs`

**Step 1: Write end-to-end tests**

Create `bkctl/tests/e2e_test.rs`. These tests use the TestServer from `client_test.rs` pattern (duplicated here for independence) and drive the App model through scripted flows without a terminal.

```rust
//! End-to-end tests: start barkeeper, connect App model, drive through user flows.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

use barkeeper::api::gateway;
use barkeeper::auth::actor::spawn_auth_actor;
use barkeeper::cluster::actor::spawn_cluster_actor;
use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::apply_broker::ApplyResultBroker;
use barkeeper::kv::apply_notifier::ApplyNotifier;
use barkeeper::kv::state_machine::spawn_state_machine;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::write_buffer::WriteBuffer;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::node::{spawn_raft_node, RaftConfig};
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;

use bkctl::app::{App, ClusterStatus, EventType, KeyMeta, Tab, WatchEvent};
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

        let port = portpicker::pick_unused_port().expect("no free port");
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let app = gateway::create_router(
            raft_handle.clone(),
            store.clone(),
            Arc::clone(&kv_store),
            watch_hub.clone(),
            Arc::clone(&lease_manager),
            cluster_manager,
            1, 1,
            Arc::clone(&raft_handle.current_term),
            auth_manager,
            Arc::new(std::sync::Mutex::new(vec![])),
            broker, notifier, write_buffer,
        );
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        Self {
            endpoint: format!("http://{}", bound),
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
    assert!(keys.contains(&"hello".to_string()));
    assert!(keys.contains(&"foo".to_string()));

    // Select "hello" and fetch its value.
    let prefixes = app.visible_prefixes();
    // hello and foo are leaf keys, find hello's index.
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

    // Load keys.
    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    app.descend("del/");
    assert!(app.visible_keys().contains(&"target".to_string()));

    // Delete via client.
    client.delete(b"/del/target").await.unwrap();

    // Refresh.
    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    assert!(!app.visible_keys().contains(&"target".to_string()));
}

#[tokio::test]
async fn test_e2e_watch_receives_events() {
    let server = TestServer::start().await;
    let client = BkClient::connect(&server.endpoint).await.unwrap();
    let mut app = App::new();

    // Start watching.
    let mut watch_rx = client.watch(b"/watched/").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Put a key.
    client.put(b"/watched/key1", b"val1").await.unwrap();

    // Receive event.
    let event = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        watch_rx.recv(),
    )
    .await
    .unwrap()
    .unwrap();

    // Push to app state as main loop would.
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

    // Set up some dashboard state.
    app.push_watch_event(WatchEvent {
        key: b"/event".to_vec(),
        event_type: EventType::Put,
        value_size: 10,
    });

    // Switch to Keys tab.
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Keys);

    // Put keys and browse.
    client.put(b"/x/y", b"z").await.unwrap();
    let kvs = client.get_prefix(b"/").await.unwrap();
    app.set_keys(kvs.iter().map(|kv| kv.key.clone()).collect());
    app.descend("x/");

    // Switch back to Dashboard.
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Dashboard);

    // Watch events preserved.
    assert_eq!(app.watch_events().len(), 1);

    // Switch to Keys — prefix preserved.
    app.toggle_tab();
    assert_eq!(app.current_prefix(), "/x/");
}
```

**Step 2: Run end-to-end tests**

Run: `cargo test -p bkctl --test e2e_test`
Expected: all 5 tests PASS

**Step 3: Commit**

```bash
git add bkctl/tests/e2e_test.rs
git commit -m "feat(bkctl): add end-to-end tests"
```

---

### Task 8: Final Verification and Cleanup

**Step 1: Run all bkctl tests**

Run: `cargo test -p bkctl`
Expected: all tests pass (7 client + 11 model + 5 rendering + 5 e2e = 28 tests)

**Step 2: Run all barkeeper tests (no regressions)**

Run: `cargo test -p barkeeper`
Expected: all existing tests pass

**Step 3: Build both crates**

Run: `cargo build`
Expected: both barkeeper and bkctl compile

**Step 4: Verify binary runs**

Run: `cargo run -p bkctl -- --help`
Expected: prints help text with `--endpoints` and `--prefix` options

**Step 5: Commit any cleanup**

```bash
git add -A
git commit -m "chore(bkctl): final cleanup and verification"
```

**Step 6: Push**

```bash
git push
```
