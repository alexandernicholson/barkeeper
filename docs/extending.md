# Extending Barkeeper

This guide covers the main extension points in barkeeper: adding gRPC services,
actor processes, Raft transports, HTTP gateway endpoints, and compatibility tests.

All paths are relative to the repository root unless noted otherwise.

---

## 1. Adding a New gRPC Service

Barkeeper uses [tonic](https://github.com/hyperium/tonic) for gRPC with proto
definitions compiled by `tonic-build` at build time. The existing services
(KV, Watch, Lease, Cluster, Maintenance, Auth) each follow the same pattern.

### Step 1: Define proto messages

Add your service definition and messages to `proto/etcdserverpb/rpc.proto`
(or create a new `.proto` file). For example:

```protobuf
service MyService {
  rpc MyMethod(MyRequest) returns (MyResponse) {}
}

message MyRequest {
  bytes key = 1;
}

message MyResponse {
  ResponseHeader header = 1;
  string result = 2;
}
```

If you create a new `.proto` file, register it in `build.rs` alongside the
existing protos:

```rust
tonic_build::configure()
    .build_server(true)
    .build_client(false)
    .compile_protos(
        &[
            "proto/etcdserverpb/rpc.proto",
            "proto/etcdserverpb/kv.proto",
            "proto/authpb/auth.proto",
            "proto/myservicepb/my_service.proto", // <-- new
        ],
        &["proto/"],
    )?;
```

### Step 2: Regenerate Rust code

Run `cargo build`. The `build.rs` script invokes `tonic-build` which reads
the `.proto` files and generates Rust types and a server trait under the
`crate::proto` module tree. The generated trait for the example above would
be `my_service_server::MyService`.

### Step 3: Create the service struct

Create `src/api/my_service.rs`. Follow the pattern established by
`src/api/kv_service.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto::etcdserverpb::my_service_server::MyService as MyServiceTrait;
use crate::proto::etcdserverpb::{MyRequest, MyResponse, ResponseHeader};

pub struct MyService {
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}

impl MyService {
    pub fn new(
        cluster_id: u64,
        member_id: u64,
        raft_term: Arc<AtomicU64>,
    ) -> Self {
        MyService { cluster_id, member_id, raft_term }
    }

    fn make_header(&self, revision: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: self.raft_term.load(Ordering::Relaxed),
        })
    }
}

#[tonic::async_trait]
impl MyServiceTrait for MyService {
    async fn my_method(
        &self,
        request: Request<MyRequest>,
    ) -> Result<Response<MyResponse>, Status> {
        let req = request.into_inner();
        // ... implementation ...
        Ok(Response::new(MyResponse {
            header: self.make_header(0),
            result: "ok".to_string(),
        }))
    }
}
```

Key conventions:
- The service struct holds `Arc` references to shared state (store, watch hub,
  lease manager, etc.) plus `cluster_id`, `member_id`, and `raft_term`.
- `make_header()` produces a `ResponseHeader` with the current Raft term.
- Errors are returned as `tonic::Status` (e.g., `Status::internal(...)`).

### Step 4: Wire into the server

In `src/api/server.rs`, import your service and its generated server wrapper,
then add it to the tonic `Server::builder()` chain:

```rust
use crate::api::my_service::MyService;
use crate::proto::etcdserverpb::my_service_server::MyServiceServer;

// Inside BarkeepServer::start():
let my_service = MyService::new(cluster_id, member_id, Arc::clone(&raft_term));

Server::builder()
    .add_service(KvServer::new(kv_service))
    .add_service(WatchServer::new(watch_service))
    // ... existing services ...
    .add_service(MyServiceServer::new(my_service))  // <-- new
    .serve(addr)
    .await?;
```

Don't forget to add `pub mod my_service;` to `src/api/mod.rs`.

---

## 2. Adding a New Actor Process

Barkeeper uses the actor-per-concern pattern with typed command channels.
Each actor is a long-running tokio task that receives commands via
`mpsc::Sender<T>` and communicates results back through `oneshot` channels.

### Step 1: Define the command enum

Add your command type in `src/actors/commands.rs`. Follow the existing pattern
of typed enums with `oneshot::Sender` reply channels:

```rust
use tokio::sync::oneshot;

/// Commands sent to the MyProcess actor.
pub enum MyCmd {
    /// Do something and return the result.
    DoWork {
        input: Vec<u8>,
        reply: oneshot::Sender<Result<MyResult, String>>,
    },
    /// Fire-and-forget notification.
    Notify {
        data: String,
    },
}

pub struct MyResult {
    pub output: Vec<u8>,
}
```

The existing command enums demonstrate two patterns:
- **Request-reply**: include a `reply: oneshot::Sender<Result<T, E>>` field
  (see `RaftCmd::Propose`, `StoreCmd::Apply`).
- **Fire-and-forget**: no reply channel (see `WatchCmd::Notify`).

### Step 2: Create the process module

Create `src/actors/my_process.rs`. The process is a `spawn` function that
returns the command sender:

```rust
use tokio::sync::mpsc;
use crate::actors::commands::MyCmd;

/// Spawn the MyProcess actor. Returns a sender for submitting commands.
pub fn spawn_my_process() -> mpsc::Sender<MyCmd> {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<MyCmd>(256);

    tokio::spawn(async move {
        my_process_loop(&mut cmd_rx).await;
    });

    cmd_tx
}

async fn my_process_loop(cmd_rx: &mut mpsc::Receiver<MyCmd>) {
    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    MyCmd::DoWork { input, reply } => {
                        let result = MyResult { output: input };
                        let _ = reply.send(Ok(result));
                    }
                    MyCmd::Notify { data } => {
                        tracing::info!(%data, "notification received");
                    }
                }
            }
        }
    }
}
```

The architecture for an actor process looks like this:

```
                       mpsc::channel(256)
                      +-----------------+
  Callers ----------->| cmd_tx | cmd_rx |-------> tokio::spawn loop
  (services,          +-----------------+           |
   other actors)            |                       v
                            |              match on MyCmd variants
                            |                       |
                            |              oneshot::Sender::send()
                            |                       |
                            v                       v
                      mpsc::Sender<MyCmd>    reply to caller
```

Key conventions:
- Channel buffer size is 256 (matching `RaftCmd` and `StoreCmd`).
- The process loop uses `tokio::select!` to multiplex commands with timers
  or other async sources (see `raft_process.rs` for election/heartbeat timers).
- The spawn function returns `mpsc::Sender<MyCmd>` which callers clone as needed.

### Step 3: Register with the supervisor

In `src/api/server.rs`, the Rebar supervisor is initialized with a
`SupervisorSpec`:

```rust
let supervisor_spec = SupervisorSpec::new(RestartStrategy::OneForAll)
    .max_restarts(5)
    .max_seconds(30);

let _supervisor = start_supervisor(runtime.clone(), supervisor_spec, vec![]).await;
```

Currently actor processes are spawned directly via `tokio::spawn`. To register
your process with the supervisor for automatic restarts, you would add a
`ChildEntry` to the supervisor's child list. The Rebar `SupervisorSpec`
accepts a `Vec<ChildEntry>` as its third argument to `start_supervisor()`.

### Step 4: Update `src/actors/mod.rs`

Add your module:

```rust
pub mod commands;
pub mod raft_process;
pub mod my_process;  // <-- new
```

---

## 3. Implementing a Custom RaftTransport

The `RaftTransport` trait in `src/raft/transport.rs` abstracts how Raft
messages are sent between nodes. Implementing a custom transport allows you
to use different networking layers.

### The trait

```rust
#[async_trait::async_trait]
pub trait RaftTransport: Send + Sync {
    /// Send a Raft message to the node identified by `to`.
    async fn send(&self, to: u64, message: RaftMessage);
}
```

`RaftMessage` is an enum wrapping all Raft RPCs:

```rust
pub enum RaftMessage {
    AppendEntriesReq(AppendEntriesRequest),
    AppendEntriesResp(AppendEntriesResponse),
    RequestVoteReq(RequestVoteRequest),
    RequestVoteResp(RequestVoteResponse),
    InstallSnapshotReq(InstallSnapshotRequest),
    InstallSnapshotResp(InstallSnapshotResponse),
}
```

Messages are serialized to JSON via `serde_json` and can be encoded into
`rmpv::Value::Binary` for Rebar messaging using the provided
`encode_raft_message()` / `decode_raft_message()` helpers in
`src/raft/messages.rs`.

### The send/recv pattern

The transport is injected into `spawn_raft_process()` as
`Option<Arc<dyn RaftTransport>>`. When the Raft core emits an
`Action::SendMessage { to, message }`, the process loop calls
`transport.send(to, message)`.

For receiving, the existing `LocalTransport` registers each node with an
`mpsc::Sender<(u64, RaftMessage)>`. Inbound messages are fed back into the
Raft core via `Event` variants (e.g., `Event::AppendEntries`,
`Event::RequestVote`).

The data flow:

```
  Node A                              Node B
  ------                              ------
  RaftCore                            RaftCore
    | Action::SendMessage               ^
    v                                   |
  RaftProcess                     RaftProcess
    | transport.send(B, msg)        cmd_rx.recv()
    v                                   ^
  RaftTransport impl  --- network -->  inbound channel
```

### Example: QUIC transport with Rebar

A QUIC-based transport for production multi-node clusters would:

1. Use Rebar's `DistributedRouter` for node discovery and addressing.
2. Serialize `RaftMessage` using `encode_raft_message()` (JSON inside
   MessagePack binary).
3. Send via QUIC streams for low-latency, multiplexed delivery.
4. On the receiving side, decode with `decode_raft_message()` and forward
   to the node's command channel.

```rust
use std::sync::Arc;
use async_trait::async_trait;
use crate::raft::messages::{RaftMessage, encode_raft_message};
use crate::raft::transport::RaftTransport;

pub struct QuicTransport {
    router: Arc<rebar_cluster::DistributedRouter>,
}

#[async_trait]
impl RaftTransport for QuicTransport {
    async fn send(&self, to: u64, message: RaftMessage) {
        let payload = encode_raft_message(&message);
        self.router.send(to, payload).await;
    }
}
```

### Existing implementation: LocalTransport

The `LocalTransport` in `src/raft/transport.rs` routes messages in-process
using a `HashMap<u64, mpsc::Sender<(u64, RaftMessage)>>`. It is used for
testing multi-node clusters in a single process:

```rust
let transport = LocalTransport::new();
transport.register(1, node1_tx).await;
transport.register(2, node2_tx).await;
// Messages from node 1 to node 2 go through the in-memory channel.
```

---

## 4. Adding HTTP Gateway Endpoints

The HTTP/JSON gateway in `src/api/gateway.rs` mirrors etcd's grpc-gateway,
accepting JSON POST requests and returning proto3-compatible JSON responses.
It is built with [axum](https://github.com/tokio-rs/axum).

### Handler pattern

Every handler follows the same structure:

```rust
async fn handle_my_endpoint(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: MyRequest = parse_json(&body);
    let key = decode_b64(&req.key);

    match do_something(&state, &key) {
        Ok(result) => {
            let rev = state.store.current_revision().unwrap_or(0);
            axum::Json(MyResponse {
                header: state.make_header(rev),
                // ... fields ...
            })
            .into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("my_endpoint: {}", e),
        )
        .into_response(),
    }
}
```

Key helpers:
- `parse_json(&body)` -- Deserializes JSON without requiring a Content-Type
  header. Returns `T::default()` for empty bodies.
- `decode_b64(&option_string)` -- Decodes a base64-encoded `Option<String>`
  to `Vec<u8>`, matching etcd's convention for byte fields.
- `proto_kv_to_json(&kv)` -- Converts a proto `KeyValue` to the JSON
  representation with base64-encoded key/value fields.
- `state.make_header(revision)` -- Builds a `JsonResponseHeader` with
  current cluster ID, member ID, and Raft term.
- `json_error(status, msg)` -- Returns a JSON error body matching etcd's
  error format.

### GatewayState

All handlers share `GatewayState` via axum's state extraction:

```rust
#[derive(Clone)]
pub struct GatewayState {
    pub store: Arc<KvStore>,
    pub watch_hub: Arc<WatchHub>,
    pub lease_manager: Arc<LeaseManager>,
    pub cluster_manager: Arc<ClusterManager>,
    pub cluster_id: u64,
    pub member_id: u64,
    pub raft_term: Arc<AtomicU64>,
}
```

If your endpoint needs additional shared state, add it here and thread it
through from `create_router()` and `BarkeepServer::start()`.

### Wire into the router

Add your route in the `create_router()` function:

```rust
pub fn create_router(/* ... */) -> Router {
    // ...
    Router::new()
        .route("/v3/kv/range", post(handle_range))
        .route("/v3/kv/put", post(handle_put))
        // ... existing routes ...
        .route("/v3/my/endpoint", post(handle_my_endpoint))  // <-- new
        .with_state(state)
}
```

### Proto3 JSON conventions

Barkeeper follows etcd's proto3 canonical JSON mapping. When adding new
response types, apply these rules:

- **int64/uint64 fields**: Serialize as strings using `#[serde(serialize_with = "ser_str_i64")]`
  or `#[serde(serialize_with = "ser_str_u64")]`.
- **Default value omission**: Fields with zero/false/empty values are omitted
  using `#[serde(skip_serializing_if = "...")]`.
- **Byte fields**: Base64-encoded in both requests and responses.
- **camelCase field names**: Use `#[serde(rename = "fieldName")]` where the
  proto field name differs from Rust naming conventions.

Example response struct:

```rust
#[derive(Debug, Serialize)]
struct MyResponse {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    count: i64,
    #[serde(skip_serializing_if = "is_empty_vec")]
    items: Vec<JsonKeyValue>,
    #[serde(rename = "myField", skip_serializing_if = "is_false")]
    my_field: bool,
}
```

### Path conventions

Follow etcd's grpc-gateway URL structure:

```
/v3/kv/range          -- KV Range
/v3/kv/put            -- KV Put
/v3/kv/deleterange    -- KV DeleteRange
/v3/kv/txn            -- KV Txn
/v3/kv/compaction     -- KV Compaction
/v3/lease/grant       -- Lease Grant
/v3/lease/revoke      -- Lease Revoke
/v3/lease/timetolive  -- Lease TimeToLive
/v3/lease/leases      -- Lease Leases
/v3/cluster/member/list  -- Cluster MemberList
/v3/maintenance/status   -- Maintenance Status
```

All endpoints use `POST` method. This matches etcd's grpc-gateway behavior
where even read operations use POST with a JSON body.

---

## 5. Writing Compatibility Tests

Compatibility tests in `tests/compat_test.rs` verify behavioral parity with
etcd's v3 HTTP API. Each test starts a full barkeeper instance and makes HTTP
requests against it.

### Test instance setup

Use `start_test_instance()` to spin up a minimal barkeeper with Raft, KV store,
and HTTP gateway on random ports:

```rust
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
    let raft_handle = spawn_raft_node(config, apply_tx, None).await;

    let lease_manager = Arc::new(LeaseManager::new());
    let cluster_manager = Arc::new(ClusterManager::new(1));
    cluster_manager
        .add_initial_member(1, "test-node".to_string(), vec![], vec![])
        .await;

    let watch_hub = Arc::new(WatchHub::new());

    let app = gateway::create_router(
        raft_handle.clone(),
        Arc::clone(&store),
        Arc::clone(&watch_hub),
        Arc::clone(&lease_manager),
        Arc::clone(&cluster_manager),
        1, 1,
        Arc::clone(&raft_handle.current_term),
    );

    let http_port = portpicker::pick_unused_port().expect("no free port");
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));
    let listener = tokio::net::TcpListener::bind(http_addr).await.expect("bind");
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    // Wait for single-node Raft election.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    (bound_addr, dir)
}
```

Note: The `TempDir` is returned and held by the test to keep the data directory
alive for the duration of the test. Dropping it cleans up automatically.

### Writing a test

Each test follows the pattern: start instance, make HTTP requests, assert
JSON responses.

```rust
#[tokio::test]
async fn test_my_feature() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();
    let base = format!("http://{}", addr);

    // Write data.
    let resp = client
        .post(format!("{}/v3/kv/put", base))
        .json(&json!({"key": b64("mykey"), "value": b64("myval")}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.unwrap();

    // Verify response structure.
    assert!(body["header"].is_object());
    assert!(str_i64(&body["header"]["revision"]) > 0);

    // Read back and verify.
    let resp = client
        .post(format!("{}/v3/kv/range", base))
        .json(&json!({"key": b64("mykey")}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let kvs = body["kvs"].as_array().unwrap();
    assert_eq!(kvs.len(), 1);
    assert_eq!(b64_decode(kvs[0]["value"].as_str().unwrap()), "myval");
}
```

### Helper functions

The test file provides these helpers:

```rust
fn b64(s: &str) -> String {
    B64.encode(s.as_bytes())
}

fn b64_decode(s: &str) -> String {
    String::from_utf8(B64.decode(s).unwrap()).unwrap()
}

/// Parse string-encoded i64 (proto3 JSON convention).
fn str_i64(val: &Value) -> i64 {
    val.as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| val.as_i64())
        .expect("expected string-encoded i64")
}

/// Parse string-encoded u64 (proto3 JSON convention).
fn str_u64(val: &Value) -> u64 {
    val.as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| val.as_u64())
        .expect("expected string-encoded u64")
}
```

### Proto3 JSON conventions to follow in assertions

When writing assertions, keep these proto3 rules in mind:

- **Numeric fields are strings**: `cluster_id`, `member_id`, `revision`,
  `raft_term`, `version`, `create_revision`, `mod_revision`, `deleted`,
  `count`, `ID`, `TTL`, `grantedTTL`, `dbSize`, `leader`, `raftIndex`,
  `raftTerm`, `raftAppliedIndex`, `dbSizeInUse` are all serialized as
  JSON strings. Use `str_i64()` and `str_u64()` to parse them.
- **Default values are omitted**: Fields with value `0`, `false`, `""`, or
  `[]` should not appear in the response. Test for absence:
  ```rust
  assert!(body.get("deleted").is_none() || str_i64(&body["deleted"]) == 0);
  ```
- **Byte fields are base64**: `key`, `value`, and `keys` (in lease TTL
  responses) are base64-encoded strings.

### Benchmarking against real etcd

The `benchmark/` directory contains scripts for capturing reference output
from a real etcd instance and comparing it with barkeeper:

- `benchmark/run_etcd_benchmark.sh` -- Starts a single-node etcd and runs
  all API operations, saving JSON output to `benchmark/results/etcd/`.
- `benchmark/run_barkeeper_benchmark.sh` -- Runs the same operations against
  a barkeeper instance, saving to `benchmark/results/barkeeper/`.
- `benchmark/compare.sh` -- Diffs the results side by side.

To add a new benchmark case:

1. Add the `curl` command to both `run_etcd_benchmark.sh` and
   `run_barkeeper_benchmark.sh`.
2. Save output to matching filenames under `results/etcd/` and
   `results/barkeeper/`.
3. Run `benchmark/compare.sh` to verify parity.

### Running tests

```bash
# Run all compatibility tests
cargo test --test compat_test

# Run a specific test
cargo test --test compat_test test_kv_put_basic

# Run with output
cargo test --test compat_test -- --nocapture
```
