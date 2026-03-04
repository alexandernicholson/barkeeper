# Barkeeper Architecture

## System Overview

Barkeeper is an etcd-compatible distributed key-value store built on the
[Rebar](https://github.com/alexandernicholson/rebar) actor runtime. It aims to
provide full wire-level compatibility with etcd's gRPC and HTTP/JSON APIs while
using a fundamentally different internal architecture: supervised actor
processes communicating over typed channels rather than tightly-coupled
goroutines.

**Goals:**

- Drop-in replacement for etcd clients (gRPC + HTTP gateway)
- Raft consensus for linearizable writes
- MVCC storage with revision-based history
- Watch API for real-time change notification
- Lease-based TTL management
- RBAC authentication
- Crash recovery via Rebar supervision trees

**etcd API Surface Implemented:**

| API       | gRPC | HTTP Gateway |
|-----------|------|--------------|
| KV        | Yes  | Yes          |
| Watch     | Yes  | No (gRPC streaming only) |
| Lease     | Yes  | Yes          |
| Cluster   | Yes  | Yes          |
| Maintenance | Yes | Yes        |
| Auth      | Yes  | Yes          |

---

## Actor Process Tree

Barkeeper uses Rebar's `OneForAll` supervision strategy. If any child process
crashes, all siblings are restarted to maintain consistent state.

```
                    BarkeepSupervisor (OneForAll)
                    max_restarts=5, max_seconds=30
                               |
          +--------------------+--------------------+
          |                    |                    |
    RaftProcess          StateMachine          (future)
    consensus engine     apply loop
          |                    |
          v                    v
    LogStore (redb)       KvStore (redb)
    raft.redb             kv.redb
```

**Current actor layout:**

```
BarkeepSupervisor (OneForAll, max_restarts=5/30s)<br>
|<br>
+-- RaftProcess          -- Raft consensus engine (tokio::spawn)<br>
|   +-- RaftCore         -- pure Event->Action state machine<br>
|   +-- LogStore         -- durable log (redb: raft.redb)<br>
|<br>
+-- StateMachine         -- applies committed entries to KvStore<br>
|   +-- KvStore          -- MVCC store (redb: kv.redb)<br>
|<br>
+-- WatchHub             -- fan-out change notifications (Arc<WatchHub>)<br>
+-- LeaseManager         -- TTL tracking, expiry detection (Arc<LeaseManager>)<br>
+-- ClusterManager       -- membership tracking (Arc<ClusterManager>)<br>
+-- AuthManager          -- RBAC users/roles/permissions (Arc<AuthManager>)<br>
```

**Planned actor layout (full Rebar processes):**

```
BarkeepSupervisor (OneForAll)<br>
+-- RaftProcess          -- consensus engine<br>
+-- StoreProcess         -- MVCC KV store<br>
+-- WatchProcess         -- change notification<br>
+-- LeaseProcess         -- TTL management<br>
+-- ClusterProcess       -- membership<br>
+-- AuthProcess          -- RBAC<br>
```

Actor command enums are already defined in `src/actors/commands.rs`:
`RaftCmd`, `StoreCmd`, `WatchCmd`, `LeaseCmd`. Each uses `oneshot::Sender`
for request-response patterns.

---

## Write Path

All writes flow through Raft consensus before being applied to the KV store.
The service layer serializes a `KvCommand`, proposes it through `RaftHandle`,
waits for commit confirmation, then applies the mutation to the store and
notifies watchers.

```
Client (etcdctl / HTTP)
        |
        v
+------------------+
| gRPC / HTTP      |
| KvService.put()  |
+------------------+
        |
        v
+------------------+
| RaftHandle       |  <-- propose(serialized KvCommand)
| .propose(data)   |
+------------------+
        |
        | single-node: commits immediately
        | multi-node: replicates to followers, waits for quorum
        v
+------------------+
| ClientProposal   |
| Result::Success  |
+------------------+
        |
        | service applies after commit confirmation
        v
+------------------+
| KvStore.put()    |
| (redb txn)       |
+------------------+
        |
        +-------> WatchHub.notify()
        |         fan-out to matching watchers
        |
        +-------> LeaseManager.attach_key()
                  (if lease != 0)
```

The state machine (`spawn_state_machine`) receives committed entries for
observability logging but does not apply them — the service layer handles
application after Raft commit to avoid double-apply.

---

## Read Path

Reads are served directly from the KV store without going through Raft.
This matches etcd's serializable read behaviour (the default for `Range`).

```
Client (etcdctl / HTTP)
        |
        v
+------------------+
| gRPC / HTTP      |
| KvService.range()|
+------------------+
        |
        v
+------------------+
| KvStore.range()  |  <-- read-only redb txn
| (redb read txn)  |      supports revision parameter
+------------------+
        |
        v
    RangeResult {
        kvs: Vec<KeyValue>,
        count: i64,
        more: bool,
    }
```

Range queries support:

- **Single key:** `range_end` is empty
- **Key range:** keys in half-open interval `[key, range_end)`
- **All keys:** `key = "\x00"`, `range_end = "\x00"`
- **Historical reads:** non-zero `revision` parameter reads at that point in time
- **Pagination:** `limit` parameter with `more` flag in response

---

## Lease Expiry Flow

Leases are tracked in-memory by the `LeaseManager`. Each lease has a TTL
(seconds) and a set of attached keys. The expiry check runs on a timer.

```
Timer tick (periodic)
        |
        v
+------------------------+
| LeaseManager           |
| .check_expired()       |
+------------------------+
        |
        | returns Vec<ExpiredLease>
        | each: { lease_id, keys: Vec<Vec<u8>> }
        v
+------------------------+
| For each expired lease |
|   KvStore.delete_range |
|   per attached key     |
+------------------------+
        |
        v
+------------------------+
| WatchHub.notify()      |
| event_type=1 (DELETE)  |
| for each deleted key   |
+------------------------+
```

**LeaseManager internals:**

```
LeaseEntry {<br>
    id: i64,<br>
    ttl: i64,              // granted TTL in seconds<br>
    granted_at: Instant,   // reset on keepalive<br>
    keys: Vec<Vec<u8>>,    // attached keys<br>
}<br>
<br>
Expired when: granted_at.elapsed().as_secs() >= ttl<br>
```

Lease lifecycle:

1. `LeaseGrant` -- creates a `LeaseEntry` with TTL
2. `Put` with `lease != 0` -- calls `attach_key(lease_id, key)`
3. `LeaseKeepAlive` -- resets `granted_at` to `Instant::now()`
4. Timer fires `check_expired()` -- removes expired entries, returns keys
5. Keys are deleted from `KvStore`, watchers are notified

---

## MVCC Storage Model

Barkeeper implements Multi-Version Concurrency Control (MVCC) using compound
keys in redb. Every mutation creates a new revision, preserving history.

### Compound Key Format

```
+--------- user key bytes ----------+--+------- 8 bytes --------+
|          "my-key"                 |\0| revision (big-endian u64)|
+-----------------------------------+--+-------------------------+
```

- The `\x00` separator byte divides the user key from the revision
- Big-endian encoding ensures lexicographic ordering matches numeric ordering
- Multiple revisions of the same key sort together, latest last

**Helper functions:**

```rust
make_compound_key(key, revision) -> Vec<u8>   // build compound key
extract_user_key(compound)       -> &[u8]     // everything before last 9 bytes
extract_revision(compound)       -> u64       // last 8 bytes as BE u64
```

### redb Tables

| Table       | Key Type        | Value Type                     | Purpose                        |
|-------------|-----------------|--------------------------------|--------------------------------|
| `kv`        | `&[u8]`         | `&[u8]` (JSON InternalKeyValue)| MVCC key-value data            |
| `revisions` | `u64`           | `&[u8]` (JSON RevisionEntry[]) | Which keys changed per revision|
| `kv_meta`   | `&str`          | `&[u8]`                        | Current revision counter       |
| `raft_log`  | `u64`           | `&[u8]` (JSON LogEntry)        | Raft log entries               |
| `raft_meta` | `&str`          | `&[u8]`                        | Raft hard state                |

### InternalKeyValue

```rust
InternalKeyValue {
    key: Vec<u8>,
    create_revision: i64,   // revision when key was first created
    mod_revision: i64,      // revision of this version
    version: i64,           // version counter (0 = tombstone)
    value: Vec<u8>,
    lease: i64,             // 0 = no lease
}
```

### Revision Lifecycle

1. **Put (new key):** `create_revision = new_rev`, `version = 1`
2. **Put (existing key):** `create_revision` preserved, `version += 1`
3. **Delete:** tombstone written with `version = 0`, `create_revision = 0`
4. **Compact(rev):** removes all but the latest entry per key at or before `rev`

### Compaction

Compaction removes historical versions while preserving the latest state:

```
Before compact(3):
  key="a" rev=1  value="v1"   <-- removed
  key="a" rev=2  value="v2"   <-- removed
  key="a" rev=3  value="v3"   <-- kept (latest at compact rev)
  key="a" rev=5  value="v5"   <-- kept (after compact rev)

After compact(3):
  key="a" rev=3  value="v3"
  key="a" rev=5  value="v5"
```

---

## Raft Consensus

### Event-Action Pure State Machine

The Raft implementation follows a clean separation between the pure state
machine (`RaftCore`) and the side-effecting actor shell. `RaftCore` contains
zero async code and no I/O -- it takes `Event` values and returns `Vec<Action>`.

```
                +------------------+
                |    RaftCore      |
                |  (pure, no I/O)  |
                |                  |
  Event ------->|  fn step(Event)  |-------> Vec<Action>
                |    -> Vec<Action>|
                +------------------+
```

**Events (inputs):**

| Event              | Source                      |
|--------------------|-----------------------------|
| `Initialize`       | Startup (restored state)    |
| `ElectionTimeout`  | Timer expiry                |
| `HeartbeatTimeout`  | Timer expiry (leader only) |
| `Proposal`         | Client write request        |
| `Message`          | Peer Raft RPC               |

**Actions (outputs):**

| Action                | Side Effect                          |
|-----------------------|--------------------------------------|
| `SendMessage`         | Transmit RPC to peer node            |
| `PersistHardState`    | Write term/votedFor to redb          |
| `AppendToLog`         | Append entries to log store          |
| `TruncateLogAfter`    | Remove conflicting log entries       |
| `ApplyEntries`        | Apply committed entries to KV store  |
| `ResetElectionTimer`  | Restart randomized election timeout  |
| `StartHeartbeatTimer` | Begin periodic heartbeat sending     |
| `StopHeartbeatTimer`  | Stop heartbeat (stepped down)        |
| `RespondToProposal`   | Reply to client with result          |

### Raft State

```
RaftState {<br>
    node_id: u64,<br>
    role: Follower | Candidate | Leader,<br>
    persistent: {           // survives restart (redb)<br>
        current_term: u64,<br>
        voted_for: Option<u64>,<br>
    },<br>
    volatile: {             // all servers<br>
        commit_index: u64,<br>
        last_applied: u64,<br>
    },<br>
    leader_state: Option<{  // leader only<br>
        next_index: HashMap<u64, u64>,<br>
        match_index: HashMap<u64, u64>,<br>
    }>,<br>
    voters: HashSet<u64>,<br>
    learners: HashSet<u64>,<br>
}<br>
```

### Log Store

The durable Raft log uses redb with two tables:

- `raft_log`: `u64 (index) -> JSON LogEntry`
- `raft_meta`: `"hard_state" -> JSON PersistentState`

Operations: `append`, `get`, `get_range`, `truncate_after`, `last_index`,
`last_term`, `save_hard_state`, `load_hard_state`.

### Transport

The `RaftTransport` trait abstracts inter-node messaging:

```rust
#[async_trait]
pub trait RaftTransport: Send + Sync {
    async fn send(&self, to: u64, message: RaftMessage);
}
```

**Implementations:**

| Transport            | Use Case                              |
|----------------------|---------------------------------------|
| `LocalTransport`     | In-process multi-node testing         |
| `GrpcRaftTransport`  | Multi-node clusters over gRPC         |

### Raft Messages

```
RaftMessage<br>
+-- AppendEntriesReq    (leader -> followers)<br>
+-- AppendEntriesResp   (followers -> leader)<br>
+-- RequestVoteReq      (candidate -> all)<br>
+-- RequestVoteResp     (all -> candidate)<br>
+-- InstallSnapshotReq  (leader -> lagging follower, placeholder)<br>
+-- InstallSnapshotResp (follower -> leader, placeholder)<br>
```

### Single-Node Fast Path

In a single-node cluster (`quorum_size() == 1`), the node immediately becomes
leader on the first election timeout and commits proposals without waiting for
follower acknowledgment.

---

## Technology Stack

| Concern              | etcd                    | Barkeeper                           |
|----------------------|-------------------------|-------------------------------------|
| Language             | Go                      | Rust                                |
| Actor Runtime        | goroutines              | Rebar (OneForAll supervisor)        |
| Consensus            | etcd/raft (Go lib)      | Custom RaftCore (Event->Action)     |
| Storage Engine       | bbolt (B+ tree)         | redb (B+ tree, Rust-native)         |
| gRPC Framework       | grpc-go                 | tonic                               |
| HTTP Gateway         | grpc-gateway            | axum (custom handler)               |
| Serialization        | protobuf                | prost (protobuf) + serde_json       |
| Async Runtime        | goroutine scheduler     | tokio                               |
| Inter-node Transport | gRPC streaming           | GrpcRaftTransport (gRPC)            |
| MVCC                 | Custom B-tree            | Compound keys in redb               |
| TLS                  | Manual + auto            | Manual + auto-TLS (rcgen)           |
| Cluster Discovery    | peer URL exchange        | `--initial-cluster` CLI flag        |

---

## Module Reference

### `src/main.rs`
CLI entry point using clap. Parses `--name`, `--data-dir`,
`--listen-client-urls`, and `--node-id`. Delegates to `BarkeepServer::start()`.

### `src/actors/`

- **`commands.rs`** -- Command enums for actor communication: `RaftCmd`,
  `StoreCmd`, `WatchCmd`, `LeaseCmd`. Each wraps a `oneshot::Sender` for
  request-response.
- **`raft_process.rs`** -- RaftCore wrapped as a tokio actor with typed
  `mpsc::Sender<RaftCmd>` interface. Implements the full propose-commit-apply
  pipeline bridging `RaftCmd::Propose` to `StoreCmd::Apply`.

### `src/api/`

- **`server.rs`** -- `BarkeepServer::start()` wires all components: opens
  KvStore, initializes Rebar runtime and supervisor, spawns Raft node and state
  machine, creates all service instances, starts gRPC (tonic) and HTTP (axum)
  servers.
- **`kv_service.rs`** -- gRPC KV service: `Range`, `Put`, `DeleteRange`, `Txn`,
  `Compact`. Proposes writes through Raft, applies to KvStore after commit,
  notifies WatchHub on mutations, attaches keys to leases.
- **`watch_service.rs`** -- gRPC Watch service with bidirectional streaming.
  Creates/cancels watches via WatchHub, streams events back to clients.
- **`lease_service.rs`** -- gRPC Lease service: `LeaseGrant`, `LeaseRevoke`,
  `LeaseKeepAlive` (bidirectional streaming), `LeaseTimeToLive`, `LeaseLeases`.
- **`cluster_service.rs`** -- gRPC Cluster service: `MemberList`, `MemberAdd`,
  `MemberRemove`, `MemberUpdate`, `MemberPromote`.
- **`maintenance_service.rs`** -- gRPC Maintenance service: `Status`,
  `HashKV`, `Alarm`, `Defragment`, `Snapshot`.
- **`auth_service.rs`** -- gRPC Auth service: `AuthEnable`, `AuthDisable`,
  `UserAdd`, `UserDelete`, `UserChangePassword`, `UserGrant`, `UserRevoke`,
  `UserList`, `UserGet`, `RoleAdd`, `RoleDelete`, `RoleGrant`, `RoleRevoke`,
  `RoleList`, `RoleGet`, `Authenticate`.
- **`gateway.rs`** -- HTTP/JSON gateway on port+1, matching etcd's
  grpc-gateway. Base64 key/value encoding, proto3 JSON rules (int64 as strings,
  default value omission).

### `src/raft/`

- **`core.rs`** -- Pure `Event -> Vec<Action>` Raft state machine. Handles
  elections, log replication, commit advancement. Zero async, zero I/O.
- **`node.rs`** -- `RaftHandle` and `spawn_raft_node()`. Wraps RaftCore in a
  tokio select loop handling election/heartbeat timers, client proposals, and
  inbound peer messages.
- **`state.rs`** -- `RaftState`, `RaftRole` (Follower/Candidate/Leader),
  `PersistentState`, `VolatileState`, `LeaderState` (next_index/match_index).
- **`messages.rs`** -- `LogEntry`, `LogEntryData` (Command/ConfigChange/Noop),
  `RaftMessage` enum, all Raft RPC request/response types, `ClientProposal`.
  Includes msgpack encode/decode helpers for Rebar messaging.
- **`log_store.rs`** -- Durable log storage backed by redb. append, get,
  get_range, truncate_after, hard state persistence.
- **`transport.rs`** -- `RaftTransport` trait and `LocalTransport`
  (in-process testing).
- **`grpc_transport.rs`** -- `GrpcRaftTransport` (client) and
  `RaftTransportServer` (server) for multi-node clusters over gRPC.
- **`snapshot.rs`** -- `SnapshotMeta` and `Snapshot` types for point-in-time
  state machine captures.

### `src/kv/`

- **`store.rs`** -- MVCC KvStore backed by redb. Compound key encoding,
  `put`, `range`, `delete_range`, `txn` (compare-and-swap), `compact`.
  Uses three redb tables: `kv`, `revisions`, `kv_meta`.
- **`state_machine.rs`** -- `StateMachine` apply loop and `KvCommand` enum
  (Put/DeleteRange/Txn/Compact). Spawned via `spawn_state_machine()`, receives
  committed log entries from Raft over an mpsc channel.

### `src/watch/`

- **`hub.rs`** -- `WatchHub` maintains a `HashMap<i64, Watcher>`. Methods:
  `create_watch` (returns receiver), `cancel_watch`, `notify` (fan-out to
  matching watchers). Supports exact key, range, and prefix matching.

### `src/lease/`

- **`manager.rs`** -- In-memory `LeaseManager`. Methods: `grant`, `revoke`,
  `keepalive`, `time_to_live`, `list`, `attach_key`, `check_expired`.
  Leases are `HashMap<i64, LeaseEntry>` with TTL-based expiry detection.

### `src/cluster/`

- **`manager.rs`** -- In-memory `ClusterManager`. Tracks members with peer/client
  URLs. Methods: `add_initial_member`, `add_member`, `remove_member`,
  `update_member`, `promote_member`, `list_members`.

### `src/auth/`

- **`manager.rs`** -- In-memory RBAC `AuthManager`. Tracks users (with bcrypt
  password hashes and roles) and roles (with key-range permissions). Full CRUD
  for users and roles, permission grant/revoke. Token generation and validation.
- **`interceptor.rs`** -- gRPC auth interceptor (`GrpcAuthLayer`). Validates
  tokens on all requests when auth is enabled; auth endpoints pass through.

### `src/tls.rs`

TLS certificate management. `TlsConfig` struct, `generate_self_signed()` for
auto-TLS, `build_tls_acceptor()` for HTTP, `build_tonic_tls()` for gRPC.

### `src/config.rs`

Configuration types (currently minimal, expanded as needed).

### `src/lib.rs`

Module declarations and protobuf includes via `tonic::include_proto!` for
`mvccpb`, `etcdserverpb`, and `authpb`.

---

## Data Flow Summary

```
                         +---------------------------+
                         |       Client (etcdctl)    |
                         +---------------------------+
                            |                    |
                       gRPC :2379          HTTP :2380
                            |                    |
                            v                    v
                    +------------+        +------------+
                    | tonic gRPC |        | axum HTTP  |
                    | services   |        | gateway    |
                    +------------+        +------------+
                            \                  /
                             \   shared Arc   /
                              v              v
          +----------+   +----------+   +-----------+
          | WatchHub |<--| KvStore  |-->| LeaseManager|
          +----------+   +----------+   +-----------+
               |               |               |
               |          redb (kv.redb)        |
               |               |           in-memory
               v               v
          watchers        +----------+
          (mpsc)          | RaftNode |
                          +----------+
                               |
                          redb (raft.redb)
```
