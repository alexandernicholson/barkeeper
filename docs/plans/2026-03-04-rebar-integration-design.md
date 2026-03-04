# Rebar Integration & etcd Parity — Design Document

**Date:** 2026-03-04
**Status:** Proposed

## 1. Goal

Transform barkeeper into a true Rebar showcase: every service runs as a supervised Rebar actor process, inter-node communication uses Rebar's QUIC transport and SWIM membership, and all remaining behavioral gaps with etcd are closed via TDD. Ship with comprehensive documentation including architecture diagrams, developer guides, and extension guides.

## 2. Architecture Overview

### Current State

```
┌─────────────────────────────────────────────┐
│  gRPC (tonic) / HTTP Gateway (axum)         │
├─────────┬───────┬───────┬───────┬───────────┤
│KvService│Watch  │Lease  │Cluster│Auth       │
│         │Service│Service│Service│Service    │
├─────────┴───────┴───────┴───────┴───────────┤
│  KvStore (direct)  │  RaftNode (bypassed)    │
│  redb               │  tokio::spawn           │
└─────────────────────┴────────────────────────┘
```

**Problems:** KV writes bypass Raft (no replication). Watch hub never receives notifications. Lease expiry never fires. Auth not enforced. Everything is `tokio::spawn` — no supervision, no fault isolation, no distributed messaging.

### Target State

```
┌──────────────────────────────────────────────────────────────┐
│  gRPC (tonic) / HTTP Gateway (axum)                          │
│  Typed mpsc::Sender<Cmd> per service                         │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  BarkeepSupervisor (Rebar, OneForAll)                        │
│  ├── RaftProcess      — consensus, log replication           │
│  ├── StoreProcess     — KvStore (redb), applies committed    │
│  ├── WatchProcess     — WatchHub, fans out mutation events   │
│  ├── LeaseProcess     — TTL tracking, expiry timer           │
│  ├── ClusterProcess   — SWIM membership, member list         │
│  └── AuthProcess      — RBAC, token enforcement              │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Rebar Runtime                                               │
│  ├── ProcessTable (DashMap)                                  │
│  ├── DistributedRouter (local + remote)                      │
│  └── ConnectionManager (QUIC via Quinn)                      │
├──────────────────────────────────────────────────────────────┤
│  SWIM Gossip              │  QUIC Transport                  │
│  (membership, failure     │  (Raft messages between          │
│   detection)              │   nodes)                         │
└───────────────────────────┴──────────────────────────────────┘
```

### Communication Pattern

Following Rebar's own bench/store example:

- **Local (same node):** Typed `tokio::mpsc::Sender<ServiceCmd>` channels. gRPC handlers hold senders, actor processes own receivers. Zero serialization overhead.
- **Remote (between nodes):** Rebar's `ctx.send(remote_pid, rmpv::Value)` over QUIC. Used exclusively for Raft RPCs between nodes.
- **Supervision:** Rebar supervisor tree with `OneForAll` strategy — if any critical service dies, restart everything to a clean state.

## 3. Process Definitions

### 3.1 RaftProcess

**Owns:** `RaftCore` (pure state machine), `LogStore` (redb)
**Receives locally:** `RaftCmd::Propose { data, reply_tx }`, `RaftCmd::ReadIndex { reply_tx }`
**Receives from peers:** `rmpv::Value`-encoded `RaftMessage` via `ProcessContext::recv()`
**Sends to peers:** `ctx.send(peer_pid, encoded_raft_msg)` via QUIC

```
Write path:
  KvService → mpsc::send(RaftCmd::Propose) → RaftProcess
  RaftProcess replicates to peers via Rebar distributed messaging
  On commit → RaftProcess sends mpsc::send(StoreCmd::Apply) → StoreProcess

Single-node optimization:
  RaftProcess commits immediately (no peers to replicate to)
  Same path, just no network round-trip
```

### 3.2 StoreProcess

**Owns:** `Arc<KvStore>` (redb), read-only `Arc<KvStore>` shared with gRPC handlers
**Receives:** `StoreCmd::Apply { entries }`, `StoreCmd::Compact { revision }`
**After mutation:** Sends `WatchCmd::Notify { key, event_type, kv, prev_kv }` to WatchProcess

Note: Range (read) operations go directly to `Arc<KvStore>` from gRPC handlers — no message passing for reads. This matches etcd's behavior where reads don't go through Raft (serializable reads) and is critical for read performance.

### 3.3 WatchProcess

**Owns:** `WatchHub` (watcher registry, fan-out logic)
**Receives:** `WatchCmd::Create { key, range_end, start_revision, reply_tx }`, `WatchCmd::Cancel { watch_id }`, `WatchCmd::Notify { key, event_type, kv, prev_kv }`
**Sends to clients:** `mpsc::Sender<WatchEvent>` per watcher (existing pattern)

The key change: StoreProcess sends `WatchCmd::Notify` after every Put/Delete/Txn. Currently nobody calls `WatchHub::notify()`.

### 3.4 LeaseProcess

**Owns:** `LeaseManager` (in-memory lease state)
**Receives:** `LeaseCmd::Grant`, `LeaseCmd::Revoke`, `LeaseCmd::KeepAlive`, `LeaseCmd::TimeToLive`, `LeaseCmd::List`
**Timer:** Runs a periodic `CheckExpiry` tick (every 500ms). For each expired lease, sends `RaftCmd::Propose(KvCommand::DeleteRange)` for all attached keys, then removes the lease.

### 3.5 ClusterProcess

**Owns:** `ClusterManager`, integrates with Rebar's `MembershipList` (SWIM)
**Receives:** `ClusterCmd::AddMember`, `ClusterCmd::RemoveMember`, `ClusterCmd::ListMembers`
**SWIM integration:** When SWIM detects a new alive node, ClusterProcess registers it. When SWIM marks a node dead, ClusterProcess removes it.

### 3.6 AuthProcess

**Owns:** `AuthManager` (user/role/permission state)
**Receives:** All auth RPCs via `AuthCmd` enum
**Enforcement:** A tonic interceptor checks auth tokens on every gRPC request (when auth is enabled). The interceptor holds a `mpsc::Sender<AuthCmd::Validate>` and validates tokens before forwarding to KV/Watch/Lease services.

## 4. Supervision Tree

```
BarkeepSupervisor (OneForAll, max_restarts=5, max_seconds=30)
├── ChildSpec("raft",    Permanent, Timeout(5s))  → RaftProcess
├── ChildSpec("store",   Permanent, Timeout(5s))  → StoreProcess
├── ChildSpec("watch",   Permanent, Timeout(2s))  → WatchProcess
├── ChildSpec("lease",   Permanent, Timeout(2s))  → LeaseProcess
├── ChildSpec("cluster", Permanent, Timeout(2s))  → ClusterProcess
└── ChildSpec("auth",    Permanent, Timeout(2s))  → AuthProcess
```

**Why OneForAll:** These processes share state dependencies. If Raft crashes, Store must re-sync. If Store crashes, Watch and Lease need fresh state. Clean restart is safer than partial recovery.

## 5. etcd Gaps — TDD Plan

Each gap follows strict TDD: write a failing test against etcd's known behavior, then implement the fix.

### Gap 1: Watch Notifications

**Test (in `tests/compat_test.rs`):**
```rust
#[tokio::test]
async fn test_watch_receives_put_event() {
    // Start barkeeper
    // Create watch on key "foo" via gRPC Watch stream
    // Put "foo" = "bar" via gRPC Put
    // Assert: watch stream receives event with PUT type, key="foo", value="bar"
}

#[tokio::test]
async fn test_watch_receives_delete_event() {
    // Put "foo" = "bar"
    // Create watch on key "foo"
    // Delete "foo"
    // Assert: watch stream receives event with DELETE type, key="foo"
}

#[tokio::test]
async fn test_watch_prefix_receives_events() {
    // Create watch on prefix "foo/" (range_end = "foo0")
    // Put "foo/a" = "1"
    // Put "foo/b" = "2"
    // Assert: watch stream receives both events
}
```

**Implementation:**
- StoreProcess sends `WatchCmd::Notify` after every mutation (put, delete, txn ops)
- WatchHub.notify() already works — it just needs to be called

### Gap 2: Lease Expiry

**Test:**
```rust
#[tokio::test]
async fn test_lease_expiry_deletes_keys() {
    // Grant lease with TTL=2
    // Put "ephemeral" with lease
    // Assert: key exists
    // Sleep 3 seconds
    // Assert: key no longer exists (range returns empty)
    // Assert: lease no longer in lease list
}

#[tokio::test]
async fn test_lease_keepalive_prevents_expiry() {
    // Grant lease TTL=2
    // Put "ephemeral" with lease
    // Sleep 1s, send keepalive
    // Sleep 1s, send keepalive
    // Assert: key still exists after 3s total
}
```

**Implementation:**
- LeaseProcess runs a 500ms tick timer
- On tick: iterate leases, check `elapsed > ttl`, collect expired
- For each expired: delete attached keys via StoreProcess, remove lease

### Gap 3: Writes Through Raft

**Test:**
```rust
#[tokio::test]
async fn test_put_goes_through_raft() {
    // Put via gRPC
    // Assert: response header has raft_term > 0
    // Assert: response header has revision > 0
}

#[tokio::test]
async fn test_delete_goes_through_raft() {
    // Put then Delete via gRPC
    // Assert: delete response has correct deleted count
    // Assert: response header has raft_term > 0
}
```

**Implementation:**
- KvService sends `RaftCmd::Propose(KvCommand::Put {...})` instead of `store.put()` directly
- RaftProcess commits (single-node: immediate), sends to StoreProcess
- StoreProcess applies and returns result via oneshot channel
- Response includes actual raft_term from RaftCore

### Gap 4: Auth Enforcement

**Test:**
```rust
#[tokio::test]
async fn test_auth_blocks_unauthenticated_kv() {
    // Enable auth
    // Add root user with password
    // Try Put without token → expect PermissionDenied
}

#[tokio::test]
async fn test_auth_allows_authenticated_kv() {
    // Enable auth
    // Add root user with root role
    // Authenticate → get token
    // Put with token → succeeds
}

#[tokio::test]
async fn test_auth_enforces_permissions() {
    // Enable auth
    // Create user with read-only role on "readonly/"
    // Authenticate as that user
    // Try Put to "readonly/x" → expect PermissionDenied
    // Try Range on "readonly/x" → succeeds
}
```

**Implementation:**
- tonic interceptor extracts `authorization` metadata from requests
- If auth enabled: validate token via AuthProcess, check permission for the operation
- If auth disabled: pass through (current behavior)
- HTTP gateway: check Authorization header

### Gap 5: Raft Term in Response Headers

**Test:**
```rust
#[tokio::test]
async fn test_response_header_has_raft_term() {
    // Put via HTTP gateway
    // Assert: response header.raft_term is a non-zero string
}
```

**Implementation:**
- RaftProcess exposes current term via a shared `AtomicU64`
- gRPC services and HTTP gateway read it when building response headers
- Already tracked by `RaftCore.current_term` — just needs to be exposed

### Gap 6: HTTP Gateway Compaction

**Test:**
```rust
#[tokio::test]
async fn test_http_compaction() {
    // Put keys to create revisions
    // POST /v3/kv/compaction with revision
    // Assert: 200 OK with proper response
}
```

**Implementation:**
- Gateway `handle_compaction` currently returns 501. Wire it to StoreProcess.

## 6. Rebar Integration Points

| Component | Before | After |
|-----------|--------|-------|
| RaftNode | `tokio::spawn` | `runtime.spawn()` + supervisor child |
| StateMachine | `tokio::spawn` | Merged into StoreProcess, `runtime.spawn()` |
| WatchHub | `Arc<WatchHub>` passive | WatchProcess, `runtime.spawn()` |
| LeaseManager | `Arc<LeaseManager>` passive | LeaseProcess with expiry timer, `runtime.spawn()` |
| ClusterManager | `Arc<ClusterManager>` | ClusterProcess + SWIM, `runtime.spawn()` |
| AuthManager | `Arc<AuthManager>` | AuthProcess + interceptor, `runtime.spawn()` |
| Inter-node Raft | `Option<Arc<dyn RaftTransport>>` (unused) | Rebar `DistributedRouter` over QUIC |
| Cluster discovery | Manual config | SWIM gossip via `rebar-cluster` |
| Fault tolerance | None (crash = dead) | Supervision tree with restart strategies |
| Node identity | CLI `--node-id` | Rebar `ProcessId(node_id, local_id)` |

## 7. Data Flow Diagrams

### Write Path (Put)

```
Client ──gRPC──→ KvService
                    │
          mpsc::send(RaftCmd::Propose)
                    │
                    ▼
              RaftProcess
                    │
            ┌───────┴───────┐
            │ single-node:  │ multi-node:
            │ commit        │ replicate via
            │ immediately   │ Rebar QUIC
            └───────┬───────┘
                    │
          mpsc::send(StoreCmd::Apply)
                    │
                    ▼
             StoreProcess
                    │
            ┌───────┴───────┐
            │ apply to      │ notify
            │ KvStore/redb  │ watchers
            └───────┬───────┘
                    │
          mpsc::send(WatchCmd::Notify)
                    │
                    ▼
            WatchProcess
                    │
               fan out to
             matching watchers
```

### Read Path (Range)

```
Client ──gRPC──→ KvService
                    │
              store.range() ← direct Arc<KvStore> access
                    │
                    ▼
            KvStore (redb)
                    │
               ◄── response ──►
```

### Lease Expiry

```
 LeaseProcess
      │
  500ms tick ──→ check all leases
      │
  expired? ──→ mpsc::send(StoreCmd::DeleteKeys { keys })
      │              │
      │              ▼
      │        StoreProcess
      │              │
      │         delete keys
      │              │
      │    mpsc::send(WatchCmd::Notify) per key
      │
  remove lease from LeaseManager
```

## 8. Documentation Plan

### 8.1 docs/architecture.md
- System overview with process tree diagram
- Data flow diagrams (write path, read path, watch, lease expiry)
- MVCC storage model explanation
- Raft consensus integration with Rebar actors
- SWIM cluster membership
- Comparison table: etcd architecture vs barkeeper

### 8.2 docs/developer-guide.md
- Prerequisites (Rust, protoc, etc.)
- Building from source
- Running a single-node cluster
- Running a multi-node cluster
- Testing with etcdctl and curl
- Running the test suite
- Project structure walkthrough
- Debugging tips (RUST_LOG, tracing)

### 8.3 docs/extending.md
- Adding a new gRPC service
- Adding a new actor process
- Implementing a custom RaftTransport
- Writing compatibility tests
- Integration testing patterns

### 8.4 docs/etcd-compatibility.md
- Complete API compatibility matrix
- Proto3 JSON conventions
- Known differences and limitations
- Benchmark comparison results

### 8.5 README.md (updated)
- Updated architecture diagram
- Rebar integration highlighted
- Updated feature list (watch notifications, lease expiry, auth enforcement)
- Multi-node quickstart
- Link to docs/ for details

## 9. Implementation Phases

### Phase 1: Rebar Actor Infrastructure
1. Add Rebar runtime initialization to `main.rs`
2. Define command enums for each process (`RaftCmd`, `StoreCmd`, `WatchCmd`, `LeaseCmd`, `ClusterCmd`, `AuthCmd`)
3. Create process spawn functions that return typed senders
4. Set up supervision tree
5. Wire gRPC services to use typed senders instead of `Arc<T>` direct access
6. Wire HTTP gateway similarly

### Phase 2: Watch Notifications (TDD)
1. Write failing watch tests
2. StoreProcess sends `WatchCmd::Notify` after mutations
3. Verify tests pass

### Phase 3: Lease Expiry (TDD)
1. Write failing lease expiry tests
2. LeaseProcess adds expiry timer tick
3. Expired leases trigger key deletion via StoreProcess
4. Verify tests pass

### Phase 4: Writes Through Raft (TDD)
1. Write failing raft_term tests
2. KvService sends writes through RaftProcess
3. RaftProcess commits and sends to StoreProcess
4. StoreProcess returns results via oneshot
5. Expose raft_term in response headers
6. Verify tests pass

### Phase 5: Auth Enforcement (TDD)
1. Write failing auth enforcement tests
2. Implement tonic interceptor
3. Wire HTTP gateway auth check
4. Verify tests pass

### Phase 6: Multi-node via Rebar (TDD)
1. Implement `RebarTransport` using Rebar's `DistributedRouter`
2. Integrate SWIM for cluster discovery
3. Write multi-node integration tests
4. Verify tests pass

### Phase 7: Documentation
1. Write `docs/architecture.md` with diagrams
2. Write `docs/developer-guide.md`
3. Write `docs/extending.md`
4. Write `docs/etcd-compatibility.md`
5. Update `README.md`
6. Update test count and feature status

### Phase 8: HTTP Gateway Remaining Stubs
1. Wire HTTP compaction endpoint
2. Verify all HTTP endpoints match etcd behavior
3. Final benchmark comparison run

## 10. Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Rebar crate API changes | Pin to specific git commit in Cargo.toml |
| Actor message passing adds latency | Reads bypass actors (direct Arc<KvStore>). Writes already have Raft latency. |
| Supervision restart loses in-flight requests | Clients retry on connection reset (standard etcd client behavior) |
| SWIM eventual consistency for membership | Membership changes are rare; SWIM converges in seconds |
| Lease expiry precision (500ms tick) | etcd also uses coarse-grained checking; sub-second precision is acceptable |

## 11. Success Criteria

- All existing 68 tests continue to pass
- New TDD tests for all 6 gaps pass
- `etcdctl` works for put/get/delete/watch/lease operations
- Watch stream receives events in real-time
- Leases expire and delete attached keys
- Auth blocks unauthenticated access when enabled
- Response headers include non-zero raft_term
- Comprehensive docs with diagrams in docs/
- Updated README reflecting new architecture
