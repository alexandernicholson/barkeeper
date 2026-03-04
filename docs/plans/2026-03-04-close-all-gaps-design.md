# Close All Remaining etcd Gaps — Design Document

**Date:** 2026-03-04
**Approach:** Bottom-up (quick wins first, then foundational changes)
**TDD:** All changes use test-driven development — failing tests first, then implementation.

---

## Gaps to Close

| # | Gap | Phase | Complexity |
|---|-----|-------|-----------|
| 7 | HTTP compaction stubbed | 1 | Small |
| 6 | Txn watch notifications missing | 1 | Small-Medium |
| 8 | No revision-based watching | 1 | Medium |
| 5 | Lease expiry timer missing in production | 2 | Medium |
| 1 | Writes bypass Raft | 3 | Medium-Large |
| 4 | Auth not enforced | 4 | Medium |
| 3 | No TLS | 5 | Small-Medium |
| 2 | Single-node only | 6 | Large |

---

## Phase 1: Quick Fixes (no architectural changes)

### Gap 7 — HTTP Compaction

Replace `handle_compaction_stub` in `gateway.rs` with a real handler. Add `CompactionRequest` (with `revision` field) and `CompactionResponse` JSON types. Call `store.compact(revision)` — identical to the gRPC path in `kv_service.rs:209-227`.

**Files:** `src/api/gateway.rs`

### Gap 6 — Txn Watch Notifications

After `store.txn()` succeeds, iterate `TxnOpResponse` results. For each `Put(r)`, construct a `KeyValue` and call `watch_hub.notify()` with event_type 0. For each `DeleteRange(r)`, loop `r.prev_kvs` and notify with event_type 1. The txn result already contains `PutResult` (with revision, prev_kv) and `DeleteResult` (with prev_kvs) — all data needed is available.

**Files:** `src/api/kv_service.rs`, `src/api/gateway.rs`

### Gap 8 — Revision-Based Watching

`WatchHub.create_watch()` already accepts `start_revision` but ignores it (`#[allow(dead_code)]`). Fix:

1. Add `KvStore::changes_since(revision) -> Vec<(key, event_type, kv, prev_kv)>` that scans `REV_TABLE` for all mutations after the given revision.
2. In `WatchHub::create_watch()`, when `start_revision > 0`, call `changes_since()` and replay as synthetic watch events before subscribing to live events.
3. If the revision has been compacted, return an error to the watcher.

**Files:** `src/kv/store.rs`, `src/watch/hub.rs`

---

## Phase 2: Production Lease Expiry Timer

### Gap 5 — Lease Expiry in Production

`server.rs` currently does NOT spawn a lease expiry timer — it only exists in the integration test. Spawn the same timer loop inside `BarkeepServer::start()`: poll `check_expired()` every 500ms, delete attached keys via `store.delete_range()`, notify watchers. Local-only for now; when writes go through Raft in Phase 3, the timer will propose expiry through consensus instead of deleting directly.

**Files:** `src/api/server.rs`

---

## Phase 3: Writes Through Raft

### Gap 1 — Route All Mutations Through Consensus

Currently `KvService.put()` and `gateway::handle_put()` call `store.put()` directly. After this change:

1. Services send `RaftHandle.propose(KvCommand)` instead of calling store directly.
2. Raft commits the entry and the state machine applies it via `apply_command()`.
3. Watch notifications and lease key attachment move from the service layer into `state_machine.rs` — the single place where mutations are applied.
4. Services await the proposal result (revision, prev_kv) before responding to clients.
5. "Not leader" errors return gRPC `UNAVAILABLE` / HTTP 503.

The state machine (`src/kv/state_machine.rs`) already handles `KvCommand::Put/DeleteRange/Txn/Compact`. `RaftHandle.propose()` works. The gap is wiring: services serialize a `KvCommand`, propose it, and await the result.

After this phase, txn watch notifications from Phase 1 and lease expiry from Phase 2 automatically become Raft-consistent since they happen at the apply layer.

**Files:** `src/api/kv_service.rs`, `src/api/gateway.rs`, `src/kv/state_machine.rs`, `src/api/server.rs`, `src/raft/node.rs`

---

## Phase 4: Auth Enforcement

### Gap 4 — Token Validation Middleware

1. **Tonic interceptor** — extracts `authorization` metadata from gRPC requests, validates token via `AuthManager.authenticate()`. If auth is enabled and token is invalid/missing, reject with `UNAUTHENTICATED`.
2. **Axum middleware** — same for HTTP gateway via `Authorization` header.
3. **Permission checks** — inside each service method, check if the authenticated user has the required permission (READ/WRITE) for the key range.
4. **Password hashing** — replace FNV `simple_hash` in `auth/manager.rs` with bcrypt.

**Files:** `src/api/server.rs`, `src/api/gateway.rs`, `src/auth/manager.rs`, new auth middleware file

---

## Phase 5: TLS

### Gap 3 — Certificate Support

Match etcd's TLS flags:

**Client-to-server:**
- `--cert-file` / `--key-file` — manual TLS cert and key
- `--trusted-ca-file` — CA for client cert validation
- `--auto-tls` — generate self-signed cert on startup via `rcgen`, store in `{data-dir}/auto-tls/`
- `--client-cert-auth` — require client certs
- `--self-signed-cert-validity` — validity in years (default 1)

**Peer-to-peer:**
- `--peer-cert-file` / `--peer-key-file` / `--peer-trusted-ca-file` — manual peer TLS
- `--peer-auto-tls` — auto-generated peer certs

**Implementation:**
- gRPC uses `tonic::transport::ServerTlsConfig`
- HTTP gateway uses `tokio-rustls`
- Auto-TLS generates certs with `rcgen` on first startup, reuses from disk on subsequent runs
- Without TLS flags, everything stays plaintext (backwards compatible)

**New dependencies:** `rcgen`, `tokio-rustls`, `rustls-pemfile`

**Files:** `src/main.rs` (CLI flags), `src/api/server.rs` (TLS setup), `src/config.rs` (TLS config), new `src/tls.rs`

---

## Phase 6: Multi-Node Networking

### Gap 2 — gRPC-Based RaftTransport

1. **`GrpcRaftTransport`** — implements `RaftTransport` trait. Sends `RaftMessage` as serialized bytes over a new gRPC `RaftTransportService`. Each node runs this service for inbound messages and feeds them to `RaftHandle.inbound_tx`. Outbound uses a connection pool to peer addresses.

2. **Cluster bootstrap** — CLI flags:
   - `--initial-cluster` — comma-separated `name=addr` pairs (e.g., `node1=http://10.0.0.1:2379,node2=http://10.0.0.2:2379`)
   - `--initial-cluster-state` — `new` (forming new cluster) or `existing` (joining existing)

3. **Peer discovery** — on startup, exchange member information with peers from `--initial-cluster`. Each node registers itself in `ClusterManager`.

4. **Existing infrastructure leveraged:**
   - `RaftCore` already handles multi-node election, log replication, commit index
   - `RaftTransport` trait is defined with `send()` + `receive()`
   - `LocalTransport` works for in-process testing
   - `RaftMessage` has serialization helpers (`encode_raft_message`/`decode_raft_message`)

**New dependency:** proto definitions for `RaftTransportService`

**Files:** new `src/raft/grpc_transport.rs`, `src/api/server.rs`, `src/main.rs` (CLI flags), `src/config.rs`, proto file for transport service

---

## TDD Approach

Each gap follows the same pattern:
1. Write failing test(s) that exercise the expected behavior
2. Run tests to verify they fail for the right reason
3. Implement the minimum code to make tests pass
4. Run full test suite to verify no regressions
5. Commit
