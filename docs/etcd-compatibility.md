# etcd API Compatibility Reference

barkeeper implements the etcd v3 API surface. This document describes what
is implemented, how it behaves, and where it diverges from etcd.

Tested against: **etcd 3.5.17**

---

## API Compatibility Matrix

### KV Service (`etcdserverpb.KV`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| Range | Yes | `POST /v3/kv/range` | Full | Prefix ranges, limit, revision queries |
| Put | Yes | `POST /v3/kv/put` | Full | Supports prev_kv, lease attachment |
| DeleteRange | Yes | `POST /v3/kv/deleterange` | Full | Prefix deletes, prev_kv |
| Txn | Yes | `POST /v3/kv/txn` | Full | All compare targets (Value, Version, Create, Mod, Lease) and result operators (Equal, Greater, Less, NotEqual) supported. Nested txn returns UNIMPLEMENTED. Watch notifications fire for mutations inside txns. |
| Compact | Yes | `POST /v3/kv/compaction` | Full | Real compaction on redb via both gRPC and HTTP gateway. |

### Watch Service (`etcdserverpb.Watch`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| Watch | Yes | Not exposed | Full | Bidirectional streaming. Supports CreateRequest, CancelRequest, ProgressRequest (stub response), and revision-based history replay via start_revision. |

### Lease Service (`etcdserverpb.Lease`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| LeaseGrant | Yes | `POST /v3/lease/grant` | Full | Auto-ID when id=0, explicit ID supported |
| LeaseRevoke | Yes | `POST /v3/lease/revoke` | Full | Returns NOT_FOUND for unknown lease |
| LeaseKeepAlive | Yes | Not exposed | Full | Bidirectional streaming, resets grant time |
| LeaseTimeToLive | Yes | `POST /v3/lease/timetolive` | Full | Returns grantedTTL, remaining TTL, attached keys |
| LeaseLeases | Yes | `POST /v3/lease/leases` | Full | Lists all active lease IDs |

### Cluster Service (`etcdserverpb.Cluster`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| MemberAdd | Yes | Not exposed | Full | Supports is_learner flag |
| MemberRemove | Yes | Not exposed | Full | Returns NOT_FOUND for unknown member |
| MemberUpdate | Yes | Not exposed | Full | Updates peer URLs |
| MemberList | Yes | `POST /v3/cluster/member/list` | Full | |
| MemberPromote | Yes | Not exposed | Full | Promotes learner to voter |

### Maintenance Service (`etcdserverpb.Maintenance`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| Status | Yes | `POST /v3/maintenance/status` | Full | Reports version, dbSize, leader, raft term |
| Alarm | Yes | Not exposed | Stub | Always returns empty alarm list |
| Defragment | Yes | Not exposed | Stub | No-op; redb manages its own compaction |
| Hash | Yes | Not exposed | Stub | Returns zero hash |
| HashKV | Yes | Not exposed | Stub | Returns zero hash |
| Snapshot | Yes | Not exposed | Stub | Returns an empty single-chunk snapshot |
| MoveLeader | Yes | Not exposed | Stub | Returns UNIMPLEMENTED (single-node) |
| Downgrade | Yes | Not exposed | Stub | Returns UNIMPLEMENTED |

### Auth Service (`etcdserverpb.Auth`)

| RPC | gRPC | HTTP Gateway | Status | Notes |
|-----|------|-------------|--------|-------|
| AuthEnable | Yes | `POST /v3/auth/enable` | Full | Enables auth flag; enforces token validation on all subsequent requests |
| AuthDisable | Yes | `POST /v3/auth/disable` | Full | Disables auth flag |
| AuthStatus | Yes | `POST /v3/auth/status` | Full | Reports enabled state, auth_revision=0 |
| Authenticate | Yes | `POST /v3/auth/authenticate` | Full | Returns token on valid credentials (bcrypt password hashing) |
| UserAdd | Yes | `POST /v3/auth/user/add` | Full | |
| UserGet | Yes | Not exposed | Full | Returns user's roles |
| UserList | Yes | Not exposed | Full | |
| UserDelete | Yes | Not exposed | Full | |
| UserChangePassword | Yes | Not exposed | Full | |
| UserGrantRole | Yes | Not exposed | Full | |
| UserRevokeRole | Yes | Not exposed | Full | |
| RoleAdd | Yes | Not exposed | Full | |
| RoleGet | Yes | Not exposed | Full | Returns permissions |
| RoleList | Yes | Not exposed | Full | |
| RoleDelete | Yes | Not exposed | Full | |
| RoleGrantPermission | Yes | Not exposed | Full | Supports perm_type, key, range_end |
| RoleRevokePermission | Yes | Not exposed | Full | |

---

## Proto3 JSON Conventions

The HTTP gateway follows the proto3 canonical JSON mapping:

- **int64/uint64 as strings.** All 64-bit integer fields (`cluster_id`,
  `member_id`, `revision`, `raft_term`, `create_revision`, `mod_revision`,
  `version`, `lease`, `ID`, `TTL`, `grantedTTL`, `dbSize`, `leader`,
  `raftIndex`, `raftTerm`, `raftAppliedIndex`, `dbSizeInUse`, `deleted`,
  `count`) are serialized as JSON strings, not numbers.

- **Default values omitted.** Fields at their zero value are omitted from
  responses: `0` for integers, `false` for booleans, `""` for strings,
  and `[]` for arrays. For example, a range response for a nonexistent key
  returns only `{"header": {...}}` with no `kvs`, `count`, or `more` fields.

- **key/value fields base64-encoded.** Byte fields (`key`, `value`) are
  encoded as standard base64 strings, matching etcd's grpc-gateway behavior.

- **Enum fields as strings.** Txn compare targets (`VALUE`, `VERSION`,
  `CREATE`, `MOD`, `LEASE`) and result operators (`EQUAL`, `GREATER`,
  `LESS`, `NOT_EQUAL`) are accepted as strings. Case-insensitive.

- **No Content-Type requirement.** The HTTP gateway accepts requests without
  the `Content-Type: application/json` header, matching etcd's grpc-gateway.

---

## Watch Notifications

Watch is implemented as a gRPC bidirectional stream on the `Watch` RPC.

- **PUT events** (type=0) are sent on both key creation and key update.
  The event includes the full `KeyValue` with `create_revision`,
  `mod_revision`, `version`, and `value`. A `prev_kv` field is included
  when a previous value existed.

- **DELETE events** (type=1) are sent on key deletion. The event `kv`
  contains a tombstone (`create_revision=0`, `version=0`, empty value)
  with the `mod_revision` set to the deletion revision. `prev_kv` contains
  the value before deletion.

- **Prefix watching** is supported via the `range_end` field in
  `WatchCreateRequest`. The matching logic follows etcd conventions:
  empty `range_end` matches exact key, `\x00` matches all keys >= key,
  otherwise matches the half-open range `[key, range_end)`.

- **CreateRequest** returns a `WatchResponse` with `created=true` and
  the assigned `watch_id`.

- **CancelRequest** returns a `WatchResponse` with `canceled=true`.
  The forwarding task is aborted and the watcher is removed from the hub.

- **ProgressRequest** is accepted but returns a stub response (empty
  events, `watch_id=-1`). Full progress notification is not implemented.

- **Txn watch notifications** fire for put and delete mutations inside
  transactions. The service layer zips the executed ops with their responses
  to reconstruct key/value data for notifications.

- **Revision-based watching** is supported via the `start_revision` field in
  `WatchCreateRequest`. When a watcher is created with `start_revision > 0`,
  the WatchHub replays historical events from the KvStore's revision table
  before delivering live events.

---

## Lease Management

- **Grant** accepts an explicit `id` or auto-generates a sequential ID
  when `id=0`. The granted TTL is stored and the grant timestamp is
  recorded.

- **TTL tracking** uses `Instant::now()` at grant time. The remaining TTL
  is computed as `granted_ttl - elapsed_seconds`. A keepalive resets the
  grant timestamp, effectively extending the lease by the full TTL.

- **Automatic expiry** is handled via `check_expired()`, which scans all
  leases and removes any where `elapsed >= ttl`. Returns the expired lease
  IDs and their attached keys for cleanup.

- **Key cleanup** on lease expiry: keys are tracked via `attach_key()`,
  which associates a key with a lease ID. When `put` is called with a
  non-zero lease, the key is attached automatically. The expiry loop uses
  the returned key list to delete keys from the KV store.

- **Revoke** immediately removes the lease. Revoking a nonexistent lease
  returns `NOT_FOUND`.

---

## Known Differences and Limitations

### Nested transactions

Nested `Txn` inside a `Txn` request op returns `UNIMPLEMENTED`.

### Maintenance stubs

`Alarm`, `Defragment`, `Hash`, `HashKV`, and `Snapshot` return valid
but empty/zero responses. `MoveLeader` and `Downgrade` return
`UNIMPLEMENTED`.

### Watch ProgressRequest

`ProgressRequest` is accepted but returns a stub response rather than a
real progress notification with the current revision.

### Instance-specific differences

These values differ from etcd by design and do not represent behavioral
incompatibilities:

| Field | etcd | barkeeper |
|-------|------|-----------|
| cluster_id / member_id | Random 64-bit IDs | Sequential |
| Lease IDs | Random 64-bit IDs | Sequential |
| Storage engine | bbolt | redb |
| version | `3.5.x` | `0.1.0` |
| Peer URLs | Separate peer port | Shares client port |

---

## Benchmark Results

Side-by-side behavioral comparison against etcd 3.5.17 shows
**28/30 tests pass**. The 2 remaining differences are instance-specific
(member_list and maintenance_status contain IDs, versions, and storage
sizes that naturally differ between implementations).

All behavioral APIs produce identical output when normalized for
instance-specific values.

### Test breakdown

- **KV via gRPC (etcdctl):** 14/14 pass -- put, get, prefix range, limit,
  delete, prefix delete, put with lease
- **Txn via gRPC (etcdctl):** 4/4 pass -- success path, failure path,
  value verification for both
- **HTTP gateway (normalized):** 10/12 pass -- put, range, delete, empty
  range, prev_kv, prefix range, txn, response headers, lease grant,
  lease list. The 2 instance-specific tests (member_list,
  maintenance_status) pass structurally but contain different IDs/versions.

Full results are in `benchmark/RESULTS.md`.

---

## Running the Benchmark Yourself

The benchmark suite runs both etcd and barkeeper through the same set of
operations and compares their output.

```bash
cd benchmark

# Run against a real etcd instance (requires etcd + etcdctl on PATH)
./run_etcd_benchmark.sh

# Run against barkeeper (requires target/release/barkeeper)
./run_barkeeper_benchmark.sh

# Compare the two result sets
./compare.sh
```

Results are written to `benchmark/results/etcd/` and
`benchmark/results/barkeeper/`. The compare script normalizes
instance-specific values (cluster_id, member_id, revision, raft_term,
lease IDs) before diffing.
