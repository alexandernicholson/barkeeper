# barkeeper

An etcd-compatible distributed key-value store built on the [Rebar](https://github.com/alexandernicholson/rebar) actor runtime.

barkeeper implements the etcd v3 API surface -- both gRPC and HTTP/JSON gateway -- on top of a Raft consensus engine written from scratch as Rebar actors, with [redb](https://github.com/cberner/redb) for durable storage. No C dependencies, no cgo, single static binary.

## Features

- **etcd v3 gRPC API** -- KV (Range, Put, DeleteRange, Txn, Compact), Watch, Lease, Cluster, Maintenance, Auth services
- **HTTP/JSON gateway** -- REST endpoints matching etcd's grpc-gateway (`/v3/kv/put`, `/v3/kv/range`, etc.)
- **Raft consensus** -- leader election, log replication, and persistence, implemented as Rebar actors
- **MVCC key-value store** -- multi-version concurrency control with revision history and compaction
- **Transactions** -- compare-and-swap via Txn with version/value comparisons
- **Leases** -- grant, revoke, keepalive, and time-to-live tracking
- **Cluster membership** -- member list, add, remove, update, promote
- **Auth (RBAC)** -- user/role/permission management
- **Snapshots** -- state machine snapshot types for recovery
- **Transport layer** -- pluggable Raft message transport for multi-node clusters
- **Pure Rust** -- no C dependencies; storage via redb, not boltdb

## Quick Start

### Build

```bash
cargo build --release
```

### Run a single-node cluster

```bash
./target/release/barkeeper
```

By default, barkeeper listens on:
- **gRPC**: `127.0.0.1:2379`
- **HTTP gateway**: `127.0.0.1:2380`

### Test with curl (HTTP gateway)

```bash
# Put a key (values are base64-encoded, matching etcd's gateway)
curl -s -X POST http://127.0.0.1:2380/v3/kv/put \
  -d '{"key":"aGVsbG8=","value":"d29ybGQ="}'

# Get the key
curl -s -X POST http://127.0.0.1:2380/v3/kv/range \
  -d '{"key":"aGVsbG8="}'

# Delete the key
curl -s -X POST http://127.0.0.1:2380/v3/kv/deleterange \
  -d '{"key":"aGVsbG8="}'

# Grant a lease (TTL in seconds)
curl -s -X POST http://127.0.0.1:2380/v3/lease/grant \
  -d '{"TTL":60}'

# List leases
curl -s -X POST http://127.0.0.1:2380/v3/lease/leases \
  -d '{}'

# Cluster status
curl -s -X POST http://127.0.0.1:2380/v3/maintenance/status \
  -d '{}'

# List members
curl -s -X POST http://127.0.0.1:2380/v3/cluster/member/list \
  -d '{}'
```

### Test with etcdctl (gRPC)

```bash
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:2379 put hello world
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:2379 get hello
```

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `default` | Human-readable name for this node |
| `--data-dir` | `data.barkeeper` | Path to data directory |
| `--listen-client-urls` | `127.0.0.1:2379` | gRPC listen address (HTTP gateway binds to port+1) |
| `--node-id` | `1` | Raft node ID |

## Supported APIs

### gRPC Services (port 2379)

| Service | RPCs | Status |
|---------|------|--------|
| KV | Range, Put, DeleteRange, Txn, Compact | Implemented |
| Watch | Watch (bidirectional stream) | Implemented |
| Lease | LeaseGrant, LeaseRevoke, LeaseKeepAlive, LeaseTimeToLive, LeaseLeases | Implemented |
| Cluster | MemberAdd, MemberRemove, MemberUpdate, MemberList, MemberPromote | Implemented |
| Maintenance | Status, Defragment, Alarm, Hash | Implemented |
| Auth | AuthEnable, UserAdd, UserGet, UserList, UserDelete, UserChangePassword, RoleAdd, RoleGet, RoleList, RoleDelete, RoleGrantPermission, RoleRevokePermission, UserGrantRole, UserRevokeRole | Implemented |

### HTTP/JSON Gateway (port 2380)

| Endpoint | Description |
|----------|-------------|
| `POST /v3/kv/put` | Put a key-value pair |
| `POST /v3/kv/range` | Get key(s) by key or key range |
| `POST /v3/kv/deleterange` | Delete key(s) |
| `POST /v3/kv/txn` | Transactional compare-and-swap (stub) |
| `POST /v3/kv/compaction` | Compact revision history (stub) |
| `POST /v3/lease/grant` | Grant a lease |
| `POST /v3/lease/revoke` | Revoke a lease |
| `POST /v3/lease/timetolive` | Query lease TTL |
| `POST /v3/lease/leases` | List all leases |
| `POST /v3/cluster/member/list` | List cluster members |
| `POST /v3/maintenance/status` | Cluster status |

Byte fields (key, value) are base64-encoded in JSON, matching etcd's HTTP gateway behavior.

## Architecture

```
Client Request
    |
    v
gRPC Service / HTTP Gateway
    |
    v
RaftNode Actor  (leader election, log replication)
    |
    v
StateMachine Actor  (applies committed entries)
    |
    v
KvStore  (MVCC store backed by redb)
```

**Raft consensus** is implemented from scratch as a Rebar actor. A `RaftCore` pure state machine processes events and produces actions (persist, append, apply, send messages). The `RaftNode` actor wraps it with timers, channels, and a transport layer.

**State machine** receives committed log entries from Raft and applies KvCommands (Put, DeleteRange, Txn, Compact) to the KvStore.

**KvStore** implements MVCC semantics with revision-indexed storage. Each mutation increments a global revision counter. Range queries can read at any historical revision. Compaction removes old revisions to reclaim space.

**Storage** uses [redb](https://github.com/cberner/redb), a pure-Rust embedded database with ACID transactions. No C dependencies (unlike boltdb/bbolt used by etcd).

## Building from Source

### Prerequisites

- Rust 1.70+ (install via [rustup](https://rustup.rs))
- protoc (Protocol Buffers compiler)

On Ubuntu/Debian:

```bash
sudo apt install -y protobuf-compiler
```

On macOS:

```bash
brew install protobuf
```

### Build

```bash
git clone https://github.com/alexandernicholson/barkeeper.git
cd barkeeper
cargo build --release
```

The binary will be at `./target/release/barkeeper`.

### Run tests

```bash
cargo test
```

This runs 55 tests covering the Raft state machine, log store, KV store (including MVCC, transactions, and compaction), and etcd HTTP gateway API compatibility.

## Differences from etcd

| Feature | etcd | barkeeper |
|---------|------|-----------|
| Language | Go | Rust |
| Actor runtime | -- | Rebar (BEAM-inspired) |
| Storage engine | bbolt (cgo) | redb (pure Rust) |
| C dependencies | Yes (cgo) | None |
| Watch notifications | Real-time | Hub implemented, notifications in progress |
| Lease expiry | Timer-driven via Raft | TTL tracked, expiry timer in progress |
| Auth | Token-based | RBAC structure implemented, token enforcement in progress |
| Multi-node | Production-ready | Transport layer implemented, cluster bootstrap in progress |
| TLS | Full support | Not yet implemented |
| MVCC | Full | Implemented with revision history and compaction |
| Transactions | Full Txn API | Compare-and-swap with version/value targets |

barkeeper is a from-scratch implementation, not a fork of etcd. It aims for API compatibility, not code compatibility.

## License

Apache License 2.0 -- see [LICENSE](LICENSE).

The protobuf definitions in `proto/` are vendored from [etcd](https://github.com/etcd-io/etcd) and are also licensed under Apache 2.0. See [proto/LICENSE](proto/LICENSE).

## Credits

- [etcd](https://github.com/etcd-io/etcd) -- the protocol buffer definitions and API design that barkeeper implements
- [Rebar](https://github.com/alexandernicholson/rebar) -- the BEAM-inspired actor runtime powering barkeeper's concurrency model
- [redb](https://github.com/cberner/redb) -- the pure-Rust embedded database used for durable storage
