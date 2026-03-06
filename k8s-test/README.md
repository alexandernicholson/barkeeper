# Kubernetes Integration Tests

Multi-node integration test suite for Barkeeper. Deploys a 3-node StatefulSet in a local [kind](https://kind.sigs.k8s.io/) cluster and exercises the full etcd v3 API surface plus reliability scenarios.

## Prerequisites

- Docker
- [kind](https://kind.sigs.k8s.io/)
- kubectl
- etcdctl
- curl
- jq

## Usage

```bash
./k8s-test/run-tests.sh
```

The script handles everything end-to-end: builds the Docker image, creates a kind cluster, deploys Barkeeper, runs all tests, writes results, and tears down the cluster on exit.

## What It Tests

### etcd API Surface (41 tests)

| Category | Tests |
|----------|-------|
| KV (gRPC) | Put, Get, Overwrite, Delete, Prefix Get, Prefix Delete |
| KV (HTTP) | Put, Get, Delete, Range prefix |
| Transactions | Success branch, Failure branch, etcdctl txn |
| Leases | Grant, Put-with-lease, TimeToLive, List, Revoke, Key cleanup, etcdctl grant |
| Watch | PUT events, DELETE events, Prefix watch |
| Cluster | MemberList (HTTP + etcdctl) |
| Maintenance | Status, Defragment, Alarm, Snapshot, etcdctl status, etcdctl health |
| Auth | UserAdd/Get/List, RoleAdd/List/Get, RoleGrantPermission, UserGrantRole, Enable, Authenticate, Enforcement, PUT with token, Disable |
| Compaction | Revision query, Compact |

### Reliability (16 tests)

| Scenario | Description |
|----------|-------------|
| Write durability | 20-key write and read-back |
| Concurrent writes | 10 parallel writers with verification |
| Large values | 64KB value round-trip |
| Pod restart | Data persists after killing one pod |
| Full cluster restart | Data survives deleting all 3 pods simultaneously |
| Rapid operations | 100 sequential put/get cycles |
| Watch under load | Event delivery during 20-key write burst |

## Architecture

```
kind cluster (single control-plane node)
└── default namespace
    ├── Service: barkeeper (headless, for peer discovery)
    ├── Service: barkeeper-client (NodePort 30379/30380)
    └── StatefulSet: barkeeper (3 replicas)
        ├── barkeeper-0 (node-id=1)
        ├── barkeeper-1 (node-id=2)
        └── barkeeper-2 (node-id=3)
```

- **Headless Service** — stable DNS names for peer-to-peer communication (`barkeeper-{0,1,2}.barkeeper.default.svc.cluster.local`)
- **NodePort Service** — external access for test runner
- **PersistentVolumeClaims** — 1Gi per pod at `/data` for WAL and snapshots
- **Leader detection** — the test script finds the Raft leader via `/v3/maintenance/status` and port-forwards to it

## Files

| File | Purpose |
|------|---------|
| `run-tests.sh` | Test runner script |
| `kind-config.yaml` | kind cluster configuration |
| `barkeeper.yaml` | Kubernetes manifests (Services + StatefulSet) |
| `results.md` | Auto-generated test results from last run |
