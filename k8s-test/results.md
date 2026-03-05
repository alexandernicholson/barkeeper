# Barkeeper Kubernetes Integration Test Results

**Date:** 2026-03-05 02:46:44 UTC
**Cluster:** kind (barkeeper-test), 3-node StatefulSet
**Image:** barkeeper:local (built from 88eb30c)

## Summary

| Metric | Count |
|--------|-------|
| **Total** | 57 |
| **Pass** | 56 |
| **Fail** | 1 |
| **Skip** | 0 |

## Test Categories

### etcd API Surface
- KV Operations (gRPC): Put, Get, Overwrite, Delete, Prefix Get, Prefix Delete
- KV Operations (HTTP): Put, Get, Delete, Range All
- Transactions: Success branch, Failure branch, etcdctl txn
- Leases: Grant, Put-with-lease, TimeToLive, List, Revoke, Key cleanup
- Watch: PUT events, DELETE events, Prefix watch
- Cluster: MemberList (HTTP + etcdctl)
- Maintenance: Status, Defragment, Alarm, Snapshot, etcdctl status/health
- Auth: UserAdd, UserGet, UserList, RoleAdd, RoleList, RoleGrantPermission,
  RoleGet, UserGrantRole, Enable, Authenticate, Enforcement, Disable
- Compaction: Revision query, Compact

### Reliability
- Write durability: 20-key write and read-back
- Concurrent writes: 10 parallel writers
- Large values: 64KB value round-trip
- Pod restart: Data persists after killing barkeeper-0
- Rolling restart: Data survives restarting all 3 pods sequentially
- Rapid operations: 100 sequential put/get cycles
- Watch under load: Event delivery during 20-key write burst


## Failures

| # | Test | Details |
|---|------|---------|
| 1 | Txn via etcdctl: Error: EOF |

## Pod Status (at test completion)

```
NAME          READY   STATUS    RESTARTS      AGE   IP            NODE                           NOMINATED NODE   READINESS GATES
barkeeper-0   1/1     Running   1 (28s ago)   28s   10.244.0.13   barkeeper-test-control-plane   <none>           <none>
barkeeper-1   1/1     Running   1 (28s ago)   28s   10.244.0.12   barkeeper-test-control-plane   <none>           <none>
barkeeper-2   1/1     Running   1 (27s ago)   28s   10.244.0.14   barkeeper-test-control-plane   <none>           <none>
```

## Pod Logs (last 20 lines per pod)

### barkeeper-0
```
[2m2026-03-05T02:46:17.481092Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m detected existing data in /data, using initial-cluster-state=existing
[2m2026-03-05T02:46:17.481127Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m1 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.481148Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-0 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-05T02:46:17.493472Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-05T02:46:17.505761Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m3 [3mremote_pid[0m[2m=[0m<3.1>
[2m2026-03-05T02:46:17.505779Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m2 [3mremote_pid[0m[2m=[0m<2.1>
[2m2026-03-05T02:46:17.505812Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.505998Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m3 [3maddr[0m[2m=[0m10.244.0.14:2381
[2m2026-03-05T02:46:17.506297Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m2 [3maddr[0m[2m=[0m10.244.0.12:2381
[2m2026-03-05T02:46:17.506312Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m1
[2m2026-03-05T02:46:17.506321Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-05T02:46:17.506453Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-05T02:46:17.506493Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-05T02:46:17.506708Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-05T02:46:17.506731Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-05T02:46:18.485425Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-05T02:46:18.504020Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
```

### barkeeper-1
```
[2m2026-03-05T02:46:17.466974Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m detected existing data in /data, using initial-cluster-state=existing
[2m2026-03-05T02:46:17.467330Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m2 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.467498Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-1 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-05T02:46:17.488702Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-05T02:46:17.501820Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m1 [3mremote_pid[0m[2m=[0m<1.1>
[2m2026-03-05T02:46:17.501843Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m3 [3mremote_pid[0m[2m=[0m<3.1>
[2m2026-03-05T02:46:17.501892Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.502132Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.13:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-05T02:46:17.502406Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m3 [3maddr[0m[2m=[0m10.244.0.14:2381
[2m2026-03-05T02:46:17.506226Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-05T02:46:18.485303Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-05T02:46:18.504043Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.13:2381
[2m2026-03-05T02:46:18.504064Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m2
[2m2026-03-05T02:46:18.504074Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-05T02:46:18.504202Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-05T02:46:18.504214Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-05T02:46:18.504532Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-05T02:46:18.504552Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
```

### barkeeper-2
```
[2m2026-03-05T02:46:17.449538Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m detected existing data in /data, using initial-cluster-state=existing
[2m2026-03-05T02:46:17.449591Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m3 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.449609Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-2 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-05T02:46:17.464771Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-05T02:46:17.483592Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m2 [3mremote_pid[0m[2m=[0m<2.1>
[2m2026-03-05T02:46:17.483615Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m1 [3mremote_pid[0m[2m=[0m<1.1>
[2m2026-03-05T02:46:17.483659Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-05T02:46:17.483989Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m2 [3maddr[0m[2m=[0m10.244.0.12:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-05T02:46:17.484395Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.13:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-05T02:46:17.502334Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-05T02:46:17.505958Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-05T02:46:18.485288Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m2 [3maddr[0m[2m=[0m10.244.0.12:2381
[2m2026-03-05T02:46:18.485521Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.13:2381
[2m2026-03-05T02:46:18.485576Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m2
[2m2026-03-05T02:46:18.485586Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-05T02:46:18.485728Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-05T02:46:18.485737Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-05T02:46:18.485969Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-05T02:46:18.485989Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
```

