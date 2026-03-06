# Barkeeper Kubernetes Integration Test Results

**Date:** 2026-03-06 04:22:26 UTC
**Cluster:** kind (barkeeper-test), 3-node StatefulSet
**Image:** barkeeper:local (built from 30a98f4)

## Summary

| Metric | Count |
|--------|-------|
| **Total** | 57 |
| **Pass** | 57 |
| **Fail** | 0 |
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


## Pod Status (at test completion)

```
NAME          READY   STATUS    RESTARTS      AGE   IP            NODE                           NOMINATED NODE   READINESS GATES
barkeeper-0   1/1     Running   1 (31s ago)   31s   10.244.0.14   barkeeper-test-control-plane   <none>           <none>
barkeeper-1   1/1     Running   1 (31s ago)   31s   10.244.0.13   barkeeper-test-control-plane   <none>           <none>
barkeeper-2   1/1     Running   1 (31s ago)   31s   10.244.0.12   barkeeper-test-control-plane   <none>           <none>
```

## Pod Logs (last 20 lines per pod)

### barkeeper-0
```
[2m2026-03-06T04:21:56.369952Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m detected existing data in /data, using initial-cluster-state=existing
[2m2026-03-06T04:21:56.369974Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m1 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.369992Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-0 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-06T04:21:56.371053Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-06T04:21:56.372229Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m2 [3mremote_pid[0m[2m=[0m<2.1>
[2m2026-03-06T04:21:56.372400Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m3 [3mremote_pid[0m[2m=[0m<3.1>
[2m2026-03-06T04:21:56.372573Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.372879Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m2 [3maddr[0m[2m=[0m10.244.0.13:2381
[2m2026-03-06T04:21:56.373189Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m3 [3maddr[0m[2m=[0m10.244.0.12:2381
[2m2026-03-06T04:21:56.373366Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m1
[2m2026-03-06T04:21:56.373467Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-06T04:21:56.373738Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-06T04:21:56.373739Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-06T04:21:56.374191Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-06T04:21:56.374213Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-06T04:21:57.361839Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-06T04:21:57.370019Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
```

### barkeeper-1
```
[2m2026-03-06T04:21:56.352843Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m2 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.352862Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-1 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-06T04:21:56.353697Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m replaying WAL entries to restore KV state [3mlast_applied[0m[2m=[0m0 [3mlast_log_index[0m[2m=[0m121
[2m2026-03-06T04:21:56.357028Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m WAL replay complete, snapshot saved [3mentries[0m[2m=[0m83 [3mlast_log_index[0m[2m=[0m121
[2m2026-03-06T04:21:56.357198Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-06T04:21:56.358099Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m1 [3mremote_pid[0m[2m=[0m<1.1>
[2m2026-03-06T04:21:56.358119Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m3 [3mremote_pid[0m[2m=[0m<3.1>
[2m2026-03-06T04:21:56.358160Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.359619Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.14:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-06T04:21:56.360003Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m3 [3maddr[0m[2m=[0m10.244.0.12:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-06T04:21:56.369144Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-06T04:21:56.372876Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-06T04:21:57.361800Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.14:2381
[2m2026-03-06T04:21:57.361971Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m3 [3maddr[0m[2m=[0m10.244.0.12:2381
[2m2026-03-06T04:21:57.361987Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m2
[2m2026-03-06T04:21:57.361997Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-06T04:21:57.362159Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-06T04:21:57.362162Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-06T04:21:57.362474Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-06T04:21:57.362496Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
```

### barkeeper-2
```
[2m2026-03-06T04:21:56.363628Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m detected existing data in /data, using initial-cluster-state=existing
[2m2026-03-06T04:21:56.363663Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m multi-node cluster mode [3mnode_id[0m[2m=[0m3 [3mpeer_count[0m[2m=[0m2 [3mlisten_peer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.363682Z[0m [32m INFO[0m [2mbarkeeper[0m[2m:[0m barkeeper starting [3mname[0m[2m=[0mbarkeeper-2 [3maddr[0m[2m=[0m0.0.0.0:2379
[2m2026-03-06T04:21:56.364486Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m replaying WAL entries to restore KV state [3mlast_applied[0m[2m=[0m0 [3mlast_log_index[0m[2m=[0m121
[2m2026-03-06T04:21:56.367723Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m WAL replay complete, snapshot saved [3mentries[0m[2m=[0m83 [3mlast_log_index[0m[2m=[0m121
[2m2026-03-06T04:21:56.367958Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m started Rebar supervisor
[2m2026-03-06T04:21:56.368745Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m2 [3mremote_pid[0m[2m=[0m<2.1>
[2m2026-03-06T04:21:56.368866Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered peer Raft PID [3mpeer_node_id[0m[2m=[0m1 [3mremote_pid[0m[2m=[0m<1.1>
[2m2026-03-06T04:21:56.368937Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m TCP peer listener started [3mpeer_addr[0m[2m=[0m0.0.0.0:2381
[2m2026-03-06T04:21:56.369138Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m2 [3maddr[0m[2m=[0m10.244.0.13:2381
[2m2026-03-06T04:21:56.369313Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m peer not ready, will retry [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.14:2381 [3mattempt[0m[2m=[0m1 [3merror[0m[2m=[0mtransport error: io error: Connection refused (os error 111)
[2m2026-03-06T04:21:56.373173Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-06T04:21:57.361940Z[0m [34mDEBUG[0m [2mbarkeeper::api::server[0m[2m:[0m accepted inbound peer connection
[2m2026-03-06T04:21:57.370027Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m connected to peer [3mnode_id[0m[2m=[0m1 [3maddr[0m[2m=[0m10.244.0.14:2381
[2m2026-03-06T04:21:57.370044Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m all peers connected [3mattempt[0m[2m=[0m2
[2m2026-03-06T04:21:57.370053Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m SWIM membership initialized with seed peers [3mpeer_count[0m[2m=[0m2
[2m2026-03-06T04:21:57.370332Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m lease expiry timer started
[2m2026-03-06T04:21:57.370334Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m registered lease_expiry_timer with supervisor
[2m2026-03-06T04:21:57.370662Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting HTTP gateway [3mhttp_addr[0m[2m=[0m0.0.0.0:2380 [3mscheme[0m[2m=[0mhttp
[2m2026-03-06T04:21:57.370688Z[0m [32m INFO[0m [2mbarkeeper::api::server[0m[2m:[0m starting gRPC server [3maddr[0m[2m=[0m0.0.0.0:2379
```

