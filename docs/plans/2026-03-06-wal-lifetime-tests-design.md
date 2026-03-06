# WAL Lifetime Test Suite Design

**Date:** 2026-03-06
**Approach:** Rust integration tests only, layered (unit + multi-node)

## Context

The WAL (Write-Ahead Log) is the source of truth for barkeeper's persistence. Currently:
- 12 unit tests cover LogStore basics (append, truncate, reopen, hard state)
- 3 replication tests cover live-cluster data flow (no restarts)
- k8s tests cover pod restart at the black-box level
- No Rust tests exercise: WAL replay, crash recovery, snapshot+WAL interplay, node restart in a cluster, or multi-node failure

## Test Files

Two new test files following the existing convention (extend existing patterns, not new files):

### `tests/wal_lifetime_test.rs` — WAL & Store Mechanics

Tests LogStore, KvStore snapshot, and the WAL replay logic directly without gRPC servers.

#### A. Crash Recovery (LogStore level)

1. **`test_torn_write_recovery`** — Write a valid entry, append partial bytes (simulating crash mid-frame), reopen LogStore, verify only the complete entry survives and the partial tail is truncated.

2. **`test_corrupt_payload_recovery`** — Write a valid length prefix followed by garbage bytes, reopen, verify truncation to last good offset.

3. **`test_large_wal_reopen`** — Append 1000 entries, close, reopen, verify all entries present with correct indexing via `get()` and `get_range()`.

4. **`test_truncate_to_zero`** — Call `truncate_after(0)`, verify WAL is empty, then append new entries and verify they work correctly.

#### B. Snapshot + WAL Replay (server.rs startup path)

Extract the replay logic from `server.rs` lines 93-138 into a testable helper function (`replay_wal_to_store` or similar) that takes a `&LogStore` and `&KvStore`.

5. **`test_wal_replay_no_snapshot`** — Write KvCommand entries to LogStore, open fresh KvStore (no snapshot file), run replay, verify all keys present in the store.

6. **`test_wal_replay_with_snapshot`** — Write 10 entries, snapshot the KvStore (sets `raft_applied_index`), write 10 more entries to WAL, reopen KvStore from snapshot, replay remaining entries, verify all 20 keys present.

7. **`test_wal_replay_idempotency`** — Run replay on a WAL, then run replay again on the same WAL+store. Verify no duplicate keys, revisions unchanged. This tests the `index <= last_applied` guard in the state machine.

8. **`test_snapshot_then_crash_mid_wal`** — Snapshot at entry N, write more entries plus a torn tail, reopen from snapshot, replay. Verify good entries after the snapshot are recovered, torn entry is discarded.

#### C. Revision Continuity

9. **`test_revisions_survive_restart`** — Write entries with revisions 1-10, snapshot, close, reopen from snapshot, apply new commands, verify revisions continue from 11+ (no reset to 0 or duplicate).

10. **`test_revision_counter_matches_store`** — After WAL replay, verify `kv_store.current_revision()` matches the last revision in the replayed entries.

### `tests/cluster_resilience_test.rs` — Multi-Node Failure & Recovery

Extends the `ReplicationCluster` pattern from `replication_test.rs`. Adds `stop_node(id)` and `restart_node(id)` methods.

#### Implementation: Node Stop/Restart

```
stop_node(id):
  - Abort the Raft tokio task and apply loop task
  - Drop the RaftHandle (closes proposal channels)
  - Remove from relay routing (messages to this node are dropped)
  - Keep the data directory intact

restart_node(id):
  - Re-open LogStore from existing data_dir/node-{id}
  - Re-open KvStore from same directory (loads snapshot)
  - Run WAL replay (same logic as server.rs startup)
  - Spawn fresh Raft actor and apply loop
  - Re-register in relay routing and peer map
  - Node catches up via AppendEntries from current leader
```

#### D. Single-Node Loss and Recovery

11. **`test_single_follower_restart_catches_up`** — Write 10 keys, stop a follower, write 10 more keys, restart the follower, verify it catches up to all 20 keys (via WAL replay of its existing entries + AppendEntries replication of new ones).

12. **`test_leader_loss_triggers_reelection`** — Write keys, identify and stop the leader, verify a new leader is elected within timeout, write more keys via the new leader successfully.

13. **`test_leader_restart_rejoins_as_follower`** — Stop the leader, wait for new leader, restart the old leader, write keys via new leader, verify old leader receives all data (WAL replay + replication catch-up).

#### E. Multi-Node Loss

14. **`test_minority_loss_cluster_continues`** — Stop 1 of 3 nodes, verify the remaining 2-node quorum can still accept and commit writes.

15. **`test_majority_loss_blocks_writes`** — Stop 2 of 3 nodes, verify proposals timeout (no quorum), restart both nodes, verify cluster recovers and can accept writes again.

16. **`test_all_nodes_restart_from_wal`** — Write 20 keys, stop all 3 nodes, restart all 3 from their WAL directories, wait for election, verify all 20 keys survive on all nodes.

#### F. WAL Divergence and Conflict Resolution

17. **`test_follower_conflict_resolution`** — Partition a follower, let the leader write entries, reconnect, verify the Raft log-matching protocol correctly overwrites any stale entries on the follower with the leader's entries (AppendEntries with prev_log_index/term mismatch triggers truncation + re-append).

18. **`test_writes_during_leader_transition`** — Write keys rapidly while killing the leader mid-stream. Verify all committed keys (those that received Success responses) survive on all nodes. Uncommitted proposals may be lost but no data corruption occurs.

## Bugs to Fix

During design exploration, one bug was identified:

- **`compacted_revision` not persisted in snapshot:** `KvInner.compacted_revision` is not included in the `Snapshot` struct. After restart, the store loses track of what was compacted. Fix: add `compacted_revision` to the `Snapshot` struct and restore it on load.

## Dependencies

- Extract WAL replay logic from `server.rs` into a reusable function (used by both server startup and tests)
- Add `stop_node`/`restart_node` to a resilience-aware cluster test harness
- No new crate dependencies required

## Success Criteria

All 18 tests pass. The test suite runs in under 60 seconds total (WAL tests ~5s, cluster tests ~45s given election timeouts).
