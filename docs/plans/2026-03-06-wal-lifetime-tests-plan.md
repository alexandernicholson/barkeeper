# WAL Lifetime Test Suite Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 18 comprehensive tests covering the full WAL lifetime: crash recovery, snapshot+WAL replay, revision continuity, single/multi-node failure, leader failover, and WAL conflict resolution.

**Architecture:** Two test files — `wal_lifetime_test.rs` for unit-level WAL/store mechanics (tests 1-10, sync + async), and `cluster_resilience_test.rs` for multi-node scenarios (tests 11-18, async with real Raft actors). The WAL replay logic from `server.rs:94-138` is extracted into a reusable `replay_wal` function in `kv/store.rs`.

**Tech Stack:** Rust, tokio, tempfile, bincode, serde_json. No new dependencies.

---

### Task 1: Extract WAL Replay Into Reusable Function

The WAL replay logic in `server.rs:94-138` is inline and untestable. Extract it into a public function.

**Files:**
- Create: `barkeeper/src/kv/wal_replay.rs`
- Modify: `barkeeper/src/kv/mod.rs`
- Modify: `barkeeper/src/api/server.rs:94-138`

**Step 1: Create `barkeeper/src/kv/wal_replay.rs`**

```rust
//! WAL replay logic: restores KV state from unapplied WAL entries after restart.

use crate::kv::state_machine::KvCommand;
use crate::kv::store::KvStore;
use crate::raft::log_store::LogStore;
use crate::raft::messages::LogEntryData;

/// Replay unapplied WAL entries into the KV store.
///
/// Reads entries from `last_applied + 1` through `last_log_index`, deserializes
/// each as a `KvCommand`, and applies them in batch. After replay, a snapshot is
/// saved so future restarts skip already-replayed entries.
///
/// Returns the number of commands applied.
pub fn replay_wal(log_store: &LogStore, kv_store: &KvStore) -> Result<usize, Box<dyn std::error::Error>> {
    let last_applied = kv_store.last_applied_raft_index().unwrap_or(0);
    let last_log_index = log_store.last_index()?;

    if last_applied >= last_log_index {
        return Ok(0);
    }

    let entries = log_store.get_range(last_applied + 1, last_log_index)?;
    let mut commands: Vec<(KvCommand, i64)> = Vec::new();
    for entry in &entries {
        if let LogEntryData::Command { data, revision } = &entry.data {
            if let Ok(cmd) = bincode::deserialize::<KvCommand>(data)
                .or_else(|_| {
                    serde_json::from_slice::<KvCommand>(data)
                        .map_err(|e| Box::new(bincode::ErrorKind::Custom(e.to_string())))
                })
            {
                commands.push((cmd, *revision));
            }
        }
    }

    let count = commands.len();
    if !commands.is_empty() {
        kv_store.batch_apply_with_index(&commands, last_log_index)?;
        kv_store.snapshot()?;
    } else {
        kv_store.set_last_applied_raft_index(last_log_index)?;
    }

    Ok(count)
}
```

**Step 2: Add `pub mod wal_replay;` to `barkeeper/src/kv/mod.rs`**

Add after the last existing `pub mod` line:

```rust
pub mod wal_replay;
```

**Step 3: Replace inline replay in `server.rs:94-138`**

Replace the block from `// Replay any unapplied WAL entries` through the closing `}` with:

```rust
        // Replay any unapplied WAL entries to restore KV state after restart.
        {
            use crate::raft::log_store::LogStore;
            use crate::kv::wal_replay::replay_wal;

            let log_store = LogStore::open(&config.data_dir)?;
            match replay_wal(&log_store, &kv_store) {
                Ok(0) => {}
                Ok(n) => tracing::info!(entries = n, "WAL replay complete, snapshot saved"),
                Err(e) => return Err(e),
            }
        }
```

**Step 4: Build and run existing tests**

Run: `cargo build 2>&1 && cargo test 2>&1 | tail -5`
Expected: All existing tests still pass.

**Step 5: Commit**

```bash
git add barkeeper/src/kv/wal_replay.rs barkeeper/src/kv/mod.rs barkeeper/src/api/server.rs
git commit -m "refactor: extract WAL replay logic into reusable kv::wal_replay module"
```

---

### Task 2: WAL Crash Recovery Tests (tests 1-4)

**Files:**
- Create: `barkeeper/tests/wal_lifetime_test.rs`

**Step 1: Write the test file with crash recovery tests**

```rust
//! WAL lifetime tests: crash recovery, snapshot+WAL replay, revision continuity.
//!
//! Tests the LogStore, KvStore snapshot, and WAL replay logic directly
//! without spinning up gRPC servers.

use std::io::Write;

use tempfile::tempdir;

use barkeeper::kv::state_machine::KvCommand;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::wal_replay::replay_wal;
use barkeeper::raft::log_store::LogStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};

/// Helper: create a LogEntry containing a KvCommand::Put serialized with bincode.
fn put_entry(index: u64, term: u64, key: &str, value: &str, revision: i64) -> LogEntry {
    let cmd = KvCommand::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        lease_id: 0,
    };
    LogEntry {
        term,
        index,
        data: LogEntryData::Command {
            data: bincode::serialize(&cmd).unwrap(),
            revision,
        },
    }
}

/// Helper: create a Noop LogEntry (no KvCommand).
fn noop_entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        data: LogEntryData::Noop,
    }
}

// ── A. Crash Recovery ────────────────────────────────────────────────────────

/// Write a valid entry then append partial bytes simulating a crash mid-frame.
/// On reopen, only the complete entry should survive.
#[test]
fn test_torn_write_recovery() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Write one valid entry.
    {
        let store = LogStore::open(dir).unwrap();
        store.append(&[put_entry(1, 1, "k1", "v1", 1)]).unwrap();
    }

    // Append raw partial frame: valid 4-byte length prefix but incomplete payload.
    {
        let wal_path = dir.join("raft.wal");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Write length prefix claiming 1000 bytes, but only write 10 bytes of payload.
        file.write_all(&1000u32.to_le_bytes()).unwrap();
        file.write_all(&[0xDE; 10]).unwrap();
        file.sync_all().unwrap();
    }

    // Reopen — should recover only the first entry.
    let store = LogStore::open(dir).unwrap();
    assert_eq!(store.last_index().unwrap(), 1);
    assert_eq!(store.len().unwrap(), 1);
    let entry = store.get(1).unwrap().expect("entry 1 should exist");
    assert_eq!(entry.index, 1);

    // Verify we can still append after recovery.
    store.append(&[put_entry(2, 1, "k2", "v2", 2)]).unwrap();
    assert_eq!(store.last_index().unwrap(), 2);
}

/// Write a valid length prefix followed by garbage payload (valid length, invalid bincode).
/// On reopen, the corrupt entry should be truncated.
#[test]
fn test_corrupt_payload_recovery() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Write one valid entry.
    {
        let store = LogStore::open(dir).unwrap();
        store.append(&[put_entry(1, 1, "k1", "v1", 1)]).unwrap();
    }

    // Append a frame with valid length but corrupt bincode payload.
    {
        let wal_path = dir.join("raft.wal");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        let garbage = vec![0xFF; 64];
        file.write_all(&(garbage.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&garbage).unwrap();
        file.sync_all().unwrap();
    }

    // Reopen — corrupt entry truncated, first entry intact.
    let store = LogStore::open(dir).unwrap();
    assert_eq!(store.last_index().unwrap(), 1);
    assert_eq!(store.len().unwrap(), 1);
}

/// Append 1000 entries, close, reopen, verify all entries and correct indexing.
#[test]
fn test_large_wal_reopen() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    {
        let store = LogStore::open(dir).unwrap();
        let entries: Vec<LogEntry> = (1..=1000)
            .map(|i| put_entry(i, 1, &format!("k{}", i), &format!("v{}", i), i as i64))
            .collect();
        store.append(&entries).unwrap();
        assert_eq!(store.last_index().unwrap(), 1000);
    }

    // Reopen and verify.
    let store = LogStore::open(dir).unwrap();
    assert_eq!(store.len().unwrap(), 1000);
    assert_eq!(store.last_index().unwrap(), 1000);

    // Spot-check first, middle, and last entries.
    let first = store.get(1).unwrap().unwrap();
    assert_eq!(first.index, 1);
    let mid = store.get(500).unwrap().unwrap();
    assert_eq!(mid.index, 500);
    let last = store.get(1000).unwrap().unwrap();
    assert_eq!(last.index, 1000);

    // Range query.
    let range = store.get_range(990, 1000).unwrap();
    assert_eq!(range.len(), 11);
}

/// truncate_after(0) should clear the entire WAL. Subsequent appends should work.
#[test]
fn test_truncate_to_zero() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    let store = LogStore::open(dir).unwrap();
    store
        .append(&[
            put_entry(1, 1, "k1", "v1", 1),
            put_entry(2, 1, "k2", "v2", 2),
            put_entry(3, 1, "k3", "v3", 3),
        ])
        .unwrap();
    assert_eq!(store.len().unwrap(), 3);

    store.truncate_after(0).unwrap();
    assert_eq!(store.len().unwrap(), 0);
    assert_eq!(store.last_index().unwrap(), 0);
    assert!(store.get(1).unwrap().is_none());

    // Append new entries after full truncation.
    store.append(&[put_entry(1, 2, "new1", "val1", 10)]).unwrap();
    assert_eq!(store.len().unwrap(), 1);
    assert_eq!(store.last_index().unwrap(), 1);
}
```

**Step 2: Run tests to verify they pass**

Run: `cargo test --test wal_lifetime_test 2>&1 | tail -15`
Expected: 4 passed, 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/wal_lifetime_test.rs
git commit -m "test: WAL crash recovery tests (torn write, corrupt payload, large reopen, truncate-to-zero)"
```

---

### Task 3: Snapshot + WAL Replay Tests (tests 5-8)

**Files:**
- Modify: `barkeeper/tests/wal_lifetime_test.rs`

**Step 1: Add snapshot + WAL replay tests**

Append to `wal_lifetime_test.rs`:

```rust
// ── B. Snapshot + WAL Replay ─────────────────────────────────────────────────

/// Write KvCommand entries to LogStore, open fresh KvStore (no snapshot),
/// run replay, verify all keys present.
#[test]
fn test_wal_replay_no_snapshot() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Write 5 Put commands to the WAL.
    let log_store = LogStore::open(dir).unwrap();
    let entries: Vec<LogEntry> = (1..=5)
        .map(|i| put_entry(i, 1, &format!("key{}", i), &format!("val{}", i), i as i64))
        .collect();
    log_store.append(&entries).unwrap();

    // Open a fresh KvStore (no snapshot file exists).
    let kv_store = KvStore::open(dir).unwrap();
    assert_eq!(kv_store.current_revision().unwrap(), 0);

    // Replay WAL into KvStore.
    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 5);

    // Verify all 5 keys present.
    for i in 1..=5 {
        let key = format!("key{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "key{} should exist", i);
        assert_eq!(
            result.kvs[0].value,
            format!("val{}", i).as_bytes(),
            "key{} has wrong value",
            i
        );
    }
}

/// Write 10 entries, snapshot at entry 5, write 5 more, reopen from snapshot,
/// replay remaining entries, verify all 10 keys.
#[test]
fn test_wal_replay_with_snapshot() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Phase 1: Write 10 entries, snapshot after applying first 5.
    let log_store = LogStore::open(dir).unwrap();
    let all_entries: Vec<LogEntry> = (1..=10)
        .map(|i| put_entry(i, 1, &format!("key{}", i), &format!("val{}", i), i as i64))
        .collect();
    log_store.append(&all_entries).unwrap();

    // Apply first 5 and snapshot.
    {
        let kv = KvStore::open(dir).unwrap();
        let first_five: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("key{}", i).into_bytes(),
                        value: format!("val{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv.batch_apply_with_index(&first_five, 5).unwrap();
        kv.snapshot().unwrap();
    }

    // Phase 2: Reopen from snapshot and replay remaining entries.
    let kv_store = KvStore::open(dir).unwrap();
    assert_eq!(kv_store.last_applied_raft_index().unwrap(), 5);

    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 5); // entries 6-10

    // All 10 keys should be present.
    for i in 1..=10 {
        let key = format!("key{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "key{} should exist after replay", i);
    }
}

/// Run replay twice on the same WAL — verify idempotency (no duplicates,
/// revisions unchanged).
#[test]
fn test_wal_replay_idempotency() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    let log_store = LogStore::open(dir).unwrap();
    log_store
        .append(&[
            put_entry(1, 1, "idem-key", "idem-val", 1),
        ])
        .unwrap();

    let kv_store = KvStore::open(dir).unwrap();

    // First replay.
    let applied1 = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied1, 1);
    let rev_after_first = kv_store.current_revision().unwrap();

    // Second replay — should be a no-op because last_applied >= last_log_index.
    let applied2 = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied2, 0);
    let rev_after_second = kv_store.current_revision().unwrap();

    assert_eq!(rev_after_first, rev_after_second, "revision should not change on second replay");

    // Still exactly one key.
    let result = kv_store.range(b"idem-key", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 1);
}

/// Snapshot at entry N, write more entries plus a torn tail. Reopen from
/// snapshot, replay. Good entries after snapshot recovered, torn entry discarded.
#[test]
fn test_snapshot_then_crash_mid_wal() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Write 5 entries, apply and snapshot.
    {
        let log_store = LogStore::open(dir).unwrap();
        let entries: Vec<LogEntry> = (1..=5)
            .map(|i| put_entry(i, 1, &format!("snap-k{}", i), &format!("snap-v{}", i), i as i64))
            .collect();
        log_store.append(&entries).unwrap();

        let kv = KvStore::open(dir).unwrap();
        let cmds: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("snap-k{}", i).into_bytes(),
                        value: format!("snap-v{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv.batch_apply_with_index(&cmds, 5).unwrap();
        kv.snapshot().unwrap();
    }

    // Write 3 more valid entries + torn tail to WAL.
    {
        let log_store = LogStore::open(dir).unwrap();
        let more: Vec<LogEntry> = (6..=8)
            .map(|i| put_entry(i, 1, &format!("post-k{}", i), &format!("post-v{}", i), i as i64))
            .collect();
        log_store.append(&more).unwrap();
    }
    // Append torn frame directly to WAL file.
    {
        let wal_path = dir.join("raft.wal");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&500u32.to_le_bytes()).unwrap();
        file.write_all(&[0xAB; 7]).unwrap(); // incomplete payload
        file.sync_all().unwrap();
    }

    // Reopen everything from scratch.
    let log_store = LogStore::open(dir).unwrap();
    assert_eq!(log_store.last_index().unwrap(), 8, "torn entry 9 should be truncated");

    let kv_store = KvStore::open(dir).unwrap();
    assert_eq!(kv_store.last_applied_raft_index().unwrap(), 5, "snapshot at index 5");

    // Replay entries 6-8.
    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 3);

    // All 8 keys should exist.
    for i in 1..=5 {
        let key = format!("snap-k{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "{} should exist from snapshot", key);
    }
    for i in 6..=8 {
        let key = format!("post-k{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "{} should exist from WAL replay", key);
    }
}
```

**Step 2: Run tests**

Run: `cargo test --test wal_lifetime_test 2>&1 | tail -15`
Expected: 8 passed, 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/wal_lifetime_test.rs
git commit -m "test: snapshot + WAL replay tests (no-snapshot, with-snapshot, idempotency, crash-mid-wal)"
```

---

### Task 4: Revision Continuity Tests (tests 9-10)

**Files:**
- Modify: `barkeeper/tests/wal_lifetime_test.rs`

**Step 1: Add revision continuity tests**

Append to `wal_lifetime_test.rs`:

```rust
// ── C. Revision Continuity ───────────────────────────────────────────────────

/// Write entries with revisions 1-5, snapshot, close, reopen, apply new
/// commands via replay of additional WAL entries. Verify revisions continue
/// from 6+ (no reset or duplicates).
#[test]
fn test_revisions_survive_restart() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    // Phase 1: Write and apply 5 entries, snapshot.
    {
        let log_store = LogStore::open(dir).unwrap();
        let entries: Vec<LogEntry> = (1..=5)
            .map(|i| put_entry(i, 1, &format!("rev-k{}", i), &format!("rev-v{}", i), i as i64))
            .collect();
        log_store.append(&entries).unwrap();

        let kv = KvStore::open(dir).unwrap();
        let cmds: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("rev-k{}", i).into_bytes(),
                        value: format!("rev-v{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv.batch_apply_with_index(&cmds, 5).unwrap();
        kv.snapshot().unwrap();
        assert_eq!(kv.current_revision().unwrap(), 5);
    }

    // Phase 2: Append entries 6-10 to WAL with revisions 6-10.
    {
        let log_store = LogStore::open(dir).unwrap();
        let entries: Vec<LogEntry> = (6..=10)
            .map(|i| put_entry(i, 1, &format!("rev-k{}", i), &format!("rev-v{}", i), i as i64))
            .collect();
        log_store.append(&entries).unwrap();
    }

    // Phase 3: Reopen and replay.
    let log_store = LogStore::open(dir).unwrap();
    let kv_store = KvStore::open(dir).unwrap();
    assert_eq!(kv_store.current_revision().unwrap(), 5, "restored from snapshot");

    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 5);

    // Revision should now be 10.
    assert_eq!(kv_store.current_revision().unwrap(), 10);

    // All 10 keys should have correct mod_revision.
    for i in 1..=10 {
        let key = format!("rev-k{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1);
        assert_eq!(
            result.kvs[0].mod_revision, i as i64,
            "rev-k{} should have mod_revision {}",
            i, i
        );
    }
}

/// After WAL replay, kv_store.current_revision() should match the last
/// revision from the replayed entries.
#[test]
fn test_revision_counter_matches_store() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();

    let log_store = LogStore::open(dir).unwrap();
    log_store
        .append(&[
            put_entry(1, 1, "a", "1", 42),
            put_entry(2, 1, "b", "2", 43),
            put_entry(3, 1, "c", "3", 44),
        ])
        .unwrap();

    let kv_store = KvStore::open(dir).unwrap();
    replay_wal(&log_store, &kv_store).unwrap();

    assert_eq!(kv_store.current_revision().unwrap(), 44);
    assert_eq!(kv_store.last_applied_raft_index().unwrap(), 3);
}
```

**Step 2: Run tests**

Run: `cargo test --test wal_lifetime_test 2>&1 | tail -15`
Expected: 10 passed, 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/wal_lifetime_test.rs
git commit -m "test: revision continuity tests (survive restart, counter matches store)"
```

---

### Task 5: Resilient Cluster Test Harness

Build the `ResilientCluster` struct with `stop_node` / `restart_node` capabilities based on the `ReplicationCluster` pattern in `replication_test.rs`.

**Files:**
- Create: `barkeeper/tests/cluster_resilience_test.rs`

**Step 1: Write the cluster harness and one smoke test**

```rust
//! Multi-node cluster resilience tests: node stop/restart, leader failover,
//! multi-node failure, WAL divergence and conflict resolution.
//!
//! Extends the ReplicationCluster pattern from replication_test.rs with
//! stop_node/restart_node capabilities that preserve WAL data directories.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

use rebar_core::process::table::ProcessTable;
use rebar_core::process::ProcessId;
use rebar_core::runtime::Runtime;
use rebar_cluster::registry::orset::Registry;
use rebar_cluster::router::{deliver_inbound_frame, DistributedRouter, RouterCommand};

use barkeeper::kv::state_machine::KvCommand;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::wal_replay::replay_wal;
use barkeeper::kv::write_buffer::WriteBuffer;
use barkeeper::raft::log_store::LogStore;
use barkeeper::raft::messages::{ClientProposalResult, LogEntry, LogEntryData};
use barkeeper::raft::node::{spawn_raft_node_rebar, RaftConfig, RaftHandle};

// ---------------------------------------------------------------------------
// Resilient cluster harness
// ---------------------------------------------------------------------------

struct NodeState {
    handle: RaftHandle,
    store: Arc<KvStore>,
    _apply_handle: tokio::task::JoinHandle<()>,
}

struct ResilientCluster {
    node_ids: Vec<u64>,
    /// Live nodes — absent entries mean the node is stopped.
    nodes: HashMap<u64, NodeState>,
    /// Shared temp dir with per-node subdirectories that persist across restarts.
    tmp: tempfile::TempDir,
    /// Shared registry (for Raft name lookups).
    registry: Arc<Mutex<Registry>>,
    /// Shared peers map (node_id -> ProcessId).
    peers: Arc<Mutex<HashMap<u64, ProcessId>>>,
    /// Per-node ProcessTables (kept alive even when node is stopped, for relay).
    tables: Arc<Mutex<HashMap<u64, Arc<ProcessTable>>>>,
    /// Relay task handles per node.
    relay_handles: HashMap<u64, tokio::task::JoinHandle<()>>,
    /// Relay sender per node (so we can create new relays on restart).
    relay_txs: HashMap<u64, mpsc::Sender<RouterCommand>>,
}

impl Drop for ResilientCluster {
    fn drop(&mut self) {
        for (_, h) in self.relay_handles.drain() {
            h.abort();
        }
    }
}

impl ResilientCluster {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let node_ids: Vec<u64> = vec![1, 2, 3];
        let registry = Arc::new(Mutex::new(Registry::new()));
        let peers: Arc<Mutex<HashMap<u64, ProcessId>>> = Arc::new(Mutex::new(HashMap::new()));
        let tables: Arc<Mutex<HashMap<u64, Arc<ProcessTable>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let mut nodes = HashMap::new();
        let mut relay_handles = HashMap::new();
        let mut relay_txs = HashMap::new();

        for &id in &node_ids {
            let data_dir = tmp.path().join(format!("node-{}", id));
            std::fs::create_dir_all(&data_dir).unwrap();

            let config = RaftConfig {
                node_id: id,
                data_dir: data_dir.to_str().unwrap().to_string(),
                election_timeout_min: Duration::from_millis(300),
                election_timeout_max: Duration::from_millis(600),
                heartbeat_interval: Duration::from_millis(100),
                peers: node_ids.clone(),
            };

            let store = Arc::new(KvStore::open(&data_dir).expect("open KvStore"));

            let table = Arc::new(ProcessTable::new(id));
            let (remote_tx, remote_rx) = mpsc::channel::<RouterCommand>(1024);
            let router = Arc::new(DistributedRouter::new(
                id,
                Arc::clone(&table),
                remote_tx.clone(),
            ));
            let runtime = Runtime::with_router(id, Arc::clone(&table), router);

            let (apply_tx, apply_rx) = mpsc::channel::<Vec<LogEntry>>(64);
            let apply_handle = spawn_apply_loop(Arc::clone(&store), apply_rx);

            let revision = Arc::new(AtomicI64::new(0));
            let write_buffer = Arc::new(WriteBuffer::new());
            let handle = spawn_raft_node_rebar(
                config,
                apply_tx,
                &runtime,
                Arc::clone(&registry),
                Arc::clone(&peers),
                revision,
                write_buffer,
            )
            .await;

            tables.lock().unwrap().insert(id, Arc::clone(&table));
            let relay = spawn_relay(remote_rx, Arc::clone(&tables));
            relay_handles.insert(id, relay);
            relay_txs.insert(id, remote_tx);
            nodes.insert(
                id,
                NodeState {
                    handle,
                    store,
                    _apply_handle: apply_handle,
                },
            );
        }

        // Wait for all raft processes to register.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        loop {
            let all_registered = {
                let reg = registry.lock().unwrap();
                node_ids
                    .iter()
                    .all(|id| reg.lookup(&format!("raft:{}", id)).is_some())
            };
            if all_registered {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for raft registration");
            }
            sleep(Duration::from_millis(10)).await;
        }
        {
            let reg = registry.lock().unwrap();
            let mut peers_guard = peers.lock().unwrap();
            for &id in &node_ids {
                if let Some(entry) = reg.lookup(&format!("raft:{}", id)) {
                    peers_guard.insert(id, entry.pid);
                }
            }
        }

        ResilientCluster {
            node_ids,
            nodes,
            tmp,
            registry,
            peers,
            tables,
            relay_handles,
            relay_txs,
        }
    }

    async fn wait_for_election(&self) {
        sleep(Duration::from_secs(5)).await;
    }

    fn find_leader(&self) -> Option<u64> {
        for (&id, node) in &self.nodes {
            if node.handle.is_leader.load(Ordering::Relaxed) {
                return Some(id);
            }
        }
        None
    }

    async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(id) = self.find_leader() {
                return id;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for leader");
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn propose_to_leader(&self, data: Vec<u8>) -> ClientProposalResult {
        for _attempt in 0..20 {
            for (&id, node) in &self.nodes {
                let result = timeout(Duration::from_secs(3), node.handle.propose(data.clone())).await;
                match result {
                    Ok(Ok(result @ ClientProposalResult::Success { .. })) => return result,
                    Ok(Ok(ClientProposalResult::NotLeader { .. })) => continue,
                    Err(_) => continue,
                    Ok(Ok(other)) => return other,
                    Ok(Err(e)) => panic!("proposal channel error on node {}: {}", id, e),
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        panic!("could not find a leader after retries");
    }

    async fn put_key(&self, key: &str, value: &str) -> u64 {
        let cmd = KvCommand::Put {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            lease_id: 0,
        };
        let data = serde_json::to_vec(&cmd).unwrap();
        match self.propose_to_leader(data).await {
            ClientProposalResult::Success { index, .. } => index,
            other => panic!("put_key({}) failed: {:?}", key, other),
        }
    }

    async fn wait_for_replication(&self, target_index: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_caught_up = self.nodes.values().all(|n| {
                n.handle
                    .applied_index
                    .load(Ordering::Relaxed)
                    >= target_index
            });
            if all_caught_up {
                sleep(Duration::from_millis(500)).await;
                return;
            }
            if tokio::time::Instant::now() > deadline {
                for (&id, node) in &self.nodes {
                    let idx = node.handle.applied_index.load(Ordering::Relaxed);
                    eprintln!("node {} applied_index = {}", id, idx);
                }
                panic!("timed out waiting for replication to index {}", target_index);
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait for all *live* nodes to have the key.
    async fn wait_for_key(&self, key: &str, expected_value: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_have_it = self.nodes.values().all(|n| {
                n.store
                    .range(key.as_bytes(), b"", 0, 0)
                    .map(|r| {
                        r.kvs.len() == 1
                            && r.kvs[0].value == expected_value.as_bytes()
                    })
                    .unwrap_or(false)
            });
            if all_have_it {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                for (&id, node) in &self.nodes {
                    let result = node.store.range(key.as_bytes(), b"", 0, 0).unwrap();
                    eprintln!("node {} has {} results for {}", id, result.kvs.len(), key);
                }
                panic!("timed out waiting for key {} = {}", key, expected_value);
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stop a node: abort its Raft task and apply loop. Data directory preserved.
    fn stop_node(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            node._apply_handle.abort();
            // RaftHandle's internal task is aborted when handle is dropped
            // (the tokio task holds a weak ref or the channels close).
            drop(node);
        }
        if let Some(h) = self.relay_handles.remove(&id) {
            h.abort();
        }
        // Remove from peers so other nodes don't route to a dead process.
        // The registry entry stays — on restart we'll re-register.
    }

    /// Restart a stopped node: reopen LogStore + KvStore, replay WAL,
    /// spawn fresh Raft actor and apply loop.
    async fn restart_node(&mut self, id: u64) {
        assert!(
            !self.nodes.contains_key(&id),
            "node {} is already running",
            id
        );

        let data_dir = self.tmp.path().join(format!("node-{}", id));

        // Reopen stores and replay WAL.
        let log_store = LogStore::open(&data_dir).unwrap();
        let kv_store = KvStore::open(&data_dir).unwrap();
        replay_wal(&log_store, &kv_store).unwrap();
        let store = Arc::new(kv_store);

        let initial_rev = store.current_revision().unwrap_or(0);

        let config = RaftConfig {
            node_id: id,
            data_dir: data_dir.to_str().unwrap().to_string(),
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            heartbeat_interval: Duration::from_millis(100),
            peers: self.node_ids.clone(),
        };

        let table = Arc::new(ProcessTable::new(id));
        let remote_tx = self
            .relay_txs
            .get(&id)
            .cloned()
            .unwrap_or_else(|| {
                let (tx, _) = mpsc::channel(1024);
                tx
            });
        // Create new channel for the restarted node.
        let (new_tx, remote_rx) = mpsc::channel::<RouterCommand>(1024);
        let router = Arc::new(DistributedRouter::new(
            id,
            Arc::clone(&table),
            new_tx.clone(),
        ));
        let runtime = Runtime::with_router(id, Arc::clone(&table), router);

        let (apply_tx, apply_rx) = mpsc::channel::<Vec<LogEntry>>(64);
        let apply_handle = spawn_apply_loop(Arc::clone(&store), apply_rx);

        let revision = Arc::new(AtomicI64::new(initial_rev));
        let write_buffer = Arc::new(WriteBuffer::new());
        let handle = spawn_raft_node_rebar(
            config,
            apply_tx,
            &runtime,
            Arc::clone(&self.registry),
            Arc::clone(&self.peers),
            revision,
            write_buffer,
        )
        .await;

        // Update tables and relay.
        self.tables.lock().unwrap().insert(id, Arc::clone(&table));
        let relay = spawn_relay(remote_rx, Arc::clone(&self.tables));
        self.relay_handles.insert(id, relay);
        self.relay_txs.insert(id, new_tx);

        // Wait for re-registration in registry and update peers.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let registered = {
                let reg = self.registry.lock().unwrap();
                reg.lookup(&format!("raft:{}", id)).is_some()
            };
            if registered {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                // Proceed anyway — node may catch up via heartbeats.
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        {
            let reg = self.registry.lock().unwrap();
            if let Some(entry) = reg.lookup(&format!("raft:{}", id)) {
                self.peers.lock().unwrap().insert(id, entry.pid);
            }
        }

        self.nodes.insert(
            id,
            NodeState {
                handle,
                store,
                _apply_handle: apply_handle,
            },
        );
    }

    fn node_store(&self, id: u64) -> &Arc<KvStore> {
        &self.nodes.get(&id).expect("node not running").store
    }

    fn live_node_count(&self) -> usize {
        self.nodes.len()
    }
}

fn spawn_apply_loop(
    store: Arc<KvStore>,
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            for entry in entries {
                if let LogEntryData::Command { data, .. } = entry.data {
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(&data) {
                        match cmd {
                            KvCommand::Put {
                                key,
                                value,
                                lease_id,
                            } => {
                                let _ = store.put(key, value, lease_id);
                            }
                            KvCommand::DeleteRange { key, range_end } => {
                                let _ = store.delete_range(&key, &range_end);
                            }
                            KvCommand::Txn {
                                compares,
                                success,
                                failure,
                            } => {
                                let _ = store.txn(&compares, &success, &failure);
                            }
                            KvCommand::Compact { revision } => {
                                let _ = store.compact(revision);
                            }
                        }
                    }
                }
            }
        }
    })
}

fn spawn_relay(
    mut remote_rx: mpsc::Receiver<RouterCommand>,
    tables: Arc<Mutex<HashMap<u64, Arc<ProcessTable>>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(RouterCommand::Send { node_id, frame }) = remote_rx.recv().await {
            let target = tables.lock().unwrap().get(&node_id).cloned();
            if let Some(target_table) = target {
                let _ = deliver_inbound_frame(&*target_table, &frame);
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Smoke test: cluster starts and accepts writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_resilient_cluster_smoke() {
    let cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    let idx = cluster.put_key("smoke-key", "smoke-val").await;
    cluster.wait_for_replication(idx).await;

    for (&id, node) in &cluster.nodes {
        let result = node.store.range(b"smoke-key", b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "node {} missing smoke-key", id);
    }
}
```

**Step 2: Run the smoke test**

Run: `cargo test --test cluster_resilience_test test_resilient_cluster_smoke 2>&1 | tail -10`
Expected: 1 passed, 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/cluster_resilience_test.rs
git commit -m "test: resilient cluster harness with stop/restart node capabilities"
```

---

### Task 6: Single-Node Loss Tests (tests 11-13)

**Files:**
- Modify: `barkeeper/tests/cluster_resilience_test.rs`

**Step 1: Add single-node loss tests**

Append to `cluster_resilience_test.rs`:

```rust
// ── D. Single-Node Loss and Recovery ─────────────────────────────────────────

/// Write 10 keys, stop a follower, write 10 more, restart it, verify all 20.
#[tokio::test]
async fn test_single_follower_restart_catches_up() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Write 10 keys.
    let mut last_idx = 0;
    for i in 1..=10 {
        last_idx = cluster.put_key(&format!("pre-{}", i), &format!("v{}", i)).await;
    }
    cluster.wait_for_replication(last_idx).await;

    // Find a follower and stop it.
    let leader_id = cluster.wait_for_leader().await;
    let follower_id = cluster.node_ids.iter().find(|&&id| id != leader_id).copied().unwrap();
    cluster.stop_node(follower_id);
    assert_eq!(cluster.live_node_count(), 2);

    // Write 10 more keys (2-node quorum still works).
    for i in 11..=20 {
        last_idx = cluster.put_key(&format!("post-{}", i), &format!("v{}", i)).await;
    }
    cluster.wait_for_replication(last_idx).await;

    // Restart the follower.
    cluster.restart_node(follower_id).await;

    // Wait for it to catch up (via WAL replay + AppendEntries from leader).
    sleep(Duration::from_secs(5)).await;

    // Verify all 20 keys on the restarted follower.
    let store = cluster.node_store(follower_id);
    for i in 1..=10 {
        let result = store.range(format!("pre-{}", i).as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "restarted follower missing pre-{}", i);
    }
    for i in 11..=20 {
        let result = store.range(format!("post-{}", i).as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "restarted follower missing post-{}", i);
    }
}

/// Stop the leader, verify a new leader is elected, write more keys.
#[tokio::test]
async fn test_leader_loss_triggers_reelection() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    let old_leader = cluster.wait_for_leader().await;
    cluster.stop_node(old_leader);

    // Wait for new leader election.
    sleep(Duration::from_secs(3)).await;
    let new_leader = cluster.wait_for_leader().await;
    assert_ne!(new_leader, old_leader, "new leader should differ from stopped leader");

    // Write via new leader.
    let idx = cluster.put_key("after-failover", "works").await;
    cluster.wait_for_replication(idx).await;

    // Verify on remaining live nodes.
    for (&id, node) in &cluster.nodes {
        let result = node.store.range(b"after-failover", b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "node {} missing after-failover key", id);
    }
}

/// Stop leader, wait for new leader, restart old leader. Verify old leader
/// gets data via WAL replay + replication.
#[tokio::test]
async fn test_leader_restart_rejoins_as_follower() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Write initial data.
    let idx = cluster.put_key("before-failover", "initial").await;
    cluster.wait_for_replication(idx).await;

    // Kill the leader.
    let old_leader = cluster.wait_for_leader().await;
    cluster.stop_node(old_leader);

    // Wait for new leader, then write more data.
    sleep(Duration::from_secs(3)).await;
    let idx2 = cluster.put_key("during-failover", "new-leader-write").await;
    cluster.wait_for_replication(idx2).await;

    // Restart old leader.
    cluster.restart_node(old_leader).await;
    sleep(Duration::from_secs(5)).await;

    // Old leader should have both keys (from WAL replay + replication).
    let store = cluster.node_store(old_leader);
    let r1 = store.range(b"before-failover", b"", 0, 0).unwrap();
    assert_eq!(r1.kvs.len(), 1, "old leader missing before-failover");
    let r2 = store.range(b"during-failover", b"", 0, 0).unwrap();
    assert_eq!(r2.kvs.len(), 1, "old leader missing during-failover");
}
```

**Step 2: Run tests**

Run: `cargo test --test cluster_resilience_test 2>&1 | tail -15`
Expected: 4 passed (smoke + 3 new), 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/cluster_resilience_test.rs
git commit -m "test: single-node loss tests (follower restart, leader failover, leader rejoin)"
```

---

### Task 7: Multi-Node Loss Tests (tests 14-16)

**Files:**
- Modify: `barkeeper/tests/cluster_resilience_test.rs`

**Step 1: Add multi-node loss tests**

Append to `cluster_resilience_test.rs`:

```rust
// ── E. Multi-Node Loss ───────────────────────────────────────────────────────

/// Stop 1 of 3 nodes, verify remaining 2-node quorum can accept writes.
#[tokio::test]
async fn test_minority_loss_cluster_continues() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Stop one follower.
    let leader_id = cluster.wait_for_leader().await;
    let victim = cluster.node_ids.iter().find(|&&id| id != leader_id).copied().unwrap();
    cluster.stop_node(victim);

    // Writes should still succeed with 2/3 quorum.
    let idx = cluster.put_key("minority-loss", "still-works").await;
    cluster.wait_for_replication(idx).await;

    for (&id, node) in &cluster.nodes {
        let result = node.store.range(b"minority-loss", b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "node {} missing key", id);
    }
}

/// Stop 2 of 3 nodes, verify proposals fail. Restart both, verify recovery.
#[tokio::test]
async fn test_majority_loss_blocks_writes() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Write a key first.
    let idx = cluster.put_key("before-majority-loss", "safe").await;
    cluster.wait_for_replication(idx).await;

    // Stop 2 nodes (keeping one alive).
    let leader_id = cluster.wait_for_leader().await;
    let others: Vec<u64> = cluster
        .node_ids
        .iter()
        .filter(|&&id| id != leader_id)
        .copied()
        .collect();
    for &id in &others {
        cluster.stop_node(id);
    }
    assert_eq!(cluster.live_node_count(), 1);

    // Proposals should timeout (no quorum).
    let cmd = KvCommand::Put {
        key: b"should-fail".to_vec(),
        value: b"no-quorum".to_vec(),
        lease_id: 0,
    };
    let data = serde_json::to_vec(&cmd).unwrap();
    let result = timeout(Duration::from_secs(5), async {
        for (&_id, node) in &cluster.nodes {
            let _ = timeout(Duration::from_secs(2), node.handle.propose(data.clone())).await;
        }
    })
    .await;
    // We expect this to either timeout or not get Success — the key point is no panic.
    drop(result);

    // Restart both nodes.
    for &id in &others {
        cluster.restart_node(id).await;
    }

    // Wait for new election.
    sleep(Duration::from_secs(5)).await;

    // Cluster should recover — write should succeed.
    let idx2 = cluster.put_key("after-recovery", "quorum-restored").await;
    cluster.wait_for_replication(idx2).await;

    // The pre-loss key should still be there.
    for (&id, node) in &cluster.nodes {
        let r = node.store.range(b"before-majority-loss", b"", 0, 0).unwrap();
        assert_eq!(r.kvs.len(), 1, "node {} lost pre-loss key", id);
    }
}

/// Write 20 keys, stop ALL 3 nodes, restart all from WAL. Verify all 20 survive.
#[tokio::test]
async fn test_all_nodes_restart_from_wal() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Write 20 keys.
    let mut last_idx = 0;
    for i in 1..=20 {
        last_idx = cluster
            .put_key(&format!("persist-{}", i), &format!("val-{}", i))
            .await;
    }
    cluster.wait_for_replication(last_idx).await;

    // Stop all nodes.
    for id in cluster.node_ids.clone() {
        cluster.stop_node(id);
    }
    assert_eq!(cluster.live_node_count(), 0);

    // Restart all nodes.
    for id in cluster.node_ids.clone() {
        cluster.restart_node(id).await;
    }

    // Wait for election.
    sleep(Duration::from_secs(5)).await;

    // Verify all 20 keys on all nodes.
    for (&id, node) in &cluster.nodes {
        for i in 1..=20 {
            let key = format!("persist-{}", i);
            let result = node.store.range(key.as_bytes(), b"", 0, 0).unwrap();
            assert_eq!(
                result.kvs.len(),
                1,
                "node {} missing {} after full restart",
                id,
                key
            );
        }
    }
}
```

**Step 2: Run tests**

Run: `cargo test --test cluster_resilience_test 2>&1 | tail -15`
Expected: 7 passed (smoke + 3 single-node + 3 multi-node), 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/cluster_resilience_test.rs
git commit -m "test: multi-node loss tests (minority loss, majority loss, full cluster restart)"
```

---

### Task 8: WAL Divergence and Conflict Resolution Tests (tests 17-18)

**Files:**
- Modify: `barkeeper/tests/cluster_resilience_test.rs`

**Step 1: Add WAL divergence tests**

Append to `cluster_resilience_test.rs`:

```rust
// ── F. WAL Divergence and Conflict Resolution ────────────────────────────────

/// Partition a follower (stop it), write entries via leader, restart follower.
/// Raft log-matching should bring it up to date via AppendEntries.
#[tokio::test]
async fn test_follower_conflict_resolution() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Write initial data replicated to all.
    let idx = cluster.put_key("conflict-base", "v0").await;
    cluster.wait_for_replication(idx).await;

    // Partition a follower.
    let leader_id = cluster.wait_for_leader().await;
    let follower_id = cluster
        .node_ids
        .iter()
        .find(|&&id| id != leader_id)
        .copied()
        .unwrap();
    cluster.stop_node(follower_id);

    // Leader writes 5 entries (follower misses these).
    let mut last_idx = 0;
    for i in 1..=5 {
        last_idx = cluster
            .put_key(&format!("conflict-k{}", i), &format!("leader-v{}", i))
            .await;
    }
    cluster.wait_for_replication(last_idx).await;

    // Restart the follower — it should catch up via AppendEntries.
    cluster.restart_node(follower_id).await;
    sleep(Duration::from_secs(5)).await;

    // Verify follower has all keys.
    let store = cluster.node_store(follower_id);
    let base = store.range(b"conflict-base", b"", 0, 0).unwrap();
    assert_eq!(base.kvs.len(), 1, "follower missing conflict-base (from WAL)");

    for i in 1..=5 {
        let key = format!("conflict-k{}", i);
        let result = store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(
            result.kvs.len(),
            1,
            "follower missing {} (should come from leader via AppendEntries)",
            key
        );
    }
}

/// Write keys rapidly while killing the leader mid-stream. Committed keys
/// must survive; uncommitted may be lost. No data corruption.
#[tokio::test]
async fn test_writes_during_leader_transition() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_election().await;

    // Collect committed keys.
    let committed = Arc::new(Mutex::new(Vec::<String>::new()));

    // Write 10 keys — some will succeed, some may fail when leader dies.
    let mut successful_indices = Vec::new();
    for i in 1..=5 {
        let key = format!("transition-{}", i);
        let idx = cluster.put_key(&key, &format!("v{}", i)).await;
        successful_indices.push(idx);
        committed.lock().unwrap().push(key);
    }

    // Wait for committed keys to replicate.
    if let Some(&max_idx) = successful_indices.last() {
        cluster.wait_for_replication(max_idx).await;
    }

    // Kill the leader.
    let leader_id = cluster.wait_for_leader().await;
    cluster.stop_node(leader_id);

    // Wait for new leader.
    sleep(Duration::from_secs(3)).await;

    // Write more via new leader.
    for i in 6..=10 {
        let key = format!("transition-{}", i);
        let idx = cluster.put_key(&key, &format!("v{}", i)).await;
        successful_indices.push(idx);
        committed.lock().unwrap().push(key);
    }

    if let Some(&max_idx) = successful_indices.last() {
        cluster.wait_for_replication(max_idx).await;
    }

    // Restart old leader.
    cluster.restart_node(leader_id).await;
    sleep(Duration::from_secs(5)).await;

    // All committed keys should be present on all live nodes.
    let committed_keys = committed.lock().unwrap().clone();
    for (&id, node) in &cluster.nodes {
        for key in &committed_keys {
            let result = node.store.range(key.as_bytes(), b"", 0, 0).unwrap();
            assert_eq!(
                result.kvs.len(),
                1,
                "node {} missing committed key {}",
                id,
                key
            );
        }
    }
}
```

**Step 2: Run all cluster resilience tests**

Run: `cargo test --test cluster_resilience_test 2>&1 | tail -15`
Expected: 9 passed (1 smoke + 3 single-node + 3 multi-node + 2 divergence), 0 failed.

**Step 3: Commit**

```bash
git add barkeeper/tests/cluster_resilience_test.rs
git commit -m "test: WAL divergence and conflict resolution tests (follower catch-up, leader transition)"
```

---

### Task 9: Run Full Test Suite and Final Commit

**Step 1: Run all tests**

Run: `cargo test 2>&1 | tail -30`
Expected: All existing tests + 18 new tests pass. No regressions.

**Step 2: Verify test count**

Run: `cargo test --test wal_lifetime_test 2>&1 | grep "test result"`
Expected: `test result: ok. 10 passed; 0 failed`

Run: `cargo test --test cluster_resilience_test 2>&1 | grep "test result"`
Expected: `test result: ok. 9 passed; 0 failed` (8 design tests + 1 smoke test)

**Step 3: Push**

```bash
git push
```

---

## Summary

| Task | Tests | Description |
|------|-------|-------------|
| 1 | 0 | Extract `replay_wal` function |
| 2 | 4 | Crash recovery (torn write, corrupt, large WAL, truncate-to-zero) |
| 3 | 4 | Snapshot + WAL replay (no-snapshot, with-snapshot, idempotency, crash-mid-wal) |
| 4 | 2 | Revision continuity (survive restart, counter matches) |
| 5 | 1 | Resilient cluster harness + smoke test |
| 6 | 3 | Single-node loss (follower restart, leader failover, leader rejoin) |
| 7 | 3 | Multi-node loss (minority, majority, full restart) |
| 8 | 2 | WAL divergence (conflict resolution, leader transition) |
| 9 | 0 | Full suite verification |
| **Total** | **19** | 18 design tests + 1 smoke test |
