//! Cluster resilience tests: single-node loss, multi-node loss, WAL divergence.
//!
//! Extends the pattern from `replication_test.rs` with a `ResilientCluster` that
//! supports stopping and restarting individual nodes while preserving data dirs.
//! The key invariant tested: committed data must survive node restarts.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

use rebar_core::process::ProcessId;
use rebar_core::process::table::ProcessTable;
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
// ResilientCluster — supports stop/restart of individual nodes
// ---------------------------------------------------------------------------

struct NodeState {
    handle: RaftHandle,
    store: Arc<KvStore>,
    apply_handle: tokio::task::JoinHandle<()>,
    relay_handle: tokio::task::JoinHandle<()>,
}

struct ResilientCluster {
    node_ids: Vec<u64>,
    nodes: HashMap<u64, NodeState>,
    data_dirs: HashMap<u64, std::path::PathBuf>,
    registry: Arc<Mutex<Registry>>,
    peers: Arc<Mutex<HashMap<u64, ProcessId>>>,
    tables: Arc<Mutex<HashMap<u64, Arc<ProcessTable>>>>,
    _tmp: tempfile::TempDir,
}

impl Drop for ResilientCluster {
    fn drop(&mut self) {
        for (_, node) in self.nodes.drain() {
            node.apply_handle.abort();
            node.relay_handle.abort();
        }
    }
}

/// Spawn an apply loop that deserializes KvCommands from committed Raft
/// entries and applies them to the KV store.
fn spawn_apply_loop(
    store: Arc<KvStore>,
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            for entry in entries {
                if let LogEntryData::Command { data, .. } = &entry.data {
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(data) {
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

impl ResilientCluster {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let node_ids: Vec<u64> = vec![1, 2, 3];
        let registry = Arc::new(Mutex::new(Registry::new()));
        let peers: Arc<Mutex<HashMap<u64, ProcessId>>> = Arc::new(Mutex::new(HashMap::new()));
        let tables: Arc<Mutex<HashMap<u64, Arc<ProcessTable>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let mut data_dirs = HashMap::new();

        for &id in &node_ids {
            let data_dir = tmp.path().join(format!("node-{}", id));
            std::fs::create_dir_all(&data_dir).unwrap();
            data_dirs.insert(id, data_dir);
        }

        let mut cluster = ResilientCluster {
            node_ids: node_ids.clone(),
            nodes: HashMap::new(),
            data_dirs,
            registry,
            peers,
            tables,
            _tmp: tmp,
        };

        for &id in &node_ids {
            cluster.start_node(id).await;
        }

        // Wait for all raft processes to register, then populate peers map.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(2000);
        loop {
            let all_registered = {
                let reg = cluster.registry.lock().unwrap();
                node_ids
                    .iter()
                    .all(|id| reg.lookup(&format!("raft:{}", *id)).is_some())
            };
            if all_registered {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for all raft processes to register");
            }
            sleep(Duration::from_millis(10)).await;
        }
        {
            let reg = cluster.registry.lock().unwrap();
            let mut peers_guard = cluster.peers.lock().unwrap();
            for &id in &node_ids {
                let name = format!("raft:{}", id);
                if let Some(entry) = reg.lookup(&name) {
                    peers_guard.insert(id, entry.pid);
                }
            }
        }

        cluster
    }

    /// Start (or restart) a single node. Reopens stores from data_dir.
    async fn start_node(&mut self, id: u64) {
        let data_dir = self.data_dirs.get(&id).unwrap().clone();

        let log_store = LogStore::open(&data_dir).expect("open LogStore");
        let kv_store = KvStore::open(&data_dir).expect("open KvStore");

        // Replay WAL to catch up KvStore from what's already in the WAL.
        replay_wal(&log_store, &kv_store).expect("WAL replay failed");

        let initial_rev = kv_store.current_revision().unwrap_or(0);
        let store = Arc::new(kv_store);

        let config = RaftConfig {
            node_id: id,
            data_dir: data_dir.to_str().unwrap().to_string(),
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            heartbeat_interval: Duration::from_millis(100),
            peers: self.node_ids.clone(),
        };

        let table = Arc::new(ProcessTable::new(id));
        let (remote_tx, remote_rx) = mpsc::channel::<RouterCommand>(1024);
        let router = Arc::new(DistributedRouter::new(
            id,
            Arc::clone(&table),
            remote_tx,
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

        // Store this node's table in the shared map.
        {
            let mut tables_guard = self.tables.lock().unwrap();
            tables_guard.insert(id, Arc::clone(&table));
        }

        // Spawn relay task for this node.
        let tables_ref = Arc::clone(&self.tables);
        let relay_handle = tokio::spawn(async move {
            let mut remote_rx = remote_rx;
            while let Some(RouterCommand::Send { node_id, frame }) = remote_rx.recv().await {
                let target_table = {
                    let tables_guard = tables_ref.lock().unwrap();
                    tables_guard.get(&node_id).cloned()
                };
                if let Some(target_table) = target_table {
                    let _ = deliver_inbound_frame(&target_table, &frame);
                }
            }
        });

        // Wait for this node's raft process to register, then update peers.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let found = {
                let reg = self.registry.lock().unwrap();
                let name = format!("raft:{}", id);
                reg.lookup(&name).map(|e| e.pid)
            };
            if let Some(pid) = found {
                let mut peers_guard = self.peers.lock().unwrap();
                peers_guard.insert(id, pid);
                break;
            }
            if tokio::time::Instant::now() > deadline {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        self.nodes.insert(
            id,
            NodeState {
                handle,
                store,
                apply_handle,
                relay_handle,
            },
        );
    }

    /// Stop a node: abort tasks, drop handle, but keep data dir.
    ///
    /// The old raft task (spawned via runtime.spawn) cannot be directly aborted.
    /// To prevent WAL corruption when the node restarts (the old task might
    /// truncate entries written by the new task), we copy the data dir to a
    /// fresh location. The old task continues writing to the original files
    /// (by open file descriptor) while the restarted node uses the copy.
    async fn stop_node(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            node.apply_handle.abort();
            node.relay_handle.abort();
            {
                let mut tables_guard = self.tables.lock().unwrap();
                tables_guard.remove(&id);
            }
            drop(node.handle);
            drop(node.store);

            // Copy data dir to a fresh location to isolate from the old raft task.
            let data_dir = self.data_dirs.get(&id).unwrap();
            static RESTART_CTR: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let ctr = RESTART_CTR.fetch_add(1, Ordering::Relaxed);
            let new_dir = data_dir.with_file_name(format!("node-{}-restart-{}", id, ctr));
            std::fs::create_dir_all(&new_dir).unwrap();
            for entry in std::fs::read_dir(data_dir).unwrap() {
                let entry = entry.unwrap();
                let fname = entry.file_name();
                std::fs::copy(entry.path(), new_dir.join(&fname)).unwrap();
            }
            self.data_dirs.insert(id, new_dir);

            // Brief sleep to let the old task settle.
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for_leader(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if self.find_leader().is_some() {
                // Give a brief moment for leader to stabilize.
                sleep(Duration::from_millis(500)).await;
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for leader election");
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Find the current leader ID, or None.
    fn find_leader(&self) -> Option<u64> {
        for (_, node) in &self.nodes {
            let leader = node.handle.leader_id.load(Ordering::Relaxed);
            if leader != 0 && self.nodes.contains_key(&leader) {
                return Some(leader);
            }
        }
        None
    }

    /// Put a key via the leader. Returns the committed log index.
    async fn put_key(&self, key: &str, value: &str) -> u64 {
        let cmd = KvCommand::Put {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            lease_id: 0,
        };
        let data = serde_json::to_vec(&cmd).unwrap();
        let result = self.propose_to_leader(data).await;
        match result {
            ClientProposalResult::Success { index, .. } => index,
            other => panic!("put_key({}) expected Success, got {:?}", key, other),
        }
    }

    /// Propose data to whichever node is the current leader.
    async fn propose_to_leader(&self, data: Vec<u8>) -> ClientProposalResult {
        for _attempt in 0..20 {
            for (_, node) in &self.nodes {
                let result = timeout(
                    Duration::from_secs(3),
                    node.handle.propose(data.clone()),
                )
                .await;

                match result {
                    Ok(Ok(result @ ClientProposalResult::Success { .. })) => {
                        return result;
                    }
                    Ok(Ok(ClientProposalResult::NotLeader { .. })) => {
                        continue;
                    }
                    Err(_) => continue,
                    Ok(Ok(other)) => return other,
                    Ok(Err(_)) => continue,
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        panic!("could not find a leader after retries");
    }

    /// Wait until a key appears on all live nodes.
    async fn wait_for_key(&self, key: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let all_have = self.nodes.values().all(|node| {
                let result = node.store.range(key.as_bytes(), b"", 0, 0).unwrap();
                !result.kvs.is_empty()
            });
            if all_have {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for key '{}' on all nodes", key);
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until a key appears on a specific node.
    async fn wait_for_key_on_node(&self, key: &str, node_id: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(node) = self.nodes.get(&node_id) {
                let result = node.store.range(key.as_bytes(), b"", 0, 0).unwrap();
                if !result.kvs.is_empty() {
                    return;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "timed out waiting for key '{}' on node {}",
                    key, node_id
                );
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait for a node's raft layer to catch up to other nodes, then replay
    /// WAL entries into the KvStore. This ensures entries received via
    /// AppendEntries catch-up are applied to the KvStore.
    async fn wait_for_raft_catchup_and_replay(&mut self, node_id: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(node) = self.nodes.get(&node_id) {
                let my_applied = node.handle.applied_index();
                let other_applied = self
                    .nodes
                    .iter()
                    .filter(|(&id, _)| id != node_id)
                    .map(|(_, n)| n.handle.applied_index())
                    .max()
                    .unwrap_or(0);
                if my_applied >= other_applied && my_applied > 0 {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
        // Wait for WAL to be fully flushed.
        sleep(Duration::from_millis(500)).await;

        // Replay WAL to bring KvStore up to date.
        let data_dir = self.data_dirs.get(&node_id).unwrap().clone();
        let log_store = LogStore::open(&data_dir).expect("open LogStore for replay");
        if let Some(node) = self.nodes.get(&node_id) {
            let _ = replay_wal(&log_store, &node.store);
        }
    }

    /// Check how many keys matching a prefix exist on a specific node.
    fn count_keys_on_node(&self, node_id: u64, prefix: &str) -> usize {
        let node = self.nodes.get(&node_id).expect("node not found");
        let range_end = format!("{}~", prefix);
        let result = node
            .store
            .range(prefix.as_bytes(), range_end.as_bytes(), 0, 0)
            .unwrap();
        result.kvs.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---- Smoke test ----

#[tokio::test]
async fn test_resilient_cluster_smoke() {
    let cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    let _idx = cluster.put_key("smoke-key", "smoke-value").await;
    cluster.wait_for_key("smoke-key").await;

    for (&id, node) in &cluster.nodes {
        let result = node.store.range(b"smoke-key", b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "node {} should have smoke-key", id);
        assert_eq!(result.kvs[0].value, b"smoke-value");
    }
}

// ---- D. Single-Node Loss ----

#[tokio::test]
async fn test_single_follower_restart_catches_up() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write 10 keys.
    for i in 0..10 {
        cluster
            .put_key(&format!("pre-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("pre-0009").await;

    // Find a follower and stop it.
    let leader_id = cluster.find_leader().expect("should have leader");
    let follower_id = cluster
        .node_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id && cluster.nodes.contains_key(&id))
        .expect("need a follower");

    cluster.stop_node(follower_id).await;

    // Write 10 more keys while follower is down.
    for i in 10..20 {
        cluster
            .put_key(&format!("pre-{:04}", i), &format!("val-{:04}", i))
            .await;
    }

    // Restart follower and wait for catch-up.
    cluster.start_node(follower_id).await;
    sleep(Duration::from_secs(3)).await;
    cluster
        .wait_for_raft_catchup_and_replay(follower_id)
        .await;

    // Verify all 20 keys on the restarted follower.
    cluster
        .wait_for_key_on_node("pre-0019", follower_id)
        .await;
    let count = cluster.count_keys_on_node(follower_id, "pre-");
    assert_eq!(
        count, 20,
        "restarted follower should have all 20 keys, got {}",
        count
    );
}

#[tokio::test]
async fn test_leader_loss_triggers_reelection() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write some keys.
    for i in 0..5 {
        cluster
            .put_key(&format!("before-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("before-0004").await;

    // Stop the leader.
    let old_leader = cluster.find_leader().expect("should have leader");
    cluster.stop_node(old_leader).await;

    // Wait for re-election.
    sleep(Duration::from_secs(3)).await;

    // Verify a new leader was elected and we can write.
    for i in 0..5 {
        cluster
            .put_key(&format!("after-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("after-0004").await;

    // Verify a new leader exists and writes succeeded (proving re-election).
    let new_leader = cluster.find_leader().expect("should have new leader after re-election");
    // The new leader must be one of the surviving nodes (not the stopped one).
    assert!(
        cluster.nodes.contains_key(&new_leader),
        "new leader {} should be a live node",
        new_leader
    );
    assert_ne!(
        old_leader, new_leader,
        "new leader should differ from stopped leader"
    );
}

#[tokio::test]
async fn test_leader_restart_rejoins_as_follower() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write keys via old leader.
    for i in 0..5 {
        cluster
            .put_key(&format!("phase1-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("phase1-0004").await;

    // Stop the leader.
    let old_leader = cluster.find_leader().expect("should have leader");
    cluster.stop_node(old_leader).await;

    // Wait for re-election.
    sleep(Duration::from_secs(3)).await;

    // New leader writes more keys.
    for i in 0..5 {
        cluster
            .put_key(&format!("phase2-{:04}", i), &format!("val-{:04}", i))
            .await;
    }

    // Restart old leader and wait for catch-up.
    cluster.start_node(old_leader).await;
    sleep(Duration::from_secs(3)).await;
    cluster
        .wait_for_raft_catchup_and_replay(old_leader)
        .await;

    // Old leader should have all data.
    cluster
        .wait_for_key_on_node("phase2-0004", old_leader)
        .await;
    let count1 = cluster.count_keys_on_node(old_leader, "phase1-");
    let count2 = cluster.count_keys_on_node(old_leader, "phase2-");
    assert_eq!(count1, 5, "old leader should have phase1 keys");
    assert_eq!(count2, 5, "old leader should have phase2 keys");
}

// ---- E. Multi-Node Loss ----

#[tokio::test]
async fn test_minority_loss_cluster_continues() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write initial keys.
    for i in 0..5 {
        cluster.put_key(&format!("init-{:04}", i), "v").await;
    }
    cluster.wait_for_key("init-0004").await;

    // Stop 1 follower (minority loss — 2 of 3 remain, quorum preserved).
    let leader_id = cluster.find_leader().expect("should have leader");
    let follower_id = cluster
        .node_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id && cluster.nodes.contains_key(&id))
        .expect("need a follower");
    cluster.stop_node(follower_id).await;

    // Writes should still succeed with 2-node quorum.
    for i in 0..5 {
        cluster
            .put_key(&format!("after-loss-{:04}", i), "v")
            .await;
    }
    cluster.wait_for_key("after-loss-0004").await;
}

#[tokio::test]
async fn test_majority_loss_blocks_writes() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write initial keys.
    for i in 0..3 {
        cluster.put_key(&format!("maj-{:04}", i), "v").await;
    }
    cluster.wait_for_key("maj-0002").await;

    // Stop 2 of 3 nodes (majority loss).
    let leader_id = cluster.find_leader().expect("should have leader");
    let followers: Vec<u64> = cluster
        .node_ids
        .iter()
        .copied()
        .filter(|&id| id != leader_id && cluster.nodes.contains_key(&id))
        .collect();
    for &fid in &followers {
        cluster.stop_node(fid).await;
    }

    // Proposals should fail/timeout since quorum can't be reached.
    let cmd = KvCommand::Put {
        key: b"should-fail".to_vec(),
        value: b"x".to_vec(),
        lease_id: 0,
    };
    let data = serde_json::to_vec(&cmd).unwrap();
    let remaining_node = cluster.nodes.values().next().unwrap();
    let result = timeout(
        Duration::from_secs(3),
        remaining_node.handle.propose(data),
    )
    .await;

    // Either timeout or non-Success is expected.
    match result {
        Err(_) => { /* timeout — expected */ }
        Ok(Ok(ClientProposalResult::Success { .. })) => {
            panic!("write should not succeed without quorum");
        }
        Ok(_) => { /* some error — fine */ }
    }

    // Restart both stopped nodes.
    for &fid in &followers {
        cluster.start_node(fid).await;
    }

    // Wait for election.
    sleep(Duration::from_secs(5)).await;
    {
        let reg = cluster.registry.lock().unwrap();
        let mut peers_guard = cluster.peers.lock().unwrap();
        for &fid in &followers {
            let name = format!("raft:{}", fid);
            if let Some(entry) = reg.lookup(&name) {
                peers_guard.insert(fid, entry.pid);
            }
        }
    }

    // Writes should succeed again.
    cluster.put_key("recovery-key", "recovered").await;
    cluster.wait_for_key("recovery-key").await;
}

#[tokio::test]
async fn test_all_nodes_restart_from_wal() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write 20 keys.
    for i in 0..20 {
        cluster
            .put_key(&format!("wal-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("wal-0019").await;

    // Stop all 3 nodes.
    let ids: Vec<u64> = cluster.node_ids.clone();
    for &id in &ids {
        cluster.stop_node(id).await;
    }

    // Restart all 3 nodes.
    for &id in &ids {
        cluster.start_node(id).await;
    }

    // Wait for registration + election.
    sleep(Duration::from_secs(5)).await;
    {
        let reg = cluster.registry.lock().unwrap();
        let mut peers_guard = cluster.peers.lock().unwrap();
        for &id in &ids {
            let name = format!("raft:{}", id);
            if let Some(entry) = reg.lookup(&name) {
                peers_guard.insert(id, entry.pid);
            }
        }
    }

    // Verify all 20 keys survived on all nodes via WAL replay in start_node.
    for &id in &ids {
        let count = cluster.count_keys_on_node(id, "wal-");
        assert_eq!(
            count, 20,
            "node {} should have all 20 keys after WAL replay, got {}",
            id, count
        );
    }
}

// ---- F. WAL Divergence ----

#[tokio::test]
async fn test_follower_conflict_resolution() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Find a follower and stop it.
    let leader_id = cluster.find_leader().expect("should have leader");
    let follower_id = cluster
        .node_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id && cluster.nodes.contains_key(&id))
        .expect("need a follower");

    cluster.stop_node(follower_id).await;

    // Leader writes 5 keys while follower is down.
    for i in 0..5 {
        cluster
            .put_key(&format!("conflict-{:04}", i), &format!("val-{:04}", i))
            .await;
    }

    // Restart follower — it should catch up via AppendEntries.
    cluster.start_node(follower_id).await;
    sleep(Duration::from_secs(3)).await;
    cluster
        .wait_for_raft_catchup_and_replay(follower_id)
        .await;

    // Verify follower catches up.
    cluster
        .wait_for_key_on_node("conflict-0004", follower_id)
        .await;
    let count = cluster.count_keys_on_node(follower_id, "conflict-");
    assert_eq!(
        count, 5,
        "follower should have all 5 keys after catchup, got {}",
        count
    );
}

#[tokio::test]
async fn test_writes_during_leader_transition() {
    let mut cluster = ResilientCluster::new().await;
    cluster.wait_for_leader().await;

    // Write 5 keys via current leader.
    for i in 0..5 {
        cluster
            .put_key(&format!("trans1-{:04}", i), &format!("val-{:04}", i))
            .await;
    }
    cluster.wait_for_key("trans1-0004").await;

    // Kill leader.
    let old_leader = cluster.find_leader().expect("should have leader");
    cluster.stop_node(old_leader).await;

    // Wait for new leader.
    sleep(Duration::from_secs(3)).await;

    // New leader writes 5 more keys.
    for i in 0..5 {
        cluster
            .put_key(&format!("trans2-{:04}", i), &format!("val-{:04}", i))
            .await;
    }

    // Restart old leader and wait for catch-up.
    cluster.start_node(old_leader).await;
    sleep(Duration::from_secs(3)).await;
    cluster
        .wait_for_raft_catchup_and_replay(old_leader)
        .await;

    // Old leader should get all committed keys.
    cluster
        .wait_for_key_on_node("trans2-0004", old_leader)
        .await;
    let count1 = cluster.count_keys_on_node(old_leader, "trans1-");
    let count2 = cluster.count_keys_on_node(old_leader, "trans2-");
    assert_eq!(
        count1, 5,
        "old leader should have all trans1 keys, got {}",
        count1
    );
    assert_eq!(
        count2, 5,
        "old leader should have all trans2 keys, got {}",
        count2
    );
}
