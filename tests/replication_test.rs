//! Multi-node Raft data replication tests.
//!
//! Spawns a 3-node cluster using the Rebar actor runtime where each node has
//! its own KvStore and an active state machine that applies committed entries.
//! Verifies that writes to the leader are replicated to all followers' stores.

use std::collections::HashMap;
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
use barkeeper::raft::messages::{ClientProposalResult, LogEntry, LogEntryData};
use barkeeper::raft::node::{spawn_raft_node_rebar, RaftConfig, RaftHandle};

// ---------------------------------------------------------------------------
// Replication test cluster with real KV stores (Rebar runtime)
// ---------------------------------------------------------------------------

struct ReplicationCluster {
    nodes: Vec<ReplicationNode>,
    _tmp: tempfile::TempDir,
    // Keep tables alive for the duration of the test so processes can deliver.
    _tables: Arc<HashMap<u64, Arc<ProcessTable>>>,
    _relay_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for ReplicationCluster {
    fn drop(&mut self) {
        for h in &self._relay_handles {
            h.abort();
        }
    }
}

struct ReplicationNode {
    id: u64,
    handle: RaftHandle,
    store: Arc<KvStore>,
}

impl ReplicationCluster {
    /// Spawn a 3-node cluster using Rebar runtimes where each node has its
    /// own KvStore and state machine that actively applies committed entries.
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let node_ids: Vec<u64> = vec![1, 2, 3];

        // Shared registry across all nodes (fine for testing).
        let registry = Arc::new(Mutex::new(Registry::new()));

        // Shared peers map — populated after all nodes are spawned.
        let peers: Arc<Mutex<HashMap<u64, ProcessId>>> = Arc::new(Mutex::new(HashMap::new()));

        // Per-node ProcessTables, stored in a shared map for relay delivery.
        let mut tables_map: HashMap<u64, Arc<ProcessTable>> = HashMap::new();
        let mut remote_rxs: Vec<(u64, mpsc::Receiver<RouterCommand>)> = Vec::new();
        let mut nodes = Vec::new();

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

            // Each node gets its own KvStore.
            let store = Arc::new(
                KvStore::open(data_dir.join("kv.redb")).expect("open KvStore"),
            );

            // Create ProcessTable and DistributedRouter for this node.
            let table = Arc::new(ProcessTable::new(id));
            let (remote_tx, remote_rx) = mpsc::channel::<RouterCommand>(1024);
            let router = Arc::new(DistributedRouter::new(
                id,
                Arc::clone(&table),
                remote_tx,
            ));
            let runtime = Runtime::with_router(id, Arc::clone(&table), router);

            // Create apply channel and spawn a real state machine that applies
            // KvCommands to this node's store.
            let (apply_tx, apply_rx) = mpsc::channel::<Vec<LogEntry>>(64);
            spawn_apply_loop(Arc::clone(&store), apply_rx);

            let handle = spawn_raft_node_rebar(
                config,
                apply_tx,
                &runtime,
                Arc::clone(&registry),
                Arc::clone(&peers),
            )
            .await;

            tables_map.insert(id, table);
            remote_rxs.push((id, remote_rx));
            nodes.push(ReplicationNode { id, handle, store });
        }

        let tables = Arc::new(tables_map);

        // Spawn relay tasks: drain each node's remote_rx and deliver to target.
        let mut relay_handles = Vec::new();
        for (_node_id, mut remote_rx) in remote_rxs {
            let tables_ref = Arc::clone(&tables);
            let h = tokio::spawn(async move {
                while let Some(RouterCommand::Send { node_id, frame }) = remote_rx.recv().await {
                    if let Some(target_table) = tables_ref.get(&node_id) {
                        let _ = deliver_inbound_frame(target_table, &frame);
                    }
                }
            });
            relay_handles.push(h);
        }

        // Poll until all raft processes have registered (or timeout)
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        loop {
            let all_registered = {
                let reg = registry.lock().unwrap();
                node_ids.iter().all(|id| reg.lookup(&format!("raft:{}", *id)).is_some())
            };
            if all_registered {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for all raft processes to register in registry");
            }
            sleep(Duration::from_millis(10)).await;
        }
        {
            let reg = registry.lock().unwrap();
            let mut peers_guard = peers.lock().unwrap();
            for &id in &node_ids {
                let name = format!("raft:{}", id);
                if let Some(entry) = reg.lookup(&name) {
                    peers_guard.insert(id, entry.pid);
                }
            }
        }

        ReplicationCluster {
            nodes,
            _tmp: tmp,
            _tables: tables,
            _relay_handles: relay_handles,
        }
    }

    /// Wait for leader election to stabilise.
    async fn wait_for_election(&self) {
        sleep(Duration::from_secs(5)).await;
    }

    /// Wait until all nodes have applied at least `target_index`.
    /// Also adds a small buffer for the apply loop to process entries.
    async fn wait_for_replication(&self, target_index: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_caught_up = self.nodes.iter().all(|n| {
                n.handle
                    .applied_index
                    .load(std::sync::atomic::Ordering::Relaxed)
                    >= target_index
            });
            if all_caught_up {
                // Give the apply loop time to actually process the entries
                // (applied_index is updated when entries are sent to the channel,
                // not when the KvStore has processed them).
                sleep(Duration::from_millis(500)).await;
                return;
            }
            if tokio::time::Instant::now() > deadline {
                for node in &self.nodes {
                    let idx = node
                        .handle
                        .applied_index
                        .load(std::sync::atomic::Ordering::Relaxed);
                    eprintln!("node {} applied_index = {}", node.id, idx);
                }
                panic!(
                    "timed out waiting for all nodes to reach applied_index >= {}",
                    target_index
                );
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Propose data to whichever node is the current leader.
    /// Retries across all nodes if the target responds with NotLeader.
    async fn propose_to_leader(&self, data: Vec<u8>) -> ClientProposalResult {
        for _attempt in 0..10 {
            for node in &self.nodes {
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
                        // Try next node.
                        continue;
                    }
                    Err(_) => {
                        // Timeout — node accepted as leader but waiting for quorum.
                        // This means it IS the leader; wait for commit.
                        continue;
                    }
                    Ok(Ok(other)) => {
                        return other;
                    }
                    Ok(Err(e)) => {
                        panic!("proposal channel error: {}", e);
                    }
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        panic!("could not find a leader after retries");
    }
}

/// Spawn an apply loop that deserializes KvCommands from committed Raft
/// entries and applies them to the KV store. This is what makes followers
/// actually store replicated data.
fn spawn_apply_loop(store: Arc<KvStore>, mut apply_rx: mpsc::Receiver<Vec<LogEntry>>) {
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            for entry in entries {
                if let LogEntryData::Command(data) = entry.data {
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(&data) {
                        match cmd {
                            KvCommand::Put { key, value, lease_id } => {
                                let _ = store.put(key, value, lease_id);
                            }
                            KvCommand::DeleteRange { key, range_end } => {
                                let _ = store.delete_range(&key, &range_end);
                            }
                            KvCommand::Txn { compares, success, failure } => {
                                let _ = store.txn(compares, success, failure);
                            }
                            KvCommand::Compact { revision } => {
                                let _ = store.compact(revision);
                            }
                        }
                    }
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Write a key to the leader and verify it appears on all nodes.
#[tokio::test]
async fn test_write_replicates_to_all_nodes() {
    let cluster = ReplicationCluster::new().await;
    cluster.wait_for_election().await;

    // Write a key via the leader.
    let cmd = KvCommand::Put {
        key: b"replicated-key".to_vec(),
        value: b"replicated-value".to_vec(),
        lease_id: 0,
    };
    let data = serde_json::to_vec(&cmd).unwrap();
    let result = cluster.propose_to_leader(data).await;
    let target_index = match result {
        ClientProposalResult::Success { index, .. } => index,
        other => panic!("expected Success, got {:?}", other),
    };

    // Wait for all nodes to apply.
    cluster.wait_for_replication(target_index).await;

    // Verify all nodes have the key.
    for node in &cluster.nodes {
        let result = node.store.range(b"replicated-key", b"", 0, 0).unwrap();
        assert_eq!(
            result.kvs.len(),
            1,
            "node {} should have the replicated key, got {} results",
            node.id,
            result.kvs.len(),
        );
        assert_eq!(
            result.kvs[0].value,
            b"replicated-value",
            "node {} has wrong value",
            node.id,
        );
    }
}

/// Write multiple keys and verify they all replicate.
#[tokio::test]
async fn test_multiple_writes_replicate() {
    let cluster = ReplicationCluster::new().await;
    cluster.wait_for_election().await;

    // Write 10 keys.
    let mut last_index = 0u64;
    for i in 0..10 {
        let cmd = KvCommand::Put {
            key: format!("key-{:04}", i).into_bytes(),
            value: format!("val-{:04}", i).into_bytes(),
            lease_id: 0,
        };
        let data = serde_json::to_vec(&cmd).unwrap();
        let result = cluster.propose_to_leader(data).await;
        match result {
            ClientProposalResult::Success { index, .. } => {
                last_index = index;
            }
            other => panic!("key-{:04}: expected Success, got {:?}", i, other),
        }
    }

    // Wait for all nodes to apply.
    cluster.wait_for_replication(last_index).await;

    // Verify all 10 keys on all nodes using a range scan.
    for node in &cluster.nodes {
        // Range scan: key-0000 to key-9999 (range_end = "key-:" because ':' > '9')
        let result = node
            .store
            .range(b"key-0000", b"key-:", 0, 0)
            .unwrap();
        assert_eq!(
            result.kvs.len(),
            10,
            "node {} should have 10 keys, got {}",
            node.id,
            result.kvs.len(),
        );
    }
}

/// Delete a key on the leader and verify deletion replicates.
#[tokio::test]
async fn test_delete_replicates_to_all_nodes() {
    let cluster = ReplicationCluster::new().await;
    cluster.wait_for_election().await;

    // Write a key.
    let put_cmd = KvCommand::Put {
        key: b"delete-me".to_vec(),
        value: b"temporary".to_vec(),
        lease_id: 0,
    };
    let data = serde_json::to_vec(&put_cmd).unwrap();
    let put_result = cluster.propose_to_leader(data).await;
    let put_index = match put_result {
        ClientProposalResult::Success { index, .. } => index,
        other => panic!("put expected Success, got {:?}", other),
    };
    cluster.wait_for_replication(put_index).await;

    // Delete the key.
    let del_cmd = KvCommand::DeleteRange {
        key: b"delete-me".to_vec(),
        range_end: b"".to_vec(),
    };
    let data = serde_json::to_vec(&del_cmd).unwrap();
    let del_result = cluster.propose_to_leader(data).await;
    let del_index = match del_result {
        ClientProposalResult::Success { index, .. } => index,
        other => panic!("delete expected Success, got {:?}", other),
    };
    cluster.wait_for_replication(del_index).await;

    // Verify key is gone on all nodes.
    for node in &cluster.nodes {
        let result = node.store.range(b"delete-me", b"", 0, 0).unwrap();
        assert_eq!(
            result.kvs.len(),
            0,
            "node {} should not have the deleted key",
            node.id,
        );
    }
}
