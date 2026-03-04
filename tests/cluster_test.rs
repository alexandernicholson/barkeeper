//! Multi-node Raft cluster integration tests.
//!
//! Spawns 3 barkeeper Raft nodes in-process using the Rebar actor runtime
//! with a virtual in-process network (DistributedRouter + relay tasks) and
//! verifies leader election, proposal routing, and NotLeader rejection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

use rebar_core::process::ProcessId;
use rebar_core::process::table::ProcessTable;
use rebar_core::runtime::Runtime;
use rebar_cluster::registry::orset::Registry;
use rebar_cluster::router::{deliver_inbound_frame, DistributedRouter, RouterCommand};

use barkeeper::raft::messages::{ClientProposalResult, LogEntry};
use barkeeper::raft::node::{spawn_raft_node_rebar, RaftConfig, RaftHandle};

// ---------------------------------------------------------------------------
// Test cluster helper using Rebar runtime
// ---------------------------------------------------------------------------

struct TestCluster {
    handles: Vec<(u64, RaftHandle)>,
    _tmp: tempfile::TempDir,
    // Keep tables alive for the duration of the test so processes can deliver.
    _tables: Arc<HashMap<u64, Arc<ProcessTable>>>,
    _relay_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for h in &self._relay_handles {
            h.abort();
        }
    }
}

impl TestCluster {
    /// Spawn a 3-node Raft cluster in-process using Rebar runtimes with
    /// a virtual in-process network for cross-node message delivery.
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
        let mut handles = Vec::new();

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

            // Create ProcessTable and DistributedRouter for this node.
            let table = Arc::new(ProcessTable::new(id));
            let (remote_tx, remote_rx) = mpsc::channel::<RouterCommand>(1024);
            let router = Arc::new(DistributedRouter::new(
                id,
                Arc::clone(&table),
                remote_tx,
            ));
            // Runtime can be dropped after spawn_raft_node_rebar returns — the spawned
            // raft actor holds Arc clones of the ProcessTable and router, keeping them alive.
            let runtime = Runtime::with_router(id, Arc::clone(&table), router);

            let (apply_tx, mut apply_rx) = mpsc::channel::<Vec<LogEntry>>(64);

            let handle = spawn_raft_node_rebar(
                config,
                apply_tx,
                &runtime,
                Arc::clone(&registry),
                Arc::clone(&peers),
            )
            .await;

            // Drain apply_rx so the channel doesn't block the raft actor.
            tokio::spawn(async move {
                while apply_rx.recv().await.is_some() {}
            });

            tables_map.insert(id, table);
            remote_rxs.push((id, remote_rx));
            handles.push((id, handle));
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

        TestCluster {
            handles,
            _tmp: tmp,
            _tables: tables,
            _relay_handles: relay_handles,
        }
    }

    /// Wait for leader election to stabilise.
    async fn wait_for_election(&self) {
        sleep(Duration::from_secs(4)).await;
    }

    /// Probe all nodes concurrently and classify responses.
    ///
    /// Uses a short timeout per probe.  Followers respond immediately with
    /// NotLeader.  The leader either succeeds (if quorum replication works)
    /// or hangs until timeout (expected when entry replication is not yet
    /// wired through heartbeats).  Both outcomes identify the leader.
    async fn probe_all(&self) -> ProbeResults {
        let mut tasks = Vec::new();

        for (id, handle) in &self.handles {
            let id = *id;
            let handle = handle.clone();
            tasks.push(tokio::spawn(async move {
                let result = timeout(
                    Duration::from_secs(1),
                    handle.propose(b"probe".to_vec()),
                )
                .await;
                (id, result)
            }));
        }

        let mut not_leader_ids = Vec::new();
        let mut success_ids = Vec::new();
        let mut timeout_ids = Vec::new();

        for task in tasks {
            let (id, result) = task.await.unwrap();
            match result {
                Ok(Ok(ClientProposalResult::NotLeader { .. })) => {
                    not_leader_ids.push(id);
                }
                Ok(Ok(ClientProposalResult::Success { .. })) => {
                    success_ids.push(id);
                }
                Err(_) => {
                    timeout_ids.push(id);
                }
                Ok(Ok(ClientProposalResult::Error(e))) => {
                    panic!("unexpected error on node {}: {}", id, e);
                }
                Ok(Err(e)) => {
                    panic!("channel error on node {}: {}", id, e);
                }
            }
        }

        ProbeResults {
            not_leader_ids,
            success_ids,
            timeout_ids,
        }
    }
}

struct ProbeResults {
    not_leader_ids: Vec<u64>,
    success_ids: Vec<u64>,
    timeout_ids: Vec<u64>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Three-node cluster should elect a leader (term advances to > 0).
#[tokio::test]
async fn test_three_node_cluster_elects_leader() {
    let cluster = TestCluster::new().await;
    cluster.wait_for_election().await;

    let max_term = cluster
        .handles
        .iter()
        .map(|(_, h)| h.current_term.load(std::sync::atomic::Ordering::Relaxed))
        .max()
        .unwrap();

    assert!(max_term > 0, "expected at least one election to occur");
}

/// Non-leaders should reject proposals with NotLeader.
#[tokio::test]
async fn test_non_leaders_reject_proposals() {
    let cluster = TestCluster::new().await;
    cluster.wait_for_election().await;

    let results = cluster.probe_all().await;

    // At least 2 nodes must be followers and reject immediately.
    assert!(
        results.not_leader_ids.len() >= 2,
        "expected at least 2 NotLeader responses, got {} (not_leader: {:?}, success: {:?}, timeout: {:?})",
        results.not_leader_ids.len(),
        results.not_leader_ids,
        results.success_ids,
        results.timeout_ids,
    );
}

/// At most one node should be the leader.  The leader either commits a
/// proposal or accepts it (timing out while waiting for quorum).  Both
/// cases are distinguishable from a NotLeader rejection.
///
/// Retries the probe a few times to handle transient election churn.
#[tokio::test]
async fn test_at_most_one_leader_accepts_proposals() {
    let cluster = TestCluster::new().await;
    cluster.wait_for_election().await;

    // Retry probing in case the first probe catches an election transition.
    let mut found_leader = false;
    for _attempt in 0..3 {
        let results = cluster.probe_all().await;
        let leader_count = results.success_ids.len() + results.timeout_ids.len();

        assert!(
            leader_count <= 1,
            "at most 1 node should act as leader, got {} (success: {:?}, timeout: {:?})",
            leader_count,
            results.success_ids,
            results.timeout_ids,
        );

        if leader_count == 1 {
            // Exactly 2 followers, 1 leader.
            assert_eq!(
                results.not_leader_ids.len(),
                2,
                "expected exactly 2 followers when 1 leader exists, got {:?}",
                results.not_leader_ids,
            );
            found_leader = true;
            break;
        }

        // No leader found this round (all NotLeader) -- election may be
        // in flight.  Wait and retry.
        sleep(Duration::from_secs(2)).await;
    }

    assert!(
        found_leader,
        "expected to find exactly 1 leader after retries"
    );
}
