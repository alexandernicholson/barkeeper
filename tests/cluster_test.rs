//! Multi-node Raft cluster integration tests.
//!
//! Spawns 3 barkeeper Raft nodes in-process using a local transport and
//! verifies leader election, proposal routing, and NotLeader rejection.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

use barkeeper::raft::messages::{ClientProposalResult, LogEntry, RaftMessage};
use barkeeper::raft::node::{spawn_raft_node, RaftConfig, RaftHandle};
use barkeeper::raft::transport::RaftTransport;

// ---------------------------------------------------------------------------
// Per-node transport wrapper
// ---------------------------------------------------------------------------
// The LocalTransport sends `(to, message)` into the target node's inbound
// channel, but the Raft actor expects `(from, message)` where `from` is the
// *sender's* node ID.  This wrapper stores the local node's ID and correctly
// tags outbound messages, following the same pattern as GrpcRaftTransport.

struct NodeTransport {
    local_id: u64,
    peers: Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<(u64, RaftMessage)>>>>,
}

impl NodeTransport {
    fn new(
        local_id: u64,
        peers: Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<(u64, RaftMessage)>>>>,
    ) -> Self {
        Self { local_id, peers }
    }
}

#[async_trait::async_trait]
impl RaftTransport for NodeTransport {
    async fn send(&self, to: u64, message: RaftMessage) {
        let peers = self.peers.lock().await;
        if let Some(tx) = peers.get(&to) {
            let _ = tx.send((self.local_id, message)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Test cluster helper
// ---------------------------------------------------------------------------

struct TestCluster {
    handles: Vec<(u64, RaftHandle)>,
    _tmp: tempfile::TempDir,
}

impl TestCluster {
    /// Spawn a 3-node Raft cluster in-process.
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let node_ids: Vec<u64> = vec![1, 2, 3];

        let peers: Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<(u64, RaftMessage)>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

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

            let (apply_tx, apply_rx) = mpsc::channel::<Vec<LogEntry>>(64);
            let transport = Arc::new(NodeTransport::new(id, Arc::clone(&peers)));
            let handle = spawn_raft_node(config, apply_tx, Some(transport)).await;

            peers
                .lock()
                .await
                .insert(id, handle.inbound_tx.clone());

            // Keep the apply_rx alive so the channel doesn't close.
            std::mem::forget(apply_rx);

            handles.push((id, handle));
        }

        TestCluster { handles, _tmp: tmp }
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
