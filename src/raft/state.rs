use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// The three Raft states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Persistent state (survives restarts -- stored in redb).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
}

/// Volatile state (all servers).
#[derive(Debug, Clone)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
}

/// Volatile state (leader only, reinitialized after election).
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each follower: next log index to send.
    pub next_index: HashMap<u64, u64>,
    /// For each follower: highest log index known to be replicated.
    pub match_index: HashMap<u64, u64>,
}

impl LeaderState {
    pub fn new(peers: &[u64], last_log_index: u64) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for &peer in peers {
            next_index.insert(peer, last_log_index + 1);
            match_index.insert(peer, 0);
        }
        LeaderState { next_index, match_index }
    }
}

/// The full Raft state for a node.
#[derive(Debug)]
pub struct RaftState {
    pub node_id: u64,
    pub role: RaftRole,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub leader_state: Option<LeaderState>,
    pub leader_id: Option<u64>,
    /// Set of voters and learners.
    pub voters: HashSet<u64>,
    pub learners: HashSet<u64>,
    /// Votes received in current election (candidate only).
    pub votes_received: HashSet<u64>,
}

impl RaftState {
    pub fn new(node_id: u64) -> Self {
        let mut voters = HashSet::new();
        voters.insert(node_id);
        RaftState {
            node_id,
            role: RaftRole::Follower,
            persistent: PersistentState {
                current_term: 0,
                voted_for: None,
            },
            volatile: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            leader_state: None,
            leader_id: None,
            voters,
            learners: HashSet::new(),
            votes_received: HashSet::new(),
        }
    }

    pub fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn is_leader(&self) -> bool {
        self.role == RaftRole::Leader
    }

    pub fn peers(&self) -> Vec<u64> {
        self.voters.iter()
            .chain(self.learners.iter())
            .filter(|&&id| id != self.node_id)
            .copied()
            .collect()
    }
}
