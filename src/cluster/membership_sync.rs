use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// Bridges SWIM membership events to Raft voter set management.
pub struct MembershipSync {
    /// Active peers: node_id -> SocketAddr.
    peers: HashMap<u64, SocketAddr>,
}

impl MembershipSync {
    pub fn new() -> Self {
        MembershipSync {
            peers: HashMap::new(),
        }
    }

    /// A peer is alive (SWIM Alive event).
    pub fn on_alive(&mut self, node_id: u64, addr: SocketAddr) {
        self.peers.insert(node_id, addr);
    }

    /// A peer is dead (SWIM Dead event).
    pub fn on_dead(&mut self, node_id: u64) {
        self.peers.remove(&node_id);
    }

    /// A peer is leaving (SWIM Leave event).
    pub fn on_leave(&mut self, node_id: u64) {
        self.peers.remove(&node_id);
    }

    /// Current set of voter node IDs.
    pub fn current_voters(&self) -> HashSet<u64> {
        self.peers.keys().copied().collect()
    }

    /// Check if a peer is tracked.
    pub fn has_peer(&self, node_id: u64) -> bool {
        self.peers.contains_key(&node_id)
    }

    /// Get all peer addresses.
    pub fn peer_addrs(&self) -> &HashMap<u64, SocketAddr> {
        &self.peers
    }
}
