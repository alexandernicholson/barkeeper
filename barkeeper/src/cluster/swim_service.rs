use std::net::SocketAddr;
use std::time::Instant;

use rebar_cluster::swim::*;

/// Runs the SWIM protocol loop for cluster membership discovery.
pub struct SwimService {
    pub config: SwimConfig,
    pub members: MembershipList,
    pub detector: FailureDetector,
    pub gossip: GossipQueue,
    pub node_id: u64,
}

impl SwimService {
    pub fn new(node_id: u64, config: SwimConfig) -> Self {
        SwimService {
            config,
            members: MembershipList::new(),
            detector: FailureDetector::new(),
            gossip: GossipQueue::new(),
            node_id,
        }
    }

    /// Add a seed peer to the membership list.
    pub fn add_seed(&mut self, node_id: u64, addr: SocketAddr) {
        self.members.add(Member::new(node_id, addr));
        self.gossip.add(GossipUpdate::Alive {
            node_id,
            addr,
            incarnation: 0,
            cert_hash: None,
        });
    }

    /// Run one SWIM protocol tick. Returns the node to probe (if any).
    pub fn tick(&mut self) -> Option<u64> {
        let now = Instant::now();

        // Check for suspect -> dead transitions
        let newly_dead = self.detector.check_suspect_timeouts(
            &mut self.members,
            &self.config,
            now,
        );
        for id in &newly_dead {
            let addr = self.members.get(*id).map(|m| m.addr).unwrap_or_else(|| {
                "0.0.0.0:0".parse().unwrap()
            });
            self.gossip.add(GossipUpdate::Dead { node_id: *id, addr });
        }

        // Remove expired dead nodes
        self.detector.remove_expired_dead(&mut self.members, &self.config, now);

        // Select a node to probe
        self.detector.tick(&self.members, self.node_id)
    }

    /// Record a successful probe response.
    pub fn record_ack(&mut self, node_id: u64) {
        self.detector.record_ack(&mut self.members, node_id);
    }

    /// Record a failed probe.
    pub fn record_nack(&mut self, node_id: u64) {
        self.detector.record_nack(&mut self.members, node_id, Instant::now());
    }

    /// Get pending gossip updates.
    pub fn drain_gossip(&mut self, max: usize) -> Vec<GossipUpdate> {
        self.gossip.drain(max)
    }

    /// Get all alive members.
    pub fn alive_members(&self) -> Vec<&Member> {
        self.members
            .all_members()
            .filter(|m| m.state == NodeState::Alive)
            .collect()
    }
}
