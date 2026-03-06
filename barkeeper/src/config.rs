use std::collections::HashMap;
use std::net::SocketAddr;

use crate::cluster::discovery::ClusterMode;

/// Configuration for cluster bootstrapping.
#[derive(Debug, Clone, Default)]
pub struct ClusterConfig {
    /// How the cluster was specified.
    pub mode: Option<ClusterMode>,
    /// Resolved peer addresses: node_id -> SocketAddr.
    pub peers: HashMap<u64, SocketAddr>,
    /// Address to listen on for peer traffic (TCP transport).
    pub listen_peer_addr: Option<SocketAddr>,
    /// Raw --initial-cluster value (for DNS re-resolution).
    pub raw_initial_cluster: Option<String>,
    /// Initial cluster state: "new" or "existing".
    pub initial_cluster_state: String,
}

impl ClusterConfig {
    /// Returns true if multi-node clustering is configured.
    pub fn is_clustered(&self) -> bool {
        !self.peers.is_empty() || self.raw_initial_cluster.is_some()
    }
}
