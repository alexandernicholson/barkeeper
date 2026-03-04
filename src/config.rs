use std::collections::HashMap;

/// Configuration for multi-node cluster bootstrapping.
#[derive(Debug, Clone, Default)]
pub struct ClusterConfig {
    /// Map of node_id -> peer gRPC URL (e.g. "http://10.0.0.1:2380").
    /// Empty when running in single-node mode.
    pub peers: HashMap<u64, String>,
    /// URL to listen on for peer traffic.
    pub listen_peer_url: String,
    /// Initial cluster state: "new" for a fresh cluster, "existing" to join.
    pub initial_cluster_state: String,
}

impl ClusterConfig {
    /// Returns true if multi-node clustering is configured.
    pub fn is_clustered(&self) -> bool {
        !self.peers.is_empty()
    }
}
