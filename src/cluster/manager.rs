use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// A cluster member tracked in memory.
#[derive(Debug, Clone)]
pub struct Member {
    pub id: u64,
    pub name: String,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub is_learner: bool,
}

/// In-memory cluster membership manager.
///
/// Tracks the set of members that belong to this cluster. The actual SWIM
/// bridge will be added when we integrate with Rebar's cluster layer;
/// for now every mutation is local.
pub struct ClusterManager {
    members: Arc<Mutex<HashMap<u64, Member>>>,
    cluster_id: u64,
    next_id: Arc<Mutex<u64>>,
}

impl ClusterManager {
    pub fn new(cluster_id: u64) -> Self {
        ClusterManager {
            members: Arc::new(Mutex::new(HashMap::new())),
            cluster_id,
            next_id: Arc::new(Mutex::new(100)), // start generated IDs at 100
        }
    }

    /// Return the cluster ID.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Add the local node as the initial cluster member.
    pub async fn add_initial_member(
        &self,
        id: u64,
        name: String,
        peer_urls: Vec<String>,
        client_urls: Vec<String>,
    ) {
        let member = Member {
            id,
            name,
            peer_urls,
            client_urls,
            is_learner: false,
        };
        self.members.lock().await.insert(id, member);

        // Keep next_id ahead of any known member ID.
        let mut next = self.next_id.lock().await;
        if id >= *next {
            *next = id + 1;
        }
    }

    /// List all members.
    pub async fn member_list(&self) -> Vec<Member> {
        self.members.lock().await.values().cloned().collect()
    }

    /// Add a new member. Returns the newly created member.
    pub async fn member_add(&self, peer_urls: Vec<String>, is_learner: bool) -> Member {
        let mut next = self.next_id.lock().await;
        let id = *next;
        *next += 1;
        drop(next);

        let member = Member {
            id,
            name: String::new(),
            peer_urls,
            client_urls: vec![],
            is_learner,
        };

        self.members.lock().await.insert(id, member.clone());
        member
    }

    /// Remove a member by ID. Returns true if the member existed.
    pub async fn member_remove(&self, id: u64) -> bool {
        self.members.lock().await.remove(&id).is_some()
    }

    /// Update a member's peer URLs. Returns true if the member existed.
    pub async fn member_update(&self, id: u64, peer_urls: Vec<String>) -> bool {
        let mut members = self.members.lock().await;
        if let Some(member) = members.get_mut(&id) {
            member.peer_urls = peer_urls;
            true
        } else {
            false
        }
    }

    /// Promote a learner member to a voting member. Returns true if the
    /// member existed and was a learner.
    pub async fn member_promote(&self, id: u64) -> bool {
        let mut members = self.members.lock().await;
        if let Some(member) = members.get_mut(&id) {
            if member.is_learner {
                member.is_learner = false;
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}
