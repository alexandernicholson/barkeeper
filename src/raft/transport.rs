use super::messages::RaftMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Trait for sending Raft messages to peer nodes.
///
/// Implementations handle the actual network transport. The `LocalTransport`
/// implementation routes messages in-process for testing multi-node clusters
/// in a single process. A future `RebarTransport` will use the Rebar
/// DistributedRouter for real multi-node communication.
#[async_trait::async_trait]
pub trait RaftTransport: Send + Sync {
    /// Send a Raft message to the node identified by `to`.
    async fn send(&self, to: u64, message: RaftMessage);
}

/// In-process transport for testing multi-node Raft in a single process.
///
/// Each registered node gets a sender half of an mpsc channel. Messages are
/// delivered by looking up the target node's sender and forwarding the message.
pub struct LocalTransport {
    peers: Arc<Mutex<HashMap<u64, mpsc::Sender<(u64, RaftMessage)>>>>,
}

impl LocalTransport {
    pub fn new() -> Self {
        LocalTransport {
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a node's inbound message channel with the transport.
    pub async fn register(&self, id: u64, tx: mpsc::Sender<(u64, RaftMessage)>) {
        self.peers.lock().await.insert(id, tx);
    }
}

impl Default for LocalTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RaftTransport for LocalTransport {
    async fn send(&self, to: u64, message: RaftMessage) {
        let peers = self.peers.lock().await;
        if let Some(tx) = peers.get(&to) {
            let _ = tx.send((to, message)).await;
        }
    }
}
