use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use crate::proto::raftpb::raft_transport_client::RaftTransportClient;
use crate::proto::raftpb::raft_transport_server::RaftTransport as RaftTransportGrpc;
use crate::proto::raftpb::{RaftMessageRequest, RaftMessageResponse};
use crate::raft::messages::RaftMessage;
use crate::raft::transport::RaftTransport;

// ---------------------------------------------------------------------------
// Client side — implements our RaftTransport trait over gRPC
// ---------------------------------------------------------------------------

/// gRPC-based transport for multi-node Raft clusters.
///
/// Maintains a map of node_id to gRPC endpoint URL. When `send` is called,
/// it connects to the appropriate peer and forwards the serialised Raft
/// message.
pub struct GrpcRaftTransport {
    /// Local node id (used as `from_node` in outgoing messages).
    local_id: u64,
    /// Map of node_id -> gRPC endpoint URL.
    peers: Arc<Mutex<HashMap<u64, String>>>,
}

impl GrpcRaftTransport {
    pub fn new(local_id: u64, peers: HashMap<u64, String>) -> Self {
        Self {
            local_id,
            peers: Arc::new(Mutex::new(peers)),
        }
    }
}

#[async_trait::async_trait]
impl RaftTransport for GrpcRaftTransport {
    async fn send(&self, to: u64, message: RaftMessage) {
        let peers = self.peers.lock().await;
        let endpoint = match peers.get(&to) {
            Some(ep) => ep.clone(),
            None => return,
        };
        drop(peers);

        let from_node = self.local_id;

        match tonic::transport::Channel::from_shared(endpoint.clone()) {
            Ok(channel_builder) => match channel_builder.connect().await {
                Ok(channel) => {
                    let mut client = RaftTransportClient::new(channel);
                    let data = serde_json::to_vec(&message).unwrap_or_default();
                    let req = tonic::Request::new(RaftMessageRequest {
                        from_node,
                        message: data,
                    });
                    if let Err(e) = client.send_message(req).await {
                        tracing::warn!(to, error = %e, "failed to send raft message");
                    }
                }
                Err(e) => {
                    tracing::warn!(to, error = %e, "failed to connect to peer");
                }
            },
            Err(e) => {
                tracing::warn!(to, error = %e, "invalid peer endpoint");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server side — receives inbound Raft messages from peers via gRPC
// ---------------------------------------------------------------------------

/// Server-side gRPC service that receives Raft messages from peers and
/// forwards them into the local Raft node via an mpsc channel.
pub struct RaftTransportServer {
    inbound_tx: mpsc::Sender<(u64, RaftMessage)>,
}

impl RaftTransportServer {
    pub fn new(inbound_tx: mpsc::Sender<(u64, RaftMessage)>) -> Self {
        Self { inbound_tx }
    }
}

#[tonic::async_trait]
impl RaftTransportGrpc for RaftTransportServer {
    async fn send_message(
        &self,
        request: tonic::Request<RaftMessageRequest>,
    ) -> Result<tonic::Response<RaftMessageResponse>, tonic::Status> {
        let req = request.into_inner();
        let message: RaftMessage = serde_json::from_slice(&req.message).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid raft message: {}", e))
        })?;

        self.inbound_tx
            .send((req.from_node, message))
            .await
            .map_err(|_| tonic::Status::unavailable("raft node is shutting down"))?;

        Ok(tonic::Response::new(RaftMessageResponse {}))
    }
}
