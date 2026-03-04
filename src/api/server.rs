use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::transport::Server;

use crate::api::kv_service::KvService;
use crate::api::lease_service::LeaseService;
use crate::api::watch_service::WatchService;
use crate::kv::state_machine::spawn_state_machine;
use crate::kv::store::KvStore;
use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::kv_server::KvServer;
use crate::proto::etcdserverpb::lease_server::LeaseServer;
use crate::proto::etcdserverpb::watch_server::WatchServer;
use crate::raft::node::{spawn_raft_node, RaftConfig};
use crate::watch::hub::WatchHub;

/// The top-level barkeeper gRPC server.
pub struct BarkeepServer;

impl BarkeepServer {
    /// Start a single-node barkeeper instance.
    ///
    /// This creates the data directory, opens the KV store, spawns the Raft
    /// node and state machine, and starts the tonic gRPC server.
    pub async fn start(
        config: RaftConfig,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Create data directory.
        std::fs::create_dir_all(&config.data_dir)?;

        // Open the shared KV store.
        let store = Arc::new(KvStore::open(format!("{}/kv.redb", config.data_dir))?);

        // Create the apply channel between Raft and the state machine.
        let (apply_tx, apply_rx) = mpsc::channel(256);

        // Spawn the state machine apply loop.
        spawn_state_machine(Arc::clone(&store), apply_rx).await;

        // Spawn the Raft node.
        let raft_handle = spawn_raft_node(config.clone(), apply_tx).await;

        // Create the KV gRPC service.
        let cluster_id = 1;
        let member_id = config.node_id;
        let kv_service = KvService::new(raft_handle, Arc::clone(&store), cluster_id, member_id);

        // Create the Watch hub and gRPC service.
        let watch_hub = Arc::new(WatchHub::new());
        let watch_service = WatchService::new(Arc::clone(&watch_hub), cluster_id, member_id);

        // Create the Lease manager and gRPC service.
        let lease_manager = Arc::new(LeaseManager::new());
        let lease_service = LeaseService::new(Arc::clone(&lease_manager), cluster_id, member_id);

        tracing::info!(%addr, "starting gRPC server");

        // Start the tonic gRPC server.
        Server::builder()
            .add_service(KvServer::new(kv_service))
            .add_service(WatchServer::new(watch_service))
            .add_service(LeaseServer::new(lease_service))
            .serve(addr)
            .await?;

        Ok(())
    }
}
