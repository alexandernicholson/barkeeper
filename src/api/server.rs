use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::transport::Server;

use rebar_core::runtime::Runtime;
use rebar_core::supervisor::engine::start_supervisor;
use rebar_core::supervisor::spec::{SupervisorSpec, RestartStrategy};

use crate::api::auth_service::AuthService;
use crate::api::cluster_service::ClusterService;
use crate::api::gateway;
use crate::api::kv_service::KvService;
use crate::api::lease_service::LeaseService;
use crate::api::maintenance_service::MaintenanceService;
use crate::api::watch_service::WatchService;
use crate::auth::manager::AuthManager;
use crate::cluster::manager::ClusterManager;
use crate::kv::state_machine::spawn_state_machine;
use crate::kv::store::KvStore;
use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::auth_server::AuthServer;
use crate::proto::etcdserverpb::cluster_server::ClusterServer;
use crate::proto::etcdserverpb::kv_server::KvServer;
use crate::proto::etcdserverpb::lease_server::LeaseServer;
use crate::proto::etcdserverpb::maintenance_server::MaintenanceServer;
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
        name: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Create data directory.
        std::fs::create_dir_all(&config.data_dir)?;

        // Open the shared KV store.
        let store = Arc::new(KvStore::open(format!("{}/kv.redb", config.data_dir))?);

        // Initialize the Rebar runtime and supervisor.
        let runtime = Arc::new(Runtime::new(config.node_id));

        let supervisor_spec = SupervisorSpec::new(RestartStrategy::OneForAll)
            .max_restarts(5)
            .max_seconds(30);

        let _supervisor = start_supervisor(runtime.clone(), supervisor_spec, vec![]).await;

        tracing::info!("started Rebar supervisor");

        // Create the apply channel between Raft and the state machine.
        let (apply_tx, apply_rx) = mpsc::channel(256);

        // Spawn the state machine apply loop.
        spawn_state_machine(Arc::clone(&store), apply_rx).await;

        // Spawn the Raft node.
        let raft_handle = spawn_raft_node(config.clone(), apply_tx, None).await;
        let raft_term = Arc::clone(&raft_handle.current_term);

        // Create the Watch hub and gRPC service.
        let cluster_id = 1;
        let member_id = config.node_id;
        let watch_hub = Arc::new(WatchHub::with_store(Arc::clone(&store)));
        let watch_service = WatchService::new(Arc::clone(&watch_hub), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Lease manager and gRPC service.
        let lease_manager = Arc::new(LeaseManager::new());

        // Create the KV gRPC service.
        let kv_service = KvService::new(
            Arc::clone(&store),
            Arc::clone(&watch_hub),
            Arc::clone(&lease_manager),
            cluster_id,
            member_id,
            Arc::clone(&raft_term),
            raft_handle.clone(),
        );
        let lease_service = LeaseService::new(Arc::clone(&lease_manager), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Cluster manager and gRPC service.
        let cluster_manager = Arc::new(ClusterManager::new(cluster_id));
        cluster_manager
            .add_initial_member(
                member_id,
                name,
                vec![format!("http://{}", addr)],
                vec![format!("http://{}", addr)],
            )
            .await;
        let cluster_service =
            ClusterService::new(Arc::clone(&cluster_manager), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Auth manager and gRPC service.
        let auth_manager = Arc::new(AuthManager::new());
        let auth_service = AuthService::new(Arc::clone(&auth_manager), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Maintenance gRPC service.
        let maintenance_service =
            MaintenanceService::new(Arc::clone(&store), cluster_id, member_id, Arc::clone(&raft_term), raft_handle.clone());

        // Spawn lease expiry timer — checks every 500ms for expired leases.
        {
            let lm = Arc::clone(&lease_manager);
            let st = Arc::clone(&store);
            let wh = Arc::clone(&watch_hub);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    let expired = lm.check_expired().await;
                    for lease in expired {
                        for key in &lease.keys {
                            if let Ok(result) = st.delete_range(key, b"") {
                                for prev in &result.prev_kvs {
                                    let tombstone = crate::proto::mvccpb::KeyValue {
                                        key: prev.key.clone(),
                                        create_revision: 0,
                                        mod_revision: result.revision,
                                        version: 0,
                                        value: vec![],
                                        lease: 0,
                                    };
                                    wh.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Start the HTTP/JSON gateway on port + 1.
        let http_addr = SocketAddr::new(addr.ip(), addr.port() + 1);
        let http_app = gateway::create_router(
            raft_handle,
            Arc::clone(&store),
            Arc::clone(&watch_hub),
            Arc::clone(&lease_manager),
            Arc::clone(&cluster_manager),
            cluster_id,
            member_id,
            Arc::clone(&raft_term),
        );

        tracing::info!(%http_addr, "starting HTTP gateway");
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(http_addr)
                .await
                .expect("bind HTTP gateway");
            axum::serve(listener, http_app)
                .await
                .expect("HTTP gateway failed");
        });

        tracing::info!(%addr, "starting gRPC server");

        // Start the tonic gRPC server.
        Server::builder()
            .add_service(KvServer::new(kv_service))
            .add_service(WatchServer::new(watch_service))
            .add_service(LeaseServer::new(lease_service))
            .add_service(ClusterServer::new(cluster_service))
            .add_service(MaintenanceServer::new(maintenance_service))
            .add_service(AuthServer::new(auth_service))
            .serve(addr)
            .await?;

        Ok(())
    }
}
