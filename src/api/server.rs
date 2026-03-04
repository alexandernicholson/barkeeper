use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::Server;

use rebar_cluster::registry::orset::Registry;
use rebar_core::process::{ExitReason, ProcessId};
use rebar_core::runtime::Runtime;
use rebar_core::supervisor::engine::{start_supervisor, ChildEntry};
use rebar_core::supervisor::spec::{ChildSpec, RestartStrategy, RestartType, SupervisorSpec};

use crate::api::auth_service::AuthService;
use crate::api::cluster_service::ClusterService;
use crate::api::gateway;
use crate::api::kv_service::KvService;
use crate::api::lease_service::LeaseService;
use crate::api::maintenance_service::MaintenanceService;
use crate::api::watch_service::WatchService;
use crate::auth::interceptor::GrpcAuthLayer;
use crate::auth::manager::AuthManager;
use crate::cluster::manager::ClusterManager;
use crate::config::ClusterConfig;
use crate::kv::state_machine::spawn_state_machine;
use crate::kv::store::KvStore;
use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::auth_server::AuthServer;
use crate::proto::etcdserverpb::cluster_server::ClusterServer;
use crate::proto::etcdserverpb::kv_server::KvServer;
use crate::proto::etcdserverpb::lease_server::LeaseServer;
use crate::proto::etcdserverpb::maintenance_server::MaintenanceServer;
use crate::proto::etcdserverpb::watch_server::WatchServer;
use crate::raft::grpc_transport::RaftTransportServer;
use crate::raft::node::{spawn_raft_node_rebar, RaftConfig};
use crate::tls::TlsConfig;
use crate::watch::hub::WatchHub;

/// The top-level barkeeper gRPC server.
pub struct BarkeepServer;

impl BarkeepServer {
    /// Start a barkeeper instance.
    ///
    /// This creates the data directory, opens the KV store, spawns the Raft
    /// node and state machine, and starts the tonic gRPC server. When
    /// `cluster_config` specifies peers, a gRPC-based Raft transport is used
    /// for inter-node communication and a peer listener is started on
    /// `listen_peer_url`.
    pub async fn start(
        config: RaftConfig,
        addr: SocketAddr,
        name: String,
        tls_config: TlsConfig,
        cluster_config: ClusterConfig,
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

        let supervisor = start_supervisor(runtime.clone(), supervisor_spec, vec![]).await;

        tracing::info!("started Rebar supervisor");

        // Create the apply channel between Raft and the state machine.
        let (apply_tx, apply_rx) = mpsc::channel(256);

        // Spawn the state machine apply loop.
        spawn_state_machine(Arc::clone(&store), apply_rx).await;

        // Create the Rebar registry and peer PID map for the Raft actor.
        let registry = Arc::new(Mutex::new(Registry::new()));
        let peers: Arc<Mutex<HashMap<u64, ProcessId>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Spawn the Raft node as a Rebar distributed actor.
        let raft_handle = spawn_raft_node_rebar(
            config.clone(),
            apply_tx,
            &runtime,
            Arc::clone(&registry),
            Arc::clone(&peers),
        )
        .await;
        let raft_term = Arc::clone(&raft_handle.current_term);

        // If clustered, start the peer gRPC listener for inbound Raft messages.
        if cluster_config.is_clustered() {
            let peer_addr: SocketAddr = cluster_config
                .listen_peer_url
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .parse()
                .expect("invalid --listen-peer-urls address");

            let raft_transport_server =
                RaftTransportServer::new(raft_handle.inbound_tx.clone());

            tracing::info!(%peer_addr, "starting peer gRPC listener for Raft transport");

            tokio::spawn(async move {
                Server::builder()
                    .add_service(
                        crate::proto::raftpb::raft_transport_server::RaftTransportServer::new(
                            raft_transport_server,
                        ),
                    )
                    .serve(peer_addr)
                    .await
                    .expect("peer gRPC server failed");
            });
        }

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

        // Resolve TLS cert/key paths (auto-generate if --auto-tls).
        let tls_paths = if tls_config.is_enabled() {
            let (cert_path, key_path) = if tls_config.auto_tls {
                let (c, k) = crate::tls::generate_self_signed(
                    &config.data_dir,
                    tls_config.self_signed_cert_validity,
                )?;
                tracing::info!(cert = %c, key = %k, "auto-TLS: using self-signed certificates");
                (c, k)
            } else {
                (
                    tls_config.cert_file.clone().unwrap(),
                    tls_config.key_file.clone().unwrap(),
                )
            };
            Some((cert_path, key_path))
        } else {
            None
        };

        let scheme = if tls_paths.is_some() { "https" } else { "http" };

        // Create the Cluster manager and gRPC service.
        let cluster_manager = Arc::new(ClusterManager::new(cluster_id));
        cluster_manager
            .add_initial_member(
                member_id,
                name,
                vec![format!("{}://{}", scheme, addr)],
                vec![format!("{}://{}", scheme, addr)],
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
        let alarms = maintenance_service.alarms();

        // Register lease expiry timer as a supervised Rebar child process.
        // The factory closure clones Arc references so restarts get fresh
        // handles to the same shared state.
        {
            let lm = Arc::clone(&lease_manager);
            let st = Arc::clone(&store);
            let wh = Arc::clone(&watch_hub);
            supervisor
                .add_child(ChildEntry::new(
                    ChildSpec::new("lease_expiry_timer")
                        .restart(RestartType::Permanent),
                    move || {
                        let lm = Arc::clone(&lm);
                        let st = Arc::clone(&st);
                        let wh = Arc::clone(&wh);
                        async move {
                            tracing::info!("lease expiry timer started");
                            loop {
                                tokio::time::sleep(Duration::from_millis(500)).await;
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
                            #[allow(unreachable_code)]
                            ExitReason::Normal
                        }
                    },
                ))
                .await
                .expect("failed to register lease_expiry_timer");
            tracing::info!("registered lease_expiry_timer with supervisor");
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
            Arc::clone(&auth_manager),
            alarms,
        );

        tracing::info!(%http_addr, %scheme, "starting HTTP gateway");
        if tls_paths.is_some() {
            // NOTE: HTTP/JSON gateway TLS is not yet implemented. The
            // gateway continues to listen on plaintext while the gRPC
            // endpoint is TLS-secured. Use the gRPC API for TLS clients.
            tracing::warn!(%http_addr, "HTTP gateway does not yet support TLS; listening on plaintext");
        }
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(http_addr)
                .await
                .expect("bind HTTP gateway");
            axum::serve(listener, http_app)
                .await
                .expect("HTTP gateway failed");
        });

        tracing::info!(%addr, "starting gRPC server");

        // Start the tonic gRPC server with auth enforcement layer.
        let auth_layer = GrpcAuthLayer::new(Arc::clone(&auth_manager));

        let mut server = Server::builder();

        if let Some((ref cert_path, ref key_path)) = tls_paths {
            let tonic_tls = crate::tls::build_tonic_tls(cert_path, key_path)?;
            server = server.tls_config(tonic_tls)?;
        }

        server
            .layer(auth_layer)
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
