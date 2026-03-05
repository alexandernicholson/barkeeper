use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::Server;

use rebar::DistributedRuntime;
use rebar_cluster::connection::manager::{ConnectionManager, TransportConnector};
use rebar_cluster::transport::tcp::TcpTransport;
use rebar_cluster::transport::{TransportConnection, TransportError, TransportListener};
use rebar_cluster::drain::{NodeDrain, DrainConfig};
use rebar_cluster::registry::orset::Registry;
use rebar_cluster::swim::gossip::GossipQueue;
use rebar_cluster::swim::SwimConfig;
use rebar_core::process::{ExitReason, ProcessId};
use rebar_core::supervisor::engine::{start_supervisor, ChildEntry};
use rebar_core::supervisor::spec::{ChildSpec, RestartStrategy, RestartType, SupervisorSpec};

use crate::api::auth_service::AuthService;
use crate::api::cluster_service::ClusterService;
use crate::api::gateway;
use crate::api::kv_service::KvService;
use crate::api::lease_service::LeaseService;
use crate::api::maintenance_service::MaintenanceService;
use crate::api::watch_service::WatchService;
use crate::auth::actor::spawn_auth_actor;
use crate::auth::interceptor::GrpcAuthLayer;
use crate::cluster::actor::spawn_cluster_actor;
use crate::cluster::swim_service::SwimService;
use crate::config::ClusterConfig;
use crate::kv::actor::spawn_kv_store_actor;
use crate::kv::apply_broker::ApplyResultBroker;
use crate::kv::apply_notifier::ApplyNotifier;
use crate::kv::state_machine::spawn_state_machine;
use crate::kv::store::KvStore;
use crate::kv::write_buffer::WriteBuffer;
use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::auth_server::AuthServer;
use crate::proto::etcdserverpb::cluster_server::ClusterServer;
use crate::proto::etcdserverpb::kv_server::KvServer;
use crate::proto::etcdserverpb::lease_server::LeaseServer;
use crate::proto::etcdserverpb::maintenance_server::MaintenanceServer;
use crate::proto::etcdserverpb::watch_server::WatchServer;
use crate::raft::node::{spawn_raft_node_rebar, RaftConfig};
use crate::tls::TlsConfig;
use crate::watch::actor::spawn_watch_hub_actor;

/// TCP connector for the ConnectionManager.
///
/// Wraps `rebar_cluster::transport::tcp::TcpTransport` to implement the
/// `TransportConnector` trait required by `ConnectionManager`.
struct TcpConnector;

#[async_trait::async_trait]
impl TransportConnector for TcpConnector {
    async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<Box<dyn TransportConnection>, TransportError> {
        let transport = rebar_cluster::transport::TcpTransport::new();
        let conn = transport.connect(addr).await?;
        Ok(Box::new(conn))
    }
}

/// The top-level barkeeper gRPC server.
pub struct BarkeepServer;

impl BarkeepServer {
    /// Start a barkeeper instance.
    ///
    /// This creates the data directory, opens the KV store, spawns the Raft
    /// node and state machine, and starts the tonic gRPC server. When
    /// `cluster_config` specifies peers, TCP transport is used for inter-node
    /// communication via the DistributedRuntime and SWIM is bootstrapped for
    /// cluster membership.
    pub async fn start(
        config: RaftConfig,
        addr: SocketAddr,
        name: String,
        tls_config: TlsConfig,
        cluster_config: ClusterConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Create data directory.
        std::fs::create_dir_all(&config.data_dir)?;

        // Open the KV store.
        let kv_store = KvStore::open(format!("{}/kv.redb", config.data_dir))?;

        // Initialize the DistributedRuntime with TCP transport.
        let connector = Box::new(TcpConnector);
        let cm = ConnectionManager::new(connector);
        let mut distributed_runtime = DistributedRuntime::new(config.node_id, cm);

        // Borrow the runtime for actor spawning. The borrow ends before
        // the select! loop where distributed_runtime is used mutably.
        let runtime = distributed_runtime.runtime();

        let supervisor_spec = SupervisorSpec::new(RestartStrategy::OneForAll)
            .max_restarts(5)
            .max_seconds(30);

        let supervisor = start_supervisor(
            Arc::new(rebar_core::runtime::Runtime::new(config.node_id)),
            supervisor_spec,
            vec![],
        )
        .await;

        tracing::info!("started Rebar supervisor");

        // Wrap KvStore in Arc for shared access (state machine + actor).
        let kv_store = Arc::new(kv_store);

        // Spawn the KvStore actor on a standalone Rebar runtime.
        let kv_runtime = rebar_core::runtime::Runtime::new(config.node_id);
        let store = spawn_kv_store_actor(&kv_runtime, Arc::clone(&kv_store)).await;

        // Create the apply channel between Raft and the state machine.
        let (apply_tx, apply_rx) = mpsc::channel(256);

        // Create the apply result broker (shared between state machine and service handlers).
        let broker = Arc::new(ApplyResultBroker::new());

        // Create the Rebar registry and peer PID map for the Raft actor.
        let registry = Arc::new(Mutex::new(Registry::new()));
        let peers: Arc<Mutex<HashMap<u64, ProcessId>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Create the write buffer for decoupling KV fsync from write response path.
        let write_buffer = Arc::new(WriteBuffer::new());

        // Seed the Raft revision counter from the KvStore's current revision.
        let initial_rev = kv_store.current_revision().unwrap_or(0);
        let revision = Arc::new(AtomicI64::new(initial_rev));

        // Spawn the Raft node as a Rebar distributed actor.
        let raft_handle = spawn_raft_node_rebar(
            config.clone(),
            apply_tx,
            runtime,
            Arc::clone(&registry),
            Arc::clone(&peers),
            revision,
            Arc::clone(&write_buffer),
        )
        .await;
        let raft_term = Arc::clone(&raft_handle.current_term);

        // If clustered, populate the peer PID map so Raft can route
        // messages to remote nodes. The Raft actor is the first process
        // spawned on each node's distributed runtime, so its local_id
        // is always 1.
        if cluster_config.is_clustered() {
            let mut peers_map = peers.lock().unwrap();
            for (&peer_node_id, _addr) in &cluster_config.peers {
                if peer_node_id != config.node_id {
                    let remote_pid = ProcessId::new(peer_node_id, 1);
                    peers_map.insert(peer_node_id, remote_pid);
                    tracing::info!(peer_node_id, %remote_pid, "registered peer Raft PID");
                }
            }
            drop(peers_map);
        }

        // If clustered, start the TCP peer listener first so other
        // nodes can connect to us, then connect outbound to peers.
        if cluster_config.is_clustered() {
            if let Some(peer_addr) = cluster_config.listen_peer_addr {
                let transport = TcpTransport::new();
                let listener = transport
                    .listen(peer_addr)
                    .await
                    .expect("failed to bind peer listener");
                tracing::info!(%peer_addr, "TCP peer listener started");

                // Spawn inbound connection handler — accepts connections
                // and delivers received frames to local processes.
                let table = Arc::clone(distributed_runtime.table());
                tokio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok(mut conn) => {
                                tracing::debug!("accepted inbound peer connection");
                                let table = Arc::clone(&table);
                                tokio::spawn(async move {
                                    loop {
                                        match conn.recv().await {
                                            Ok(frame) => {
                                                tracing::trace!(msg_type = ?frame.msg_type, "received inbound frame");
                                                if let Err(e) = rebar_cluster::router::deliver_inbound_frame(&*table, &frame) {
                                                    tracing::warn!(error = %e, "failed to deliver inbound frame");
                                                }
                                            }
                                            Err(rebar_cluster::transport::TransportError::ConnectionClosed) => break,
                                            Err(e) => {
                                                tracing::debug!(error = %e, "inbound connection error");
                                                break;
                                            }
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to accept peer connection");
                            }
                        }
                    }
                });
            }

            // Retry connecting to peers — in Kubernetes, pods start
            // concurrently and peers may not be listening yet.
            let cm = distributed_runtime.connection_manager_mut();
            let mut pending: Vec<(u64, SocketAddr)> = cluster_config
                .peers
                .iter()
                .filter(|(id, _)| **id != config.node_id)
                .map(|(id, addr)| (*id, *addr))
                .collect();

            for attempt in 1..=30 {
                let mut still_pending = Vec::new();
                for (node_id, addr) in &pending {
                    if let Err(e) = cm.on_node_discovered(*node_id, *addr).await {
                        tracing::debug!(
                            node_id = node_id,
                            %addr,
                            attempt,
                            error = %e,
                            "peer not ready, will retry"
                        );
                        still_pending.push((*node_id, *addr));
                    } else {
                        tracing::info!(node_id = node_id, %addr, "connected to peer");
                    }
                }
                if still_pending.is_empty() {
                    tracing::info!(attempt, "all peers connected");
                    break;
                }
                pending = still_pending;
                if attempt < 30 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    for (node_id, addr) in &pending {
                        tracing::warn!(
                            node_id = node_id,
                            %addr,
                            "failed to connect to peer after 30 attempts"
                        );
                    }
                }
            }

            // Initialize SWIM service for membership discovery.
            let swim_config = SwimConfig::default();
            let mut swim = SwimService::new(config.node_id, swim_config);
            for (node_id, addr) in &cluster_config.peers {
                if *node_id != config.node_id {
                    swim.add_seed(*node_id, *addr);
                }
            }

            tracing::info!(
                peer_count = cluster_config.peers.len().saturating_sub(1),
                "SWIM membership initialized with seed peers"
            );

            // Spawn the SWIM protocol tick loop.
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if let Some(probe_target) = swim.tick() {
                        tracing::trace!(probe_target, "SWIM probing node");
                    }
                }
            });
        }

        // Create the Watch hub actor and gRPC service.
        let cluster_id = 1;
        let member_id = config.node_id;
        let watch_runtime = rebar_core::runtime::Runtime::new(config.node_id);
        let watch_hub = spawn_watch_hub_actor(&watch_runtime, Some(store.clone())).await;
        let watch_service = WatchService::new(watch_hub.clone(), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Lease manager and gRPC service.
        let lease_manager = Arc::new(LeaseManager::new());

        // Spawn the state machine apply loop (needs Arc<KvStore>, watch_hub, lease_manager, broker).
        let notifier = ApplyNotifier::new(0);
        spawn_state_machine(
            apply_rx,
            Arc::clone(&kv_store),
            watch_hub.clone(),
            Arc::clone(&lease_manager),
            Arc::clone(&broker),
            notifier.clone(),
            Arc::clone(&write_buffer),
        ).await;

        // Create the KV gRPC service.
        let kv_service = KvService::new(
            store.clone(),
            Arc::clone(&kv_store),
            Arc::clone(&lease_manager),
            cluster_id,
            member_id,
            Arc::clone(&raft_term),
            raft_handle.clone(),
            Arc::clone(&broker),
            notifier.clone(),
        );
        let lease_service = LeaseService::new(Arc::clone(&lease_manager), store.clone(), cluster_id, member_id, Arc::clone(&raft_term));

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

        // Spawn the Cluster actor on a standalone runtime (not tied to the
        // DistributedRuntime borrow) and get a handle.
        let cluster_runtime = rebar_core::runtime::Runtime::new(config.node_id);
        let cluster_manager = spawn_cluster_actor(&cluster_runtime, cluster_id).await;
        cluster_manager
            .add_initial_member(
                member_id,
                name,
                vec![format!("{}://{}", scheme, addr)],
                vec![format!("{}://{}", scheme, addr)],
            )
            .await;
        let cluster_service =
            ClusterService::new(cluster_manager.clone(), cluster_id, member_id, Arc::clone(&raft_term));

        // Spawn the Auth actor on a standalone Rebar runtime and get a handle.
        let auth_runtime = rebar_core::runtime::Runtime::new(config.node_id);
        let auth_manager = spawn_auth_actor(&auth_runtime).await;
        let auth_service = AuthService::new(auth_manager.clone(), cluster_id, member_id, Arc::clone(&raft_term));

        // Create the Maintenance gRPC service.
        let maintenance_service =
            MaintenanceService::new(store.clone(), cluster_id, member_id, Arc::clone(&raft_term), raft_handle.clone());
        let alarms = maintenance_service.alarms();

        // Register lease expiry timer as a supervised Rebar child process.
        // The factory closure clones Arc references so restarts get fresh
        // handles to the same shared state.
        {
            let lm = Arc::clone(&lease_manager);
            let st = store.clone();
            let wh = watch_hub.clone();
            supervisor
                .add_child(ChildEntry::new(
                    ChildSpec::new("lease_expiry_timer")
                        .restart(RestartType::Permanent),
                    move || {
                        let lm = Arc::clone(&lm);
                        let st = st.clone();
                        let wh = wh.clone();
                        async move {
                            tracing::info!("lease expiry timer started");
                            loop {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                let expired = lm.check_expired().await;
                                for lease in expired {
                                    for key in &lease.keys {
                                        if let Ok(result) = st.delete_range(key.clone(), vec![]).await {
                                            for prev in &result.prev_kvs {
                                                let tombstone = crate::proto::mvccpb::KeyValue {
                                                    key: prev.key.clone(),
                                                    create_revision: 0,
                                                    mod_revision: result.revision,
                                                    version: 0,
                                                    value: vec![],
                                                    lease: 0,
                                                };
                                                wh.notify(prev.key.clone(), 1, tombstone, Some(prev.clone())).await;
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
            store.clone(),
            Arc::clone(&kv_store),
            watch_hub.clone(),
            Arc::clone(&lease_manager),
            cluster_manager,
            cluster_id,
            member_id,
            Arc::clone(&raft_term),
            auth_manager.clone(),
            alarms,
            Arc::clone(&broker),
            notifier,
            write_buffer,
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

        // Build the tonic gRPC server with auth enforcement layer.
        let auth_layer = GrpcAuthLayer::new(auth_manager);

        let mut server = Server::builder();

        if let Some((ref cert_path, ref key_path)) = tls_paths {
            let tonic_tls = crate::tls::build_tonic_tls(cert_path, key_path)?;
            server = server.tls_config(tonic_tls)?;
        }

        let grpc_future = server
            .layer(auth_layer)
            .add_service(KvServer::new(kv_service))
            .add_service(WatchServer::new(watch_service))
            .add_service(LeaseServer::new(lease_service))
            .add_service(ClusterServer::new(cluster_service))
            .add_service(MaintenanceServer::new(maintenance_service))
            .add_service(AuthServer::new(auth_service))
            .serve(addr);

        // Determine the peer address for drain announcements.
        let node_id = config.node_id;
        let peer_addr = cluster_config.listen_peer_addr.unwrap_or(addr);

        // Run the gRPC server, outbound message loop, and signal handler
        // concurrently. The distributed_runtime stays owned here so it is
        // accessible for the drain protocol on shutdown.
        tokio::select! {
            result = grpc_future => {
                // gRPC server exited (should not happen under normal operation).
                result?;
            }
            _ = async {
                loop {
                    if distributed_runtime.process_outbound().await {
                        // message routed
                    } else {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            } => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received shutdown signal, initiating graceful drain");

                // Phase 1: Announce departure via SWIM Leave gossip and
                // unregister all names owned by this node from the registry.
                let drain = NodeDrain::new(DrainConfig::default());
                let mut gossip = GossipQueue::new();
                let names_removed = {
                    let mut reg = registry.lock().unwrap();
                    drain.announce(node_id, peer_addr, &mut gossip, &mut reg)
                };
                tracing::info!(
                    names_removed,
                    "phase 1: announced departure via SWIM Leave gossip"
                );

                // Phase 2 is skipped — the outbound loop has already stopped
                // because we exited the select!.

                // Phase 3: Close all peer connections.
                let cm = distributed_runtime.connection_manager_mut();
                let closed = cm.drain_connections().await;
                tracing::info!(
                    connections_closed = closed,
                    "phase 3: connections drained, shutdown complete"
                );
            }
        }

        Ok(())
    }
}
