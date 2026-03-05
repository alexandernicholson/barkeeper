use barkeeper::api::server::BarkeepServer;
use barkeeper::cluster::discovery;
use barkeeper::config::ClusterConfig;
use barkeeper::raft::node::RaftConfig;
use barkeeper::tls::TlsConfig;
use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "barkeeper", about = "etcd-compatible distributed KV store on Rebar")]
struct Cli {
    #[arg(long, default_value = "default")]
    name: String,

    #[arg(long, default_value = "data.barkeeper")]
    data_dir: String,

    #[arg(long, default_value = "127.0.0.1:2379")]
    listen_client_urls: String,

    #[arg(long, default_value = "1")]
    node_id: u64,

    /// Path to the client server TLS cert file.
    #[arg(long)]
    cert_file: Option<String>,

    /// Path to the client server TLS key file.
    #[arg(long)]
    key_file: Option<String>,

    /// Path to the client server TLS trusted CA cert file.
    #[arg(long)]
    trusted_ca_file: Option<String>,

    /// Automatically generate self-signed certs for client connections.
    #[arg(long, default_value = "false")]
    auto_tls: bool,

    /// Enable client certificate authentication.
    #[arg(long, default_value = "false")]
    client_cert_auth: bool,

    /// The validity period of the self-signed certificate, in years.
    #[arg(long, default_value = "1")]
    self_signed_cert_validity: u32,

    /// Path to the peer TLS cert file.
    #[arg(long)]
    peer_cert_file: Option<String>,

    /// Path to the peer TLS key file.
    #[arg(long)]
    peer_key_file: Option<String>,

    /// Path to the peer TLS trusted CA cert file.
    #[arg(long)]
    peer_trusted_ca_file: Option<String>,

    /// Automatically generate self-signed certs for peer connections.
    #[arg(long, default_value = "false")]
    peer_auto_tls: bool,

    /// Comma-separated list of initial cluster members.
    /// Format: "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380,3=http://10.0.0.3:2380"
    #[arg(long)]
    initial_cluster: Option<String>,

    /// Initial cluster state: "new" for fresh cluster, "existing" to join.
    #[arg(long, default_value = "new")]
    initial_cluster_state: String,

    /// URL to listen on for peer traffic.
    #[arg(long, default_value = "http://localhost:2380")]
    listen_peer_urls: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();

    let tls_config = TlsConfig {
        cert_file: cli.cert_file,
        key_file: cli.key_file,
        trusted_ca_file: cli.trusted_ca_file,
        auto_tls: cli.auto_tls,
        client_cert_auth: cli.client_cert_auth,
        self_signed_cert_validity: cli.self_signed_cert_validity,
    };

    // Parse cluster configuration using the discovery module.
    // Uses async resolution to support DNS hostnames in --initial-cluster.
    let cluster_config = if let Some(ref raw) = cli.initial_cluster {
        let (mode, peers) = discovery::parse_initial_cluster_async(raw, None, 2380).await?;
        let peer_ids: Vec<u64> = peers.keys().filter(|id| **id != cli.node_id).copied().collect();

        let listen_peer_addr: SocketAddr = cli.listen_peer_urls
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .parse()
            .expect("invalid --listen-peer-urls address");

        // Auto-detect cluster state: if data-dir has existing Raft log,
        // treat as "existing" regardless of --initial-cluster-state.
        let effective_state = if cli.initial_cluster_state == "new"
            && std::path::Path::new(&cli.data_dir).join("raft.redb").exists()
        {
            tracing::info!("detected existing data in {}, using initial-cluster-state=existing", cli.data_dir);
            "existing".to_string()
        } else {
            cli.initial_cluster_state
        };

        tracing::info!(
            node_id = cli.node_id,
            peer_count = peer_ids.len(),
            %listen_peer_addr,
            "multi-node cluster mode"
        );

        let config = RaftConfig {
            node_id: cli.node_id,
            data_dir: cli.data_dir,
            peers: peer_ids,
            ..Default::default()
        };
        (
            config,
            ClusterConfig {
                mode: Some(mode),
                peers,
                listen_peer_addr: Some(listen_peer_addr),
                raw_initial_cluster: Some(raw.clone()),
                initial_cluster_state: effective_state,
            },
        )
    } else {
        let config = RaftConfig {
            node_id: cli.node_id,
            data_dir: cli.data_dir,
            ..Default::default()
        };
        (config, ClusterConfig::default())
    };

    let (raft_config, cluster) = cluster_config;
    let addr: SocketAddr = cli.listen_client_urls.parse()?;

    tracing::info!(name = %cli.name, %addr, "barkeeper starting");

    BarkeepServer::start(raft_config, addr, cli.name, tls_config, cluster).await?;

    Ok(())
}
