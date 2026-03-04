use barkeeper::api::server::BarkeepServer;
use barkeeper::config::ClusterConfig;
use barkeeper::raft::node::RaftConfig;
use barkeeper::tls::TlsConfig;
use clap::Parser;
use std::collections::HashMap;
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

/// Parse `--initial-cluster` value into a map of node_id -> peer URL.
///
/// Expected format: "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380"
fn parse_initial_cluster(raw: &str) -> Result<HashMap<u64, String>, String> {
    let mut peers = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id_str, url) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid cluster entry (expected id=url): '{}'", entry))?;
        let id: u64 = id_str
            .trim()
            .parse()
            .map_err(|e| format!("invalid node id '{}': {}", id_str, e))?;
        peers.insert(id, url.trim().to_string());
    }
    Ok(peers)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let tls_config = TlsConfig {
        cert_file: cli.cert_file,
        key_file: cli.key_file,
        trusted_ca_file: cli.trusted_ca_file,
        auto_tls: cli.auto_tls,
        client_cert_auth: cli.client_cert_auth,
        self_signed_cert_validity: cli.self_signed_cert_validity,
    };

    // Parse cluster configuration.
    let cluster_config = if let Some(ref raw) = cli.initial_cluster {
        let peers = parse_initial_cluster(raw)?;
        let peer_ids: Vec<u64> = peers.keys().filter(|id| **id != cli.node_id).copied().collect();
        tracing::info!(
            node_id = cli.node_id,
            peer_count = peer_ids.len(),
            listen_peer_url = %cli.listen_peer_urls,
            "multi-node cluster mode"
        );
        // Set Raft peers from the cluster config (excluding self).
        let config = RaftConfig {
            node_id: cli.node_id,
            data_dir: cli.data_dir,
            peers: peer_ids,
            ..Default::default()
        };
        (
            config,
            ClusterConfig {
                peers,
                listen_peer_url: cli.listen_peer_urls,
                initial_cluster_state: cli.initial_cluster_state,
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
