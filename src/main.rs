use barkeeper::api::server::BarkeepServer;
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

    let config = RaftConfig {
        node_id: cli.node_id,
        data_dir: cli.data_dir,
        ..Default::default()
    };

    let addr: SocketAddr = cli.listen_client_urls.parse()?;

    tracing::info!(name = %cli.name, %addr, "barkeeper starting");

    BarkeepServer::start(config, addr, cli.name, tls_config).await?;

    Ok(())
}
