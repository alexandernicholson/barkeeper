use barkeeper::api::server::BarkeepServer;
use barkeeper::raft::node::RaftConfig;
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let config = RaftConfig {
        node_id: cli.node_id,
        data_dir: cli.data_dir,
        ..Default::default()
    };

    let addr: SocketAddr = cli.listen_client_urls.parse()?;

    tracing::info!(name = %cli.name, %addr, "barkeeper starting");

    BarkeepServer::start(config, addr, cli.name).await?;

    Ok(())
}
