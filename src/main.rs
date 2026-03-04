use clap::Parser;

#[derive(Parser)]
#[command(name = "barkeeper", about = "etcd-compatible distributed KV store on Rebar")]
struct Cli {
    #[arg(long, default_value = "default")]
    name: String,

    #[arg(long, default_value = "data.barkeeper")]
    data_dir: String,

    #[arg(long, default_value = "http://localhost:2379")]
    listen_client_urls: String,

    #[arg(long)]
    initial_cluster: Option<String>,

    #[arg(long, default_value = "new")]
    initial_cluster_state: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    tracing::info!(name = %cli.name, "barkeeper starting");
}
