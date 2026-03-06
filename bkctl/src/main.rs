use clap::Parser;

#[derive(Parser)]
#[command(name = "bkctl", about = "TUI for exploring barkeeper clusters")]
struct Cli {
    /// Comma-separated gRPC endpoints
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoints: String,

    /// Initial watch/browse prefix
    #[arg(long, default_value = "/")]
    prefix: String,
}

fn main() {
    let _cli = Cli::parse();
    println!("bkctl stub");
}
