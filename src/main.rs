#![allow(unused_imports)]
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

/// A simple caching service server that listens on a port.
#[derive(Parser, Debug)]
#[clap(author, version, about = "A blazing fast caching server", long_about = None)]
struct ServerArgs {
    /// Port to listen on.
    #[clap(short, long, default_value = "6379")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging.
    tracing_subscriber::fmt::init();

    // Parse command-line arguments.
    let args = ServerArgs::parse();
    
    codecrafters_redis::start_server(args.port).await?;

    Ok(())
}
