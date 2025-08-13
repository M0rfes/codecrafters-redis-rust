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
    let addr = format!("0.0.0.0:{}", args.port);

    // Bind a TCP listener to the specified address.
    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);


    // Accept incoming connections in a loop.
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                info!("Accepted connection from {}", peer_addr);
                codecrafters_redis::handle_connection(socket).await?;
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}
