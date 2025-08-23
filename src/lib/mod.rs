pub mod command;
pub mod parser;
pub mod kv_manager;
mod writer;

use futures::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{error, info};



use crate::kv_manager::KvManager;




pub async fn handle_connection(stream: TcpStream, kv: Arc<KvManager>) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr().unwrap().to_string();
    info!("Accepted connection from {}", peer_addr);

    // Wrap the TCP stream with a RESP codec.
    let framed: Framed<TcpStream, parser::RespCodec> = Framed::new(stream, parser::RespCodec);
    let (mut skink, mut reader) = framed.split();
    let mut writer = writer::Writer::new(kv);

    while let Some(Ok(frame)) = reader.next().await {
        let Ok(command) = command::Command::try_from(frame) else {
            error!("Failed to parse command");
            continue;
        };
        let response = writer.process(command).await;
        info!("Received response: {:?}", response);
        skink.send(response.to_bytes()).await?;
    }

    Ok(())
}

pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);
    let kv = Arc::new(KvManager::new());

    let kv_ref = kv.clone();
    let _ = tokio::spawn(async move {
        let mut gc_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            gc(kv_ref.clone()).await;
            gc_interval.tick().await;
        }
    });
    
    loop {
        let kv_ref = kv.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(socket, kv_ref.clone()).await.unwrap();
        });
    }
}

pub async fn gc(kv: Arc<KvManager>) {
    kv.evect_expired().await;
}
