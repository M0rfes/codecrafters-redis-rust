pub mod command;
mod writer;
pub mod parser;
mod reader;

use std::time::Duration;
use chrono::Utc;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::codec::Framed;
use tracing::{error, info};
use uuid::Uuid;

use crate::command::CommandResponse;

use dashmap::DashMap;
use once_cell::sync::Lazy;

static KV: Lazy<(DashMap<String, String>, DashMap<String, u64>)> =
    Lazy::new(|| (DashMap::new(), DashMap::new()));

static ADDRESS_TO_UUID: Lazy<DashMap<String, Uuid>> = Lazy::new(|| DashMap::new());

pub async fn handle_connection(
    stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr().unwrap().to_string();
    info!("Accepted connection from {}", peer_addr);
    let client_id = ADDRESS_TO_UUID
        .get(&peer_addr)
        .map(|id| id.clone())
        .unwrap_or_else(|| {
            let id = Uuid::new_v4();
            ADDRESS_TO_UUID.insert(peer_addr, id);
            id
        });

    // Wrap the TCP stream with a RESP codec.
    let framed: Framed<TcpStream, parser::RespCodec> = Framed::new(stream, parser::RespCodec);

    let (writer_sink, reader_stream) = framed.split();
    let (command_tx, command_rx) = mpsc::channel::<CommandResponse>(32);

    let mut writer = writer::Writer::new(&KV, command_rx, writer_sink);
    let _ = tokio::spawn(async move {
      let Ok(_) = writer.run().await else {
        error!("Failed to start writer");
        return;
      };
    });

    info!("Added client {} to data store", client_id);

    let reader = reader::Reader::new(reader_stream, command_tx, client_id);
    reader.run().await?;

    Ok(())
}

pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    let _ = tokio::spawn(async move {
        let mut gc_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            gc().await;
            gc_interval.tick().await;
        }
    });

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(socket).await.unwrap();
        });
    }
}

pub async fn gc() {
    let uuids = ADDRESS_TO_UUID.iter().map(|entry| entry.value().clone()).collect::<Vec<_>>();
    for uuid in uuids {
        let kv1 = KV.0.clone();
        let kv2 = KV.1.clone();
        let now = Utc::now().timestamp_millis();
        let expired = kv2.iter().filter(|entry| now > (*entry.value() as i64)).map(|entry| entry.key().clone()).collect::<Vec<_>>();
        for key in expired {
            kv1.remove(&key);
            kv2.remove(&key);
        }
        if kv1.len() > 0 && kv2.len() > 0 {
            info!("GCed client {} with {} keys and {} values", uuid, kv1.len(), kv2.len());
        }
    }
}

