mod reader;
mod data_store;
pub mod command;
pub mod parser;

use std::sync::Arc;

use bytes::BytesMut;
use futures::stream::SplitSink;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio_util::codec::Framed;
use tracing::info;
use uuid::Uuid;

use crate::command::CommandResponse;
use crate::data_store::DataStore;
use crate::parser::RespCodec;

use dashmap::DashMap;
use once_cell::sync::Lazy;

type DS = DashMap<Uuid,SplitSink<Framed<TcpStream, RespCodec>, BytesMut>>;

static CLIENTS: Lazy<DS> = Lazy::new(|| DashMap::new());


pub async fn handle_connection(stream: TcpStream, command_tx: Sender<CommandResponse>)->Result<(),Box<dyn std::error::Error>> {
    let client_id = Uuid::new_v4();
    let peer_addr = stream.peer_addr().unwrap();
    info!("Accepted connection from {} with client id {}", peer_addr, client_id);

    // Wrap the TCP stream with a RESP codec.
    let framed = Framed::new(stream, parser::RespCodec);

    let (writer_sink, reader_stream) = framed.split();

    CLIENTS.insert(client_id, writer_sink);

    info!("Added client {} to data store", client_id);

    let reader = reader::Reader::new(reader_stream, command_tx, client_id);
    reader.run().await?;

    info!("Connection with {} closed", peer_addr);
    Ok(())
}

pub async fn start_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    let (command_tx, command_rx) = mpsc::channel::<CommandResponse>(32);

    let mut data_store = DataStore::new(&CLIENTS,command_rx);

    let _ = tokio::spawn(async move {
        data_store.run().await.unwrap();
    });
    let mut client_count = 0;

    loop {
        let (socket, _) = listener.accept().await?;
        client_count += 1;
        info!("Client count: {}", client_count);
        let command_tx_clone = command_tx.clone();
        tokio::spawn(async move {
            handle_connection(socket, command_tx_clone).await.unwrap();
        });
    }
}