mod reader;
mod data_store;
pub mod command;
pub mod parser;



use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::net::unix::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio_util::codec::Framed;
use tracing::info;
use uuid::Uuid;

use crate::command::CommandResponse;
use crate::data_store::{Client, DataStore};

use dashmap::DashMap;
use once_cell::sync::Lazy;

type DS = DashMap<Uuid,Client>;

static CLIENTS: Lazy<DS> = Lazy::new(|| DashMap::new());
static KV: Lazy<DashMap<String,String>> = Lazy::new(|| DashMap::new());
static ADDRESS_TO_UUID: Lazy<DashMap<String,Uuid>> = Lazy::new(|| DashMap::new());
// static KV: Lazy<DashMap<Uuid,&'static HashMap<String,String>>> = Lazy::new(|| DashMap::new());


pub async fn handle_connection(stream: TcpStream, command_tx: Sender<CommandResponse>)->Result<(),Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr().unwrap().to_string();
    info!("Accepted connection from {}", peer_addr);
    let client_id = ADDRESS_TO_UUID.get(&peer_addr).map(|id| id.clone()).unwrap_or_else(|| {
        let id = Uuid::new_v4();
        ADDRESS_TO_UUID.insert(peer_addr, id);
        id
    });


    // Wrap the TCP stream with a RESP codec.
    let framed: Framed<TcpStream, parser::RespCodec> = Framed::new(stream, parser::RespCodec);

    let (writer_sink, reader_stream) = framed.split();
    let client = Client::new(writer_sink, &KV);

    CLIENTS.insert(client_id, client);

    info!("Added client {} to data store", client_id);

    let reader = reader::Reader::new(reader_stream, command_tx, client_id);
    reader.run().await?;

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

    loop {
        let (socket, _) = listener.accept().await?;
        info!("Client count: {}", CLIENTS.len() + 1);
        let command_tx_clone = command_tx.clone();
        tokio::spawn(async move {
            handle_connection(socket, command_tx_clone).await.unwrap();
        });
    }
}