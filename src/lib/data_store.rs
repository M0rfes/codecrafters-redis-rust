use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use futures::{stream::SplitSink, SinkExt};
use once_cell::sync::Lazy;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio_util::codec::Framed;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::command::{Command, CommandResponse, Response};
use crate::parser::RespCodec;

type Client = SplitSink<Framed<TcpStream, RespCodec>, BytesMut>;

pub struct DataStore {
    clients: &'static Lazy<DashMap<Uuid, Client>>,
    command_rx: Receiver<CommandResponse>,
}

#[derive(Debug, Error)]
pub enum DataStoreError {
    #[error("Send error occurred")]
    SendError(String),
}

impl DataStore {
    pub fn new(clients: &'static Lazy<DashMap<Uuid, Client>>, command_rx: Receiver<CommandResponse>) -> Self {
        Self { clients, command_rx }
    }


    pub fn remove_client(&self, client_id: Uuid) {
        self.clients.remove(&client_id);
    }

    pub async fn send_response(&self, client_id: Uuid, response: Response) {
        if let Some(mut client) = self.clients.get_mut(&client_id) {
           if let Err(_) = client.send(response.to_bytes()).await {
            error!("Failed to send response to client {}", client_id);
            let _ =  client.close().await;
            self.remove_client(client_id);
           }
        }
    }

    pub async fn run(&mut self) -> Result<(), DataStoreError> {
        info!("DataStore started");
        while let Some(command) = self.command_rx.recv().await {
            match command {
                CommandResponse { client_id, response } => {
                    self.send_response(client_id, response).await;
                }
            }
        }
        Ok(())
    }
}