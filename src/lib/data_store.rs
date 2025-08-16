use std::collections::HashMap;
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

pub struct Client {
    socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
    kv:     DashMap<String, String>,
}

impl Client {
    pub fn new(socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>, kv: DashMap<String, String>) -> Self {
        Self {
            socket,
            kv,
        }
    }
}
pub struct DataStore {
    clients: &'static Lazy<DashMap<Uuid, Client>>,
    command_rx: Receiver<CommandResponse>,
}

#[derive(Debug, Error)]
pub enum DataStoreError {
    #[error("Send error occurred")]
    SendError(std::io::Error),
}

impl DataStore {
    pub fn new(
        clients: &'static Lazy<DashMap<Uuid, Client>>,
        command_rx: Receiver<CommandResponse>,
    ) -> Self {
        Self {
            clients,
            command_rx,
        }
    }

    pub fn remove_client(&self, client_id: Uuid) {
        self.clients.remove(&client_id);
    }


    pub async fn send_response(&self, client_id: Uuid, response: Response) -> Result<(), DataStoreError> {
        if let Some(mut client) = self.clients.get_mut(&client_id) {
            match response {
                Response::GET(key) => {
                    let kv = client.kv.clone();
                    info!("Getting key {} from client {} {:?}", key, client_id, kv);
                    let mut null = String::from("NULL");
                   if let Some(value) = client.kv.get(&key) {
                    null = value.clone();
                   }
                  if let Err(e) = client.socket.send(Response::GET(null).to_bytes()).await {
                    error!("Failed to send response to client {}", client_id);
                    self.remove_client(client_id);
                    return Err(DataStoreError::SendError(e));
                  }
                }

                Response::SET(key, value) => {
                    info!("Setting key {} to value {}", key, value);
                    client.kv.insert(key.clone(), value.clone());
                    info!("=======================>   {:?}",client.kv);
                    if let Err(e) = client.socket.send(Response::OK.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        self.remove_client(client_id);
                        return Err(DataStoreError::SendError(e));
                    }
                }

                _ => {
                    if let Err(e) = client.socket.send(response.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        self.remove_client(client_id);
                        return Err(DataStoreError::SendError(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), DataStoreError> {
        info!("DataStore started");
        while let Some(command) = self.command_rx.recv().await {
            self.send_response(command.client_id, command.response)
                .await;
        }
        Ok(())
    }
}
