use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use chrono::{TimeDelta, Utc};
use dashmap::DashMap;
use futures::{stream::SplitSink, SinkExt};
use once_cell::sync::Lazy;
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio_util::codec::Framed;
use tracing::{error, info};
use uuid::Uuid;

use crate::command::{CommandResponse, Response};
use crate::parser::RespCodec;

pub struct Client {
    socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
    kv: Arc<(DashMap<String, String>,DashMap<String, u64>)>,
}

impl Client {
    pub fn new(
        socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
        kv: Arc<(DashMap<String, String>,DashMap<String, u64>)>,
    ) -> Self {
        Self { socket, kv }
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

    pub async fn send_response(
        &self,
        client_id: Uuid,
        response: Response,
    ) -> Result<(), DataStoreError> {
        if let Some(mut client) = self.clients.get_mut(&client_id) {
            match response {
                Response::GET(key) => {
                    let mut response = if let Some(value) = client.kv.0.get(&key) {
                        info!("Value: {:?}", value.value());
                        Response::GET(value.clone())
                    } else {
                        Response::NULL
                    };

                    let mut expired = false;
                    if let Response::GET(_) = response {
                        if let Some(expiry) = client.kv.1.get(&key) {
                            let now = Utc::now().timestamp_millis();
                            if now > (*expiry as i64) {
                                expired = true;
                                response = Response::NULL;
                            }
                        }
                    }

                    if expired {
                        client.kv.0.remove(&key);
                        client.kv.1.remove(&key);
                    }

                    if let Err(e) = client.socket.send(response.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        self.remove_client(client_id);
                        return Err(DataStoreError::SendError(e));
                    }
                }

                Response::SET(key, value, expiry) => {
                    client.kv.0.insert(key.clone(), value.clone());
                    if let Some(expiry) = expiry {
                        let now = Utc::now().timestamp_millis();
                        let expiry = expiry as i64 + now;
                        info!("Setting expiry for key {} to {}", key, expiry);
                        client.kv.1.insert(key.clone(), expiry as u64);
                    }
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

        // let mut gc_interval = tokio::time::interval(Duration::from_secs(1));
        // let clients = self.clients;
        // let _ = tokio::spawn(async move {
        //     info!("GC started");
        //     loop {
        //         gc(&clients).await;
        //         gc_interval.tick().await;
        //     }
        // }); 
        
        while let Some(command) = self.command_rx.recv().await {
            let _ = self
                .send_response(command.client_id, command.response)
                .await;
        }
        Ok(())
    }

}
// async fn gc(clients: &'static Lazy<DashMap<Uuid, Client>>) {
//     for entry in clients.iter() {
//         let keys_to_remove = entry
//             .value()
//             .kv
//             .1
//             .iter()
//             .filter_map(|entry| {
//                 let now = Utc::now().timestamp_millis();
//                 if now > (*entry.value() as i64) {
//                     Some(entry.key().clone())
//                 } else {
//                     None
//                 }
//             })
//             .collect::<Vec<_>>();

//         info!("Keys to remove: {:?}", keys_to_remove);

//         for key in keys_to_remove {
//             entry.value().kv.0.remove(&key);
//             entry.value().kv.1.remove(&key);
//         }
//     }
// }
