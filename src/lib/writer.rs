use std::sync::Arc;

use bytes::BytesMut;
use chrono::Utc;
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

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("Send error occurred")]
    SendError(std::io::Error),
}


pub struct Writer {
    kv: &'static Lazy<(DashMap<String, String>,DashMap<String, u64>)>,
    command_tx: Receiver<CommandResponse>,
    socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
}

impl Writer {
    pub fn new(
        kv: &'static Lazy<(DashMap<String, String>,DashMap<String, u64>)>,
        command_tx: Receiver<CommandResponse>,
        socket: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
    ) -> Self {
        Self { kv, command_tx, socket }
    }

    pub async fn run(&mut self) -> Result<(), WriterError> {
        info!("Writer started");
        while let Some(command) = self.command_tx.recv().await {
            let _ = self.send_response(command.client_id, command.response).await?;
        }
        Ok(())
    }

    async fn send_response(
        &mut self,
        client_id: Uuid,
        response: Response,
    ) -> Result<(), WriterError> {
        match response {
             Response::GET(key) => {
                    let mut response = if let Some(value) = self.kv.0.get(&key) {
                        info!("Value: {:?}", value.value());
                        Response::GET(value.clone())
                    } else {
                        Response::NULL
                    };

                    let mut expired = false;
                    if let Response::GET(_) = response {
                        if let Some(expiry) = self.kv.1.get(&key) {
                            let now = Utc::now().timestamp_millis();
                            if now > (*expiry as i64) {
                                expired = true;
                                response = Response::NULL;
                            }
                        }
                    }

                    if expired {
                        self.kv.0.remove(&key);
                        self.kv.1.remove(&key);
                    }

                    if let Err(e) = self.socket.send(response.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        return Err(WriterError::SendError(e));
                    }
                }

                Response::SET(key, value, expiry) => {
                    self.kv.0.insert(key.clone(), value.clone());
                    if let Some(expiry) = expiry {
                        let now = Utc::now().timestamp_millis();
                        let expiry = expiry as i64 + now;
                        info!("Setting expiry for key {} to {}", key, expiry);
                        self.kv.1.insert(key.clone(), expiry as u64);
                    }
                    if let Err(e) = self.socket.send(Response::OK.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        return Err(WriterError::SendError(e));
                    }
                }

                _ => {
                    if let Err(e) = self.socket.send(response.to_bytes()).await {
                        error!("Failed to send response to client {}", client_id);
                        return Err(WriterError::SendError(e));
                    }
                }
        }
        Ok(())
    }
}