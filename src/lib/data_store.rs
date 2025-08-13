use bytes::BytesMut;
use futures::{stream::SplitSink, SinkExt};
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio_util::codec::Framed;
use tracing::{debug, error, info};

use crate::command::Response;
use crate::reader::RespCodec;

pub struct DataStore {
    command_rx: Receiver<Response>,
    writer_sink: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>,
}

#[derive(Debug, Error)]
pub enum DataStoreError {
    #[error("Send error occurred")]
    SendError(String),
}

impl DataStore {
    pub fn new(command_rx: Receiver<Response>, writer_sink: SplitSink<Framed<TcpStream, RespCodec>, BytesMut>) -> Self {
        Self { command_rx, writer_sink }
    }

    pub async fn run(mut self) -> Result<(), DataStoreError> {
        info!("DataStore started");
        while let Some(command) = self.command_rx.recv().await {
            match command {
                Response::PONG => {
                    info!("Sending PONG response");
                    self.writer_sink.send(BytesMut::from("+PONG\r\n")).await.map_err(|e| DataStoreError::SendError(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

}