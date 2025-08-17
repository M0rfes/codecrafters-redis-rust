use bytes::BytesMut;
use futures::{stream::SplitStream, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc::Sender,
};
use tokio_util::codec::Framed;
use tracing::debug;
use uuid::Uuid;
use crate::{command::{Command, CommandResponse, ReaderError, Response}, parser::RespCodec};

pub struct Reader {
    stream: SplitStream<Framed<TcpStream, RespCodec>>,
    command_tx: Sender<CommandResponse>,
    client_id: Uuid,
}

impl Reader {
    pub fn new(stream: SplitStream<Framed<TcpStream, RespCodec>>, command_tx: Sender<CommandResponse>, client_id: Uuid) -> Self {
        Self { stream, command_tx, client_id }
    }

     pub async fn run(mut self) -> Result<(), ReaderError> {
        debug!("Reader started");
        while let Some(msg) = self.stream.next().await {
            match msg {
                Ok(msg) => {
                    self.process_message(msg).await?;
                }
                Err(e) => {
                    eprintln!("Failed to read from socket: {}", e);
                    return Err(ReaderError::ReadError);
                }
            }
        }
        Ok(())
    }

    async fn process_message(&self, msg: BytesMut) -> Result<(), ReaderError> {
       match Command::try_from(msg) {
        Ok(command) =>  match command {
            Command::PING => {
                debug!("Received PING command");
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::PONG }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
            Command::ECHO(s) => {
                debug!("Received ECHO command: {}", s);
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::ECHO(s) }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
            Command::SET(key, value, expiry) => {
                debug!("Received SET command: {} = {}", key, value);
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::SET(key, value, expiry) }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
            Command::GET(key) => {
                debug!("Received GET command: {}", key);
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::GET(key) }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
            Command::DOCS => {
                debug!("Received DOCS command");
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::OK }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
           
        }
        Err(e) => {
            debug!("Received ERROR command: {}", e);
            self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::ERROR(e.to_string()) }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
        }
       }
        Ok(())
    }

}