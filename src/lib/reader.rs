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
        let command = Command::try_from(msg)?;
        match command {
            Command::PING => {
                debug!("Received PING command");
                self.command_tx.send(CommandResponse { client_id: self.client_id, response: Response::PONG }).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
        }
        Ok(())
    }

}