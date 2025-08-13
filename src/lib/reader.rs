use bytes::{Buf, BytesMut};
use futures::{stream::SplitStream, Stream, StreamExt};
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::mpsc::{error::SendError, Sender},
};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info};

use crate::command::{Command, ReaderError, Response};

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = std::io::Cursor::new(&src[..]);
        let start_pos = cursor.position() as usize;

        // Try to parse a complete RESP message
        if let Ok(Some(end_pos)) = self.parse_resp(&mut cursor) {
            let data = src.split_to(end_pos);
            return Ok(Some(data));
        }

        Ok(None)
    }
}

impl Encoder<BytesMut> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}

impl RespCodec {
    fn parse_resp(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<usize>, std::io::Error> {
        let start_pos = cursor.position() as usize;
        
        if cursor.remaining() == 0 {
            return Ok(None);
        }

        let byte = cursor.get_u8();
        match byte {
            b'*' => self.parse_array(cursor, start_pos),
            b'$' => self.parse_bulk_string(cursor, start_pos),
            b'+' => self.parse_simple_string(cursor, start_pos),
            b'-' => self.parse_error(cursor, start_pos),
            b':' => self.parse_integer(cursor, start_pos),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid RESP type")),
        }
    }

    fn parse_array(&self, cursor: &mut std::io::Cursor<&[u8]>, start_pos: usize) -> Result<Option<usize>, std::io::Error> {
        let count = self.read_until_crlf(cursor)?;
        let count: i64 = count.parse().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid array count")
        })?;

        if count < 0 {
            return Ok(Some(cursor.position() as usize));
        }

        for _ in 0..count {
            if self.parse_resp(cursor)?.is_none() {
                return Ok(None);
            }
        }

        Ok(Some(cursor.position() as usize))
    }

    fn parse_bulk_string(&self, cursor: &mut std::io::Cursor<&[u8]>, _start_pos: usize) -> Result<Option<usize>, std::io::Error> {
        let length = self.read_until_crlf(cursor)?;
        let length: i64 = length.parse().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid bulk string length")
        })?;

        if length < 0 {
            return Ok(Some(cursor.position() as usize));
        }

        if cursor.remaining() < (length + 2) as usize {
            return Ok(None);
        }

        cursor.advance(length as usize + 2); // +2 for \r\n
        Ok(Some(cursor.position() as usize))
    }

    fn parse_simple_string(&self, cursor: &mut std::io::Cursor<&[u8]>, _start_pos: usize) -> Result<Option<usize>, std::io::Error> {
        self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }

    fn parse_error(&self, cursor: &mut std::io::Cursor<&[u8]>, _start_pos: usize) -> Result<Option<usize>, std::io::Error> {
        self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }

    fn parse_integer(&self, cursor: &mut std::io::Cursor<&[u8]>, _start_pos: usize) -> Result<Option<usize>, std::io::Error> {
        self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }

    fn read_until_crlf(&self, cursor: &mut std::io::Cursor<&[u8]>) -> Result<String, std::io::Error> {
        let mut result = String::new();
        let mut found_cr = false;

        while cursor.remaining() > 0 {
            let byte = cursor.get_u8();
            if byte == b'\r' {
                found_cr = true;
            } else if byte == b'\n' && found_cr {
                break;
            } else {
                if found_cr {
                    result.push('\r');
                    found_cr = false;
                }
                result.push(byte as char);
            }
        }

        if !found_cr {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"));
        }

        Ok(result)
    }
}

pub struct Reader {
    stream: SplitStream<Framed<TcpStream, RespCodec>>,
    command_tx: Sender<Response>,
}

impl Reader {
    pub fn new(stream: SplitStream<Framed<TcpStream, RespCodec>>, command_tx: Sender<Response>) -> Self {
        Self { stream, command_tx }
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
                self.command_tx.send(Response::PONG).await.map_err(|e| ReaderError::SendError(e.to_string()))?;
            }
        }
        Ok(())
    }

}