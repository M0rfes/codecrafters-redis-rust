use bytes::BytesMut;
use thiserror::Error;
use uuid::Uuid;

use crate::parser;
pub enum Command {
    PING,
}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("Parse error occurred")]
    ParseError,

    #[error("Read Error")]
    ReadError,

    #[error("Send error occurred")]
    SendError(String),
}

impl TryFrom<BytesMut> for Command {
    type Error = ReaderError;

    fn try_from(value: BytesMut) -> Result<Self, Self::Error> {
       parser::RespCodec.parse_command(&mut std::io::Cursor::new(&value[..])).map_err(|_| ReaderError::ParseError)
    }
}


pub enum Response {
    PONG,
}

impl Response {
    pub fn to_bytes(&self) -> BytesMut {
        match self {
            Response::PONG => BytesMut::from("+PONG\r\n"),
        }
    }
}

pub struct CommandResponse {
    pub client_id: Uuid,
    pub response: Response,
}