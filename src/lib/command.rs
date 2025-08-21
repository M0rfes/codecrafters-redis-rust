use std::sync::Arc;

use bytes::{Buf, BytesMut};
use thiserror::Error;
use tracing::info;

pub enum Command {
    PING,
    ECHO(Arc<str>),
    SET(Arc<str>, Arc<str>,Option<u64>),
    GET(Arc<str>),
    RPUSH(Arc<str>, Vec<Box<str>>),
    DOCS,

}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("Parse error occurred: {0}")]
    ParseError(std::io::Error),

    #[error("Read Error")]
    ReadError,

    #[error("Send error occurred")]
    SendError(String),

    #[error("Invalid expiry")]
    InvalidExpiry(String),

    #[error("Invalid command")]
    InvalidCommand(String),
}

impl TryFrom<BytesMut> for Command {
    type Error = ReaderError;

    fn try_from(value: BytesMut) -> Result<Self, Self::Error> {
        let mut parser = CommandParser { cursor: std::io::Cursor::new(&value[..]) };
        parser.parse_command()
    }
}

struct CommandParser<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
}
impl<'a> CommandParser<'a> {
        pub fn parse_command(&mut self) -> Result<Command, ReaderError> {
        if self.cursor.remaining() == 0 {
            return Err(ReaderError::ReadError);
        }

        let mut commands = self.parse().map_err(|_| ReaderError::ReadError)?;
        info!("Commands: {:?}", commands);
        match commands.remove(0).as_ref() {
            "ping"  => Ok(Command::PING),
            "echo"  => {
                let echo = commands.remove(0);
                Ok(Command::ECHO(echo.into()))
            },
            "set"  => {
                if commands.len() == 4 && (commands[2].as_ref() == "px") {
                    info!("SET with TTL: {:?}", commands);
                    let key = commands.remove(0);
                    let value = commands.remove(0);
                    let ttl = commands.remove(1);
                    info!("SET with TTL: {:?}, {:?}, {:?}", key, value, ttl);
                    Ok(Command::SET(key.into(), value.into(), Some(ttl.parse().map_err(|_| ReaderError::InvalidExpiry(ttl.to_string()))?)))
                } else {
                    let key = commands.remove(0);
                    let value = commands.remove(0);
                    Ok(Command::SET(key.into(), value.into(), None))
                }
            },
            "get" => {
                let key = commands.remove(0);
                Ok(Command::GET(key.into()))
            },
            "rpush" => {
                let key = commands.remove(0);
                let value = commands;
                Ok(Command::RPUSH(key.into(), value))
            },
            "command"  => Ok(Command::DOCS),
            command => {
                info!("Invalid command: {:?}", command);
                Err(ReaderError::InvalidCommand(commands.join(" ")))},
        }
    }

    // "*1\r\n$4\r\nPING\r\n"
    // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n.
    fn parse(&mut self) -> Result<Vec<Box<str>>, std::io::Error> {
        let byte = self.cursor.get_u8();
        if byte == b'*' {
            return self.parse_array();
        } else if byte == b'$' {
            return self.parse_bulk_string()
            .map(|s| vec![s]);
        } else if byte == b'+' {
            return self.parse_simple_string()
            .map(|s| vec![s]);
        } else if byte == b'-' {
            return self.parse_error()
            .map(|s| vec![s]);
        } else if byte == b':' {
            return self.parse_integer()
            .map(|s| vec![s]);
        } else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid command"));
        }
    }

    fn parse_array(&mut self) -> Result<Vec<Box<str>>, std::io::Error> {
        let count = self.get_string()?;
        let count: i64 = count.parse().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid array count")
        })?;
        
        if count < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid array count"));
        }
        let mut result = Vec::new();
        for _ in 0..count {
            let commands = self.parse()?;
            result.extend(commands);
        }

        Ok(result)
    }

    fn parse_bulk_string(&mut self) -> Result<Box<str>, std::io::Error> {
        let length = self.get_string()?;
        let length: i64 = length.parse().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid bulk string length")
        })?;
        
        if length < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid bulk string length"));
        }
        let mut result = String::new();
        for _ in 0..length {
            let byte = self.cursor.get_u8();
            result.push(byte as char);
        }
        self.cursor.advance( 2); // consume the \r\n
        Ok(result.to_lowercase().into())
    }

    fn parse_simple_string(&mut self) -> Result<Box<str>, std::io::Error> {
        self.get_string()
    }

    fn parse_error(&mut self) -> Result<Box<str>, std::io::Error> {
        self.get_string()
    }

    fn parse_integer(&mut self) -> Result<Box<str>, std::io::Error> {
        self.get_string()
    }

    fn get_string(&mut self) -> Result<Box<str>, std::io::Error> {
        let mut result = String::new();
        while self.cursor.remaining() > 0 {
            let first_byte = self.cursor.get_u8();
            let second_byte = self.cursor.get_u8();
            if first_byte == b'\r' && second_byte == b'\n' {
                return Ok(result.to_lowercase().into());
            } else {
                result.push(first_byte as char);
                if second_byte == b'\r' {
                    self.cursor.set_position(self.cursor.position() - 1);
                } else {
                    result.push(second_byte as char);
                }
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"))
    }
}


pub enum Response {
    PONG,
    ECHO(Arc<str>),
    OK,
    GET(Arc<str>),
    LEN(usize),
    ERROR(Arc<str>),
    NULL,
}

impl Response {
    pub fn to_bytes(self) -> BytesMut {
        match self {
            Response::PONG => BytesMut::from("+PONG\r\n"),
            Response::ECHO(s) => BytesMut::from(format!("${}\r\n{}\r\n", s.len(), s).as_str()),
            Response::OK  => BytesMut::from("+OK\r\n"),
            Response::GET(s) => BytesMut::from(format!("${}\r\n{}\r\n", s.len(), s).as_str()),
            Response::ERROR(s) => BytesMut::from(format!("${}\r\n{}\r\n", s.len(), s).as_str()),
            Response::NULL => BytesMut::from("$-1\r\n"),
            Response::LEN(len) => BytesMut::from(format!(":{}\r\n", len).as_str()),
        }
    }
}

