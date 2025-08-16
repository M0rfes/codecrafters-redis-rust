use bytes::{Buf, BytesMut};
use thiserror::Error;
use uuid::Uuid;

pub enum Command {
    PING,
    ECHO(String),
}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("Parse error occurred: {0}")]
    ParseError(std::io::Error),

    #[error("Read Error")]
    ReadError,

    #[error("Send error occurred")]
    SendError(String),
}

impl TryFrom<BytesMut> for Command {
    type Error = ReaderError;

    fn try_from(value: BytesMut) -> Result<Self, Self::Error> {
        let mut parser = CommandParser { cursor: std::io::Cursor::new(&value[..]) };
        parser.parse_command().map_err(|err| ReaderError::ParseError(err))
    }
}

struct CommandParser<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
}
impl<'a> CommandParser<'a> {
        pub fn parse_command(&mut self) -> Result<Command, std::io::Error> {
        if self.cursor.remaining() == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"));
        }

        let commands = self.parse()?;
        match commands[0].as_str() {
            "PING" => Ok(Command::PING),
            "ECHO" => Ok(Command::ECHO(commands[1].clone())),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid command")),
        }
    }

    // "*1\r\n$4\r\nPING\r\n"
    // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n.
    fn parse(&mut self) -> Result<Vec<String>, std::io::Error> {
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

    fn parse_array(&mut self) -> Result<Vec<String>, std::io::Error> {
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

    fn parse_bulk_string(&mut self) -> Result<String, std::io::Error> {
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
        Ok(result)
    }

    fn parse_simple_string(&mut self) -> Result<String, std::io::Error> {
        self.get_string()
    }

    fn parse_error(&mut self) -> Result<String, std::io::Error> {
        self.get_string()
    }

    fn parse_integer(&mut self) -> Result<String, std::io::Error> {
        self.get_string()
    }

    fn get_string(&mut self) -> Result<String, std::io::Error> {
        let mut result = String::new();
        while self.cursor.remaining() > 0 {
            let first_byte = self.cursor.get_u8();
            let second_byte = self.cursor.get_u8();
            if first_byte == b'\r' && second_byte == b'\n' {
                return Ok(result);
            } else {
                result.push(first_byte as char);
                if second_byte == b'\r' {
                    self.cursor.set_position(self.cursor.position() - 1);
                }
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"))
    }
}


pub enum Response {
    PONG,
    ECHO(String),
}

impl Response {
    pub fn to_bytes(&self) -> BytesMut {
        match self {
            Response::PONG => BytesMut::from("+PONG\r\n"),
            Response::ECHO(s) => BytesMut::from(format!("${}\r\n{}\r\n", s.len(), s).as_str()),
        }
    }
}

pub struct CommandResponse {
    pub client_id: Uuid,
    pub response: Response,
}