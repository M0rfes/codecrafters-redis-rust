use bytes::{Buf, BytesMut};
use thiserror::Error;
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
        let mut cursor = std::io::Cursor::new(&value[..]);
        
        // Parse RESP array
        let byte = cursor.get_u8();
        if byte != b'*' {
            return Err(ReaderError::ParseError);
        }
        
        // Read array count
        let count = read_until_crlf(&mut cursor)?;
        let count: i64 = count.parse().map_err(|_| ReaderError::ParseError)?;
        
        if count != 1 {
            return Err(ReaderError::ParseError);
        }
        
        // Parse bulk string
        let byte = cursor.get_u8();
        if byte != b'$' {
            return Err(ReaderError::ParseError);
        }
        
        // Read string length
        let length = read_until_crlf(&mut cursor)?;
        let length: usize = length.parse().map_err(|_| ReaderError::ParseError)?;
        
        // Read the actual command string
        let mut command_bytes = vec![0u8; length];
        cursor.copy_to_slice(&mut command_bytes);
        
        // Skip \r\n
        if cursor.remaining() < 2 {
            return Err(ReaderError::ParseError);
        }
        cursor.advance(2);
        
        let command_str = String::from_utf8(command_bytes).map_err(|_| ReaderError::ParseError)?;
        
        match command_str.as_str() {
            "PING" => Ok(Command::PING),
            _ => Err(ReaderError::ParseError),
        }
    }
}

fn read_until_crlf(cursor: &mut std::io::Cursor<&[u8]>) -> Result<String, ReaderError> {
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
        return Err(ReaderError::ParseError);
    }

    Ok(result)
}

pub enum Response {
    PONG,
}