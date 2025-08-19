use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = std::io::Cursor::new(&src[..]);

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
    pub fn parse_resp(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {

        if cursor.remaining() == 0 {
            return Ok(None);
        }

        let byte = cursor.get_u8();
        match byte {
            b'*' => self.parse_array(cursor),
            b'$' => self.parse_bulk_string(cursor),
            b'+' => self.parse_simple_string(cursor),
            b'-' => self.parse_error(cursor),
            b':' => self.parse_integer(cursor),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid RESP type",
            )),
        }
    }
}

impl RespCodec {
    fn parse_array(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {
        let count = self.get_string(cursor)?;
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

    fn parse_bulk_string(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {
        let length = self.get_string(cursor)?;
        let length: i64 = length.parse().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid bulk string length",
            )
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

    fn parse_simple_string(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {
        self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }

    fn parse_error(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {
            self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }

    fn parse_integer(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> Result<Option<usize>, std::io::Error> {
        self.read_until_crlf(cursor)?;
        Ok(Some(cursor.position() as usize))
    }


    fn get_string(&self,cursor: &mut std::io::Cursor<&[u8]>) -> Result<Box<str>, std::io::Error> {
        let mut result = String::new();
        while cursor.remaining() > 0 {
            let first_byte = cursor.get_u8();
            let second_byte = cursor.get_u8();
            if first_byte == b'\r' && second_byte == b'\n' {
                return Ok(result.into());
            } else {
                result.push(first_byte as char);
                if second_byte == b'\r' {
                    // pull the cursor back by 1 to make first_byte the \r
                    // and check if the next byte is \n
                    cursor.set_position(cursor.position() - 1);
                } else {
                    result.push(second_byte as char);
                }
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"))
    }

    fn read_until_crlf(&self,cursor: &mut std::io::Cursor<&[u8]>) -> Result<(), std::io::Error> {
        while cursor.remaining() > 0 {
            let first_byte = cursor.get_u8();
            let second_byte = cursor.get_u8();
            if first_byte == b'\r' && second_byte == b'\n' {
                return Ok(());
            } else {
                if second_byte == b'\r' {
                    // pull the cursor back by 1 to make first_byte the \r
                    // and check if the next byte is \n
                    cursor.set_position(cursor.position() - 1);
                }
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Missing CRLF"))
    }
}
