use ::tokio_codec::{Encoder, Decoder};
use ::bytes::{BytesMut, BufMut, Bytes};
use std::io::{Error, ErrorKind};

pub struct Codec {
    max_length: u32
}

impl Codec {
    pub fn new() -> Self {
        Codec {
            max_length: std::u32::MAX
        }
    }
}

impl Encoder for Codec {
    type Item = Bytes;
    type Error = Error;

    fn encode(&mut self, item: Bytes, buf: &mut BytesMut) -> Result<(), Error> {
        // first four bytes is the length of the buffer
        buf.reserve(item.len() + 4);
        if item.len() > self.max_length as usize {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "Input is too long"
            ))
        } else {
            buf.put_u32_le(item.len() as u32);
            buf.put(item);
            Ok(())
        }
    }
}

impl Decoder for Codec {
    type Item = Bytes;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, Error> {
        if buf.len() < 4 {
            // not enough bytes to start decoding
            return Ok(None)
        }
        let mut len_bytes: [u8; 4] = [0; 4];
        len_bytes.copy_from_slice(&buf[0..4]);
        let len = unsafe {
            std::mem::transmute::<[u8; 4], u32>(len_bytes)
        }.to_le();
        if buf.len() < 4 + len as usize {
            // not enough bytes, keep waiting
            Ok(None)
        } else {
            let res = Bytes::from(&buf[4..4 + len as usize]);
            buf.advance(4 + len as usize);
            Ok(Some(res))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_simple() {
        let mut codec = Codec::new();
        let msg = b"hello, world!".to_vec();
        let mut buffer = BytesMut::new();
        codec.encode(msg.clone().into(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();
        assert_eq!(decoded.to_vec(), msg);
    }
}