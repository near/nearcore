use ::tokio_codec::{Encoder, Decoder};
use ::bytes::{BytesMut, BufMut, Bytes};
use std::io::{Error, ErrorKind};
use ::primitives::traits::{Encode, Decode};
use super::ServiceEvent;

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
    type Item = ServiceEvent;
    type Error = Error;

    fn encode(&mut self, item: ServiceEvent, buf: &mut BytesMut) -> Result<(), Error> {
        let bytes = Bytes::from(
            item.encode().ok_or_else(|| Error::new(ErrorKind::InvalidInput, "cannot encode event"))?
        );
        // first four bytes is the length of the buffer
        buf.reserve(bytes.len() + 4);
        if bytes.len() > self.max_length as usize {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "Input is too long"
            ))
        } else {
            buf.put_u32_le(bytes.len() as u32);
            buf.put(bytes);
            Ok(())
        }
    }
}

impl Decoder for Codec {
    type Item = ServiceEvent;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ServiceEvent>, Error> {
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
            let event = Decode::decode(&buf[4..4 + len as usize])
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "cannot decode event"))?;
            buf.advance(4 + len as usize);
            Ok(Some(event))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::primitives::hash::hash_struct;

    #[test]
    fn test_codec_simple() {
        let mut codec = Codec::new();
        let event = ServiceEvent::Message {
            peer_id: hash_struct(&0),
            data: b"hello, world".to_vec()
        };
        let mut buffer = BytesMut::new();
        codec.encode(event.clone(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();
        assert_eq!(decoded, event);
    }
}