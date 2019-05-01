use std::io;

use bytes::{BufMut, BytesMut};
use tokio::codec::{Decoder, Encoder};

/// Implements a simple single line codec.
///
/// Terminates by '\n' character.
pub struct LineCodec;

impl Encoder for LineCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn encode(&mut self, bytes: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(bytes.len() + 1);
        dst.extend(bytes);
        dst.put_u8(b'\n');
        Ok(())
    }
}

impl Decoder for LineCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(pos) = src.iter().position(|&x| x == b'\n') {
            // Split the message at the first new line.
            let mut msg = src.split_to(pos + 1);
            // Strip the new line at the end before returning.
            msg.split_off(pos);
            return Ok(Some(msg));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let mut empty = BytesMut::from(vec![]);
        let mut codec = LineCodec;
        assert_eq!(None, codec.decode(&mut empty).unwrap());
    }

    #[test]
    fn test_no_newline() {
        let mut input = BytesMut::from(b"abcd".to_vec());
        let original = input.clone();
        let mut codec = LineCodec;
        // No newline, no output.
        assert_eq!(None, codec.decode(&mut input).unwrap());
        // Original input is unchanged.
        assert_eq!(original, input);
    }

    #[test]
    fn test_combination() {
        let mut newlines = BytesMut::from(b"xyz\n\n\nabc\n".to_vec());
        let empty = BytesMut::from(vec![]);
        let mut codec = LineCodec;
        assert_eq!(Some(BytesMut::from(b"xyz".to_vec())), codec.decode(&mut newlines).unwrap());
        assert_eq!(Some(empty.clone()), codec.decode(&mut newlines).unwrap());
        assert_eq!(Some(empty.clone()), codec.decode(&mut newlines).unwrap());
        assert_eq!(Some(BytesMut::from(b"abc".to_vec())), codec.decode(&mut newlines).unwrap());
        assert_eq!(newlines.len(), 0);
        assert_eq!(None, codec.decode(&mut newlines).unwrap());
    }

    #[test]
    fn test_encode_decode() {
        let mut input = BytesMut::from(b"xyz\nabc\n".to_vec());
        let original = input.clone();
        let mut codec = LineCodec;

        let mut decoded = vec![];
        while let Some(x) = codec.decode(&mut input).unwrap() {
            decoded.push(x);
        }

        let mut buf = BytesMut::from(vec![]);
        for msg in decoded {
            codec.encode(msg, &mut buf).unwrap();
        }
        assert_eq!(original, buf);
    }
}
