use std::convert::TryInto;
use std::io::{Error, ErrorKind};

use bytes::{BufMut, BytesMut};
use protobuf::{parse_from_bytes, Message};
use tokio::codec::{Decoder, Encoder};

use near_protos::network::PeerMessage as ProtoMessage;

use crate::types::PeerMessage;

pub struct Codec {
    max_length: u32,
}

impl Codec {
    pub fn new() -> Self {
        Codec { max_length: std::u32::MAX }
    }
}

impl Encoder for Codec {
    type Item = Vec<u8>;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Error> {
        if item.len() > self.max_length as usize {
            Err(Error::new(ErrorKind::InvalidInput, "Input is too long"))
        } else {
            // First four bytes is the length of the buffer.
            buf.reserve(item.len() + 4);
            buf.put_u32_le(item.len() as u32);
            buf.put(item);
            Ok(())
        }
    }
}

impl Decoder for Codec {
    type Item = Vec<u8>;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>, Error> {
        if buf.len() < 4 {
            // not enough bytes to start decoding
            return Ok(None);
        }
        let mut len_bytes: [u8; 4] = [0; 4];
        len_bytes.copy_from_slice(&buf[0..4]);
        let len = unsafe { std::mem::transmute::<[u8; 4], u32>(len_bytes) }.to_le();
        if buf.len() < 4 + len as usize {
            // not enough bytes, keep waiting
            Ok(None)
        } else {
            let res = Some(buf[4..4 + len as usize].to_vec());
            buf.advance(4 + len as usize);
            Ok(res)
        }
    }
}

pub fn peer_message_to_bytes(
    peer_message: PeerMessage,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let proto: ProtoMessage = peer_message.into();
    proto.write_to_bytes().map_err(|err| err.into())
}

pub fn bytes_to_peer_message(bytes: &[u8]) -> Result<PeerMessage, Box<dyn std::error::Error>> {
    let proto: ProtoMessage = parse_from_bytes(bytes)?;
    proto.try_into()
}

#[cfg(test)]
mod test {
    use crate::types::{Handshake, PeerChainInfo, PeerInfo};

    use super::*;

    fn test_codec(msg: PeerMessage) {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        codec.encode(peer_message_to_bytes(msg.clone()).unwrap(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();
        assert_eq!(bytes_to_peer_message(&decoded).unwrap(), msg);
    }

    #[test]
    fn test_peer_message_handshake() {
        let peer_info = PeerInfo::random();
        let fake_handshake = Handshake {
            version: 1,
            peer_id: peer_info.id,
            listen_port: None,
            chain_info: PeerChainInfo {
                genesis: Default::default(),
                height: 0,
                total_weight: 0.into(),
            },
        };
        let msg = PeerMessage::Handshake(fake_handshake);
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_info_gossip() {
        let peer_info1 = PeerInfo::random();
        let peer_info2 = PeerInfo::random();
        let msg = PeerMessage::PeersResponse(vec![peer_info1, peer_info2]);
        test_codec(msg);
    }
}
