use tokio::codec::{Encoder, Decoder};
use bytes::{BytesMut, BufMut};
use std::io::{Error, ErrorKind};
use std::convert::TryInto;
use primitives::network::PeerMessage;
use near_protos::network::PeerMessage as ProtoMessage;
use protobuf::{ProtobufError, parse_from_bytes, Message};

// we could write our custom error type. For now we just 
// use io::Error
fn convert_protobuf_error(err: ProtobufError) -> Error {
    match err {
        ProtobufError::IoError(e) => e,
        ProtobufError::MessageNotInitialized { message } => Error::new(
            ErrorKind::InvalidInput,
            format!("protobuf not initialized: {}", message)
        ),
        ProtobufError::Utf8(e) => Error::new(
            ErrorKind::InvalidInput,
            format!("Utf8 error: {}", e)
        ),
        ProtobufError::WireError(e) => Error::new(
            ErrorKind::InvalidInput,
            format!("WireError: {:?}", e)
        )
    }
}

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
    type Item = PeerMessage;
    type Error = Error;

     fn encode(&mut self, item: PeerMessage, buf: &mut BytesMut) -> Result<(), Error> {
        let proto: ProtoMessage = item.into();
        let bytes = proto.write_to_bytes().map_err(convert_protobuf_error)?;
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
    type Item = PeerMessage;
    type Error = Error;

     fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<PeerMessage>, Error> {
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
            let res: ProtoMessage = parse_from_bytes(&buf[4..4 + len as usize]).map_err(convert_protobuf_error)?;
            buf.advance(4 + len as usize);
            res.try_into().map_err(|e| Error::new(ErrorKind::InvalidData, e)).map(Some)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use primitives::network::{Handshake, PeerInfo, ConnectedInfo};
    use primitives::chain::ChainState;
    use primitives::types::PeerId;
    use primitives::hash::{CryptoHash, hash_struct};

    fn test_codec(msg: PeerMessage) {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        codec.encode(msg.clone(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_peer_message_handshake() {
        let peer_info = PeerInfo {
            id: PeerId::default(),
            addr: None,
            account_id: None,
        };
        let connected_info = ConnectedInfo {
            chain_state: ChainState {
                genesis_hash: CryptoHash::default(),
                last_index: 0,
            }
        };
        let fake_handshake = Handshake {
            version: 1,
            peer_id: PeerId::default(),
            account_id: Some("alice.near".to_string()),
            listen_port: None,
            peers_info: vec![peer_info],
            connected_info,
        };
        let msg = PeerMessage::Handshake(fake_handshake);
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_info_gossip() {
        let peer_info1 =  PeerInfo{
            id: hash_struct(&1),
            addr: Some("127.0.0.1:3000".parse().unwrap()),
            account_id: Some("test1.near".to_string()),
        };
        let peer_info2 = PeerInfo {
            id: hash_struct(&2),
            addr: Some("127.0.0.1:3001".parse().unwrap()),
            account_id: Some("test2.near".to_string())
        };
        let msg = PeerMessage::InfoGossip(vec![peer_info1, peer_info2]);
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_message() {
        let msg = PeerMessage::Message(b"hello, world!".to_vec());
        test_codec(msg);
    }
}
