use std::io::{Error, ErrorKind};

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::types::{PeerMessage, ReasonForBan};

const NETWORK_MESSAGE_MAX_SIZE: u32 = 512 << 20; // 512MB

pub struct Codec {
    max_length: u32,
}

#[allow(clippy::new_without_default)]
impl Codec {
    pub fn new() -> Self {
        Codec { max_length: NETWORK_MESSAGE_MAX_SIZE }
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
            buf.put(&item[..]);
            Ok(())
        }
    }
}

impl Decoder for Codec {
    type Item = Result<Vec<u8>, ReasonForBan>;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < 4 {
            // not enough bytes to start decoding
            return Ok(None);
        }

        let mut len_bytes: [u8; 4] = [0; 4];
        len_bytes.copy_from_slice(&buf[0..4]);
        let len = u32::from_le_bytes(len_bytes);

        if len > self.max_length {
            // If this point is reached, abusive peer is banned.
            return Ok(Some(Err(ReasonForBan::Abusive)));
        }

        if buf.len() < 4 + len as usize {
            // not enough bytes, keep waiting
            Ok(None)
        } else {
            let res = Some(Ok(buf[4..4 + len as usize].to_vec()));
            buf.advance(4 + len as usize);
            Ok(res)
        }
    }
}

pub fn peer_message_to_bytes(peer_message: PeerMessage) -> Result<Vec<u8>, std::io::Error> {
    peer_message.try_to_vec()
}

pub fn bytes_to_peer_message(bytes: &[u8]) -> Result<PeerMessage, std::io::Error> {
    PeerMessage::try_from_slice(bytes)
}

#[cfg(test)]
mod test {
    use near_crypto::{KeyType, SecretKey};
    use near_primitives::block::{Approval, ApprovalInner};
    use near_primitives::hash::CryptoHash;
    use near_primitives::network::AnnounceAccount;
    use near_primitives::types::EpochId;

    use crate::routing::EdgeInfo;
    use crate::types::{
        Handshake, PeerChainInfo, PeerIdOrHash, PeerInfo, RoutedMessage, RoutedMessageBody,
        SyncData,
    };

    use super::*;

    fn test_codec(msg: PeerMessage) {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        codec.encode(peer_message_to_bytes(msg.clone()).unwrap(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap().unwrap();
        assert_eq!(bytes_to_peer_message(&decoded).unwrap(), msg);
    }

    #[test]
    fn test_peer_message_handshake() {
        let peer_info = PeerInfo::random();
        let fake_handshake = Handshake {
            version: 1,
            oldest_supported_version: 1,
            peer_id: peer_info.id.clone(),
            target_peer_id: peer_info.id,
            listen_port: None,
            chain_info: PeerChainInfo {
                genesis_id: Default::default(),
                height: 0,
                tracked_shards: vec![],
            },
            edge_info: EdgeInfo::default(),
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

    #[test]
    fn test_peer_message_announce_account() {
        let sk = SecretKey::from_random(KeyType::ED25519);
        let network_sk = SecretKey::from_random(KeyType::ED25519);
        let signature = sk.sign(vec![].as_slice());
        let msg = PeerMessage::RoutingTableSync(SyncData {
            edges: Vec::new(),
            accounts: vec![AnnounceAccount {
                account_id: "test1".to_string(),
                peer_id: network_sk.public_key().into(),
                epoch_id: EpochId::default(),
                signature,
            }],
        });
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_announce_routed_block_approval() {
        let sk = SecretKey::from_random(KeyType::ED25519);
        let hash = CryptoHash::default();
        let signature = sk.sign(hash.as_ref());

        let msg = PeerMessage::Routed(RoutedMessage {
            target: PeerIdOrHash::PeerId(sk.public_key().into()),
            author: sk.public_key().into(),
            signature: signature.clone(),
            ttl: 100,
            body: RoutedMessageBody::BlockApproval(Approval {
                account_id: "test2".to_string(),
                inner: ApprovalInner::Endorsement(CryptoHash::default()),
                target_height: 1,
                signature,
            }),
        });
        test_codec(msg);
    }

    #[test]
    fn test_account_id_bytes() {
        let account_id = "near0".to_string();
        let enc = account_id.as_bytes();
        let dec_account_id = String::from_utf8_lossy(enc).to_string();
        assert_eq!(account_id, dec_account_id);
    }

    #[test]
    fn test_abusive() {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE + 1);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }

    #[test]
    fn test_not_abusive() {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE);
        assert_ne!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }
}
