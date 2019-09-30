use std::io::{Error, ErrorKind};

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::{BufMut, BytesMut};
use tokio::codec::{Decoder, Encoder};

use crate::types::PeerMessage;

pub struct Codec {
    max_length: u32,
}

#[allow(clippy::new_without_default)]
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

pub fn peer_message_to_bytes(peer_message: PeerMessage) -> Result<Vec<u8>, std::io::Error> {
    peer_message.try_to_vec()
}

pub fn bytes_to_peer_message(bytes: &[u8]) -> Result<PeerMessage, std::io::Error> {
    PeerMessage::try_from_slice(bytes)
}

#[cfg(test)]
mod test {
    use near_crypto::{BlsSecretKey, KeyType, SecretKey};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::EpochId;

    use crate::types::{
        AccountOrPeerSignature, AnnounceAccount, Handshake, PeerChainInfo, PeerInfo, RoutedMessage,
        RoutedMessageBody,
    };

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

    #[test]
    fn test_peer_message_announce_account() {
        let sk = BlsSecretKey::from_random();
        let network_sk = SecretKey::from_random(KeyType::ED25519);
        let signature = sk.sign(vec![].as_slice());
        let msg = PeerMessage::AnnounceAccount(AnnounceAccount::new(
            "test1".to_string(),
            EpochId::default(),
            network_sk.public_key().into(),
            CryptoHash::default(),
            AccountOrPeerSignature::AccountSignature(signature),
        ));
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_announce_routed_block_approval() {
        let sk = BlsSecretKey::from_random();
        let network_sk = SecretKey::from_random(KeyType::ED25519);
        let hash = CryptoHash::default();
        let signature = sk.sign(hash.as_ref());
        let msg = PeerMessage::Routed(RoutedMessage {
            account_id: "test1".to_string(),
            author: network_sk.public_key().into(),
            signature: AccountOrPeerSignature::AccountSignature(signature.clone()),
            body: RoutedMessageBody::BlockApproval(
                "test2".to_string(),
                CryptoHash::default(),
                signature,
            ),
        });
        test_codec(msg);
    }
}
