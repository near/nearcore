/// The purpose of this crate is to encode/decode messages on the network layer.
/// Each message contains:
///     - 4 bytes - length of the message as u32
///     - the message itself, which is encoded with `borsh`
///
/// NOTES:
///     - Code has an extra logic to ban peers if they sent messages that are too large.
use crate::stats::metrics;
use bytes::{Buf, BufMut, BytesMut};
use bytesize::{GIB, MIB};
use near_network_primitives::types::ReasonForBan;
use near_performance_metrics::framed_write::EncoderCallBack;
use std::io::{Error, ErrorKind};
use tokio_util::codec::{Decoder, Encoder};
use tracing::error;

/// Maximum size of network message in encoded format.
/// The size of message is stored as `u32`, so the limit has type `u32`
const NETWORK_MESSAGE_MAX_SIZE_BYTES: u32 = 512 * MIB as u32;
/// Maximum capacity of write buffer in bytes.
const MAX_WRITE_BUFFER_CAPACITY_BYTES: usize = GIB as usize;

#[derive(Default)]
pub struct Codec {}

impl EncoderCallBack for Codec {
    #[allow(unused)]
    fn drained(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize) {
        #[cfg(feature = "performance_stats")]
        {
            let stat = near_performance_metrics::stats_enabled::get_thread_stats_logger();
            stat.lock().unwrap().log_drain_write_buffer(bytes, buf_len, buf_capacity);
        }
    }
}

impl Encoder<Vec<u8>> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Vec<u8>, buf: &mut BytesMut) -> Result<(), Error> {
        if item.len() > NETWORK_MESSAGE_MAX_SIZE_BYTES as usize {
            Err(Error::new(ErrorKind::InvalidInput, "Input is too long"))
        } else {
            #[cfg(feature = "performance_stats")]
            {
                let stat = near_performance_metrics::stats_enabled::get_thread_stats_logger();
                stat.lock().unwrap().log_add_write_buffer(
                    item.len() + 4,
                    buf.len(),
                    buf.capacity(),
                );
            }
            if buf.capacity() >= MAX_WRITE_BUFFER_CAPACITY_BYTES
                && item.len() + 4 + buf.len() > buf.capacity()
            {
                #[cfg(feature = "performance_stats")]
                let tid = near_rust_allocator_proxy::allocator::get_tid();
                #[cfg(not(feature = "performance_stats"))]
                let tid = 0;
                error!(target: "network", "{} throwing away message, because buffer is full item.len(): {} buf.capacity: {}", 
                    tid,
                    item.len(), buf.capacity());

                metrics::DROPPED_MESSAGES_COUNT.inc_by(1);
                return Err(Error::new(ErrorKind::Other, "Buf max capacity exceeded"));
            }
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

        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if len > NETWORK_MESSAGE_MAX_SIZE_BYTES {
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

#[cfg(test)]
mod test {
    use crate::peer::codec::{Codec, NETWORK_MESSAGE_MAX_SIZE_BYTES};
    use crate::types::{Handshake, PeerMessage, RoutingTableUpdate};
    use borsh::{BorshDeserialize, BorshSerialize};
    use bytes::{BufMut, BytesMut};
    use near_crypto::{KeyType, SecretKey};
    use near_network_primitives::types::{
        PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash, PeerInfo, ReasonForBan, RoutedMessage,
        RoutedMessageBody,
    };
    use near_primitives::block::{Approval, ApprovalInner};
    use near_primitives::hash::CryptoHash;
    use near_primitives::network::{AnnounceAccount, PeerId};
    use near_primitives::types::EpochId;
    use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION};
    use tokio_util::codec::{Decoder, Encoder};

    fn test_codec(msg: PeerMessage) {
        let mut codec = Codec::default();
        let mut buffer = BytesMut::new();
        codec.encode(msg.try_to_vec().unwrap(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap().unwrap();
        assert_eq!(PeerMessage::try_from_slice(&decoded).unwrap(), msg);
    }

    #[test]
    fn test_peer_message_handshake() {
        let peer_info = PeerInfo::random();
        let fake_handshake = Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: peer_info.id.clone(),
            target_peer_id: peer_info.id,
            sender_listen_port: None,
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: Default::default(),
                height: 0,
                tracked_shards: vec![],
                archival: false,
            },
            partial_edge_info: PartialEdgeInfo::default(),
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
        let msg = PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_accounts(vec![
            AnnounceAccount {
                account_id: "test1".parse().unwrap(),
                peer_id: PeerId::new(network_sk.public_key()),
                epoch_id: EpochId::default(),
                signature,
            },
        ]));
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_announce_routed_block_approval() {
        let sk = SecretKey::from_random(KeyType::ED25519);
        let hash = CryptoHash::default();
        let signature = sk.sign(hash.as_ref());

        let msg = PeerMessage::Routed(RoutedMessage {
            target: PeerIdOrHash::PeerId(PeerId::new(sk.public_key())),
            author: PeerId::new(sk.public_key()),
            signature: signature.clone(),
            ttl: 100,
            body: RoutedMessageBody::BlockApproval(Approval {
                account_id: "test2".parse().unwrap(),
                inner: ApprovalInner::Endorsement(CryptoHash::default()),
                target_height: 1,
                signature,
            }),
        });
        test_codec(msg);
    }

    #[test]
    fn test_account_id_bytes() {
        use near_primitives::types::AccountId;
        let account_id = "near0".parse::<AccountId>().unwrap();
        let enc = account_id.as_ref().as_bytes();
        let dec_account_id = String::from_utf8_lossy(enc).parse().unwrap();
        assert_eq!(account_id, dec_account_id);
    }

    #[test]
    fn test_abusive() {
        let mut codec = Codec::default();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE_BYTES + 1);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }

    #[test]
    fn test_not_abusive() {
        let mut codec = Codec::default();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE_BYTES);
        assert_ne!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }
}
