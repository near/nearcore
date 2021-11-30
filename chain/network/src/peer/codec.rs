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
use near_rust_allocator_proxy::allocator::get_tid;
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
                error!(target: "network", "{} throwing away message, because buffer is full item.len(): {} buf.capacity: {}", get_tid(), item.len(), buf.capacity());

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

        let mut len_bytes: [u8; 4] = [0; 4];
        len_bytes.copy_from_slice(&buf[0..4]);
        let len = u32::from_le_bytes(len_bytes);
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

/// Determines size of `PeerId` based on first byte of it's representation.
/// Size of `PeerId` depends on type of `PublicMessage it stores`.
/// PublicKey::ED25519 -> 1 + 32 bytes
/// PublicKey::SECP256K1 -> 1 + 64 bytes
fn peer_id_type_field_len(enum_var: u8) -> Option<usize> {
    // 1 byte for enum variant, then some number depending on the
    // public key type
    match enum_var {
        0 => Some(1 + 32),
        1 => Some(1 + 64),
        _ => None,
    }
}

/// Checks `bytes` represents `PeerMessage::Routed(RoutedMessage)`,
/// and `RoutedMessage.body` has type of `RoutedMessageBody::ForwardTx`.
///
/// This is done to avoid expensive borsch-deserializing.
pub(crate) fn is_forward_transaction(bytes: &[u8]) -> Option<bool> {
    // PeerMessage::Routed variant == 13
    let peer_message_variant = *bytes.get(0)?;
    if peer_message_variant != 13 {
        return Some(false);
    }

    // target: PeerIdOrHash
    let author_variant_idx = {
        let target_field_len = {
            let target_field_variant = *bytes.get(1)?;
            if target_field_variant == 0 {
                // PeerIdOrHash::PeerId
                let peer_id_variant = *bytes.get(2)?;
                peer_id_type_field_len(peer_id_variant)?
            } else if target_field_variant == 1 {
                // PeerIdOrHash::Hash is always 32 bytes
                32
            } else {
                error!("Unsupported variant of PeerIdOrHash {}", target_field_variant);
                return None;
            }
        };
        2 + target_field_len
    };

    // author: PeerId
    let signature_variant_idx = {
        let author_variant = *bytes.get(author_variant_idx)?;
        let author_field_len = peer_id_type_field_len(author_variant)?;

        author_variant_idx + author_field_len
    };

    // ttl: u8
    let ttl_idx = {
        let signature_variant = *bytes.get(signature_variant_idx)?;

        // pub signature: Signature
        let signature_field_len = match signature_variant {
            0 => 1 + 64, // Signature::ED25519
            1 => 1 + 65, // Signature::SECP256K1
            _ => {
                return None;
            }
        };
        signature_variant_idx + signature_field_len
    };

    // pub ttl: u8
    let message_body_idx = ttl_idx + 1;

    // check if type is `RoutedMessageBody::ForwardTx`
    let message_body_variant = *bytes.get(message_body_idx)?;
    Some(message_body_variant == 1)
}

#[cfg(test)]
mod test {
    use crate::peer::codec::{is_forward_transaction, Codec, NETWORK_MESSAGE_MAX_SIZE_BYTES};
    use crate::routing::edge::EdgeInfo;
    use crate::types::{Handshake, HandshakeFailureReason, HandshakeV2, PeerMessage, SyncData};
    use crate::PeerInfo;
    use borsh::BorshDeserialize;
    use borsh::BorshSerialize;
    use bytes::{BufMut, BytesMut};
    use near_crypto::{KeyType, PublicKey, SecretKey};
    use near_network_primitives::types::{
        PeerChainInfo, PeerChainInfoV2, PeerIdOrHash, ReasonForBan, RoutedMessage,
        RoutedMessageBody,
    };
    use near_primitives::block::{Approval, ApprovalInner};
    use near_primitives::hash::{self, CryptoHash};
    use near_primitives::network::{AnnounceAccount, PeerId};
    use near_primitives::transaction::{SignedTransaction, Transaction};
    use near_primitives::types::EpochId;
    use near_primitives::version::{OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION};
    use tokio_util::codec::{Decoder, Encoder};

    fn test_codec(msg: PeerMessage) {
        let mut codec = Codec::default();
        let mut buffer = BytesMut::new();
        codec.encode(msg.try_to_vec().unwrap(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap().unwrap();
        assert_eq!(PeerMessage::try_from_slice(&decoded).unwrap(), msg);
    }

    #[derive(Debug, Copy, Clone)]
    enum ForwardTxTargetType {
        Hash,
        PublicKey(KeyType),
    }

    #[derive(Debug, Copy, Clone)]
    struct ForwardTxType {
        target: ForwardTxTargetType,
        author: KeyType,
        tx: KeyType,
    }

    fn create_tx_forward(schema: ForwardTxType) -> PeerMessage {
        let target = match schema.target {
            ForwardTxTargetType::Hash => PeerIdOrHash::Hash(hash::hash(b"peer_id_hash")),
            ForwardTxTargetType::PublicKey(key_type) => {
                let secret_key = SecretKey::from_seed(key_type, "target_secret_key");
                PeerIdOrHash::PeerId(PeerId::new(secret_key.public_key()))
            }
        };

        let (author, signature) = {
            let secret_key = SecretKey::from_seed(schema.author, "author_secret_key");
            let public_key = secret_key.public_key();
            let author = PeerId::new(public_key);
            let msg_data = hash::hash(b"msg_data");
            let signature = secret_key.sign(msg_data.as_ref());

            (author, signature)
        };

        let tx = {
            let secret_key = SecretKey::from_seed(schema.tx, "tx_secret_key");
            let public_key = secret_key.public_key();
            let tx_hash = hash::hash(b"this_great_tx_data");
            let signature = secret_key.sign(tx_hash.as_ref());

            SignedTransaction::new(
                signature,
                Transaction::new(
                    "test_x".parse().unwrap(),
                    public_key,
                    "test_y".parse().unwrap(),
                    7,
                    tx_hash,
                ),
            )
        };

        PeerMessage::Routed(RoutedMessage {
            target,
            author,
            signature,
            ttl: 99,
            body: RoutedMessageBody::ForwardTx(tx),
        })
    }

    #[test]
    fn test_tx_forward() {
        let targets = [
            ForwardTxTargetType::PublicKey(KeyType::ED25519),
            ForwardTxTargetType::PublicKey(KeyType::SECP256K1),
            ForwardTxTargetType::Hash,
        ];
        let authors = [KeyType::ED25519, KeyType::SECP256K1];
        let txs_keys = [KeyType::ED25519, KeyType::SECP256K1];

        let schemas = targets
            .iter()
            .flat_map(|target| authors.iter().map(move |author| (*target, *author)))
            .flat_map(|(target, author)| {
                txs_keys.iter().map(move |tx| ForwardTxType { target, author, tx: *tx })
            });

        schemas.for_each(|s| {
            let msg = create_tx_forward(s);
            let bytes = msg.try_to_vec().unwrap();
            assert!(is_forward_transaction(&bytes).unwrap());
        })
    }

    #[test]
    fn test_peer_message_handshake() {
        let peer_info = PeerInfo::random();
        let fake_handshake = Handshake {
            version: PROTOCOL_VERSION,
            oldest_supported_version: OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION,
            peer_id: peer_info.id.clone(),
            target_peer_id: peer_info.id,
            listen_port: None,
            chain_info: PeerChainInfoV2 {
                genesis_id: Default::default(),
                height: 0,
                tracked_shards: vec![],
                archival: false,
            },
            edge_info: EdgeInfo::default(),
        };
        let msg = PeerMessage::Handshake(fake_handshake);
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_handshake_v2() {
        let peer_info = PeerInfo::random();
        let fake_handshake = HandshakeV2 {
            version: PROTOCOL_VERSION,
            oldest_supported_version: OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION,
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
        let msg = PeerMessage::HandshakeV2(fake_handshake);
        test_codec(msg);
    }

    #[test]
    fn test_peer_message_handshake_v2_00() {
        let fake_handshake = HandshakeV2 {
            version: 0,
            oldest_supported_version: 0,
            peer_id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
            target_peer_id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
            listen_port: None,
            chain_info: PeerChainInfo {
                genesis_id: Default::default(),
                height: 0,
                tracked_shards: vec![],
            },
            edge_info: EdgeInfo::default(),
        };
        let msg = PeerMessage::HandshakeV2(fake_handshake);

        let mut codec = Codec::default();
        let mut buffer = BytesMut::new();
        codec.encode(msg.try_to_vec().unwrap(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap().unwrap();

        let err = PeerMessage::try_from_slice(&decoded).unwrap_err();

        assert_eq!(
            *err.get_ref()
                .map(|inner| inner.downcast_ref::<HandshakeFailureReason>())
                .unwrap()
                .unwrap(),
            HandshakeFailureReason::ProtocolVersionMismatch {
                version: 0,
                oldest_supported_version: 0,
            }
        );
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
                account_id: "test1".parse().unwrap(),
                peer_id: PeerId::new(network_sk.public_key()),
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
