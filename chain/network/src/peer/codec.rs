/// The purpose of this crate is to encode/decode messages on the network layer.
/// Each message contains:
///     - 4 bytes - length of the message as u32
///     - the message itself, which is encoded with `borsh`
///
/// NOTES:
///     - Code has an extra logic to ban peers if they sent messages that are too large.
use crate::stats::metrics;
use crate::types::ReasonForBan;
use bytes::{Buf, BufMut, BytesMut};
use bytesize::{GIB, MIB};
use near_performance_metrics::framed_write::EncoderCallBack;
use std::io::{Error, ErrorKind};
use tokio_util::codec::{Decoder, Encoder};
use tracing::error;

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;
/// Maximum capacity of write buffer in bytes.
const MAX_WRITE_BUFFER_CAPACITY_BYTES: usize = GIB as usize;

pub(crate) struct Codec {
    /// Metric which tracks the size of the read or write buffer this codec is
    /// used for.
    ///
    /// This is perhaps a bit hacky -- ideally, the owner of the buffer should
    /// track it's size. But `Codec` already doubles as a size-tracking callback
    /// (`EncoderCallBack`), so sticking a gauge here works well with the
    /// current shape of the code. Note, however, that this means we might want
    /// to report a metric even when the `Codec` doesn't modify the buffer (eg,
    /// when more data was added to the buffer, but not enough to form a full
    /// message).
    buf_size_metric: near_metrics::IntGauge,
}

impl Codec {
    pub fn new(buf_size_metric: near_metrics::IntGauge) -> Codec {
        Codec { buf_size_metric }
    }
}

impl EncoderCallBack for Codec {
    #[allow(unused)]
    fn drained(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize) {
        self.buf_size_metric.set(buf_len as i64);
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
        if item.len() > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            // TODO(mina86): Is there some way we can know what message we’re
            // encoding?
            metrics::MessageDropped::InputTooLong.inc_unknown_msg();
            return Err(Error::new(ErrorKind::InvalidInput, "Input is too long"));
        }

        #[cfg(feature = "performance_stats")]
        {
            let stat = near_performance_metrics::stats_enabled::get_thread_stats_logger();
            stat.lock().unwrap().log_add_write_buffer(item.len() + 4, buf.len(), buf.capacity());
        }
        if buf.capacity() >= MAX_WRITE_BUFFER_CAPACITY_BYTES
            && item.len() + 4 + buf.len() > buf.capacity()
        {
            #[cfg(feature = "performance_stats")]
            let tid = near_rust_allocator_proxy::get_tid();
            #[cfg(not(feature = "performance_stats"))]
            let tid = 0;
            error!(target: "network", "{} throwing away message, because buffer is full item.len(): {} buf.capacity: {}",
                   tid,
                   item.len(), buf.capacity());

            // TODO(mina86): Is there some way we can know what message
            // we’re encoding?
            metrics::MessageDropped::MaxCapacityExceeded.inc_unknown_msg();
            return Err(Error::new(ErrorKind::Other, "Buf max capacity exceeded"));
        }
        // First four bytes is the length of the buffer.
        buf.reserve(item.len() + 4);
        buf.put_u32_le(item.len() as u32);
        buf.put(&item[..]);
        self.buf_size_metric.set(buf.len() as i64);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Result<Vec<u8>, ReasonForBan>;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Notify about current buffer size even if we won't drain it.
        self.buf_size_metric.set(buf.len() as i64);

        let len_buf = match buf.get(..4).and_then(|s| <[u8; 4]>::try_from(s).ok()) {
            // not enough bytes to start decoding
            None => return Ok(None),
            Some(res) => res,
        };

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            // If this point is reached, abusive peer is banned.
            return Ok(Some(Err(ReasonForBan::Abusive)));
        }

        if let Some(data_buf) = buf.get(4..4 + len) {
            let res = Some(Ok(data_buf.to_vec()));
            buf.advance(4 + len);
            if buf.is_empty() && buf.capacity() > 0 {
                *buf = BytesMut::new();
            }
            self.buf_size_metric.set(buf.len() as i64);
            Ok(res)
        } else {
            // not enough bytes, keep waiting
            Ok(None)
        }
    }
}

impl Drop for Codec {
    fn drop(&mut self) {
        self.buf_size_metric.set(0)
    }
}

#[cfg(test)]
mod test {
    use crate::network_protocol::{
        PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash, PeerInfo, RoutedMessage, RoutedMessageBody,
        RoutedMessageV2,
    };
    use crate::peer::codec::{Codec, NETWORK_MESSAGE_MAX_SIZE_BYTES};
    use crate::types::{Handshake, PeerMessage, RoutingTableUpdate};
    use bytes::{BufMut, BytesMut};
    use near_crypto::{KeyType, SecretKey};
    use near_primitives::block::{Approval, ApprovalInner};
    use near_primitives::hash::CryptoHash;
    use near_primitives::network::{AnnounceAccount, PeerId};
    use near_primitives::types::EpochId;
    use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION};
    use tokio_util::codec::{Decoder, Encoder};

    pub(crate) fn make_codec() -> Codec {
        use near_metrics::prometheus;
        let opts = prometheus::Opts::new("test", "test");
        let gauge = prometheus::IntGaugeVec::new(opts, &["test"]).unwrap();
        Codec::new(gauge.with_label_values(&["127.0.0.1:3030"]))
    }

    fn test_codec(msg: PeerMessage) {
        for enc in
            [crate::network_protocol::Encoding::Proto, crate::network_protocol::Encoding::Borsh]
        {
            let mut codec = make_codec();
            let mut buffer = BytesMut::new();
            codec.encode(msg.serialize(enc), &mut buffer).unwrap();
            let decoded = codec.decode(&mut buffer).unwrap().unwrap().unwrap();
            assert_eq!(PeerMessage::deserialize(enc, &decoded).unwrap(), msg);
        }
    }

    #[test]
    fn test_decode_too_short() {
        let mut buffer = BytesMut::new();
        buffer.put_u8(4u8);
        buffer.put_u8(0u8);
        buffer.put_u8(0u8);
        let mut codec = make_codec();
        // length is too short
        match codec.decode(&mut buffer) {
            Ok(None) => {}
            _ => {
                panic!("expected Ok(None)")
            }
        }

        buffer.put_u8(0u8);
        buffer.put_u8(0u8);
        buffer.put_u8(0u8);
        buffer.put_u8(0u8);
        // Length 4 + body of length 3 should also to decode
        match codec.decode(&mut buffer) {
            Ok(None) => {}
            _ => {
                panic!("expected Ok(None)")
            }
        }
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
        let msg = PeerMessage::Tier2Handshake(fake_handshake);
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

        let msg = PeerMessage::Routed(
            RoutedMessageV2 {
                msg: RoutedMessage {
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
                },
                created_at: None,
            }
            .into(),
        );
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
        let mut codec = make_codec();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE_BYTES as u32 + 1);
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }

    #[test]
    fn test_not_abusive() {
        let mut codec = make_codec();
        let mut buffer = BytesMut::new();
        buffer.reserve(4);
        buffer.put_u32_le(NETWORK_MESSAGE_MAX_SIZE_BYTES as u32);
        assert_ne!(codec.decode(&mut buffer).unwrap(), Some(Err(ReasonForBan::Abusive)));
    }
}
