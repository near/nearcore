//! This module facilitates the initialization and the storage
//! of rate limits per message.

use enum_map::{enum_map, EnumMap};
use near_async::time::Duration;

use crate::network_protocol::{PeerMessage, RoutedMessageBody};

use super::token_bucket::{TokenBucket, TokenBucketError};

/// Object responsible to manage the rate limits of all network messages
/// for a single connection/peer.
#[derive(Default)]
pub struct RateLimits {
    buckets: EnumMap<RateLimitedPeerMessageKey, Option<TokenBucket>>,
}

impl RateLimits {
    pub fn from_config(config: &Config) -> Self {
        let mut buckets = enum_map! { _ => None };
        // Configuration is assumed to be correct. Any failure to build a bucket is ignored.
        for message_config in &config.rate_limits {
            let key = message_config.message_key;
            let initial_size = message_config.initial_size.unwrap_or(message_config.maximum_size);
            match TokenBucket::new(
                initial_size,
                message_config.maximum_size,
                message_config.refill_rate,
            ) {
                Ok(bucket) => buckets[key] = Some(bucket),
                Err(err) => {
                    tracing::warn!(target: "network", "ignoring rate limit for {key} due to an error ({err})")
                }
            }
        }
        Self { buckets }
    }

    /// Checks if the given message is under the rate limits.
    ///
    /// Returns `true` if the message should be allowed to continue. Otherwise,
    /// if it should be rate limited, returns `false`.
    pub fn is_allowed(&self, message: &PeerMessage) -> bool {
        if let Some((key, cost)) = get_key_and_token_cost(message) {
            if let Some(bucket) = &self.buckets[key] {
                return bucket.acquire(cost);
            }
        }
        true
    }

    /// Updates all rate limits according to their own refresh rate, using `duration` as
    /// the time elapsed since the previous update.
    pub fn update(&self, duration: Duration) {
        for (_, maybe_bucket) in &self.buckets {
            maybe_bucket.as_ref().map(|bucket| bucket.refill(duration));
        }
    }
}

/// Rate limit configuration for a single network message.
#[derive(Clone)]
pub struct SingleMessageConfig {
    pub message_key: RateLimitedPeerMessageKey,
    pub maximum_size: u32,
    pub refill_rate: f32,
    /// Optional initial size. Defaults to `maximum_size` if absent.
    pub initial_size: Option<u32>,
}

impl SingleMessageConfig {
    pub fn new(
        message_key: RateLimitedPeerMessageKey,
        maximum_size: u32,
        refill_rate: f32,
        initial_size: Option<u32>,
    ) -> Self {
        Self { message_key, maximum_size, refill_rate, initial_size }
    }
}

/// Network messages rate limits configuration.
#[derive(Default, Clone)]
pub struct Config {
    pub rate_limits: Vec<SingleMessageConfig>,
}

impl Config {
    /// Validates this configuration object.
    ///
    /// # Errors
    ///
    /// If at least one error is present, returns the list of all configuration errors.  
    pub fn validate(&self) -> Result<(), Vec<(RateLimitedPeerMessageKey, TokenBucketError)>> {
        let mut errors = Vec::new();
        for message_config in &self.rate_limits {
            if let Err(err) = TokenBucket::validate_refill_rate(message_config.refill_rate) {
                errors.push((message_config.message_key, err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// This enum represents the variants of [PeerMessage] that can be rate limited.
/// It is meant to be used as an index for mapping peer messages to a value.
#[derive(Clone, Copy, enum_map::Enum, strum::Display, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum RateLimitedPeerMessageKey {
    SyncRoutingTable,
    DistanceVector,
    RequestUpdateNonce,
    SyncAccountsData,
    PeersRequest,
    PeersResponse,
    BlockHeadersRequest,
    BlockHeaders,
    BlockRequest,
    Block,
    Transaction,
    SyncSnapshotHosts,
    StateRequestHeader,
    StateRequestPart,
    VersionedStateResponse,
    BlockApproval,
    ForwardTx,
    TxStatusRequest,
    TxStatusResponse,
    StateResponse,
    PartialEncodedChunkRequest,
    PartialEncodedChunkResponse,
    VersionedPartialEncodedChunk,
    PartialEncodedChunkForward,
    ChunkEndorsement,
    ChunkStateWitnessAck,
    PartialEncodedStateWitness,
    PartialEncodedStateWitnessForward,
}

/// Given a `PeerMessage` returns a tuple containing the `RateLimitedPeerMessageKey`
/// corresponding to the message's type and its the cost (in tokens) for rate limiting
/// purposes.
///
/// Returns `Some` if the message has the potential to be rate limited (through the correct configuration).
/// Returns `None` if the message is not meant to be rate limited in any scenario.
fn get_key_and_token_cost(message: &PeerMessage) -> Option<(RateLimitedPeerMessageKey, u32)> {
    use RateLimitedPeerMessageKey::*;
    match message {
        PeerMessage::SyncRoutingTable(_) => Some((SyncRoutingTable, 1)),
        PeerMessage::DistanceVector(_) => Some((DistanceVector, 1)),
        PeerMessage::RequestUpdateNonce(_) => Some((RequestUpdateNonce, 1)),
        PeerMessage::SyncAccountsData(_) => Some((SyncAccountsData, 1)),
        PeerMessage::PeersRequest(_) => Some((PeersRequest, 1)),
        PeerMessage::PeersResponse(_) => Some((PeersResponse, 1)),
        PeerMessage::BlockHeadersRequest(_) => Some((BlockHeadersRequest, 1)),
        PeerMessage::BlockHeaders(_) => Some((BlockHeaders, 1)),
        PeerMessage::BlockRequest(_) => Some((BlockRequest, 1)),
        PeerMessage::Block(_) => Some((Block, 1)),
        PeerMessage::Transaction(_) => Some((Transaction, 1)),
        PeerMessage::Routed(msg) => match msg.body {
            RoutedMessageBody::BlockApproval(_) => Some((BlockApproval, 1)),
            RoutedMessageBody::ForwardTx(_) => Some((ForwardTx, 1)),
            RoutedMessageBody::TxStatusRequest(_, _) => Some((TxStatusRequest, 1)),
            RoutedMessageBody::TxStatusResponse(_) => Some((TxStatusResponse, 1)),
            RoutedMessageBody::StateResponse(_) => Some((StateResponse, 1)),
            RoutedMessageBody::PartialEncodedChunkRequest(_) => {
                Some((PartialEncodedChunkRequest, 1))
            }
            RoutedMessageBody::PartialEncodedChunkResponse(_) => {
                Some((PartialEncodedChunkResponse, 1))
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(_) => {
                Some((VersionedPartialEncodedChunk, 1))
            }
            RoutedMessageBody::PartialEncodedChunkForward(_) => {
                Some((PartialEncodedChunkForward, 1))
            }
            RoutedMessageBody::ChunkEndorsement(_) => Some((ChunkEndorsement, 1)),
            RoutedMessageBody::ChunkStateWitnessAck(_) => Some((ChunkStateWitnessAck, 1)),
            RoutedMessageBody::PartialEncodedStateWitness(_) => {
                Some((PartialEncodedStateWitness, 1))
            }
            RoutedMessageBody::PartialEncodedStateWitnessForward(_) => {
                Some((PartialEncodedStateWitnessForward, 1))
            }
            RoutedMessageBody::Ping(_)
            | RoutedMessageBody::Pong(_)
            | RoutedMessageBody::_UnusedChunkStateWitness
            | RoutedMessageBody::_UnusedVersionedStateResponse
            | RoutedMessageBody::_UnusedPartialEncodedChunk
            | RoutedMessageBody::_UnusedQueryRequest
            | RoutedMessageBody::_UnusedQueryResponse
            | RoutedMessageBody::_UnusedReceiptOutcomeRequest(_)
            | RoutedMessageBody::_UnusedReceiptOutcomeResponse
            | RoutedMessageBody::_UnusedStateRequestHeader
            | RoutedMessageBody::_UnusedStateRequestPart => None,
        },
        PeerMessage::SyncSnapshotHosts(_) => Some((SyncSnapshotHosts, 1)),
        PeerMessage::StateRequestHeader(_, _) => Some((StateRequestHeader, 1)),
        PeerMessage::StateRequestPart(_, _, _) => Some((StateRequestPart, 1)),
        PeerMessage::VersionedStateResponse(_) => Some((VersionedStateResponse, 1)),
        PeerMessage::Tier1Handshake(_)
        | PeerMessage::Tier2Handshake(_)
        | PeerMessage::HandshakeFailure(_, _)
        | PeerMessage::LastEdge(_)
        | PeerMessage::Disconnect(_)
        | PeerMessage::Challenge(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;

    use crate::network_protocol::{Disconnect, PeerMessage};

    use super::*;

    #[test]
    fn is_allowed() {
        let disconnect =
            PeerMessage::Disconnect(Disconnect { remove_from_connection_store: false });
        let block_request = PeerMessage::BlockRequest(CryptoHash::default());

        // Test message that can't be rate limited.
        {
            let limits = RateLimits::default();
            assert!(limits.is_allowed(&disconnect));
        }

        // Test message that might be rate limited, but the system is not configured to do so.
        {
            let limits = RateLimits::default();
            assert!(limits.is_allowed(&block_request));
        }

        // Test rate limited message with enough tokens.
        {
            let mut limits = RateLimits::default();
            limits.buckets[RateLimitedPeerMessageKey::BlockRequest] =
                Some(TokenBucket::new(1, 1, 0.0).unwrap());
            assert!(limits.is_allowed(&block_request));
        }

        // Test rate limited message without enough tokens.
        {
            let mut limits = RateLimits::default();
            limits.buckets[RateLimitedPeerMessageKey::BlockRequest] =
                Some(TokenBucket::new(0, 1, 0.0).unwrap());
            assert!(!limits.is_allowed(&block_request));
        }
    }

    #[test]
    fn configuration() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();

        config.rate_limits.push(SingleMessageConfig::new(Block, 5, 1.0, Some(1)));
        config.rate_limits.push(SingleMessageConfig::new(BlockApproval, 5, 1.0, None));
        config.rate_limits.push(SingleMessageConfig::new(BlockHeaders, 1, -4.0, None));

        let limits = RateLimits::from_config(&config);

        // Bucket should exist with capacity = 1.
        assert!(!limits.buckets[Block].as_ref().unwrap().acquire(2));
        // Bucket should exist with capacity = 5.
        assert!(limits.buckets[BlockApproval].as_ref().unwrap().acquire(2));
        // Bucket should not exist due to a config error.
        assert!(limits.buckets[BlockHeaders].is_none());
    }

    #[test]
    fn configuration_errors() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.rate_limits.push(SingleMessageConfig::new(Block, 0, 1.0, None));
        assert!(config.validate().is_ok());

        config.rate_limits.push(SingleMessageConfig::new(BlockApproval, 0, -1.0, None));
        assert_eq!(
            config.validate(),
            Err(vec![(BlockApproval, TokenBucketError::InvalidRefillRate(-1.0))])
        );

        config.rate_limits.push(SingleMessageConfig::new(BlockHeaders, 0, -2.0, None));
        assert_eq!(
            config.validate(),
            Err(vec![
                (BlockApproval, TokenBucketError::InvalidRefillRate(-1.0)),
                (BlockHeaders, TokenBucketError::InvalidRefillRate(-2.0))
            ])
        );
    }

    #[test]
    fn update() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();

        config.rate_limits.push(SingleMessageConfig::new(Block, 5, 1.0, Some(0)));
        config.rate_limits.push(SingleMessageConfig::new(BlockApproval, 5, 1.0, Some(0)));

        let limits = RateLimits::from_config(&config);

        assert!(!limits.buckets[Block].as_ref().unwrap().acquire(1));
        assert!(!limits.buckets[BlockApproval].as_ref().unwrap().acquire(1));

        limits.update(Duration::seconds(1));

        assert!(limits.buckets[Block].as_ref().unwrap().acquire(1));
        assert!(limits.buckets[BlockApproval].as_ref().unwrap().acquire(1));
    }
}
