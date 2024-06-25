//! This module facilitates the initialization and the storage
//! of rate limits per message.

use std::collections::HashMap;

use enum_map::{enum_map, EnumMap};
use near_async::time::Instant;

use crate::network_protocol::{PeerMessage, RoutedMessageBody};

use super::token_bucket::{TokenBucket, TokenBucketError};

/// Object responsible to manage the rate limits of all network messages
/// for a single connection/peer.
#[derive(Default)]
pub struct RateLimits {
    buckets: EnumMap<RateLimitedPeerMessageKey, Option<TokenBucket>>,
}

impl RateLimits {
    /// Creates all buckets as configured in `config`.
    /// See also [TokenBucket::new].
    pub fn from_config(config: &Config, start_time: Instant) -> Self {
        let mut buckets = enum_map! { _ => None };
        // Configuration is assumed to be correct. Any failure to build a bucket is ignored.
        for (key, message_config) in &config.rate_limits {
            let initial_size = message_config.initial_size.unwrap_or(message_config.maximum_size);
            match TokenBucket::new(
                initial_size,
                message_config.maximum_size,
                message_config.refill_rate,
                start_time,
            ) {
                Ok(bucket) => buckets[*key] = Some(bucket),
                Err(err) => {
                    tracing::warn!(target: "network", "ignoring rate limit for {key} due to an error ({err})")
                }
            }
        }
        Self { buckets }
    }

    /// Checks if the given message is under the rate limits.
    ///
    /// # Arguments
    ///
    /// * `message` - The network message to be checked
    /// * `now` - Current time
    ///
    /// Returns `true` if the message should be allowed to continue. Otherwise,
    /// if it should be rate limited, returns `false`.
    pub fn is_allowed(&mut self, message: &PeerMessage, now: Instant) -> bool {
        if let Some((key, cost)) = get_key_and_token_cost(message) {
            if let Some(bucket) = &mut self.buckets[key] {
                return bucket.acquire(cost, now);
            }
        }
        true
    }
}

/// Rate limit configuration for a single network message.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SingleMessageConfig {
    pub maximum_size: u32,
    pub refill_rate: f32,
    /// Optional initial size. Defaults to `maximum_size` if absent.
    pub initial_size: Option<u32>,
}

impl SingleMessageConfig {
    pub fn new(maximum_size: u32, refill_rate: f32, initial_size: Option<u32>) -> Self {
        Self { maximum_size, refill_rate, initial_size }
    }
}

/// Network messages rate limits configuration.
#[derive(Default, Clone)]
pub struct Config {
    pub rate_limits: HashMap<RateLimitedPeerMessageKey, SingleMessageConfig>,
}

/// Struct to manage user defined overrides for [Config]. The key difference with the base struct
/// is that in this values can be set to `None` to disable preset rate limits.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug)]
pub struct OverrideConfig {
    pub rate_limits: HashMap<RateLimitedPeerMessageKey, Option<SingleMessageConfig>>,
}

impl Config {
    /// Validates this configuration object.
    ///
    /// # Errors
    ///
    /// If at least one error is present, returns the list of all configuration errors.  
    pub fn validate(&self) -> Result<(), Vec<(RateLimitedPeerMessageKey, TokenBucketError)>> {
        let mut errors = Vec::new();
        for (key, message_config) in &self.rate_limits {
            if let Err(err) = TokenBucket::validate_refill_rate(message_config.refill_rate) {
                errors.push((*key, err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Returns a good preset of rate limit configuration valid for any type of node.
    pub fn standard_preset() -> Self {
        use RateLimitedPeerMessageKey::*;
        let mut rate_limits = HashMap::new();

        // Token usage is defined per message.
        // Some messages consume 1 token flat while for others the price depends on the payload.
        // See `get_key_and_token_cost` for more details.

        rate_limits.insert(SyncRoutingTable, SingleMessageConfig::new(400_000, 4_000.0, None));
        rate_limits.insert(DistanceVector, SingleMessageConfig::new(250_000, 2_500.0, None));
        rate_limits.insert(RequestUpdateNonce, SingleMessageConfig::new(10, 0.1, None));
        rate_limits.insert(SyncAccountsData, SingleMessageConfig::new(25_000, 250.0, None));
        rate_limits.insert(PeersRequest, SingleMessageConfig::new(20, 0.2, None));
        rate_limits.insert(PeersResponse, SingleMessageConfig::new(10_000, 100.0, None));
        rate_limits.insert(BlockRequest, SingleMessageConfig::new(50, 0.5, None));
        rate_limits.insert(Block, SingleMessageConfig::new(300, 3.0, None));
        rate_limits.insert(SyncSnapshotHosts, SingleMessageConfig::new(1_000, 10.0, None));
        rate_limits.insert(
            PartialEncodedChunkRequest,
            SingleMessageConfig::new(1_000_000, 10_000.0, None),
        );
        rate_limits
            .insert(PartialEncodedChunkResponse, SingleMessageConfig::new(10_000, 100.0, None));
        rate_limits
            .insert(PartialEncodedChunkForward, SingleMessageConfig::new(25_000, 250.0, None));
        rate_limits.insert(VersionedPartialEncodedChunk, SingleMessageConfig::new(200, 2.0, None));
        rate_limits.insert(ForwardTx, SingleMessageConfig::new(400, 4.0, None));
        rate_limits.insert(BlockApproval, SingleMessageConfig::new(200, 2.0, None));
        rate_limits.insert(TxStatusRequest, SingleMessageConfig::new(10, 0.1, None));
        rate_limits.insert(TxStatusResponse, SingleMessageConfig::new(10, 0.1, None));

        Self { rate_limits }
    }

    /// Applies rate limits configuration overrides to `self`. In practice, merges the two configurations
    /// giving preference to the values defined by the `overrides` parameter.
    pub fn apply_overrides(&mut self, overrides: OverrideConfig) {
        for (key, message_config) in overrides.rate_limits {
            match message_config {
                Some(value) => self.rate_limits.insert(key, value),
                None => self.rate_limits.remove(&key),
            };
        }
    }
}

/// This enum represents the variants of [PeerMessage] that can be rate limited.
/// It is meant to be used as an index for mapping peer messages to a value.
#[derive(
    Clone,
    Copy,
    enum_map::Enum,
    strum::Display,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
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

/// Adds together a variable length list of values through `saturating_add`.
macro_rules! safe_add_into_u32 {
    ($x:expr) => ($x as u32);
    ($x:expr, $($y:expr),+) => {
        ($x as u32).saturating_add(safe_add_into_u32!($($y),+))
    }
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
        PeerMessage::SyncRoutingTable(inner) => {
            // Pay one token for each account and for each edge.
            Some((SyncRoutingTable, safe_add_into_u32!(inner.accounts.len(), inner.edges.len())))
        }
        PeerMessage::DistanceVector(inner) => {
            // Pay one token for each advertised distance and for each edge.
            Some((DistanceVector, safe_add_into_u32!(inner.distances.len(), inner.edges.len())))
        }
        PeerMessage::RequestUpdateNonce(_) => Some((RequestUpdateNonce, 1)),
        PeerMessage::SyncAccountsData(inner) => {
            // Pay one token for each signed account data.
            Some((SyncAccountsData, safe_add_into_u32!(inner.accounts_data.len())))
        }
        PeerMessage::PeersRequest(_) => Some((PeersRequest, 1)),
        PeerMessage::PeersResponse(inner) => {
            // Pay one token for each peer regardless of its type.
            Some((PeersResponse, safe_add_into_u32!(inner.peers.len(), inner.direct_peers.len())))
        }
        PeerMessage::BlockHeadersRequest(inner) => {
            // Pay one token for each requested hash.
            Some((BlockHeadersRequest, safe_add_into_u32!(inner.len())))
        }
        PeerMessage::BlockHeaders(inner) => {
            // Pay one token for each header.
            Some((BlockHeaders, safe_add_into_u32!(inner.len())))
        }
        PeerMessage::BlockRequest(_) => Some((BlockRequest, 1)),
        PeerMessage::Block(_) => Some((Block, 1)),
        PeerMessage::Transaction(_) => Some((Transaction, 1)),
        PeerMessage::Routed(msg) => match &msg.body {
            RoutedMessageBody::BlockApproval(_) => Some((BlockApproval, 1)),
            RoutedMessageBody::ForwardTx(_) => Some((ForwardTx, 1)),
            RoutedMessageBody::TxStatusRequest(_, _) => Some((TxStatusRequest, 1)),
            RoutedMessageBody::TxStatusResponse(_) => Some((TxStatusResponse, 1)),
            RoutedMessageBody::StateResponse(_) => Some((StateResponse, 1)),
            RoutedMessageBody::PartialEncodedChunkRequest(inner) => {
                // Pay one token per part.
                Some((PartialEncodedChunkRequest, safe_add_into_u32!(inner.part_ords.len())))
            }
            RoutedMessageBody::PartialEncodedChunkResponse(_) => {
                // Pay one token per message since the limits on incoming receipts are unclear.
                Some((PartialEncodedChunkResponse, 1))
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(_) => {
                // Pay one token per message since the limits on incoming receipts are unclear.
                Some((VersionedPartialEncodedChunk, 1))
            }
            RoutedMessageBody::PartialEncodedChunkForward(inner) => {
                // Pay one token per part.
                Some((PartialEncodedChunkForward, safe_add_into_u32!(inner.parts.len())))
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
        PeerMessage::SyncSnapshotHosts(inner) => {
            // Pay one token for each host.
            Some((SyncSnapshotHosts, safe_add_into_u32!(inner.hosts.len())))
        }
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
    use near_async::time::Duration;
    use near_primitives::hash::CryptoHash;

    use crate::network_protocol::{Disconnect, PeerMessage};

    use super::*;

    #[test]
    fn is_allowed() {
        let disconnect =
            PeerMessage::Disconnect(Disconnect { remove_from_connection_store: false });
        let block_request = PeerMessage::BlockRequest(CryptoHash::default());
        let now = Instant::now();

        // Test message that can't be rate limited.
        {
            let mut limits = RateLimits::default();
            assert!(limits.is_allowed(&disconnect, now));
        }

        // Test message that might be rate limited, but the system is not configured to do so.
        {
            let mut limits = RateLimits::default();
            assert!(limits.is_allowed(&block_request, now));
        }

        // Test rate limited message with enough tokens.
        {
            let mut limits = RateLimits::default();
            limits.buckets[RateLimitedPeerMessageKey::BlockRequest] =
                Some(TokenBucket::new(1, 1, 0.0, now).unwrap());
            assert!(limits.is_allowed(&block_request, now));
        }

        // Test rate limited message without enough tokens.
        {
            let mut limits = RateLimits::default();
            limits.buckets[RateLimitedPeerMessageKey::BlockRequest] =
                Some(TokenBucket::new(0, 1, 0.0, now).unwrap());
            assert!(!limits.is_allowed(&block_request, now));
        }
    }

    #[test]
    fn configuration() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();

        config.rate_limits.insert(Block, SingleMessageConfig::new(5, 1.0, Some(1)));
        config.rate_limits.insert(BlockApproval, SingleMessageConfig::new(5, 1.0, None));
        config.rate_limits.insert(BlockHeaders, SingleMessageConfig::new(1, -4.0, None));

        let now = Instant::now();
        let mut limits = RateLimits::from_config(&config, now);

        // Bucket should exist with capacity = 1.
        assert!(!limits.buckets[Block].as_mut().unwrap().acquire(2, now));
        // Bucket should exist with capacity = 5.
        assert!(limits.buckets[BlockApproval].as_mut().unwrap().acquire(2, now));
        // Bucket should not exist due to a config error.
        assert!(limits.buckets[BlockHeaders].is_none());
        // Buckets are not instantiated for message types not present in the config.
        assert!(limits.buckets[RequestUpdateNonce].is_none());
    }

    #[test]
    fn configuration_errors() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.rate_limits.insert(Block, SingleMessageConfig::new(0, 1.0, None));
        assert!(config.validate().is_ok());

        config.rate_limits.insert(BlockApproval, SingleMessageConfig::new(0, -1.0, None));
        assert_eq!(
            config.validate(),
            Err(vec![(BlockApproval, TokenBucketError::InvalidRefillRate(-1.0))])
        );

        config.rate_limits.insert(BlockHeaders, SingleMessageConfig::new(0, -2.0, None));
        let result = config.validate();
        let error = result.expect_err("a configuration error is expected");
        assert!(error
            .iter()
            .find(|(key, err)| *key == BlockApproval
                && *err == TokenBucketError::InvalidRefillRate(-1.0))
            .is_some());
        assert!(error
            .iter()
            .find(|(key, err)| *key == BlockHeaders
                && *err == TokenBucketError::InvalidRefillRate(-2.0))
            .is_some());
    }

    #[test]
    fn buckets_get_refreshed() {
        use RateLimitedPeerMessageKey::*;
        let mut config = Config::default();
        let now = Instant::now();

        config.rate_limits.insert(Block, SingleMessageConfig::new(5, 1.0, Some(0)));
        config.rate_limits.insert(BlockApproval, SingleMessageConfig::new(5, 1.0, Some(0)));

        let mut limits = RateLimits::from_config(&config, now);

        assert!(!limits.buckets[Block].as_mut().unwrap().acquire(1, now));
        assert!(!limits.buckets[BlockApproval].as_mut().unwrap().acquire(1, now));

        let now = now + Duration::seconds(1);

        assert!(limits.buckets[Block].as_mut().unwrap().acquire(1, now));
        assert!(limits.buckets[BlockApproval].as_mut().unwrap().acquire(1, now));
    }

    #[test]
    fn apply_overrides() {
        use RateLimitedPeerMessageKey::*;

        // Create a config with three entries.
        let mut config = Config::default();
        config.rate_limits.insert(Block, SingleMessageConfig::new(1, 1.0, None));
        config.rate_limits.insert(BlockApproval, SingleMessageConfig::new(2, 1.0, None));
        config.rate_limits.insert(BlockHeaders, SingleMessageConfig::new(3, 1.0, None));

        // Override the config with the following patch:
        // - one entry is modified
        // - one entry is untouched
        // - one entry is removed
        // - one entry is added
        let mut overrides = OverrideConfig::default();
        overrides.rate_limits.insert(Block, Some(SingleMessageConfig::new(4, 1.0, None)));
        overrides.rate_limits.insert(BlockHeaders, None);
        overrides
            .rate_limits
            .insert(StateRequestHeader, Some(SingleMessageConfig::new(5, 1.0, None)));

        config.apply_overrides(overrides);
        assert_eq!(config.rate_limits.len(), 3);
        assert_eq!(config.rate_limits.get(&Block), Some(&SingleMessageConfig::new(4, 1.0, None)));
        assert_eq!(config.rate_limits.get(&BlockHeaders), None);
        assert_eq!(
            config.rate_limits.get(&StateRequestHeader),
            Some(&SingleMessageConfig::new(5, 1.0, None))
        );
    }

    #[test]
    fn override_config_deserialization() {
        use RateLimitedPeerMessageKey::*;

        // Check object with no entries.
        let json = serde_json::json!({"rate_limits": {}});
        let config: OverrideConfig =
            serde_json::from_value(json).expect("deserializing OverrideConfig should work");
        assert_eq!(config.rate_limits.len(), 0);

        // Check object with a single entry.
        let json = serde_json::json!({"rate_limits": {
            "Block": {
                "maximum_size": 1,
                "refill_rate": 1.0,
                "initial_size": 1,
            }
        }});
        let config: OverrideConfig =
            serde_json::from_value(json).expect("deserializing OverrideConfig should work");
        assert_eq!(config.rate_limits.len(), 1);
        assert!(config.rate_limits.contains_key(&Block));

        // Check object with multiple entries.
        let json = serde_json::json!({"rate_limits": {
            "Block": {
                "maximum_size": 1,
                "refill_rate": 1.0,
                "initial_size": 1,
            },
            "BlockApproval": {
                "maximum_size": 2,
                "refill_rate": 1.0,
            }
        }});
        let config: OverrideConfig =
            serde_json::from_value(json).expect("deserializing OverrideConfig should work");
        assert_eq!(config.rate_limits.len(), 2);
        assert!(config.rate_limits.contains_key(&Block));
        assert!(config.rate_limits.contains_key(&BlockApproval));

        // Check object with errors.
        let json = serde_json::json!({"rate_limits": {
            "Block": {
                "foo": 1,
            }
        }});
        assert!(serde_json::from_value::<OverrideConfig>(json).is_err());
    }

    #[test]
    fn safe_add_into_u32() {
        assert_eq!(safe_add_into_u32!(4 as u32), 4);
        assert_eq!(safe_add_into_u32!(1, 2, 3), 6);
        assert_eq!(safe_add_into_u32!(2 as u32, 0 as u32), 2);
        assert_eq!(safe_add_into_u32!(u32::MAX, u32::MAX), u32::MAX);
        assert_eq!(safe_add_into_u32!(u64::MAX, 43 as u64), u32::MAX);
        assert_eq!(safe_add_into_u32!(usize::MAX, 43 as usize, 2 as usize), u32::MAX);
    }
}
