//! Contains files used for a few different purposes:
//! - Changes related to network config - TODO - move to another file
//! - actix messages - used for communicating with `PeerManagerActor` - TODO move to another file
//! - internal types used by `peer-store` only - TODO move to `peer_store.rs`
//! - some types used for different purposes that don't meet any of the criteria above
//! - unused code - TODO remove?
//! - Some types types that are neither of the above
//!
//! NOTE:
//! - We also export publicly types from `crate::network_protocol`
use crate::time;
use actix::Message;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_crypto::SecretKey;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Exported types, which are part of network protocol.
pub use crate::network_protocol::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    PeerChainInfo, PeerChainInfoV2, PeerIdOrHash, PeerInfo, Ping, Pong, RoutedMessage,
    RoutedMessageBody, RoutedMessageV2, StateResponseInfo, StateResponseInfoV1,
    StateResponseInfoV2,
};

pub use crate::blacklist::{Blacklist, Entry as BlacklistEntry};
pub use crate::config::{NetworkConfig, ValidatorConfig, ValidatorEndpoints};
pub use crate::config_json::Config as ConfigJSON;
pub use crate::network_protocol::edge::{Edge, EdgeState, PartialEdgeInfo};

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;
/// On every message from peer don't update `last_time_received_message`
/// but wait some "small" timeout between updates to avoid a lot of messages between
/// Peer and PeerManager.
pub const UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE: std::time::Duration =
    std::time::Duration::from_secs(60);
/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_NUM_PEERS: usize = 128;

/// Peer type.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, strum::IntoStaticStr)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

// Don't need Borsh ?
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash)]
/// Defines the destination for a network request.
/// The request should be sent either to the `account_id` as a routed message, or directly to
/// any peer that tracks the shard.
/// If `prefer_peer` is `true`, should be sent to the peer, unless no peer tracks the shard, in which
/// case fall back to sending to the account.
/// Otherwise, send to the account, unless we do not know the route, in which case send to the peer.
pub struct AccountIdOrPeerTrackingShard {
    /// Target account to send the the request to
    pub account_id: Option<AccountId>,
    /// Whether to check peers first or target account first
    pub prefer_peer: bool,
    /// Select peers that track shard `shard_id`
    pub shard_id: ShardId,
    /// Select peers that are archival nodes if it is true
    pub only_archival: bool,
    /// Only send messages to peers whose latest chain height is no less `min_height`
    pub min_height: BlockHeight,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash, Serialize)]
pub enum AccountOrPeerIdOrHash {
    AccountId(AccountId),
    PeerId(PeerId),
    Hash(CryptoHash),
}

impl AccountOrPeerIdOrHash {
    fn peer_id_or_hash(&self) -> Option<PeerIdOrHash> {
        match self {
            AccountOrPeerIdOrHash::AccountId(_) => None,
            AccountOrPeerIdOrHash::PeerId(peer_id) => Some(PeerIdOrHash::PeerId(peer_id.clone())),
            AccountOrPeerIdOrHash::Hash(hash) => Some(PeerIdOrHash::Hash(*hash)),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct RawRoutedMessage {
    pub target: AccountOrPeerIdOrHash,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(
        self,
        author: PeerId,
        secret_key: &SecretKey,
        routed_message_ttl: u8,
        now: Option<time::Utc>,
    ) -> Box<RoutedMessageV2> {
        let target = self.target.peer_id_or_hash().unwrap();
        let hash = RoutedMessage::build_hash(&target, &author, &self.body);
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessageV2 {
            msg: RoutedMessage {
                target,
                author,
                signature,
                ttl: routed_message_ttl,
                body: self.body,
            },
            created_at: now,
        }
        .into()
    }
}

/// Routed Message wrapped with previous sender of the message.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "bool")]
pub struct RoutedMessageFrom {
    /// Routed messages.
    pub msg: Box<RoutedMessageV2>,
    /// Previous hop in the route. Used for messages that needs routing back.
    pub from: PeerId,
}

/// Status of the known peers.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, time::Utc),
}

impl KnownPeerStatus {
    pub fn is_banned(&self) -> bool {
        matches!(self, KnownPeerStatus::Banned(_, _))
    }
}

/// Information node stores about known peers.
#[derive(Debug, Clone)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: time::Utc,
    pub last_seen: time::Utc,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo, now: time::Utc) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: now,
            last_seen: now,
        }
    }
}

/// Actor message that holds the TCP stream from an inbound TCP connection
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for InboundTcpConnect {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

/// Ban reason.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Copy)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
    Abusive = 6,
    InvalidSignature = 7,
    InvalidPeerId = 8,
    InvalidHash = 9,
    InvalidEdge = 10,
    EpochSyncNoResponse = 11,
    EpochSyncInvalidResponse = 12,
    EpochSyncInvalidFinalizationResponse = 13,
    Blacklisted = 14,
}

/// Banning signal sent from Peer instance to PeerManager
/// just before Peer instance is stopped.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

/// Messages from PeerManager to Peer
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerManagerRequest {
    BanPeer(ReasonForBan),
    UnregisterPeer,
}

/// Messages from PeerManager to Peer with a tracing Context.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PeerManagerRequestWithContext {
    pub msg: PeerManagerRequest,
    pub context: opentelemetry::Context,
}

#[derive(Debug, Clone)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
    pub next_hops: Option<Vec<PeerId>>,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for KnownProducer {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.account_id.deep_size_of_children(context) + self.peer_id.deep_size_of_children(context)
    }
}

#[cfg(feature = "test_features")]
#[derive(Debug)]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvSwitchToHeight(u64),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvCheckStorageConsistency,
    AdvSetSyncInfo(u64),
}

#[derive(actix::Message, strum::IntoStaticStr)]
#[rtype(result = "NetworkViewClientResponses")]
pub enum NetworkViewClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(NetworkAdversarialMessage),

    /// Transaction status query
    TxStatus { tx_hash: CryptoHash, signer_account_id: AccountId },
    /// Transaction status response
    TxStatusResponse(Box<FinalExecutionOutcomeView>),
    /// Request for receipt outcome
    ReceiptOutcomeRequest(CryptoHash),
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// State request header.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash },
    /// State request part.
    StateRequestPart { shard_id: ShardId, sync_hash: CryptoHash, part_id: u64 },
    /// A request for a light client info during Epoch Sync
    EpochSyncRequest { epoch_id: EpochId },
    /// A request for headers and proofs during Epoch Sync
    EpochSyncFinalizationRequest { epoch_id: EpochId },
    /// Account announcements that needs to be validated before being processed.
    /// They are paired with last epoch id known to this announcement, in order to accept only
    /// newer announcements.
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
}

/// Set of account keys.
/// This is information which chain pushes to network to implement tier1.
/// See ChainInfo.
pub type AccountKeys = HashMap<(EpochId, AccountId), PublicKey>;

/// Network-relevant data about the chain.
// TODO(gprusak): it is more like node info, or sth.
#[derive(Debug, Clone, Default)]
pub struct ChainInfo {
    pub tracked_shards: Vec<ShardId>,
    pub height: BlockHeight,
    // Public keys of accounts participating in the BFT consensus
    // (both accounts from current and next epoch are important, that's why
    // the map is indexed by (EpochId,AccountId) pair).
    // It currently includes "block producers", "chunk producers" and "approvers".
    // They are collectively known as "validators".
    // Peers acting on behalf of these accounts have a higher
    // priority on the NEAR network than other peers.
    pub tier1_accounts: Arc<AccountKeys>,
}

#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
pub struct SetChainInfo(pub ChainInfo);

#[derive(Debug, actix::MessageResponse)]
pub enum NetworkViewClientResponses {
    /// Transaction execution outcome
    TxStatus(Box<FinalExecutionOutcomeView>),
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Block response.
    Block(Box<Block>),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Response to state request.
    StateResponse(Box<StateResponseInfo>),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
    /// A response to a request for a light client block during Epoch Sync
    EpochSyncResponse(Box<EpochSyncResponse>),
    /// A response to a request for headers and proofs during Epoch Sync
    EpochSyncFinalizationResponse(Box<EpochSyncFinalizationResponse>),
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
    /// Response not needed
    NoResponse,
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::syncing::ShardStateSyncResponseV1;

    // NOTE: this has it's counterpart in `near_network::types::tests`
    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_enum_size() {
        assert_size!(PeerType);
        assert_size!(RoutedMessageBody);
        assert_size!(PeerIdOrHash);
        assert_size!(KnownPeerStatus);
        assert_size!(ReasonForBan);
        assert_size!(PeerManagerRequest);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(AnnounceAccount);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessage);
        assert_size!(RoutedMessageFrom);
        assert_size!(KnownPeerState);
        assert_size!(InboundTcpConnect);
        assert_size!(OutboundTcpConnect);
        assert_size!(Ban);
        assert_size!(StateResponseInfoV1);
        assert_size!(PartialEncodedChunkRequestMsg);
    }

    #[test]
    fn routed_message_body_compatibility_smoke_test() {
        #[track_caller]
        fn check(msg: RoutedMessageBody, expected: &[u8]) {
            let actual = msg.try_to_vec().unwrap();
            assert_eq!(actual.as_slice(), expected);
        }

        check(
            RoutedMessageBody::TxStatusRequest("test_x".parse().unwrap(), CryptoHash([42; 32])),
            &[
                2, 6, 0, 0, 0, 116, 101, 115, 116, 95, 120, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42,
            ],
        );

        check(
            RoutedMessageBody::VersionedStateResponse(StateResponseInfo::V1(StateResponseInfoV1 {
                shard_id: 62,
                sync_hash: CryptoHash([92; 32]),
                state_response: ShardStateSyncResponseV1 { header: None, part: None },
            })),
            &[
                17, 0, 62, 0, 0, 0, 0, 0, 0, 0, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92,
                92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 0, 0,
            ],
        );
    }
}
