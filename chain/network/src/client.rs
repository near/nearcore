use crate::network_protocol::StateResponseInfo;
use crate::types::{NetworkInfo, ReasonForBan};
use near_async::messaging::{AsyncSender, Sender};
use near_async::span_wrapped_msg::SpanWrapped;
use near_async::{Message, MultiSend, MultiSenderFrom};
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::state_sync::{PartIdOrHeader, StateRequestAck};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;
use std::sync::Arc;

/// Transaction status query
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct TxStatusRequest {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

/// Transaction status response
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct TxStatusResponse(pub Box<FinalExecutionOutcomeView>);

/// Request a block.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct BlockRequest(pub CryptoHash);

/// Block response.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct BlockResponse {
    pub block: Arc<Block>,
    pub peer_id: PeerId,
    pub was_requested: bool,
}

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct BlockApproval(pub Approval, pub PeerId);

/// Request headers.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct BlockHeadersRequest(pub Vec<CryptoHash>);

/// Headers response.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct BlockHeadersResponse(pub Vec<Arc<BlockHeader>>, pub PeerId);

/// State request header.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct StateRequestHeader {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

/// State request part.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct StateRequestPart {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_id: u64,
}

/// Outgoing response to received state request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatePartOrHeader(pub Box<StateResponseInfo>);

/// Incoming response to a prior outgoing state request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateResponse {
    Ack(StateRequestAck),
    State(Box<StateResponseInfo>),
}

impl StateResponse {
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::Ack(ack) => ack.shard_id,
            Self::State(state) => state.shard_id(),
        }
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::Ack(ack) => ack.sync_hash,
            Self::State(state) => state.sync_hash(),
        }
    }

    pub fn part_id_or_header(&self) -> PartIdOrHeader {
        match self {
            Self::Ack(ack) => ack.part_id_or_header,
            Self::State(state) => match state.part_id() {
                Some(part_id) => PartIdOrHeader::Part { part_id },
                None => PartIdOrHeader::Header,
            },
        }
    }
}

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct StateResponseReceived {
    pub peer_id: PeerId,
    pub state_response: StateResponse,
}

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct SetNetworkInfo(pub NetworkInfo);

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct ProcessTxRequest {
    pub transaction: SignedTransaction,
    pub is_forwarded: bool,
    pub check_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessTxResponse {
    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// The node being queried does not track the shard needed and therefore cannot provide useful
    /// response.
    DoesNotTrackShard,
}

/// Account announcements that needs to be validated before being processed.
/// They are paired with last epoch id known to this announcement, in order to accept only
/// newer announcements.
#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct AnnounceAccountRequest(pub Vec<(AnnounceAccount, Option<EpochId>)>);

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct ChunkEndorsementMessage(pub ChunkEndorsement);

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct SpiceChunkEndorsementMessage(pub SpiceChunkEndorsement);

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct EpochSyncRequestMessage {
    pub from_peer: PeerId,
}

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct EpochSyncResponseMessage {
    pub from_peer: PeerId,
    pub proof: CompressedEpochSyncProof,
}

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct OptimisticBlockMessage {
    pub optimistic_block: OptimisticBlock,
    pub from_peer: PeerId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForNetwork {
    pub tx_status_request: AsyncSender<TxStatusRequest, Option<Box<FinalExecutionOutcomeView>>>,
    pub tx_status_response: AsyncSender<TxStatusResponse, ()>,
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
    pub state_response: AsyncSender<SpanWrapped<StateResponseReceived>, ()>,
    pub block_approval: AsyncSender<SpanWrapped<BlockApproval>, ()>,
    pub block_request: AsyncSender<BlockRequest, Option<Arc<Block>>>,
    pub block_headers_request: AsyncSender<BlockHeadersRequest, Option<Vec<Arc<BlockHeader>>>>,
    pub block: AsyncSender<SpanWrapped<BlockResponse>, ()>,
    pub block_headers: AsyncSender<SpanWrapped<BlockHeadersResponse>, Result<(), ReasonForBan>>,
    pub network_info: AsyncSender<SpanWrapped<SetNetworkInfo>, ()>,
    pub announce_account:
        AsyncSender<AnnounceAccountRequest, Result<Vec<AnnounceAccount>, ReasonForBan>>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
    pub spice_chunk_endorsement: AsyncSender<SpiceChunkEndorsementMessage, ()>,
    pub epoch_sync_request: Sender<EpochSyncRequestMessage>,
    pub epoch_sync_response: Sender<EpochSyncResponseMessage>,
    pub optimistic_block_receiver: Sender<SpanWrapped<OptimisticBlockMessage>>,
}
