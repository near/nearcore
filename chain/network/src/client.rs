use crate::concurrency::outgoing_queue_limiter::OutgoingPermit;
use crate::network_protocol::StateResponseInfo;
use crate::recv_permit::RecvMessagePermit;
use crate::types::{NetworkInfo, ReasonForBan};
use near_async::messaging::{AsyncSender, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::state_part::StatePartIndex;
use near_primitives::state_sync::{PartOrHeader, StateRequestAck};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochHeight, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;
use std::sync::Arc;

/// Transaction status query
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxStatusRequest {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

/// Transaction status response
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxStatusResponse(pub Box<FinalExecutionOutcomeView>);

/// Request a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRequest(pub CryptoHash);

/// Block response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockResponse {
    pub block: Arc<Block>,
    pub peer_id: PeerId,
    pub was_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockApproval(pub Approval, pub PeerId);

/// Request headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeadersRequest(pub Vec<CryptoHash>);

/// Headers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeadersResponse(pub Vec<Arc<BlockHeader>>, pub PeerId);

/// State request header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequestHeader {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

/// State request part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequestPart {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_idx: StatePartIndex,
}

/// Outgoing response to received state request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatePartOrHeader(pub Box<StateResponseInfo>);

/// The client's answer to a received state request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateRequestResponse {
    /// The client served the requested header or part.
    Respond(StatePartOrHeader),
    /// The client throttled the request.
    Busy,
    /// The client failed to serve the requested header or part.
    Error,
}

/// Update on a prior outgoing state request: a message from the host, or the network's report
/// that the request was not sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateRequestUpdate {
    Ack(StateRequestAck),
    State(Box<StateResponseInfo>),
    /// Reported by the network rather than the host: the request was dropped without being
    /// sent, e.g. because the connection to the host failed. No response will arrive for it.
    NotSent {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_or_header: PartOrHeader,
    },
}

impl StateRequestUpdate {
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::Ack(ack) => ack.shard_id,
            Self::State(state) => state.shard_id(),
            Self::NotSent { shard_id, .. } => *shard_id,
        }
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::Ack(ack) => ack.sync_hash,
            Self::State(state) => state.sync_hash(),
            Self::NotSent { sync_hash, .. } => *sync_hash,
        }
    }

    pub fn part_or_header(&self) -> PartOrHeader {
        match self {
            Self::Ack(ack) => ack.part_or_header,
            Self::State(state) => match state.part_idx() {
                Some(part_idx) => PartOrHeader::Part { part_idx },
                None => PartOrHeader::Header,
            },
            Self::NotSent { part_or_header, .. } => *part_or_header,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequestUpdateMessage {
    pub peer_id: PeerId,
    pub update: StateRequestUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetNetworkInfo(pub NetworkInfo);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTxRequest {
    pub transaction: SignedTransaction,
    pub is_forwarded: bool,
    pub check_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessTxResponse {
    /// The node dropped the transaction without acting on it (e.g. a forwarded transaction
    /// that this node will neither relay nor include).
    Dropped,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// The node being queried does not track the shard needed and therefore cannot provide useful
    /// response.
    DoesNotTrackShard,
    /// Processing the transaction failed with an internal error; the string carries debug context.
    InternalError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkEndorsementMessage(pub ChunkEndorsement);

#[derive(Debug)]
pub struct SpiceChunkEndorsementMessage(pub SpiceChunkEndorsement, pub RecvMessagePermit);

#[derive(Debug)]
pub struct EpochSyncRequestMessage {
    pub from_peer: PeerId,
    pub recv_permit: RecvMessagePermit,
    /// Outgoing-queue reservation for the response message. Acquired in
    /// the network layer when this request arrived. The client holds it
    /// through proof derivation and uses it when sending the response.
    pub response_permit: OutgoingPermit,
}

#[derive(Debug)]
pub struct EpochSyncResponseMessage {
    pub from_peer: PeerId,
    pub proof: CompressedEpochSyncProof,
    pub recv_permit: RecvMessagePermit,
}

#[derive(Debug)]
pub struct OptimisticBlockMessage {
    pub optimistic_block: OptimisticBlock,
    pub from_peer: PeerId,
    pub recv_permit: RecvMessagePermit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetCurrentEpochHeight;

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForNetwork {
    pub tx_status_request: AsyncSender<TxStatusRequest, Option<Box<FinalExecutionOutcomeView>>>,
    pub tx_status_response: AsyncSender<TxStatusResponse, ()>,
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
    pub state_request_update: AsyncSender<SpanWrapped<StateRequestUpdateMessage>, ()>,
    pub block_approval: AsyncSender<SpanWrapped<BlockApproval>, ()>,
    pub block_request: AsyncSender<BlockRequest, Option<Arc<Block>>>,
    pub block_headers_request: AsyncSender<BlockHeadersRequest, Option<Vec<Arc<BlockHeader>>>>,
    pub block: AsyncSender<SpanWrapped<BlockResponse>, ()>,
    pub block_headers: AsyncSender<SpanWrapped<BlockHeadersResponse>, Result<(), ReasonForBan>>,
    pub network_info: AsyncSender<SpanWrapped<SetNetworkInfo>, ()>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
    pub epoch_sync_request: Sender<EpochSyncRequestMessage>,
    pub epoch_sync_response: Sender<EpochSyncResponseMessage>,
    pub optimistic_block_receiver: Sender<SpanWrapped<OptimisticBlockMessage>>,
    pub current_epoch_height_request: AsyncSender<GetCurrentEpochHeight, Option<EpochHeight>>,
}
