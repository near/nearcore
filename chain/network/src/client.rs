use crate::network_protocol::StateResponseInfo;
use crate::types::{NetworkInfo, ReasonForBan};
use near_async::messaging::AsyncSender;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::stateless_validation::{
    ChunkEndorsement, ChunkStateWitnessAck, SignedEncodedChunkStateWitness,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

/// Transaction status query
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Option<Box<FinalExecutionOutcomeView>>")]
pub struct TxStatusRequest {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

/// Transaction status response
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct TxStatusResponse(pub Box<FinalExecutionOutcomeView>);

/// Request a block.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Option<Box<Block>>")]
pub struct BlockRequest(pub CryptoHash);

/// Block response.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct BlockResponse {
    pub block: Block,
    pub peer_id: PeerId,
    pub was_requested: bool,
}

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct BlockApproval(pub Approval, pub PeerId);

/// Request headers.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Option<Vec<BlockHeader>>")]
pub struct BlockHeadersRequest(pub Vec<CryptoHash>);

/// Headers response.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Result<(),ReasonForBan>")]
pub struct BlockHeadersResponse(pub Vec<BlockHeader>, pub PeerId);

/// State request header.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Option<StateResponse>")]
pub struct StateRequestHeader {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

/// State request part.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Option<StateResponse>")]
pub struct StateRequestPart {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_id: u64,
}

/// Response to state request.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct StateResponse(pub Box<StateResponseInfo>);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct SetNetworkInfo(pub NetworkInfo);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct RecvChallenge(pub Challenge);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "ProcessTxResponse")]
pub struct ProcessTxRequest {
    pub transaction: SignedTransaction,
    pub is_forwarded: bool,
    pub check_only: bool,
}

#[derive(actix::MessageResponse, Debug, Clone, PartialEq, Eq)]
pub enum ProcessTxResponse {
    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// The node being queried does not track the shard needed and therefore cannot provide userful
    /// response.
    DoesNotTrackShard,
}

/// Account announcements that needs to be validated before being processed.
/// They are paired with last epoch id known to this announcement, in order to accept only
/// newer announcements.
#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "Result<Vec<AnnounceAccount>,ReasonForBan>")]
pub struct AnnounceAccountRequest(pub Vec<(AnnounceAccount, Option<EpochId>)>);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessMessage(pub SignedEncodedChunkStateWitness);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(actix::Message, Debug, Clone, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkEndorsementMessage(pub ChunkEndorsement);

#[derive(
    Clone, near_async::MultiSend, near_async::MultiSenderFrom, near_async::MultiSendMessage,
)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSenderForNetwork {
    pub tx_status_request: AsyncSender<TxStatusRequest, Option<Box<FinalExecutionOutcomeView>>>,
    pub tx_status_response: AsyncSender<TxStatusResponse, ()>,
    pub state_request_header: AsyncSender<StateRequestHeader, Option<StateResponse>>,
    pub state_request_part: AsyncSender<StateRequestPart, Option<StateResponse>>,
    pub state_response: AsyncSender<StateResponse, ()>,
    pub block_approval: AsyncSender<BlockApproval, ()>,
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
    pub block_request: AsyncSender<BlockRequest, Option<Box<Block>>>,
    pub block_headers_request: AsyncSender<BlockHeadersRequest, Option<Vec<BlockHeader>>>,
    pub block: AsyncSender<BlockResponse, ()>,
    pub block_headers: AsyncSender<BlockHeadersResponse, Result<(), ReasonForBan>>,
    pub challenge: AsyncSender<RecvChallenge, ()>,
    pub network_info: AsyncSender<SetNetworkInfo, ()>,
    pub announce_account:
        AsyncSender<AnnounceAccountRequest, Result<Vec<AnnounceAccount>, ReasonForBan>>,
    pub chunk_state_witness: AsyncSender<ChunkStateWitnessMessage, ()>,
    pub chunk_state_witness_ack: AsyncSender<ChunkStateWitnessAckMessage, ()>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
}
