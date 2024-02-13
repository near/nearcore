use crate::network_protocol::StateResponseInfo;

use crate::types::{NetworkInfo, ReasonForBan};

use near_async::actix::AsyncSendError;
use near_async::messaging::{AsyncSender, SendAsync};
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::stateless_validation::{ChunkEndorsement, ChunkStateWitness};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

/// Transaction status query
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<Box<FinalExecutionOutcomeView>>")]
pub struct TxStatusRequest {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

/// Transaction status response
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct TxStatusResponse(pub Box<FinalExecutionOutcomeView>);

/// Request a block.
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<Box<Block>>")]
pub struct BlockRequest(pub CryptoHash);

/// Block response.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct BlockResponse {
    pub block: Block,
    pub peer_id: PeerId,
    pub was_requested: bool,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct BlockApproval(pub Approval, pub PeerId);

/// Request headers.
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<Vec<BlockHeader>>")]
pub struct BlockHeadersRequest(pub Vec<CryptoHash>);

/// Headers response.
#[derive(actix::Message, Debug)]
#[rtype(result = "Result<(),ReasonForBan>")]
pub struct BlockHeadersResponse(pub Vec<BlockHeader>, pub PeerId);

/// State request header.
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<StateResponse>")]
pub struct StateRequestHeader {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

/// State request part.
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<StateResponse>")]
pub struct StateRequestPart {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_id: u64,
}

/// Response to state request.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct StateResponse(pub Box<StateResponseInfo>);

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct SetNetworkInfo(pub NetworkInfo);

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct RecvChallenge(pub Challenge);

#[derive(actix::Message, Debug)]
#[rtype(result = "ProcessTxResponse")]
pub struct ProcessTxRequest {
    pub transaction: SignedTransaction,
    pub is_forwarded: bool,
    pub check_only: bool,
}

#[derive(actix::MessageResponse, Debug, PartialEq, Eq)]
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
#[derive(actix::Message, Debug)]
#[rtype(result = "Result<Vec<AnnounceAccount>,ReasonForBan>")]
pub struct AnnounceAccountRequest(pub Vec<(AnnounceAccount, Option<EpochId>)>);

#[derive(actix::Message, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessMessage {
    pub witness: ChunkStateWitness,
    pub peer_id: PeerId,
    pub attempts_remaining: usize,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ChunkEndorsementMessage(pub ChunkEndorsement);

#[derive(
    Clone, near_async::MultiSend, near_async::MultiSenderFrom, near_async::MultiSendMessage,
)]
pub struct ClientSenderForNetwork {
    pub tx_status_request: AsyncSender<
        TxStatusRequest,
        Result<Option<Box<FinalExecutionOutcomeView>>, AsyncSendError>,
    >,
    pub tx_status_response: AsyncSender<TxStatusResponse, Result<(), AsyncSendError>>,
    pub state_request_header:
        AsyncSender<StateRequestHeader, Result<Option<StateResponse>, AsyncSendError>>,
    pub state_request_part:
        AsyncSender<StateRequestPart, Result<Option<StateResponse>, AsyncSendError>>,
    pub state_response: AsyncSender<StateResponse, Result<(), AsyncSendError>>,
    pub block_approval: AsyncSender<BlockApproval, Result<(), AsyncSendError>>,
    pub transaction: AsyncSender<ProcessTxRequest, Result<ProcessTxResponse, AsyncSendError>>,
    pub block_request: AsyncSender<BlockRequest, Result<Option<Box<Block>>, AsyncSendError>>,
    pub block_headers_request:
        AsyncSender<BlockHeadersRequest, Result<Option<Vec<BlockHeader>>, AsyncSendError>>,
    pub block: AsyncSender<BlockResponse, Result<(), AsyncSendError>>,
    pub block_headers:
        AsyncSender<BlockHeadersResponse, Result<Result<(), ReasonForBan>, AsyncSendError>>,
    pub challenge: AsyncSender<RecvChallenge, Result<(), AsyncSendError>>,
    pub network_info: AsyncSender<SetNetworkInfo, Result<(), AsyncSendError>>,
    pub announce_account: AsyncSender<
        AnnounceAccountRequest,
        Result<Result<Vec<AnnounceAccount>, ReasonForBan>, AsyncSendError>,
    >,
    pub chunk_state_witness: AsyncSender<ChunkStateWitnessMessage, Result<(), AsyncSendError>>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, Result<(), AsyncSendError>>,
}

impl ClientSenderForNetwork {
    pub async fn tx_status_request(
        &self,
        account_id: AccountId,
        tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>> {
        self.send_async(TxStatusRequest { tx_hash, signer_account_id: account_id })
            .await
            .ok()
            .flatten()
    }

    pub async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView) {
        self.send_async(TxStatusResponse(Box::new(tx_result))).await.ok();
    }

    pub async fn state_request_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Option<StateResponseInfo> {
        self.send_async(StateRequestHeader { shard_id, sync_hash })
            .await
            .ok()
            .flatten()
            .map(|response| *response.0)
    }

    pub async fn state_request_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
    ) -> Option<StateResponseInfo> {
        self.send_async(StateRequestPart { shard_id, sync_hash, part_id })
            .await
            .ok()
            .flatten()
            .map(|response| *response.0)
    }

    pub async fn state_response(&self, info: StateResponseInfo) {
        self.send_async(StateResponse(Box::new(info))).await.ok();
    }

    pub async fn block_approval(&self, approval: Approval, peer_id: PeerId) {
        self.send_async(BlockApproval(approval, peer_id)).await.ok();
    }

    pub async fn transaction(&self, transaction: SignedTransaction, is_forwarded: bool) {
        self.send_async(ProcessTxRequest { transaction, is_forwarded, check_only: false })
            .await
            .ok();
    }

    pub async fn block_request(&self, hash: CryptoHash) -> Option<Box<Block>> {
        self.send_async(BlockRequest(hash)).await.ok().flatten()
    }

    pub async fn block_headers_request(&self, hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        self.send_async(BlockHeadersRequest(hashes)).await.ok().flatten()
    }

    pub async fn block(&self, block: Block, peer_id: PeerId, was_requested: bool) {
        self.send_async(BlockResponse { block, peer_id, was_requested }).await.ok();
    }

    pub async fn block_headers(
        &self,
        headers: Vec<BlockHeader>,
        peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        self.send_async(BlockHeadersResponse(headers, peer_id)).await.unwrap_or(Ok(()))
    }

    pub async fn challenge(&self, challenge: Challenge) {
        self.send_async(RecvChallenge(challenge)).await.ok();
    }

    pub async fn network_info(&self, info: NetworkInfo) {
        self.send_async(SetNetworkInfo(info)).await.ok();
    }

    pub async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        self.send_async(AnnounceAccountRequest(accounts)).await.unwrap_or(Ok(vec![]))
    }

    pub async fn chunk_state_witness(&self, witness: ChunkStateWitness, peer_id: PeerId) {
        self.send_async(ChunkStateWitnessMessage { witness, peer_id, attempts_remaining: 5 })
            .await
            .ok();
    }

    pub async fn chunk_endorsement(&self, endorsement: ChunkEndorsement) {
        self.send_async(ChunkEndorsementMessage(endorsement)).await.ok();
    }
}
