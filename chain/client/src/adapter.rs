use crate::client_actor::ClientActor;
use crate::view_client::ViewClientActor;
use near_network::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkInfo, PartialEncodedChunkForwardMsg,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, ReasonForBan, StateResponseInfo,
};
//use near_o11y::{WithSpanContext, WithSpanContextExt};
use near_network::time;
use near_o11y::WithSpanContextExt;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

/// Transaction status query
#[derive(actix::Message)]
#[rtype(result = "Option<Box<FinalExecutionOutcomeView>>")]
pub(crate) struct TxStatusRequest {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

/// Transaction status response
#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct TxStatusResponse(pub Box<FinalExecutionOutcomeView>);

/// Request a block.
#[derive(actix::Message)]
#[rtype(result = "Option<Box<Block>>")]
pub(crate) struct BlockRequest(pub CryptoHash);

/// Block response.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct BlockResponse(pub Box<Block>);

/// Request headers.
#[derive(actix::Message)]
#[rtype(result = "Option<Vec<BlockHeader>>")]
pub(crate) struct BlockHeadersRequest(pub Vec<CryptoHash>);

/// Headers response.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct BlockHeadersResponse(pub Vec<BlockHeader>);

/// State request header.
#[derive(actix::Message)]
#[rtype(result = "Option<StateResponse>")]
pub(crate) struct StateRequestHeader {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

/// State request part.
#[derive(actix::Message)]
#[rtype(result = "Option<StateResponse>")]
pub(crate) struct StateRequestPart {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_id: u64,
}

/// Response to state request.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct StateResponse(pub Box<StateResponseInfo>);

/// Account announcements that needs to be validated before being processed.
/// They are paired with last epoch id known to this announcement, in order to accept only
/// newer announcements.
#[derive(actix::Message)]
#[rtype(result = "Result<Vec<AnnounceAccount>,ReasonForBan>")]
pub(crate) struct AnnounceAccountRequest(pub Vec<(AnnounceAccount, Option<EpochId>)>);

pub struct Adapter {
    /// Address of the client actor.
    client_addr: actix::Addr<ClientActor>,
    /// Address of the view client actor.
    view_client_addr: actix::Addr<ViewClientActor>,
}

impl Adapter {
    pub fn new(
        client_addr: actix::Addr<ClientActor>,
        view_client_addr: actix::Addr<ViewClientActor>,
    ) -> Self {
        Self { client_addr, view_client_addr }
    }
}

#[async_trait::async_trait]
impl near_network::client::Client for Adapter {
    async fn tx_status_request(
        &self,
        account_id: AccountId,
        tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>> {
        match self
            .view_client_addr
            .send(
                TxStatusRequest { tx_hash: tx_hash, signer_account_id: account_id }
                    .with_span_context(),
            )
            .await
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView) {
        match self
            .view_client_addr
            .send(TxStatusResponse(Box::new(tx_result.clone())).with_span_context())
            .await
        {
            Ok(()) => {}
            Err(err) => {
                tracing::error!("mailbox error: {err}");
            }
        }
    }

    async fn state_request_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        match self
            .view_client_addr
            .send(
                StateRequestHeader { shard_id: shard_id, sync_hash: sync_hash }.with_span_context(),
            )
            .await
        {
            Ok(Some(StateResponse(resp))) => Ok(Some(*resp)),
            Ok(None) => Ok(None),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    async fn state_request_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        match self
            .view_client_addr
            .send(
                StateRequestPart { shard_id: shard_id, sync_hash: sync_hash, part_id: part_id }
                    .with_span_context(),
            )
            .await
        {
            Ok(Some(StateResponse(resp))) => Ok(Some(*resp)),
            Ok(None) => Ok(None),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    async fn state_response(&self, info: StateResponseInfo) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::StateResponse(info).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn block_approval(
        &self,
        approval: Approval,
        peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::BlockApproval(approval, peer_id).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn transaction(
        &self,
        transaction: SignedTransaction,
        is_forwarded: bool,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(
                NetworkClientMessages::Transaction { transaction, is_forwarded, check_only: false }
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkClientResponses::ValidTx) => Ok(()),
            Ok(NetworkClientResponses::InvalidTx(err)) => {
                tracing::warn!(target: "network", ?err, "Received invalid tx");
                // TODO: count as malicious behavior?
                Ok(())
            }
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::RequestRouted) => Ok(()),
            Ok(NetworkClientResponses::DoesNotTrackShard) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            #[allow(unreachable_patterns)]
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn partial_encoded_chunk_request(
        &self,
        req: PartialEncodedChunkRequestMsg,
        msg_hash: CryptoHash,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(
                NetworkClientMessages::PartialEncodedChunkRequest(req, msg_hash)
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn partial_encoded_chunk_response(
        &self,
        resp: PartialEncodedChunkResponseMsg,
        timestamp: time::Instant,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(
                NetworkClientMessages::PartialEncodedChunkResponse(resp, timestamp.into())
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn partial_encoded_chunk(&self, chunk: PartialEncodedChunk) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::PartialEncodedChunk(chunk).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn partial_encoded_chunk_forward(
        &self,
        msg: PartialEncodedChunkForwardMsg,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::PartialEncodedChunkForward(msg).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn block_request(&self, hash: CryptoHash) -> Option<Box<Block>> {
        match self.view_client_addr.send(BlockRequest(hash).with_span_context()).await {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn block_headers_request(&self, hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        match self.view_client_addr.send(BlockHeadersRequest(hashes).with_span_context()).await {
            Ok(headers) => headers,
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn block(
        &self,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::Block(block, peer_id, was_requested).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn block_headers(
        &self,
        headers: Vec<BlockHeader>,
        peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::BlockHeaders(headers, peer_id).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn challenge(&self, challenge: Challenge) -> Result<(), ReasonForBan> {
        match self
            .client_addr
            .send(NetworkClientMessages::Challenge(challenge).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    async fn network_info(&self, info: NetworkInfo) {
        match self
            .client_addr
            .send(NetworkClientMessages::NetworkInfo(info).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        match self.view_client_addr.send(AnnounceAccountRequest(accounts).with_span_context()).await
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(vec![])
            }
        }
    }
}
