use crate::client_actor::ClientActor;
use crate::view_client::ViewClientActor;
use near_network::time;
use near_network::types::{
    NetworkInfo, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, ReasonForBan, StateResponseInfo,
};
use near_o11y::WithSpanContextExt;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

#[derive(actix::Message, Debug, strum::AsRefStr, strum::IntoStaticStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
#[rtype(result = "NetworkClientResponses")]
pub enum NetworkClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(near_network::types::NetworkAdversarialMessage),

    /// Received transaction.
    Transaction {
        transaction: SignedTransaction,
        /// Whether the transaction is forwarded from other nodes.
        is_forwarded: bool,
        /// Whether the transaction needs to be submitted.
        check_only: bool,
    },
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Block approval.
    BlockApproval(Approval, PeerId),
    /// State response.
    StateResponse(StateResponseInfo),

    /// Request chunk parts and/or receipts.
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg, CryptoHash),
    /// Response to a request for  chunk parts and/or receipts.
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg, std::time::Instant),
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunk(PartialEncodedChunk),
    /// Forwarding parts to those tracking the shard (so they don't need to send requests)
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),

    /// A challenge to invalidate the block.
    Challenge(Challenge),

    NetworkInfo(NetworkInfo),
}

// TODO(#1313): Use Box
#[derive(Eq, PartialEq, Debug, actix::MessageResponse)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientResponses {
    /// Adv controls.
    #[cfg(feature = "test_features")]
    AdvResult(u64),

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
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
}

#[derive(actix::Message, strum::IntoStaticStr)]
#[rtype(result = "NetworkViewClientResponses")]
pub enum NetworkViewClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(near_network::types::NetworkAdversarialMessage),

    /// Transaction status query
    TxStatus { tx_hash: CryptoHash, signer_account_id: AccountId },
    /// Transaction status response
    TxStatusResponse(Box<FinalExecutionOutcomeView>),
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

#[derive(Debug, actix::MessageResponse)]
pub enum NetworkViewClientResponses {
    /// Transaction execution outcome
    TxStatus(Box<FinalExecutionOutcomeView>),
    /// Block response.
    Block(Box<Block>),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Response to state request.
    StateResponse(Box<StateResponseInfo>),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
    /// Response not needed
    NoResponse,
}

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
                NetworkViewClientMessages::TxStatus {
                    tx_hash: tx_hash,
                    signer_account_id: account_id,
                }
                .with_span_context(),
            )
            .await
        {
            Ok(NetworkViewClientResponses::TxStatus(tx_result)) => Some(tx_result),
            Ok(NetworkViewClientResponses::NoResponse) => None,
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView) {
        match self
            .view_client_addr
            .send(
                NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result.clone()))
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkViewClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
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
                NetworkViewClientMessages::StateRequestHeader {
                    shard_id: shard_id,
                    sync_hash: sync_hash,
                }
                .with_span_context(),
            )
            .await
        {
            Ok(NetworkViewClientResponses::StateResponse(resp)) => Ok(Some(*resp)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
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
                NetworkViewClientMessages::StateRequestPart {
                    shard_id: shard_id,
                    sync_hash: sync_hash,
                    part_id: part_id,
                }
                .with_span_context(),
            )
            .await
        {
            Ok(NetworkViewClientResponses::StateResponse(resp)) => Ok(Some(*resp)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    async fn state_response(&self, info: StateResponseInfo) {
        match self
            .client_addr
            .send(NetworkClientMessages::StateResponse(info).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn block_approval(&self, approval: Approval, peer_id: PeerId) {
        match self
            .client_addr
            .send(NetworkClientMessages::BlockApproval(approval, peer_id).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn transaction(&self, transaction: SignedTransaction, is_forwarded: bool) {
        match self
            .client_addr
            .send(
                NetworkClientMessages::Transaction { transaction, is_forwarded, check_only: false }
                    .with_span_context(),
            )
            .await
        {
            // Almost all variants of NetworkClientResponse are used only in response
            // to NetworkClientMessages::Transaction (except for Ban). It will be clearer
            // once NetworkClientMessage is split into separate requests.
            Ok(resp @ NetworkClientResponses::Ban { .. }) => {
                panic!("unexpected ClientResponse: {resp:?}")
            }
            Ok(_) => {}
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn partial_encoded_chunk_request(
        &self,
        req: PartialEncodedChunkRequestMsg,
        msg_hash: CryptoHash,
    ) {
        match self
            .client_addr
            .send(
                NetworkClientMessages::PartialEncodedChunkRequest(req, msg_hash)
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn partial_encoded_chunk_response(
        &self,
        resp: PartialEncodedChunkResponseMsg,
        timestamp: time::Instant,
    ) {
        match self
            .client_addr
            .send(
                NetworkClientMessages::PartialEncodedChunkResponse(resp, timestamp.into())
                    .with_span_context(),
            )
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn partial_encoded_chunk(&self, chunk: PartialEncodedChunk) {
        match self
            .client_addr
            .send(NetworkClientMessages::PartialEncodedChunk(chunk).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn partial_encoded_chunk_forward(&self, msg: PartialEncodedChunkForwardMsg) {
        match self
            .client_addr
            .send(NetworkClientMessages::PartialEncodedChunkForward(msg).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn block_request(&self, hash: CryptoHash) -> Option<Box<Block>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::BlockRequest(hash).with_span_context())
            .await
        {
            Ok(NetworkViewClientResponses::Block(block)) => Some(block),
            Ok(NetworkViewClientResponses::NoResponse) => None,
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn block_headers_request(&self, hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::BlockHeadersRequest(hashes).with_span_context())
            .await
        {
            Ok(NetworkViewClientResponses::BlockHeaders(block_headers)) => Some(block_headers),
            Ok(NetworkViewClientResponses::NoResponse) => None,
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            }
        }
    }

    async fn block(&self, block: Block, peer_id: PeerId, was_requested: bool) {
        match self
            .client_addr
            .send(NetworkClientMessages::Block(block, peer_id, was_requested).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
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

    async fn challenge(&self, challenge: Challenge) {
        match self
            .client_addr
            .send(NetworkClientMessages::Challenge(challenge).with_span_context())
            .await
        {
            Ok(NetworkClientResponses::NoResponse) => {}
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => tracing::error!("mailbox error: {err}"),
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
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::AnnounceAccount(accounts).with_span_context())
            .await
        {
            Ok(NetworkViewClientResponses::AnnounceAccount(accounts)) => Ok(accounts),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(vec![]),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(vec![])
            }
        }
    }
}
