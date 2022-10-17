use crate::network_protocol::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    StateResponseInfo,
};
use crate::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkInfo, NetworkViewClientMessages,
    NetworkViewClientResponses, ReasonForBan,
};
use near_o11y::{WithSpanContext, WithSpanContextExt};
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

pub struct Client {
    /// Address of the client actor.
    pub client_addr: actix::Recipient<WithSpanContext<NetworkClientMessages>>,
    /// Address of the view client actor.
    pub view_client_addr: actix::Recipient<NetworkViewClientMessages>,
}

pub type Result<T> = std::result::Result<T, ReasonForBan>;

impl Client {
    pub async fn tx_status_request(
        &self,
        account_id: AccountId,
        tx_hash: CryptoHash,
    ) -> Result<Option<FinalExecutionOutcomeView>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::TxStatus {
                tx_hash: tx_hash,
                signer_account_id: account_id,
            })
            .await
        {
            Ok(NetworkViewClientResponses::TxStatus(tx_result)) => Ok(Some(*tx_result)),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    pub async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView) -> Result<()> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result.clone())))
            .await
        {
            Ok(NetworkViewClientResponses::NoResponse) => Ok(()),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    pub async fn state_request_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<Option<StateResponseInfo>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::StateRequestHeader {
                shard_id: shard_id,
                sync_hash: sync_hash,
            })
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

    pub async fn state_request_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
    ) -> Result<Option<StateResponseInfo>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::StateRequestPart {
                shard_id: shard_id,
                sync_hash: sync_hash,
                part_id: part_id,
            })
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

    pub async fn state_response(&self, info: StateResponseInfo) -> Result<()> {
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

    pub async fn block_approval(&self, approval: Approval, peer_id: PeerId) -> Result<()> {
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

    pub async fn transaction(
        &self,
        transaction: SignedTransaction,
        is_forwarded: bool,
    ) -> Result<()> {
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
            Ok(NetworkClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            }
        }
    }

    pub async fn partial_encoded_chunk_request(
        &self,
        req: PartialEncodedChunkRequestMsg,
        msg_hash: CryptoHash,
    ) -> Result<()> {
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

    pub async fn partial_encoded_chunk_response(
        &self,
        resp: PartialEncodedChunkResponseMsg,
        timestamp: time::Instant,
    ) -> Result<()> {
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

    pub async fn partial_encoded_chunk(&self, chunk: PartialEncodedChunk) -> Result<()> {
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

    pub async fn partial_encoded_chunk_forward(
        &self,
        msg: PartialEncodedChunkForwardMsg,
    ) -> Result<()> {
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

    pub async fn block_request(&self, hash: CryptoHash) -> Result<Option<Block>> {
        match self.view_client_addr.send(NetworkViewClientMessages::BlockRequest(hash)).await {
            Ok(NetworkViewClientResponses::Block(block)) => Ok(Some(*block)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    pub async fn block_headers_request(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Option<Vec<BlockHeader>>> {
        match self
            .view_client_addr
            .send(NetworkViewClientMessages::BlockHeadersRequest(hashes))
            .await
        {
            Ok(NetworkViewClientResponses::BlockHeaders(block_headers)) => Ok(Some(block_headers)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban { ban_reason }) => Err(ban_reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            }
        }
    }

    pub async fn block(&self, block: Block, peer_id: PeerId, was_requested: bool) -> Result<()> {
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

    pub async fn block_headers(&self, headers: Vec<BlockHeader>, peer_id: PeerId) -> Result<()> {
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

    pub async fn challenge(&self, challenge: Challenge) -> Result<()> {
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

    pub async fn network_info(&self, info: NetworkInfo) {
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

    pub async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>> {
        match self.view_client_addr.send(NetworkViewClientMessages::AnnounceAccount(accounts)).await
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
