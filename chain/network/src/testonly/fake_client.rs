use crate::client;
use crate::network_protocol::StateResponseInfo;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::sink::Sink;
use crate::types::{NetworkInfo, ReasonForBan, StateResponseInfoV2};
use near_async::messaging;
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{ChunkHash, PartialEncodedChunkPart};
use near_primitives::syncing::{ShardStateSyncResponse, ShardStateSyncResponseV2};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
    Block(Block),
    BlockHeaders(Vec<BlockHeader>),
    BlockApproval(Approval, PeerId),
    BlockHeadersRequest(Vec<CryptoHash>),
    BlockRequest(CryptoHash),
    Challenge(Challenge),
    Chunk(Vec<PartialEncodedChunkPart>),
    ChunkRequest(ChunkHash),
    Transaction(SignedTransaction),
}

pub(crate) struct Fake {
    pub event_sink: Sink<Event>,
}

#[async_trait::async_trait]
impl client::Client for Fake {
    async fn tx_status_request(
        &self,
        _account_id: AccountId,
        _tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>> {
        unimplemented!();
    }

    async fn tx_status_response(&self, _tx_result: FinalExecutionOutcomeView) {}

    async fn state_request_header(
        &self,
        _shard_id: ShardId,
        _sync_hash: CryptoHash,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        unimplemented!();
    }

    async fn state_request_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        let part = Some((part_id, vec![]));
        let state_response =
            ShardStateSyncResponse::V2(ShardStateSyncResponseV2 { header: None, part });
        let result = Some(StateResponseInfo::V2(StateResponseInfoV2 {
            shard_id,
            sync_hash,
            state_response,
        }));
        Ok(result)
    }

    async fn state_response(&self, _info: StateResponseInfo) {
        unimplemented!();
    }

    async fn block_approval(&self, approval: Approval, peer_id: PeerId) {
        self.event_sink.push(Event::BlockApproval(approval, peer_id));
    }

    async fn transaction(&self, transaction: SignedTransaction, _is_forwarded: bool) {
        self.event_sink.push(Event::Transaction(transaction));
    }

    async fn block_request(&self, hash: CryptoHash) -> Option<Box<Block>> {
        self.event_sink.push(Event::BlockRequest(hash));
        None
    }

    async fn block_headers_request(&self, hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        self.event_sink.push(Event::BlockHeadersRequest(hashes));
        None
    }

    async fn block(&self, block: Block, _peer_id: PeerId, _was_requested: bool) {
        self.event_sink.push(Event::Block(block));
    }

    async fn block_headers(
        &self,
        headers: Vec<BlockHeader>,
        _peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        self.event_sink.push(Event::BlockHeaders(headers));
        Ok(())
    }

    async fn challenge(&self, challenge: Challenge) {
        self.event_sink.push(Event::Challenge(challenge));
    }

    async fn network_info(&self, _info: NetworkInfo) {}

    async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        self.event_sink.push(Event::AnnounceAccount(accounts.clone()));
        Ok(accounts.into_iter().map(|a| a.0).collect())
    }
}

impl messaging::CanSend<ShardsManagerRequestFromNetwork> for Fake {
    fn send(&self, message: ShardsManagerRequestFromNetwork) {
        match message {
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request,
                ..
            } => {
                self.event_sink.push(Event::ChunkRequest(partial_encoded_chunk_request.chunk_hash));
            }
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                partial_encoded_chunk_response,
                ..
            } => {
                self.event_sink.push(Event::Chunk(partial_encoded_chunk_response.parts));
            }
            _ => {}
        }
    }
}
