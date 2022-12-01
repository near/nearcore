use crate::client;
use crate::network_protocol::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    StateResponseInfo,
};
use crate::sink::Sink;
use crate::types::{NetworkInfo, ReasonForBan, StateResponseInfoV2};
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart};
use near_primitives::syncing::{ShardStateSyncResponse, ShardStateSyncResponseV2};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
    Block(Block),
    BlockHeaders(Vec<BlockHeader>),
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

    async fn block_approval(&self, _approval: Approval, _peer_id: PeerId) {
        unimplemented!();
    }

    async fn transaction(&self, transaction: SignedTransaction, _is_forwarded: bool) {
        self.event_sink.push(Event::Transaction(transaction));
    }

    async fn partial_encoded_chunk_request(
        &self,
        req: PartialEncodedChunkRequestMsg,
        _msg_hash: CryptoHash,
    ) {
        self.event_sink.push(Event::ChunkRequest(req.chunk_hash));
    }

    async fn partial_encoded_chunk_response(
        &self,
        resp: PartialEncodedChunkResponseMsg,
        _timestamp: time::Instant,
    ) {
        self.event_sink.push(Event::Chunk(resp.parts));
    }

    async fn partial_encoded_chunk(&self, _chunk: PartialEncodedChunk) {
        unimplemented!();
    }

    async fn partial_encoded_chunk_forward(&self, _msg: PartialEncodedChunkForwardMsg) {
        unimplemented!();
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
