use actix::Message;
use near_network::types::MsgRecipient;
use near_primitives::sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunk};

pub trait ClientAdapterForShardsManager {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    );
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk);
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerResponse {
    ChunkCompleted { partial_chunk: PartialEncodedChunk, shard_chunk: Option<ShardChunk> },
    InvalidChunk(EncodedShardChunk),
}

impl<A: MsgRecipient<ShardsManagerResponse>> ClientAdapterForShardsManager for A {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    ) {
        self.do_send(ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk });
    }
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk) {
        self.do_send(ShardsManagerResponse::InvalidChunk(chunk));
    }
}
