use actix::Message;
use near_network::types::MsgRecipient;
use near_primitives::sharding::ShardChunkHeader;

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerResponse {
    ChunkCompleted(ShardChunkHeader),
}

pub trait ClientAdapterForShardsManager: MsgRecipient<ShardsManagerResponse> {}

impl<A: MsgRecipient<ShardsManagerResponse>> ClientAdapterForShardsManager for A {}
