use primitives::hash::CryptoHash;
use primitives::sharding::ShardChunkHeader;
use reed_solomon_erasure::Shard;

pub struct RequestChunkHeaderMsg {}

pub struct ChunkHeaderMsg {
    pub chunk_hash: CryptoHash,
    pub header: ShardChunkHeader,
}
pub struct ChunkPartMsg {
    pub chunk_hash: CryptoHash,
    pub part_id: u64,
    pub part: Shard,
}

pub struct ChunkHeaderAndPartMsg {
    pub chunk_hash: CryptoHash,
    pub header: ShardChunkHeader,
    pub part_id: u64,
    pub part: Shard,
}
