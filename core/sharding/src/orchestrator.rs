use primitives::hash::CryptoHash;
use primitives::types::AuthorityId;

pub trait BaseOrchestrator: Send + Sync {
    fn is_shard_chunk_producer(
        &self,
        authority_id: AuthorityId,
        shard_id: u64,
        height: u64,
    ) -> bool;
    fn is_block_producer(&self, authority_id: AuthorityId, height: u64) -> bool;

    fn get_total_chunk_parts_num(&self) -> usize;
    fn get_data_chunk_parts_num(&self) -> usize;
}
