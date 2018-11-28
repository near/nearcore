use primitives::hash::CryptoHash;
use primitives::traits::Block;
use primitives::types::BlockId;

/// abstraction that communicates chain info to network
pub trait Client<B: Block> {
    // get block from id
    fn get_block(&self, id: &BlockId) -> Option<B>;
    // get block header from id
    fn get_header(&self, id: &BlockId) -> Option<B::Header>;
    // hash of latest block
    fn best_hash(&self) -> CryptoHash;
    // index of latest block
    fn best_index(&self) -> u64;
    // genesis hash
    fn genesis_hash(&self) -> CryptoHash;
    // import blocks
    fn import_blocks(&self, blocks: Vec<B>);
    // produce block (Put here for now due to trait constraints, might want to change in the future)
    fn prod_block(&self) -> B;
}
