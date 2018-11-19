use error::Error;
use primitives::hash::CryptoHash;
use primitives::traits::Block;
use primitives::types::BlockId;

/// abstraction that communicates chain info to network
pub trait Client<B: Block>: Send + Sync {
    // get block from id
    fn get_block(&self, id: &BlockId) -> Result<B, Error>;
    // get block header from id
    fn get_header(&self, id: &BlockId) -> Result<B::Header, Error>;
    // hash of latest block
    fn best_hash(&self) -> CryptoHash;
    // index of latest block
    fn best_number(&self) -> u64;
    // genesis hash
    fn genesis_hash(&self) -> CryptoHash;
    // import blocks
    fn import_blocks(&self, blocks: Vec<B>);
}
