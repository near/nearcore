use near_primitives::hash::CryptoHash;

pub type BlockHash = CryptoHash;

pub struct BlockInfo {
    hash: BlockHash,
    prev: BlockHash,
}