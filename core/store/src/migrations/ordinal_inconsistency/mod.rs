mod find;
mod read_db;
mod repair;
mod timer;

pub use find::find_ordinal_inconsistencies;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, NumBlocks};
pub use repair::repair_ordinal_inconsistencies;

pub struct OrdinalInconsistency {
    pub block_height: BlockHeight,
    pub block_ordinal: NumBlocks,
    pub correct_block_hash: CryptoHash,
    pub actual_block_hash: CryptoHash,
}
