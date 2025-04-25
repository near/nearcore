use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;

mod block;
#[cfg(feature = "solomon")]
mod chunk;

pub use block::{genesis_block, prod_genesis_block};
#[cfg(feature = "solomon")]
pub use chunk::{genesis_chunks, prod_genesis_chunks};

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}
