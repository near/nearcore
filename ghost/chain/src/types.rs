use primitives::hash::CryptoHash;
use primitives::types::BlockIndex;
use chrono::prelude::{DateTime, NaiveDateTime, Utc};
use primitives::crypto::signature::Signature;

pub struct BlockHeader {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: CryptoHash,
    /// Timestamp at which the block was built.
    pub timestamp: DateTime<Utc>,
    /// Authority signatures.
    pub signatures: Vec<Signature>,
}

impl BlockHeader {
    pub fn hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
}

pub struct Bytes(Vec<u8>);

pub struct Block {
    pub header: BlockHeader,
    transactions: Vec<Bytes>,
}

impl Block {
    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}

pub enum BlockStatus {
    /// Block is the "next" block, updating the chain head.
    Next,
    /// Block does not update the chain head and is a fork.
    Fork,
    /// Block updates the chain head via a (potentially disruptive) "reorg".
    /// Previous block was not our previous chain head.
    Reorg,
}

/// Bridge between the chain and the rest of the system.
/// Handles downstream processing of valid blocks by the rest tof the system.
pub trait ChainAdapter {
    fn block_accepted(&self, block: &Block, status: BlockStatus);
}

pub struct NoopAdapter {}

impl ChainAdapter for NoopAdapter {
    fn block_accepted(&self, _block: &Block, _status: BlockStatus) {}
}

/// Bridge between the chain and the runtime.
/// Handles updating state given transactions.
pub trait RuntimeAdapter {}
