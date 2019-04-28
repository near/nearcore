use chrono::prelude::{DateTime, NaiveDateTime, Utc};
use chrono::serde::ts_nanoseconds;

use near_store::StoreUpdate;
use primitives::crypto::signature::Signature;
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use primitives::types::{BlockIndex, MerkleHash};

#[derive(Serialize, Deserialize)]
pub struct BlockHeader {
    /// Height of this block since the genesis block (height 0).
    pub height: BlockIndex,
    /// Hash of the block previous to this in the chain.
    pub prev_hash: CryptoHash,
    /// Root hash of the state at the previous block.
    pub prev_state_root: CryptoHash,
    /// Timestamp at which the block was built.
    #[serde(with = "ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
    /// Authority signatures.
    pub signatures: Vec<Signature>,
    /// Total weight.
    pub total_weight: Weight,
}

impl BlockHeader {
    pub fn genesis(timestamp: DateTime<Utc>, state_root: MerkleHash) -> BlockHeader {
        BlockHeader {
            height: 0,
            prev_hash: CryptoHash::default(),
            prev_state_root: state_root,
            timestamp,
            signatures: vec![],
            total_weight: 0.into(),
        }
    }
    pub fn hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
}

impl Default for BlockHeader {
    fn default() -> BlockHeader {
        BlockHeader {
            height: 0,
            prev_hash: CryptoHash::default(),
            prev_state_root: CryptoHash::default(),
            timestamp: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
            signatures: vec![],
            total_weight: 0.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Bytes(Vec<u8>);

#[derive(Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Bytes>,
}

impl Block {
    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(timestamp: DateTime<Utc>, state_root: MerkleHash) -> Self {
        Block { header: BlockHeader::genesis(timestamp, state_root), transactions: vec![] }
    }
    pub fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}

#[derive(Eq, PartialEq)]
pub enum BlockStatus {
    /// Block is the "next" block, updating the chain head.
    Next,
    /// Block does not update the chain head and is a fork.
    Fork,
    /// Block updates the chain head via a (potentially disruptive) "reorg".
    /// Previous block was not our previous chain head.
    Reorg,
}

/// Options for block origin.
#[derive(Eq, PartialEq)]
pub enum Provenance {
    /// No provenance.
    NONE,
    /// Adds block while in syncing mode.
    SYNC,
    /// Block we produced ourselves.
    PRODUCED,
}

/// Information about valid transaction that was processed by chain + runtime.
pub struct ValidTransaction {
    pub transaction: SignedTransaction,
}

/// Bridge between the chain and the runtime.
/// Handles updating state given transactions.
pub trait RuntimeAdapter {
    /// Initialize state to genesis state and returns StoreUpdate and state root.
    /// StoreUpdate can be discarded if the chain past the genesis.
    fn genesis_state(&self) -> (StoreUpdate, MerkleHash);
}

/// The weight is defined as the number of unique authorities approving this fork.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct Weight {
    num: u64,
}

impl Weight {
    pub fn to_num(&self) -> u64 {
        self.num
    }
}

impl From<u64> for Weight {
    fn from(num: u64) -> Self {
        Weight { num }
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience and the total weight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: u64,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Total weight on that fork
    pub total_weight: Weight,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.height,
            last_block_hash: header.hash(),
            prev_block_hash: header.prev_hash,
            total_weight: header.total_weight,
        }
    }

    /// The hash of the underlying block.
    fn hash(&self) -> CryptoHash {
        self.last_block_hash
    }
}
