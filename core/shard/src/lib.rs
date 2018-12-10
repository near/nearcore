extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate parking_lot;

extern crate chain;
extern crate primitives;
extern crate storage;

use std::sync::Arc;
use primitives::hash::{hash_struct, CryptoHash};
use primitives::types::{AuthorityMask, MerkleHash, BLSSignature, SignedTransaction, ReceiptTransaction};
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::traits::{Block, Header, Signer};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockHeaderBody {
    pub parent_hash: CryptoHash,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockHeader {
    pub body: ShardBlockHeaderBody,
    pub block_hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: BLSSignature,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockBody {
    pub header: ShardBlockHeaderBody,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptTransaction>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlock {
    pub body: ShardBlockBody,
    pub authority_mask: AuthorityMask,
    pub signature: BLSSignature,
}

impl Header for ShardBlockHeader {
    fn hash(&self) -> CryptoHash {
        self.block_hash
    }
    fn index(&self) -> u64 {
        self.body.index
    }
    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

impl ShardBlock {
    pub fn new(index: u64, parent_hash: CryptoHash, merkle_root_state: MerkleHash, transactions: Vec<SignedTransaction>, receipts: Vec<ReceiptTransaction>) -> Self {
        ShardBlock {
            body: ShardBlockBody {
                header: ShardBlockHeaderBody {
                    index,
                    parent_hash,
                    merkle_root_state,
                },
                transactions,
                receipts,
            },
            signature: DEFAULT_SIGNATURE,
            authority_mask: vec![],
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> ShardBlock {
        ShardBlock::new(0, CryptoHash::default(), merkle_root_state, vec![], vec![])
    }

    pub fn sign(&self, signer: &Arc<Signer>) -> BLSSignature {
        signer.sign(&self.hash())
    }
}

impl Block for ShardBlock {
    type Header = ShardBlockHeader;

    fn header(&self) -> Self::Header {
        ShardBlockHeader {
            body: self.body.header.clone(),
            block_hash: self.hash(),
            authority_mask: self.authority_mask.clone(),
            signature: self.signature
        }

    }

    fn hash(&self) -> CryptoHash {
        // TODO: cache?
        hash_struct(&self.body)
    }
}

pub type ShardBlockChain = chain::BlockChain<ShardBlock>;