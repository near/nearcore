extern crate chain;
extern crate parking_lot;
extern crate primitives;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate storage;

use chain::{Block, Header};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::types::{AuthorityMask, MerkleHash, MultiSignature, PartialSignature, ReceiptTransaction, SignedTransaction};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockHeaderBody {
    pub parent_hash: CryptoHash,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockHeader {
    pub body: ShardBlockHeaderBody,
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
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
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
}

impl Header for ShardBlockHeader {
    fn block_hash(&self) -> CryptoHash {
        self.hash
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
        let header = ShardBlockHeaderBody {
            index,
            parent_hash,
            merkle_root_state,
        };
        let hash = hash_struct(&header);
        ShardBlock {
            body: ShardBlockBody {
                header,
                transactions,
                receipts,
            },
            hash,
            signature: vec![],
            authority_mask: vec![],
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> ShardBlock {
        ShardBlock::new(0, CryptoHash::default(), merkle_root_state, vec![], vec![])
    }
}

impl Block for ShardBlock {
    type Header = ShardBlockHeader;

    fn header(&self) -> Self::Header {
        ShardBlockHeader {
            body: self.body.header.clone(),
            hash: self.hash,
            signature: self.signature.clone(),
            authority_mask: self.authority_mask.clone(),
        }

    }

    fn block_hash(&self) -> CryptoHash {
        self.hash
    }

    fn add_signature(&mut self, signature: PartialSignature) {
        self.signature.push(signature);
    }
}

pub type ShardBlockChain = chain::BlockChain<ShardBlock>;