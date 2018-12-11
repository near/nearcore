extern crate chain;
extern crate parking_lot;
extern crate primitives;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate storage;

use chain::{SignedBlock, SignedHeader};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::types::{AuthorityMask, MerkleHash, MultiSignature, PartialSignature, ReceiptTransaction, SignedTransaction};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlockHeader {
    pub parent_hash: CryptoHash,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedShardBlockHeader {
    pub body: ShardBlockHeader,
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardBlock {
    pub header: ShardBlockHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptTransaction>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedShardBlock {
    pub body: ShardBlock,
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
}

impl SignedHeader for SignedShardBlockHeader {
    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }
    #[inline]
    fn index(&self) -> u64 {
        self.body.index
    }
    #[inline]
    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

impl SignedShardBlock {
    pub fn new(index: u64, parent_hash: CryptoHash, merkle_root_state: MerkleHash, transactions: Vec<SignedTransaction>, receipts: Vec<ReceiptTransaction>) -> Self {
        let header = ShardBlockHeader {
            index,
            parent_hash,
            merkle_root_state,
        };
        let hash = hash_struct(&header);
        SignedShardBlock {
            body: ShardBlock {
                header,
                transactions,
                receipts,
            },
            hash,
            signature: vec![],
            authority_mask: vec![],
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> SignedShardBlock {
        SignedShardBlock::new(0, CryptoHash::default(), merkle_root_state, vec![], vec![])
    }
}

impl SignedBlock for SignedShardBlock {
    type SignedHeader = SignedShardBlockHeader;

    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }

    fn header(&self) -> Self::SignedHeader {
        SignedShardBlockHeader {
            body: self.body.header.clone(),
            hash: self.hash,
            signature: self.signature.clone(),
            authority_mask: self.authority_mask.clone(),
        }

    }

    fn add_signature(&mut self, signature: PartialSignature) {
        self.signature.push(signature);
    }
}

pub type ShardBlockChain = chain::BlockChain<SignedShardBlock>;