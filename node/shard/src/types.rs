use chain::{SignedBlock, SignedHeader};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::types::{
    GroupSignature, MerkleHash, PartialSignature, ShardId,
};
use transaction::Transaction;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlockHeader {
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedShardBlockHeader {
    pub body: ShardBlockHeader,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlock {
    pub header: ShardBlockHeader,
    pub transactions: Vec<Transaction>,
    pub new_receipts: Vec<Transaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedShardBlock {
    pub body: ShardBlock,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
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
    pub fn new(
        shard_id: ShardId,
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_state: MerkleHash,
        transactions: Vec<Transaction>,
        new_receipts: Vec<Transaction>,
    ) -> Self {
        let header = ShardBlockHeader {
            shard_id,
            index,
            parent_hash,
            merkle_root_state,
        };
        let hash = hash_struct(&header);
        SignedShardBlock {
            body: ShardBlock {
                header,
                transactions,
                new_receipts,
            },
            hash,
            signature: GroupSignature::default(),
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> SignedShardBlock {
        SignedShardBlock::new(
            0, 0, CryptoHash::default(), merkle_root_state, vec![], vec![]
        )
    }

    #[inline]
    pub fn merkle_root_state(&self) -> MerkleHash {
        self.body.header.merkle_root_state
    }
}

impl SignedBlock for SignedShardBlock {
    type SignedHeader = SignedShardBlockHeader;

    fn header(&self) -> Self::SignedHeader {
        SignedShardBlockHeader {
            body: self.body.header.clone(),
            hash: self.hash,
            signature: self.signature.clone(),
        }
    }

    #[inline]
    fn index(&self) -> u64 {
        self.body.header.index
    }

    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }

    fn add_signature(&mut self, signature: &PartialSignature, authority_id: usize) {
        self.signature.add_signature(signature, authority_id);
    }

    fn weight(&self) -> u128 {
        1
    }
}
