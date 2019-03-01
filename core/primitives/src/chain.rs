use std::hash::{Hash, Hasher};

use serde_derive::{Deserialize, Serialize};

use super::block_traits::{SignedBlock, SignedHeader};
use super::consensus::Payload;
use super::hash::{CryptoHash, hash_struct};
use super::merkle::MerklePath;
use super::transaction::{ReceiptTransaction, SignedTransaction};
use super::types::{AuthorityId, GroupSignature, MerkleHash, PartialSignature, ShardId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBlockHeader {
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
    /// if there is no receipt generated in this block, the root is None
    pub receipt_merkle_root: MerkleHash,
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
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedShardBlock {
    pub body: ShardBlock,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct ReceiptBlock {
    pub header: SignedShardBlockHeader,
    pub path: MerklePath,
    pub receipts: Vec<ReceiptTransaction>,
}

impl PartialEq for ReceiptBlock {
    fn eq(&self, other: &ReceiptBlock) -> bool {
        self.header.hash == other.header.hash
            && self.path == other.path
            && self.receipts == other.receipts
    }
}

impl Hash for ReceiptBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(hash_struct(&self).as_ref());
    }
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
        transactions: Vec<SignedTransaction>,
        receipts: Vec<ReceiptBlock>,
        receipt_merkle_root: MerkleHash,
    ) -> Self {
        let header = ShardBlockHeader {
            shard_id,
            index,
            parent_hash,
            merkle_root_state,
            receipt_merkle_root,
        };
        let hash = hash_struct(&header);
        SignedShardBlock {
            body: ShardBlock { header, transactions, receipts },
            hash,
            signature: GroupSignature::default(),
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> SignedShardBlock {
        SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            merkle_root_state,
            vec![],
            vec![],
            CryptoHash::default(),
        )
    }

    #[inline]
    pub fn merkle_root_state(&self) -> MerkleHash {
        self.body.header.merkle_root_state
    }

    #[inline]
    pub fn shard_id(&self) -> ShardId {
        self.body.header.shard_id
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

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
pub struct ChainPayload {
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptBlock>,
}

impl Payload for ChainPayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn union_update(&mut self, mut other: Self) {
        self.transactions.extend(other.transactions.drain(..));
        self.receipts.extend(other.receipts.drain(..))
    }

    fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.receipts.is_empty()
    }

    fn new() -> Self {
        Self { transactions: vec![], receipts: vec![] }
    }
}

pub enum PayloadRequest {
    General(Vec<CryptoHash>, Vec<CryptoHash>),
    BlockProposal(AuthorityId, CryptoHash),
}

pub enum PayloadResponse {
    General(ChainPayload),
    BlockProposal(AuthorityId, ChainPayload),
}
