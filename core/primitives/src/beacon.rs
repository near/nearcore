use super::block_traits::{SignedBlock, SignedHeader};
use super::hash::{hash_struct, CryptoHash};
use super::types::{AuthorityStake, GroupSignature, PartialSignature};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    /// Parent hash.
    pub parent_hash: CryptoHash,
    /// Block index.
    pub index: u64,
    /// Authority proposals.
    pub authority_proposal: Vec<AuthorityStake>,
    /// Hash of the shard block.
    pub shard_block_hash: CryptoHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedBeaconBlockHeader {
    pub body: BeaconBlockHeader,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub header: BeaconBlockHeader,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SignedBeaconBlock {
    pub body: BeaconBlock,
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl Borrow<CryptoHash> for SignedBeaconBlock {
    fn borrow(&self) -> &CryptoHash {
        &self.hash
    }
}

impl Hash for SignedBeaconBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl PartialEq for SignedBeaconBlock {
    fn eq(&self, other: &SignedBeaconBlock) -> bool {
        self.hash == other.hash
    }
}

impl Eq for SignedBeaconBlock {}

impl SignedHeader for SignedBeaconBlockHeader {
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

impl SignedBeaconBlock {
    pub fn new(
        index: u64,
        parent_hash: CryptoHash,
        authority_proposal: Vec<AuthorityStake>,
        shard_block_hash: CryptoHash,
    ) -> SignedBeaconBlock {
        let header = BeaconBlockHeader { index, parent_hash, authority_proposal, shard_block_hash };
        let hash = hash_struct(&header);
        SignedBeaconBlock {
            body: BeaconBlock { header },
            hash,
            signature: GroupSignature::default(),
        }
    }

    pub fn genesis(shard_block_hash: CryptoHash) -> SignedBeaconBlock {
        SignedBeaconBlock::new(0, CryptoHash::default(), vec![], shard_block_hash)
    }
}

impl SignedBlock for SignedBeaconBlock {
    type SignedHeader = SignedBeaconBlockHeader;

    fn header(&self) -> Self::SignedHeader {
        SignedBeaconBlockHeader {
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
        // TODO(#279): sum stakes instead of counting them
        self.signature.authority_count() as u128
    }
}
