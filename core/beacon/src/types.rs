use primitives::hash::{hash_struct, CryptoHash};
use primitives::signature::{PublicKey, DEFAULT_SIGNATURE};
use primitives::traits::{Block, Header, Signer};
use primitives::types::{AuthorityMask, BLSSignature};
use std::sync::Arc;
use std::hash::{Hash, Hasher};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AuthorityProposal {
    /// Public key of the proposed authority.
    pub public_key: PublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeaderBody {
    /// Parent hash.
    pub parent_hash: CryptoHash,
    /// Block index.
    pub index: u64,
    /// Authority proposals.
    pub authority_proposal: Vec<AuthorityProposal>,
    /// Hash of the shard block.
    pub shard_block_hash: CryptoHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    pub body: BeaconBlockHeaderBody,
    pub block_hash: CryptoHash,
    pub signature: BLSSignature,
    pub authority_mask: AuthorityMask,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockBody {
    pub header: BeaconBlockHeaderBody,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub body: BeaconBlockBody,
    pub signature: BLSSignature,
    pub authority_mask: AuthorityMask,
}

impl Header for BeaconBlockHeader {
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

impl BeaconBlock {
    pub fn new(index: u64, parent_hash: CryptoHash, authority_proposal: Vec<AuthorityProposal>, shard_block_hash: CryptoHash) -> BeaconBlock {
        BeaconBlock {
            body: BeaconBlockBody {
                header: BeaconBlockHeaderBody {
                    index,
                    parent_hash,
                    authority_proposal,
                    shard_block_hash
                }
            },
            signature: DEFAULT_SIGNATURE,
            authority_mask: vec![],
        }
    }

    pub fn genesis(shard_block_hash: CryptoHash) -> BeaconBlock {
        BeaconBlock::new(0, CryptoHash::default(), vec![], shard_block_hash)
    }

    pub fn sign(&self, signer: &Signer) -> BLSSignature {
        signer.sign(&self.hash())
    }
}

impl Block for BeaconBlock {
    type Header = BeaconBlockHeader;

    fn header(&self) -> Self::Header {
        BeaconBlockHeader {
            body: self.body.header.clone(),
            block_hash: self.hash(),
            signature: self.signature,
            authority_mask: self.authority_mask.clone(),
        }
    }
    fn hash(&self) -> CryptoHash {
        hash_struct(&self.body)
    }
}

pub type BeaconBlockChain = chain::BlockChain<BeaconBlock>;
