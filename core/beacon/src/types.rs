use primitives::hash::{hash_struct, CryptoHash};
use primitives::signature::{DEFAULT_SIGNATURE, PublicKey};
use primitives::traits::{Block, Header, Signer};
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AuthorityProposal {
    /// Public key of the proposed authority.
    pub public_key: PublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

//pub struct BeaconBlockBody {
//    /// Parent hash.
//    pub parent_hash: CryptoHash,
//    /// Block index.
//    pub index: u64,
//    /// Authority proposals.
//    pub authority_proposal: Vec<AuthorityProposal>,
//    /// Shard block hash.
//    pub shard_block_hash: CryptoHash,
//}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeaderBody {
    /// Parent hash.
    pub parent_hash: CryptoHash,
    /// Block index.
    pub index: u64,
    pub merkle_root_tx: MerkleHash,
    pub merkle_root_state: MerkleHash,
    pub authority_proposal: Vec<AuthorityProposal>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    pub body: BeaconBlockHeaderBody,
    pub signature: BLSSignature,
    pub authority_mask: Vec<bool>,
}

impl BeaconBlockHeader {
    pub fn new(
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_tx: MerkleHash,
        merkle_root_state: MerkleHash,
        signature: BLSSignature,
        authority_mask: Vec<bool>,
        authority_proposal: Vec<AuthorityProposal>,
    ) -> Self {
        BeaconBlockHeader {
            body: BeaconBlockHeaderBody {
                index,
                parent_hash,
                merkle_root_tx,
                merkle_root_state,
                authority_proposal,
            },
            signature,
            authority_mask,
        }
    }
    pub fn empty(index: u64, parent_hash: CryptoHash, merkle_root_state: MerkleHash) -> Self {
        BeaconBlockHeader {
            body: BeaconBlockHeaderBody {
                index,
                parent_hash,
                merkle_root_tx: MerkleHash::default(),
                merkle_root_state,
                authority_proposal: vec![],
            },
            authority_mask: vec![],
            signature: DEFAULT_SIGNATURE,
        }
    }
}

impl Header for BeaconBlockHeader {
    fn hash(&self) -> CryptoHash {
        // WTF?
        hash_struct(&self)
    }

    fn index(&self) -> u64 {
        self.body.index
    }

    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub header: BeaconBlockHeader,
    pub transactions: Vec<SignedTransaction>,
    // TODO: weight
}

impl BeaconBlock {
    pub fn new(
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_state: MerkleHash,
        transactions: Vec<SignedTransaction>,
    ) -> Self {
        BeaconBlock {
            header: BeaconBlockHeader::empty(index, parent_hash, merkle_root_state),
            transactions,
        }
    }

    pub fn sign(&mut self, signer: &Arc<Signer>) {
        self.header.signature = signer.sign(&self.hash());
    }
}

impl Block for BeaconBlock {
    type Header = BeaconBlockHeader;
    type Body = Vec<SignedTransaction>;

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn body(&self) -> &Self::Body {
        &self.transactions
    }

    fn deconstruct(self) -> (Self::Header, Self::Body) {
        (self.header, self.transactions)
    }

    fn new(header: Self::Header, body: Self::Body) -> Self {
        BeaconBlock { header, transactions: body }
    }

    fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}
