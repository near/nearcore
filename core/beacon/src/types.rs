use primitives::hash::{hash_struct, CryptoHash};
use primitives::signature::{PublicKey, DEFAULT_SIGNATURE};
use primitives::traits::{Block, Header, Signer};
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction, ReceiptTransaction};
use std::sync::Arc;

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
    pub merkle_root_tx: MerkleHash,
    pub merkle_root_state: MerkleHash,
    /// Authority proposals.
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
        // TODO: must be hash of the block.
        hash_struct(&self.body)
    }

    fn index(&self) -> u64 {
        self.body.index
    }

    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockBody {
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<ReceiptTransaction>,
}

impl BeaconBlockBody {
    pub fn new(transactions: Vec<SignedTransaction>, receipts: Vec<ReceiptTransaction>) -> Self {
        BeaconBlockBody {
            transactions,
            receipts,
        }
    }

    pub fn len(&self) -> usize {
        self.transactions.len() + self.receipts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub header: BeaconBlockHeader,
    pub body: BeaconBlockBody
    // TODO: weight
}

impl BeaconBlock {
    pub fn new(
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_state: MerkleHash,
        transactions: Vec<SignedTransaction>,
        receipts: Vec<ReceiptTransaction>,
    ) -> Self {
        BeaconBlock {
            header: BeaconBlockHeader::empty(index, parent_hash, merkle_root_state),
            body: BeaconBlockBody::new(transactions, receipts),
        }
    }

    pub fn sign(&mut self, signer: &Arc<Signer>) {
        self.header.signature = signer.sign(&self.hash());
    }
}

impl Block for BeaconBlock {
    type Header = BeaconBlockHeader;
    type Body = BeaconBlockBody;

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn body(&self) -> &Self::Body {
        &self.body
    }

    fn deconstruct(self) -> (Self::Header, Self::Body) {
        (self.header, self.body)
    }

    fn new(header: Self::Header, body: Self::Body) -> Self {
        BeaconBlock { header, body }
    }

    fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}
