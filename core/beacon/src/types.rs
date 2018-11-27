use primitives::hash::{hash_struct, CryptoHash};
use primitives::traits::{Block, Header, Signer};
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    /// Parent hash.
    pub parent_hash: CryptoHash,
    /// Block index.
    pub index: u64,
    pub merkle_root_tx: MerkleHash,
    pub merkle_root_state: MerkleHash,
    // TODO: time, height?
    pub signature: BLSSignature,
}

impl Header for BeaconBlockHeader {
    fn hash(&self) -> CryptoHash {
        hash_struct(&self)
    }

    fn index(&self) -> u64 {
        self.index
    }

    fn parent_hash(&self) -> CryptoHash {
        self.parent_hash
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
            header: BeaconBlockHeader {
                parent_hash,
                merkle_root_tx: MerkleHash::default(),
                merkle_root_state,
                signature: primitives::signature::default_signature(),
                index,
            },
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
