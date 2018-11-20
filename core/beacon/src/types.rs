use primitives::hash::{hash_struct, CryptoHash};
use primitives::traits::{Block, Header};
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    prev_hash: CryptoHash,
    merkle_root_tx: MerkleHash,
    merkle_root_state: MerkleHash,
    // TODO: time, height?
    signature: BLSSignature,
    index: u64,
}

impl Header for BeaconBlockHeader {
    fn hash(&self) -> CryptoHash {
        hash_struct(&self)
    }

    fn number(&self) -> u64 {
        self.index
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    header: BeaconBlockHeader,
    transactions: Vec<SignedTransaction>,
    // TODO: weight
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
        BeaconBlock {
            header,
            transactions: body,
        }
    }

    fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}
