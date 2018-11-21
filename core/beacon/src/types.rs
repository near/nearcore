use primitives::hash::{hash_struct, CryptoHash};
use primitives::traits::{Block, Header};
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    pub prev_hash: CryptoHash,
    pub merkle_root_tx: MerkleHash,
    pub merkle_root_state: MerkleHash,
    // TODO: time, height?
    pub signature: BLSSignature,
    pub index: u64,
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
    pub header: BeaconBlockHeader,
    pub transactions: Vec<SignedTransaction>,
    // TODO: weight
}

impl BeaconBlock {
    pub fn new(
        index: u64,
        prev_hash: CryptoHash,
        signature: BLSSignature,
        transactions: Vec<SignedTransaction>,
    ) -> Self {
        BeaconBlock {
            header: BeaconBlockHeader {
                prev_hash,
                merkle_root_tx: MerkleHash::default(),
                merkle_root_state: MerkleHash::default(),
                signature,
                index,
            },
            transactions,
        }
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
        BeaconBlock {
            header,
            transactions: body,
        }
    }

    fn hash(&self) -> CryptoHash {
        self.header.hash()
    }
}
