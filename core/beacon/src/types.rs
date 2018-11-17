use primitives::hash::CryptoHash;
use primitives::types::{BLSSignature, MerkleHash, SignedTransaction};

use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BeaconBlockHeader {
    prev_hash: CryptoHash,
    merkle_root_tx: MerkleHash,
    merkle_root_state: MerkleHash,
    // TODO: time, height?
    signature: BLSSignature,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeaconBlock {
    header: BeaconBlockHeader,
    transactions: Vec<SignedTransaction>,
    // TODO: weight
}

#[derive(Debug)]
pub struct BeaconChain {
    /// hash of tip
    best_hash: CryptoHash,
    /// headers indexed by hash
    headers: HashMap<CryptoHash, BeaconBlockHeader>,
    /// blocks indexed by hash
    blocks: HashMap<CryptoHash, BeaconBlock>,
    // TODO: state?
}
