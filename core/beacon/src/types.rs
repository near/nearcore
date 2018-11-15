use std::cell::RefCell;
use std::rc::{Rc};

use primitives::types::{StructHash, MerkleHash, BLSSignature, SignedTransaction};

use std::collections::{HashMap};

type BlockRef = Rc<RefCell<BeaconBlock>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct BeaconBlockHeader{
    prev_hash: StructHash,
    merkle_root_tx: MerkleHash,
    merkle_root_state: MerkleHash,
    // TODO: time?
    signature: BLSSignature,
}

#[derive(Debug)]
pub struct BeaconBlock{
    header: BeaconBlockHeader,
    transactions: Vec<SignedTransaction>,
    height: u32,
    weight: u64,
    previous: BlockRef,
    // TODO: state_view
    hash: StructHash,
}

#[derive(Debug)]
pub struct BeaconChain{
    // currently active chain
    chain: Vec<BlockRef>,
    // all blocks with known ancestry up to the genesis block
    blocks: HashMap<StructHash, BlockRef>,
    // blocks with unknown ancestry, indexed by parent hash
    orphans: HashMap<StructHash, Vec<BlockRef>>,
    // TODO: state?
}
