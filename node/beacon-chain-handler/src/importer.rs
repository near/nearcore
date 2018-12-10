//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use futures::{Future, future, Stream};
use futures::sync::mpsc::Receiver;
use parking_lot::RwLock;

use beacon::types::{BeaconBlock, BeaconBlockChain};
use chain::Block;
use node_runtime::{ApplyState, Runtime};
use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use shard::{ShardBlock, ShardBlockChain};
use storage::StateDb;

pub fn create_beacon_block_importer_task(
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    receiver: Receiver<BeaconBlock>
) -> impl Future<Item = (), Error = ()> {
    let beacon_block_importer = RefCell::new(BeaconBlockImporter::new(
        beacon_chain,
        shard_chain,
        runtime,
        state_db,
    ));
    receiver.fold(beacon_block_importer, |beacon_block_importer, body| {
        beacon_block_importer.borrow_mut().import_block(body);
        future::ok(beacon_block_importer)
    }).and_then(|_| Ok(()))
}

pub struct BeaconBlockImporter {
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    /// Stores blocks that cannot be added yet.
    pending_beacon_blocks: HashMap<CryptoHash, BeaconBlock>,
    pending_shard_blocks: HashMap<CryptoHash, ShardBlock>,
}

impl BeaconBlockImporter {
    pub fn new(
        beacon_chain: Arc<BeaconBlockChain>,
        shard_chain: Arc<ShardBlockChain>,
        runtime: Arc<RwLock<Runtime>>,
        state_db: Arc<StateDb>,
    ) -> Self {
        Self {
            beacon_chain,
            shard_chain,
            runtime,
            state_db,
            pending_beacon_blocks: HashMap::new(),
            pending_shard_blocks: HashMap::new(),
        }
    }

    fn validate_signature(&self, _block: &BeaconBlock) -> bool {
        // TODO: validate multisig
        true
    }

    pub fn import_shard_block(&mut self, shard_block: ShardBlock) {
        let hash = shard_block.hash();
        if !self.pending_shard_blocks.contains_key(&hash) {
            self.pending_shard_blocks.insert(hash, shard_block);
        }
    }

    fn add_block(&self, block: BeaconBlock, shard_block: ShardBlock) {
        let parent_hash = block.body.header.parent_hash;
        let parent_shard_hash = shard_block.body.header.parent_hash;
        let num_transactions = shard_block.body.transactions.len();
        let num_receipts = shard_block.body.receipts.len();
        // we can unwrap because parent is guaranteed to exist
        let prev_header = self.beacon_chain
            .get_header(&BlockId::Hash(parent_hash))
            .expect("Parent is known but header not found.");
        let prev_shard_header = self.shard_chain
            .get_header(&BlockId::Hash(parent_shard_hash))
            .expect("At this moment shard chain should be present together with beacon chain");
        let apply_state = ApplyState {
            root: prev_shard_header.body.merkle_root_state,
            block_index: prev_header.body.index,
            parent_block_hash: parent_hash,
        };
        let (filtered_transactions, filtered_receipts, mut apply_result) =
            self.runtime.write().apply(&apply_state, shard_block.body.transactions.clone(), shard_block.body.receipts.clone());
        if apply_result.root != prev_shard_header.body.merkle_root_state {
            println!("Merkle root {} is not equal to received {} after applying the transactions from {:?}", prev_shard_header.body.merkle_root_state, apply_result.root, block);
            return;
        }
        if filtered_transactions.len() != num_transactions {
            println!("Imported block has transactions that were filtered out while merkle roots match in {:?}", block);
            return;
        }
        if filtered_receipts.len() != num_receipts {
            println!("Imported block has receipts that were filtered out while merkle roots match in {:?}.", block);
            return;
        }
        self.state_db.commit(&mut apply_result.transaction).ok();
        // TODO: figure out where to store apply_result.authority_change_set.
        self.shard_chain.insert_block(shard_block);
        self.beacon_chain.insert_block(block);
    }

    fn blocks_to_process(&self, pending_beacon_blocks: &HashMap<CryptoHash, BeaconBlock>) -> (Vec<BeaconBlock>, HashMap<CryptoHash, BeaconBlock>) {
        let mut part_add = vec![];
        let mut part_pending = HashMap::default();
        // TODO: do this with drains and whatnot. For now, that doesn't work because self is both mutable and immutable.
        for (hash, other) in pending_beacon_blocks.iter() {
            if self.beacon_chain.is_known(&other.body.header.parent_hash) && (
                self.shard_chain.is_known(&other.body.header.shard_block_hash) ||
                    self.pending_shard_blocks.contains_key(&other.body.header.shard_block_hash)) {
                part_add.push(other.clone());
            } else {
                part_pending.insert(hash.clone(), other.clone());
            }
        }
        (part_add, part_pending)
    }

    pub fn import_block(&mut self, block: BeaconBlock) {
        // Check if this block was either already added, or it is already pending, or it has
        // invalid signature.
        let hash = block.hash();
        if self.beacon_chain.is_known(&hash)
            || self.pending_beacon_blocks.contains_key(&hash)
            || !self.validate_signature(&block) {
            return
        }
        self.pending_beacon_blocks.insert(hash, block);

        let mut blocks_to_add: Vec<BeaconBlock> = vec![];

        // Loop until we run out of blocks to add.
        loop {
            // Only keep those blocks in `pending_blocks` that are still pending.
            // Otherwise put it in `blocks_to_add`.
            let (part_add, part_pending) = self.blocks_to_process(&self.pending_beacon_blocks);
            blocks_to_add.extend(part_add);
            self.pending_beacon_blocks = part_pending;

            // Get the next block to add, unless there are no more blocks left.
            let next_b = match blocks_to_add.pop() {
                Some(b) => b,
                None => break,
            };
            let hash = next_b.hash();
            if self.beacon_chain.is_known(&hash) { continue; }

            let next_shard_block = self.pending_shard_blocks.remove(&next_b.body.header.shard_block_hash).expect("Expected to have shard block present when processing beacon block");
            self.add_block(next_b, next_shard_block);
        }
    }
}
