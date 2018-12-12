//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use futures::{Future, future, Stream};
use futures::sync::mpsc::Receiver;
use parking_lot::RwLock;

use beacon::types::{SignedBeaconBlock, BeaconBlockChain};
use chain::SignedBlock;
use node_runtime::{ApplyState, Runtime};
use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use shard::{SignedShardBlock, ShardBlockChain};
use storage::StateDb;

pub fn spawn_block_importer(
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    receiver: Receiver<SignedBeaconBlock>
) {
    let beacon_block_importer = RefCell::new(BlockImporter::new(
        beacon_chain,
        shard_chain,
        runtime,
        state_db,
    ));
    let task = receiver.fold(beacon_block_importer, |beacon_block_importer, body| {
        beacon_block_importer.borrow_mut().import_beacon_block(body);
        future::ok(beacon_block_importer)
    }).and_then(|_| Ok(()));
    tokio::spawn(task);
}

pub struct BlockImporter {
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    /// Stores blocks that cannot be added yet.
    pending_beacon_blocks: HashMap<CryptoHash, SignedBeaconBlock>,
    pending_shard_blocks: HashMap<CryptoHash, SignedShardBlock>,
}

impl BlockImporter {
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

    fn validate_signature(&self, _block: &SignedBeaconBlock) -> bool {
        // TODO: validate multisig
        true
    }

    pub fn import_shard_block(&mut self, shard_block: SignedShardBlock) {
        let hash = shard_block.block_hash();
        self.pending_shard_blocks.entry(hash).or_insert(shard_block);
    }

    fn add_block(&self, beacon_block: SignedBeaconBlock, shard_block: SignedShardBlock) {
        let parent_hash = beacon_block.body.header.parent_hash;
        let parent_shard_hash = shard_block.body.header.parent_hash;
        let num_transactions = shard_block.body.transactions.len();
        // we can unwrap because parent is guaranteed to exist
        let prev_header = self.beacon_chain
            .get_header(&BlockId::Hash(parent_hash))
            .expect("Parent is known but header not found.");
        let prev_shard_block = self.shard_chain
            .get_block(&BlockId::Hash(parent_shard_hash))
            .expect("At this moment shard chain should be present together with beacon chain");
        let prev_shard_header = prev_shard_block.header();
        let apply_state = ApplyState {
            root: prev_shard_header.body.merkle_root_state,
            block_index: prev_header.body.index,
            parent_block_hash: parent_hash,
            shard_id: shard_block.body.header.shard_id,
        };
        let mut apply_result = self.runtime.write().apply(
            &apply_state,
            &prev_shard_block.body.new_receipts,
            shard_block.body.transactions.clone()
        );
        if apply_result.root != prev_shard_header.body.merkle_root_state {
            info!(
                "Merkle root {} is not equal to received {} after applying the transactions from {:?}",
                prev_shard_header.body.merkle_root_state,
                apply_result.root,
                beacon_block
            );
            return;
        }
        if apply_result.filtered_transactions.len() != num_transactions {
            info!(
                "Imported block has transactions that were filtered out while merkle roots match in {:?}",
                beacon_block
            );
            return;
        }
        self.state_db.commit(&mut apply_result.transaction).ok();
        // TODO: figure out where to store apply_result.authority_change_set.
        self.shard_chain.insert_block(shard_block);
        self.beacon_chain.insert_block(beacon_block);
    }

    fn blocks_to_process(&self, pending_beacon_blocks: &HashMap<CryptoHash, SignedBeaconBlock>) -> (Vec<SignedBeaconBlock>, HashMap<CryptoHash, SignedBeaconBlock>) {
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

    pub fn import_beacon_block(&mut self, beacon_block: SignedBeaconBlock) {
        // Check if this block was either already added, or it is already pending, or it has
        // invalid signature.
        let hash = beacon_block.block_hash();
        if self.beacon_chain.is_known(&hash)
            || self.pending_beacon_blocks.contains_key(&hash)
            || !self.validate_signature(&beacon_block) {
            return
        }
        self.pending_beacon_blocks.insert(hash, beacon_block);

        let mut blocks_to_add: Vec<SignedBeaconBlock> = vec![];

        // Loop until we run out of blocks to add.
        loop {
            // Only keep those blocks in `pending_blocks` that are still pending.
            // Otherwise put it in `blocks_to_add`.
            let (part_add, part_pending) = self.blocks_to_process(&self.pending_beacon_blocks);
            blocks_to_add.extend(part_add);
            self.pending_beacon_blocks = part_pending;

            // Get the next block to add, unless there are no more blocks left.
            let next_beacon_block = match blocks_to_add.pop() {
                Some(b) => b,
                None => break,
            };
            let hash = next_beacon_block.block_hash();
            if self.beacon_chain.is_known(&hash) { continue; }

            let next_shard_block = self.pending_shard_blocks.remove(&next_beacon_block.body.header.shard_block_hash).expect("Expected to have shard block present when processing beacon block");
            self.add_block(next_beacon_block, next_shard_block);
        }
    }
}
