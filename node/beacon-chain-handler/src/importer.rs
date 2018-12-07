//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use beacon::types::{BeaconBlock, BeaconBlockBody};
use chain::BlockChain;
use node_runtime::{ApplyState, Runtime};
use primitives::traits::{Block, Header};
use primitives::types::BlockId;
use std::sync::Arc;
use std::cell::RefCell;
use storage::StateDb;
use parking_lot::RwLock;
use futures::sync::mpsc::Receiver;
use futures::{Future, Stream, future};
use std::collections::HashSet;


pub fn create_beacon_block_importer_task(
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    receiver: Receiver<BeaconBlock>
) -> impl Future<Item = (), Error = ()> {
    let beacon_block_importer = RefCell::new(BeaconBlockImporter::new(
        beacon_chain,
        runtime,
        state_db,
    ));
    receiver.fold(beacon_block_importer, |beacon_block_importer, body| {
        beacon_block_importer.borrow_mut().import_block(body);
        future::ok(beacon_block_importer)
    }).and_then(|_| Ok(()))
}

pub struct BeaconBlockImporter {
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    runtime: Arc<RwLock<Runtime>>,
    state_db: Arc<StateDb>,
    /// Stores blocks that cannot be added yet.
    pending_blocks: HashSet<BeaconBlock>,
}

impl BeaconBlockImporter {
    pub fn new(
        beacon_chain: Arc<BlockChain<BeaconBlock>>,
        runtime: Arc<RwLock<Runtime>>,
        state_db: Arc<StateDb>,
    ) -> Self {
        Self {
            beacon_chain,
            runtime,
            state_db,
            pending_blocks: HashSet::new(),
        }
    }

    fn validate_signature(&self, _block: &BeaconBlock) -> bool {
        // TODO: validate multisig
        true
    }

    pub fn import_block(&mut self, block: BeaconBlock) {
        // Check if this block was either already added, or it is already pending, or it has
        // invalid signature.
        let hash = &block.header_hash();
        if self.beacon_chain.is_known(hash)
            || self.pending_blocks.contains(&block)
            || !self.validate_signature(&block) {
            return
        }
        let mut blocks_to_add;
        let parent_hash = block.header().parent_hash();
        if self.beacon_chain.is_known(&parent_hash) {
            blocks_to_add = vec![];
            blocks_to_add.push(block);
        } else {
            self.pending_blocks.insert(block);
            return;
        }

        // Loop until we run out of blocks to add.
        loop {
            // Get the next block to add, unless there are no more blocks left.
            let next_b = match blocks_to_add.pop() {
                Some(b) => b,
                None => break,
            };
            let hash = next_b.header_hash();
            if self.beacon_chain.is_known(&hash) { continue; }

            let (header, mut body) = next_b.deconstruct();
            let num_transactions = body.transactions.len();
            let num_receipts = body.receipts.len();
            // we can unwrap because parent is guaranteed to exist
            let prev_header = self.beacon_chain
                .get_header(&BlockId::Hash(parent_hash))
                .expect("Parent is known but header not found.");
            let apply_state = ApplyState {
                root: prev_header.body.merkle_root_state,
                block_index: prev_header.body.index,
                parent_block_hash: parent_hash,
            };
            let (filtered_transactions, filtered_receipts, mut apply_result) =
                self.runtime.write().apply(&apply_state, body.transactions, &mut body.receipts);
            assert_eq!(apply_result.root, header.body.merkle_root_state,
                       "Merkle roots are not equal after applying the transactions.");
            // TODO: This should be handled.
            assert_eq!(filtered_transactions.len(), num_transactions,
                       "Imported block has transactions that were filtered out.");
            assert_eq!(filtered_receipts.len(), num_receipts,
            "Imported block has receipts that were filtered out.");
            self.state_db.commit(&mut apply_result.transaction).ok();
            // TODO: figure out where to store apply_result.authority_change_set.
            let block_body = BeaconBlockBody::new(filtered_transactions, filtered_receipts);
            let block = Block::new(header, block_body);
            self.beacon_chain.insert_block(block);

            // Only keep those blocks in `pending_blocks` that are still pending.
            // Otherwise put it in `blocks_to_add`.
            let (mut part_add, part_pending): (HashSet<BeaconBlock>, HashSet<BeaconBlock>) =
                self.pending_blocks.drain().partition(|other|
                    other.header().parent_hash() == hash );
            self.pending_blocks = part_pending;
            blocks_to_add.extend(part_add.drain());
        }
    }
}
