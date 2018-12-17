//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::{Future, future, Stream};
use futures::sync::mpsc::Receiver;
use parking_lot::RwLock;

use beacon::types::{SignedBeaconBlock, BeaconBlockChain};
use chain::SignedBlock;
use node_runtime::{ApplyState, Runtime};
use primitives::traits::Signer;
use primitives::types::BlockId;
use primitives::types::{ConsensusBlockBody, ChainPayload};
use shard::{SignedShardBlock, ShardBlockChain};
use storage::StateDb;
use std::io;
use std::io::prelude::*;

pub type ChainConsensusBlockBody = ConsensusBlockBody<ChainPayload>;

pub fn spawn_block_producer(
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
    receiver: Receiver<ChainConsensusBlockBody>
) {
    let beacon_block_producer = BlockProducer::new(
        beacon_chain,
        shard_chain,
        runtime,
        signer,
        state_db,
    );
    let task = receiver.fold(beacon_block_producer, |beacon_block_producer, body| {
        beacon_block_producer.produce_block(body);
        future::ok(beacon_block_producer)
    }).and_then(|_| Ok(()));
    tokio::spawn(task);
}

pub struct BlockProducer {
    beacon_chain: Arc<BeaconBlockChain>,
    shard_chain: Arc<ShardBlockChain>,
    runtime: Arc<RwLock<Runtime>>,
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
}

impl BlockProducer {
    pub fn new(
        beacon_chain: Arc<BeaconBlockChain>,
        shard_chain: Arc<ShardBlockChain>,
        runtime: Arc<RwLock<Runtime>>,
        signer: Arc<Signer>,
        state_db: Arc<StateDb>,
    ) -> Self {
        Self {
            beacon_chain,
            shard_chain,
            runtime,
            signer,
            state_db,
        }
    }

    // TODO(#191): Properly consume the consensus body.
    #[allow(clippy::needless_pass_by_value)]
    pub fn produce_block(&self, body: ChainConsensusBlockBody) {
        // TODO: verify signature
        let mut transactions = body.messages.iter()
            .flat_map(|message| message.body.payload.body.clone())
            .collect();

        let mut last_block = self.beacon_chain.best_block();
        let mut last_shard_block = self.shard_chain
            .get_block(&BlockId::Hash(last_block.body.header.shard_block_hash))
            .expect("At the moment we should have shard blocks accompany beacon blocks");
        let shard_id = last_shard_block.body.header.shard_id;
        let mut apply_state = ApplyState {
            root: last_shard_block.body.header.merkle_root_state,
            parent_block_hash: last_block.block_hash(),
            block_index: last_block.body.header.index + 1,
            shard_id,
        };
        loop {
            let mut apply_result = self.runtime.write().apply(
                &apply_state,
                &last_shard_block.body.new_receipts,
                transactions
            );
            self.state_db.commit(&mut apply_result.transaction).ok();
            let mut shard_block = SignedShardBlock::new(
                shard_id,
                last_shard_block.body.header.index + 1,
                last_shard_block.block_hash(),
                apply_result.root,
                apply_result.filtered_transactions,
                apply_result.new_receipts,
            );
            let mut block = SignedBeaconBlock::new(
                last_block.body.header.index + 1,
                last_block.block_hash(),
                apply_result.authority_proposals,
                shard_block.block_hash()
            );
            let signature = shard_block.sign(self.signer.clone());
            shard_block.add_signature(signature);
            let signature = block.sign(self.signer.clone());
            block.add_signature(signature);
            self.shard_chain.insert_block(shard_block.clone());
            self.beacon_chain.insert_block(block.clone());
            info!(target: "block_producer", "Block body: {:?}", block.body);
            info!(target: "block_producer", "Shard block body: {:?}", shard_block.body);
            io::stdout().flush().expect("Could not flush stdout");
            if shard_block.body.new_receipts.is_empty() {
                break;
            }
            apply_state = ApplyState {
                root: shard_block.body.header.merkle_root_state,
                shard_id,
                parent_block_hash: shard_block.block_hash(),
                block_index: shard_block.body.header.index + 1,
            };
            transactions = vec![];
            last_shard_block = shard_block;
            last_block = block;
        }
    }
}
