//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::{Future, future, Stream, Sink};
use futures::sync::mpsc::{Sender, Receiver};
use parking_lot::RwLock;

use beacon::types::{SignedBeaconBlock, BeaconBlockChain};
use beacon::authority::Authority;
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
    authority: Arc<RwLock<Authority>>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
) {
    let beacon_block_producer = BlockProducer::new(
        beacon_chain,
        shard_chain,
        runtime,
        signer,
        state_db,
        authority,
        block_announce_tx,
        new_block_tx,
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
    authority: Arc<RwLock<Authority>>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
}

impl BlockProducer {
    pub fn new(
        beacon_chain: Arc<BeaconBlockChain>,
        shard_chain: Arc<ShardBlockChain>,
        runtime: Arc<RwLock<Runtime>>,
        signer: Arc<Signer>,
        state_db: Arc<StateDb>,
        authority: Arc<RwLock<Authority>>,
        block_announce_tx: Sender<SignedBeaconBlock>,
        new_block_tx: Sender<SignedBeaconBlock>,
    ) -> Self {
        Self {
            beacon_chain,
            shard_chain,
            runtime,
            signer,
            state_db,
            authority,
            block_announce_tx,
            new_block_tx
        }
    }

    pub fn produce_block(&self, body: ChainConsensusBlockBody) {
        // TODO: verify signature
        let mut transactions = body.messages.into_iter()
            .flat_map(|message| message.body.payload.body)
            .collect();

        let mut last_block = self.beacon_chain.best_block();
        let mut last_shard_block = self.shard_chain
            .get_block(&BlockId::Hash(last_block.body.header.shard_block_hash))
            .expect("At the moment we should have shard blocks accompany beacon blocks");
        let authorities = self.authority.read().get_authorities(last_block.body.header.index)
            .expect("Authorities should be present for given block to produce it");
        let shard_id = last_shard_block.body.header.shard_id;
        let mut apply_state = ApplyState {
            root: last_shard_block.body.header.merkle_root_state,
            parent_block_hash: last_block.block_hash(),
            block_index: last_block.body.header.index + 1,
            shard_id,
        };
        loop {
            let apply_result = self.runtime.write().apply(
                &apply_state,
                &last_shard_block.body.new_receipts,
                transactions
            );
            self.state_db.commit(apply_result.transaction).ok();
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
            let authority_mask: Vec<bool> = authorities.iter().map(|a| a.account_id == self.signer.account_id()).collect();
            let signature = shard_block.sign(&*self.signer);
            shard_block.add_signature(signature);
            shard_block.authority_mask = authority_mask.clone();
            let signature = block.sign(&*self.signer);
            block.add_signature(signature);
            block.authority_mask = authority_mask;
            self.shard_chain.insert_block(shard_block.clone());
            self.beacon_chain.insert_block(block.clone());
            info!(target: "block_producer", "Block body: {:?}", block.body);
            info!(target: "block_producer", "Shard block body: {:?}", shard_block.body);
            io::stdout().flush().expect("Could not flush stdout");
            // send beacon block to network
            tokio::spawn({
                let block_announce_tx = self.block_announce_tx.clone();
                block_announce_tx
                    .send(block.clone())
                    .map(|_| ())
                    .map_err(|e| error!("Error sending block: {:?}", e))
            });
            // send beacon block to authority handler
            tokio::spawn({
                let new_block_tx = self.new_block_tx.clone();
                new_block_tx
                    .send(block.clone())
                    .map(|_| ())
                    .map_err(|e| error!("Error sending block: {:?}", e))
            });
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
