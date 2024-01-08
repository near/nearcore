// FIXME(nagisa): Is there a good reason we're triggering this? Luckily though this is just test
// code so we're in the clear.
#![allow(clippy::arc_with_non_send_sync)]

use std::mem::swap;
use std::sync::{Arc, RwLock};

use crate::Client;
use actix_rt::{Arbiter, System};
use itertools::Itertools;
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest};
use near_chain::resharding::ReshardingRequest;
use near_chain::test_utils::{wait_for_all_blocks_in_processing, wait_for_block_in_processing};
use near_chain::{Chain, ChainStoreAccess, Provenance};
use near_client_primitives::types::Error;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{EncodedShardChunk, ReedSolomonWrapper};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::PROTOCOL_VERSION;
use num_rational::Ratio;

impl Client {
    /// Unlike Client::start_process_block, which returns before the block finishes processing
    /// This function waits until the block is processed.
    /// `should_produce_chunk`: Normally, if a block is accepted, client will try to produce
    ///                         chunks for the next block if it is the chunk producer.
    ///                         If `should_produce_chunk` is set to false, client will skip the
    ///                         chunk production. This is useful in tests that need to tweak
    ///                         the produced chunk content.
    fn process_block_sync_with_produce_chunk_options(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        should_produce_chunk: bool,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.start_process_block(block, provenance, Arc::new(|_| {}))?;
        wait_for_all_blocks_in_processing(&mut self.chain);
        let (accepted_blocks, errors) =
            self.postprocess_ready_blocks(Arc::new(|_| {}), should_produce_chunk);
        assert!(errors.is_empty(), "unexpected errors when processing blocks: {errors:#?}");
        Ok(accepted_blocks)
    }

    pub fn process_block_test(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, true)
    }

    pub fn process_block_test_no_produce_chunk(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false)
    }

    /// This function finishes processing all blocks that started being processed.
    pub fn finish_blocks_in_processing(&mut self) -> Vec<CryptoHash> {
        let mut accepted_blocks = vec![];
        while wait_for_all_blocks_in_processing(&mut self.chain) {
            accepted_blocks.extend(self.postprocess_ready_blocks(Arc::new(|_| {}), true).0);
        }
        accepted_blocks
    }

    /// This function finishes processing block with hash `hash`, if the processing of that block
    /// has started.
    pub fn finish_block_in_processing(&mut self, hash: &CryptoHash) -> Vec<CryptoHash> {
        if let Ok(()) = wait_for_block_in_processing(&mut self.chain, hash) {
            let (accepted_blocks, _) = self.postprocess_ready_blocks(Arc::new(|_| {}), true);
            return accepted_blocks;
        }
        vec![]
    }
}

fn create_chunk_on_height_for_shard(
    client: &mut Client,
    next_height: BlockHeight,
    shard_id: ShardId,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>) {
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block = client.chain.get_block(&last_block_hash).unwrap();
    client
        .produce_chunk(
            last_block_hash,
            &client.epoch_manager.get_epoch_id_from_prev_block(&last_block_hash).unwrap(),
            Chain::get_prev_chunk_header(client.epoch_manager.as_ref(), &last_block, shard_id)
                .unwrap(),
            next_height,
            shard_id,
        )
        .unwrap()
        .unwrap()
}

pub fn create_chunk_on_height(
    client: &mut Client,
    next_height: BlockHeight,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>) {
    create_chunk_on_height_for_shard(client, next_height, 0)
}

pub fn create_chunk_with_transactions(
    client: &mut Client,
    transactions: Vec<SignedTransaction>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    create_chunk(client, Some(transactions), None)
}

/// Create a chunk with specified transactions and possibly a new state root.
/// Useful for writing tests with challenges.
pub fn create_chunk(
    client: &mut Client,
    replace_transactions: Option<Vec<SignedTransaction>>,
    replace_tx_root: Option<CryptoHash>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    let last_block = client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap();
    let next_height = last_block.header().height() + 1;
    let (mut chunk, mut merkle_paths, receipts) = client
        .produce_chunk(
            *last_block.hash(),
            last_block.header().epoch_id(),
            last_block.chunks()[0].clone(),
            next_height,
            0,
        )
        .unwrap()
        .unwrap();
    let should_replace = replace_transactions.is_some() || replace_tx_root.is_some();
    let transactions = replace_transactions.unwrap_or_else(Vec::new);
    let tx_root = match replace_tx_root {
        Some(root) => root,
        None => merklize(&transactions).0,
    };
    // reconstruct the chunk with changes (if any)
    if should_replace {
        // The best way it to decode chunk, replace transactions and then recreate encoded chunk.
        let total_parts = client.chain.epoch_manager.num_total_parts();
        let data_parts = client.chain.epoch_manager.num_data_parts();
        let decoded_chunk = chunk.decode_chunk(data_parts).unwrap();
        let parity_parts = total_parts - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

        let signer = client.validator_signer.as_ref().unwrap().clone();
        let header = chunk.cloned_header();
        let (mut encoded_chunk, mut new_merkle_paths) = EncodedShardChunk::new(
            *header.prev_block_hash(),
            header.prev_state_root(),
            header.prev_outcome_root(),
            header.height_created(),
            header.shard_id(),
            &mut rs,
            header.prev_gas_used(),
            header.gas_limit(),
            header.prev_balance_burnt(),
            tx_root,
            header.prev_validator_proposals().collect(),
            transactions,
            decoded_chunk.prev_outgoing_receipts(),
            header.prev_outgoing_receipts_root(),
            &*signer,
            PROTOCOL_VERSION,
        )
        .unwrap();
        swap(&mut chunk, &mut encoded_chunk);
        swap(&mut merkle_paths, &mut new_merkle_paths);
    }
    match &mut chunk {
        EncodedShardChunk::V1(chunk) => {
            chunk.header.height_included = next_height;
        }
        EncodedShardChunk::V2(chunk) => {
            *chunk.header.height_included_mut() = next_height;
        }
    }
    let block_merkle_tree =
        client.chain.chain_store().get_block_merkle_tree(last_block.hash()).unwrap();
    let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
    block_merkle_tree.insert(*last_block.hash());
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        last_block.header(),
        next_height,
        last_block.header().block_ordinal() + 1,
        vec![chunk.cloned_header()],
        last_block.header().epoch_id().clone(),
        last_block.header().next_epoch_id().clone(),
        None,
        vec![],
        Ratio::new(0, 1),
        0,
        100,
        None,
        vec![],
        vec![],
        &*client.validator_signer.as_ref().unwrap().clone(),
        *last_block.header().next_bp_hash(),
        block_merkle_tree.root(),
        None,
    );
    (chunk, merkle_paths, receipts, block)
}

/// Keep running catchup until there is no more catchup work that can be done
/// Note that this function does not necessarily mean that all blocks are caught up.
/// It's possible that some blocks that need to be caught up are still being processed
/// and the catchup process can't catch up on these blocks yet.
pub fn run_catchup(
    client: &mut Client,
    highest_height_peers: &[HighestHeightPeerInfo],
) -> Result<(), Error> {
    let f = |_| {};
    let block_messages = Arc::new(RwLock::new(vec![]));
    let block_inside_messages = block_messages.clone();
    let block_catch_up = move |msg: BlockCatchUpRequest| {
        block_inside_messages.write().unwrap().push(msg);
    };
    let resharding_messages = Arc::new(RwLock::new(vec![]));
    let resharding_inside_messages = resharding_messages.clone();
    let resharding = move |msg: ReshardingRequest| {
        resharding_inside_messages.write().unwrap().push(msg);
    };
    let _ = System::new();
    let state_parts_arbiter_handle = Arbiter::new().handle();
    loop {
        client.run_catchup(
            highest_height_peers,
            &f,
            &block_catch_up,
            &resharding,
            Arc::new(|_| {}),
            &state_parts_arbiter_handle,
        )?;
        let mut catchup_done = true;
        for msg in block_messages.write().unwrap().drain(..) {
            let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work)
                .into_iter()
                .map(|res| res.1)
                .collect_vec();
            if let Some((_, _, blocks_catch_up_state)) =
                client.catchup_state_syncs.get_mut(&msg.sync_hash)
            {
                assert!(blocks_catch_up_state.scheduled_blocks.remove(&msg.block_hash));
                blocks_catch_up_state.processed_blocks.insert(msg.block_hash, results);
            } else {
                panic!("block catch up processing result from unknown sync hash");
            }
            catchup_done = false;
        }
        for msg in resharding_messages.write().unwrap().drain(..) {
            let response = Chain::build_state_for_split_shards(msg);
            if let Some((sync, _, _)) = client.catchup_state_syncs.get_mut(&response.sync_hash) {
                // We are doing catchup
                sync.set_resharding_result(response.shard_id, response.new_state_roots);
            } else {
                client
                    .state_sync
                    .set_resharding_result(response.shard_id, response.new_state_roots);
            }
            catchup_done = false;
        }
        if catchup_done {
            break;
        }
    }
    Ok(())
}
