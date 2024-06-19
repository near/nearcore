// FIXME(nagisa): Is there a good reason we're triggering this? Luckily though this is just test
// code so we're in the clear.
#![allow(clippy::arc_with_non_send_sync)]

use std::mem::swap;
use std::sync::{Arc, RwLock};

use crate::client::ProduceChunkResult;
use crate::Client;
use actix_rt::{Arbiter, System};
use itertools::Itertools;
use near_async::futures::ActixArbiterHandleFutureSpawner;
use near_async::messaging::{noop, IntoSender, Sender};
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest};
use near_chain::resharding::ReshardingRequest;
use near_chain::test_utils::{wait_for_all_blocks_in_processing, wait_for_block_in_processing};
use near_chain::{Chain, ChainStoreAccess, Provenance};
use near_client_primitives::types::Error;
use near_network::types::HighestHeightPeerInfo;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, PartialMerkleTree};
use near_primitives::sharding::{EncodedShardChunk, ShardChunk};
use near_primitives::stateless_validation::ChunkEndorsement;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::PROTOCOL_VERSION;
use num_rational::Ratio;
use reed_solomon_erasure::galois_8::ReedSolomon;

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
        allow_errors: bool,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let signer = self.validator_signer.get();
        self.start_process_block(block, provenance, None, &signer)?;
        wait_for_all_blocks_in_processing(&mut self.chain);
        let (accepted_blocks, errors) =
            self.postprocess_ready_blocks(None, should_produce_chunk, &signer);
        if !allow_errors {
            assert!(errors.is_empty(), "unexpected errors when processing blocks: {errors:#?}");
        }
        Ok(accepted_blocks)
    }

    pub fn process_block_test(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, true, false)
    }

    pub fn process_block_test_no_produce_chunk(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false, false)
    }

    pub fn process_block_test_no_produce_chunk_allow_errors(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false, true)
    }

    /// This function finishes processing all blocks that started being processed.
    pub fn finish_blocks_in_processing(&mut self) -> Vec<CryptoHash> {
        let signer = self.validator_signer.get();
        let mut accepted_blocks = vec![];
        while wait_for_all_blocks_in_processing(&mut self.chain) {
            accepted_blocks.extend(self.postprocess_ready_blocks(None, true, &signer).0);
        }
        accepted_blocks
    }

    /// This function finishes processing block with hash `hash`, if the processing of that block
    /// has started.
    pub fn finish_block_in_processing(&mut self, hash: &CryptoHash) -> Vec<CryptoHash> {
        if let Ok(()) = wait_for_block_in_processing(&mut self.chain, hash) {
            let signer = self.validator_signer.get();
            let (accepted_blocks, _) = self.postprocess_ready_blocks(None, true, &signer);
            return accepted_blocks;
        }
        vec![]
    }

    /// Manually produce a single chunk on the given shard and send out the corresponding network messages
    pub fn produce_one_chunk(&mut self, height: BlockHeight, shard_id: ShardId) -> ShardChunk {
        let ProduceChunkResult {
            chunk: encoded_chunk,
            encoded_chunk_parts_paths: merkle_paths,
            receipts,
            transactions_storage_proof,
        } = create_chunk_on_height_for_shard(self, height, shard_id);
        let signer = self.validator_signer.get();
        let shard_chunk = self
            .persist_and_distribute_encoded_chunk(
                encoded_chunk,
                merkle_paths,
                receipts,
                signer.as_ref().unwrap().validator_id().clone(),
            )
            .unwrap();
        let prev_block = self.chain.get_block(shard_chunk.prev_block()).unwrap();
        let prev_chunk_header = Chain::get_prev_chunk_header(
            self.epoch_manager.as_ref(),
            &prev_block,
            shard_chunk.shard_id(),
        )
        .unwrap();
        self.send_chunk_state_witness_to_chunk_validators(
            &self.epoch_manager.get_epoch_id_from_prev_block(shard_chunk.prev_block()).unwrap(),
            prev_block.header(),
            &prev_chunk_header,
            &shard_chunk,
            transactions_storage_proof,
            &signer,
        )
        .unwrap();
        shard_chunk
    }
}

fn create_chunk_on_height_for_shard(
    client: &mut Client,
    next_height: BlockHeight,
    shard_id: ShardId,
) -> ProduceChunkResult {
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block = client.chain.get_block(&last_block_hash).unwrap();
    let signer = client.validator_signer.get();
    client
        .try_produce_chunk(
            &last_block,
            &client.epoch_manager.get_epoch_id_from_prev_block(&last_block_hash).unwrap(),
            Chain::get_prev_chunk_header(client.epoch_manager.as_ref(), &last_block, shard_id)
                .unwrap(),
            next_height,
            shard_id,
            signer,
        )
        .unwrap()
        .unwrap()
}

pub fn create_chunk_on_height(client: &mut Client, next_height: BlockHeight) -> ProduceChunkResult {
    create_chunk_on_height_for_shard(client, next_height, 0)
}

pub fn create_chunk_with_transactions(
    client: &mut Client,
    transactions: Vec<SignedTransaction>,
) -> (ProduceChunkResult, Block) {
    create_chunk(client, Some(transactions), None)
}

/// Create a chunk with specified transactions and possibly a new state root.
/// Useful for writing tests with challenges.
pub fn create_chunk(
    client: &mut Client,
    replace_transactions: Option<Vec<SignedTransaction>>,
    replace_tx_root: Option<CryptoHash>,
) -> (ProduceChunkResult, Block) {
    let last_block = client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap();
    let next_height = last_block.header().height() + 1;
    let signer = client.validator_signer.get();
    let ProduceChunkResult {
        mut chunk,
        encoded_chunk_parts_paths: mut merkle_paths,
        receipts,
        transactions_storage_proof,
    } = client
        .try_produce_chunk(
            &last_block,
            last_block.header().epoch_id(),
            last_block.chunks()[0].clone(),
            next_height,
            0,
            signer.clone(),
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
        let rs = ReedSolomon::new(data_parts, parity_parts).unwrap();

        let signer = signer.unwrap();
        let header = chunk.cloned_header();
        let (mut encoded_chunk, mut new_merkle_paths) = EncodedShardChunk::new(
            *header.prev_block_hash(),
            header.prev_state_root(),
            header.prev_outcome_root(),
            header.height_created(),
            header.shard_id(),
            &rs,
            header.prev_gas_used(),
            header.gas_limit(),
            header.prev_balance_burnt(),
            tx_root,
            header.prev_validator_proposals().collect(),
            transactions,
            decoded_chunk.prev_outgoing_receipts(),
            header.prev_outgoing_receipts_root(),
            header.congestion_info(),
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

    let signer = client.validator_signer.get().unwrap();
    let endorsement = ChunkEndorsement::new(chunk.cloned_header().chunk_hash(), signer.as_ref());
    block_merkle_tree.insert(*last_block.hash());
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        last_block.header(),
        next_height,
        last_block.header().block_ordinal() + 1,
        vec![chunk.cloned_header()],
        vec![vec![Some(Box::new(endorsement.signature))]],
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
        &*client.validator_signer.get().unwrap(),
        *last_block.header().next_bp_hash(),
        block_merkle_tree.root(),
        client.clock.now_utc(),
    );
    (
        ProduceChunkResult {
            chunk,
            encoded_chunk_parts_paths: merkle_paths,
            receipts,
            transactions_storage_proof,
        },
        block,
    )
}

/// Keep running catchup until there is no more catchup work that can be done
/// Note that this function does not necessarily mean that all blocks are caught up.
/// It's possible that some blocks that need to be caught up are still being processed
/// and the catchup process can't catch up on these blocks yet.
pub fn run_catchup(
    client: &mut Client,
    highest_height_peers: &[HighestHeightPeerInfo],
) -> Result<(), Error> {
    let block_messages = Arc::new(RwLock::new(vec![]));
    let block_inside_messages = block_messages.clone();
    let block_catch_up = Sender::from_fn(move |msg: BlockCatchUpRequest| {
        block_inside_messages.write().unwrap().push(msg);
    });
    let resharding_messages = Arc::new(RwLock::new(vec![]));
    let resharding_inside_messages = resharding_messages.clone();
    let resharding = Sender::from_fn(move |msg: ReshardingRequest| {
        resharding_inside_messages.write().unwrap().push(msg);
    });
    let _ = System::new();
    let state_parts_future_spawner = ActixArbiterHandleFutureSpawner(Arbiter::new().handle());
    loop {
        let signer = client.validator_signer.get();
        client.run_catchup(
            highest_height_peers,
            &noop().into_sender(),
            &noop().into_sender(),
            &block_catch_up,
            &resharding,
            None,
            &state_parts_future_spawner,
            &signer,
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
