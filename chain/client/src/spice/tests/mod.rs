use near_async::time::Clock;
use near_chain::Chain;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::test_utils::get_fake_next_block_chunk_headers;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::sharding::ShardChunk;
use near_primitives::test_utils::{TestBlockBuilder, pre_spice_protocol_version};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolFeature;
use std::sync::Arc;

mod chunk_executor_actor;
mod chunk_validator_actor;
mod data_distributor_actor;

/// Saves `block` and records it in the epoch manager the way block postprocessing
/// does, without processing the block: the epoch manager has to know the block
/// to answer activation-boundary questions about it, and its chunks have to be on
/// disk for the executor to read them.
pub(crate) fn save_and_record_block(chain: &mut Chain, block: &Arc<Block>) {
    let protocol_version =
        chain.epoch_manager.get_epoch_protocol_version(block.header().epoch_id()).unwrap();
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block(block.clone());
    store_update.save_block_header(block.header().clone()).unwrap();
    for chunk_header in block.chunks().iter_raw() {
        store_update.save_chunk(ShardChunk::new(chunk_header.clone(), vec![], vec![]));
    }
    let block_info = BlockInfo::from_header(
        block.header(),
        block.header().height().saturating_sub(2),
        protocol_version,
    );
    let epoch_manager_update = chain
        .epoch_manager
        .add_validator_proposals(block_info, *block.header().random_value())
        .unwrap();
    store_update.merge(epoch_manager_update.into());
    store_update.commit().unwrap();
}

/// Extends `chain` with fabricated pre-spice blocks that vote for spice until the
/// tip is a last pre-spice block, and returns it.
/// The epoch after the returned block is the first spice epoch.
pub(crate) fn build_to_last_pre_spice_block(
    chain: &mut Chain,
    signer: &Arc<ValidatorSigner>,
) -> Arc<Block> {
    let mut block = chain.genesis_block();
    for _ in 0..MAX_BLOCKS_TO_ACTIVATION {
        let epoch_manager = chain.epoch_manager.clone();
        let chunks = get_fake_next_block_chunk_headers(&block, epoch_manager.as_ref());
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(block.hash()).unwrap();
        let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(block.hash()).unwrap();
        let height = block.header().height().checked_add(1).unwrap();
        // The epoch info aggregator asserts one bitmap slot per assigned chunk
        // validator, so the endorsement vectors have to be sized from the epoch.
        let chunk_endorsements = epoch_manager
            .get_shard_layout(&epoch_id)
            .unwrap()
            .shard_ids()
            .map(|shard_id| {
                let assignments = epoch_manager
                    .get_chunk_validator_assignments(&epoch_id, shard_id, height)
                    .unwrap();
                vec![Some(Box::new(signer.sign_bytes(&[]))); assignments.assignments().len()]
            })
            .collect();
        let mut next = TestBlockBuilder::from_prev_block(Clock::real(), &block, signer.clone())
            .chunks(chunks)
            .chunk_endorsements(chunk_endorsements)
            .epoch_id(epoch_id)
            .next_epoch_id(next_epoch_id)
            .protocol_version(pre_spice_protocol_version())
            .build_owned();
        next.mut_header().set_latest_protocol_version(ProtocolFeature::Spice.protocol_version());
        next.mut_header().resign(signer.as_ref());
        let next = Arc::new(next);
        save_and_record_block(chain, &next);
        block = next;
        if is_last_pre_spice_block(chain.epoch_manager.as_ref(), block.hash()).unwrap() {
            return block;
        }
    }
    panic!("chain never reached a last pre-spice block")
}

const MAX_BLOCKS_TO_ACTIVATION: usize = 30;
