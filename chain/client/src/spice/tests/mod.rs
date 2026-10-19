use near_async::time::Clock;
use near_chain::Chain;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::test_utils::get_fake_next_block_chunk_headers;
use near_chain_configs::Genesis;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Block;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::{
    TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
};
use near_primitives::types::{Balance, BlockHeight, ShardId};
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

/// A pre-spice genesis for the activation-boundary tests, with `producers` and
/// `chunk_validators_only` as the epoch validators and the first producer funded.
pub(crate) fn pre_spice_boundary_genesis(
    producers: &[&str],
    chunk_validators_only: &[&str],
    shard_layout: ShardLayout,
) -> Genesis {
    TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .epoch_length(5)
        .transaction_validity_period(10)
        .protocol_version(pre_spice_protocol_version())
        .shard_layout(shard_layout)
        .validators_spec(ValidatorsSpec::desired_roles(producers, chunk_validators_only))
        .add_user_account_simple(producers[0].parse().unwrap(), Balance::from_near(1))
        .build()
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
        let height = block.header().height().checked_add(1).unwrap();
        block = build_pre_spice_block(chain, &block, height, signer);
        if is_last_pre_spice_block(chain.epoch_manager.as_ref(), block.hash()).unwrap() {
            return block;
        }
    }
    panic!("chain never reached a last pre-spice block")
}

/// Saves and records a fabricated pre-spice block at `height` on `prev_block`, voting
/// for spice, and returns it.
pub(crate) fn build_pre_spice_block(
    chain: &mut Chain,
    prev_block: &Block,
    height: BlockHeight,
    signer: &Arc<ValidatorSigner>,
) -> Arc<Block> {
    let epoch_manager = chain.epoch_manager.clone();
    let chunks = pre_spice_chunk_headers(chain, prev_block, height);
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block.hash()).unwrap();
    let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(prev_block.hash()).unwrap();
    // The epoch info aggregator asserts one bitmap slot per assigned chunk
    // validator, so the endorsement vectors have to be sized from the epoch.
    let chunk_endorsements = epoch_manager
        .get_shard_layout(&epoch_id)
        .unwrap()
        .shard_ids()
        .map(|shard_id| {
            let assignments =
                epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height).unwrap();
            vec![Some(Box::new(signer.sign_bytes(&[]))); assignments.assignments().len()]
        })
        .collect();
    let mut block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer.clone())
        .height(height)
        .chunks(chunks)
        .chunk_endorsements(chunk_endorsements)
        .epoch_id(epoch_id)
        .next_epoch_id(next_epoch_id)
        .protocol_version(pre_spice_protocol_version())
        .build_owned();
    block.mut_header().set_latest_protocol_version(ProtocolFeature::Spice.protocol_version());
    block.mut_header().resign(signer.as_ref());
    let block = Arc::new(block);
    save_and_record_block(chain, &block);
    block
}

/// Fake chunk headers for the block at `height` on `prev_block`, each committing to an empty
/// outgoing receipt list as a real chunk commits to its predecessor's receipts, so
/// that the proofs of [`empty_outgoing_receipt_proofs`] verify against them.
fn pre_spice_chunk_headers(
    chain: &Chain,
    prev_block: &Block,
    height: BlockHeight,
) -> Vec<ShardChunkHeader> {
    let epoch_manager = chain.epoch_manager.as_ref();
    get_fake_next_block_chunk_headers(prev_block, epoch_manager)
        .into_iter()
        .map(|fake| {
            let shard_id = fake.shard_id();
            let (receipts_root, _) =
                empty_outgoing_receipt_proofs(chain, prev_block.hash(), shard_id);
            let chunk_producer = epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    shard_id,
                    epoch_id: *prev_block.header().epoch_id(),
                    height_created: height,
                })
                .unwrap();
            let chunk_signer = create_test_signer(chunk_producer.account_id().as_str());
            let mut chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                *prev_block.hash(),
                CryptoHash::default(),
                CryptoHash::default(),
                CryptoHash::default(),
                0,
                height,
                shard_id,
                Gas::ZERO,
                Gas::ZERO,
                Balance::ZERO,
                receipts_root,
                CryptoHash::default(),
                vec![],
                CongestionInfo::default(),
                BandwidthRequests::empty(),
                None,
                &chunk_signer,
                pre_spice_protocol_version(),
            ));
            *chunk_header.height_included_mut() = height;
            chunk_header
        })
        .collect()
}

/// The outgoing receipts root and per-shard proofs of a fabricated pre-spice chunk of
/// `from_shard_id` in the block after `prev_block_hash`, which sends no receipts.
pub(crate) fn empty_outgoing_receipt_proofs(
    chain: &Chain,
    prev_block_hash: &CryptoHash,
    from_shard_id: ShardId,
) -> (CryptoHash, Vec<ReceiptProof>) {
    let shard_layout =
        chain.epoch_manager.get_shard_layout_from_prev_block(prev_block_hash).unwrap();
    Chain::create_receipts_proofs_from_outgoing_receipts(&shard_layout, from_shard_id, vec![])
        .unwrap()
}

const MAX_BLOCKS_TO_ACTIVATION: usize = 30;
