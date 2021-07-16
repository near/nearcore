use near_chain_primitives::Error;
use near_primitives::block::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::EpochId;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{BlockHeaderInnerLiteView, LightClientBlockView};

use crate::{ChainStoreAccess, RuntimeAdapter};

pub fn get_epoch_block_producers_view(
    epoch_id: &EpochId,
    prev_hash: &CryptoHash,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<Vec<ValidatorStakeView>, Error> {
    Ok(runtime_adapter
        .get_epoch_block_producers_ordered(epoch_id, prev_hash)?
        .iter()
        .map(|x| x.0.clone().into())
        .collect::<Vec<_>>())
}

/// Creates the `LightClientBlock` from the information in the chain store for a given block.
///
/// # Arguments
///  * `block_header` - block header of the block for which the `LightClientBlock` is computed
///  * `chain_store`
///  * `block_producers` - the ordered list of block producers in the epoch of the block that
///                   corresponds to `block_header`
///  * `next_block_producers` - the ordered list of block producers in the next epoch, if the light
///                   client should contain them (otherwise `None`)
///
/// # Requirements:
///    `get_next_final_block_hash` in the chain is set for the block that `block_header` corresponds
///                   to and for the next block, and the three blocks must have sequential heights.
pub fn create_light_client_block_view(
    block_header: &BlockHeader,
    chain_store: &mut dyn ChainStoreAccess,
    next_block_producers: Option<Vec<ValidatorStakeView>>,
) -> Result<LightClientBlockView, Error> {
    let inner_lite_view = BlockHeaderInnerLiteView {
        height: block_header.height(),
        epoch_id: block_header.epoch_id().0,
        next_epoch_id: block_header.next_epoch_id().0,
        prev_state_root: *block_header.prev_state_root(),
        outcome_root: *block_header.outcome_root(),
        timestamp: block_header.raw_timestamp(),
        timestamp_nanosec: block_header.raw_timestamp(),
        next_bp_hash: *block_header.next_bp_hash(),
        block_merkle_root: *block_header.block_merkle_root(),
    };
    let inner_rest_hash = hash(&block_header.inner_rest_bytes());

    let next_block_hash = chain_store.get_next_block_hash(&block_header.hash())?.clone();
    let next_block_header = chain_store.get_block_header(&next_block_hash)?;
    let next_block_inner_hash = BlockHeader::compute_inner_hash(
        &next_block_header.inner_lite_bytes(),
        &next_block_header.inner_rest_bytes(),
    );

    let after_next_block_hash = chain_store.get_next_block_hash(&next_block_hash)?.clone();
    let after_next_block_header = chain_store.get_block_header(&after_next_block_hash)?;
    let approvals_after_next = after_next_block_header.approvals().to_vec();

    Ok(LightClientBlockView {
        prev_block_hash: *block_header.prev_hash(),
        next_block_inner_hash,
        inner_lite: inner_lite_view,
        inner_rest_hash,
        next_bps: next_block_producers,
        approvals_after_next,
    })
}
