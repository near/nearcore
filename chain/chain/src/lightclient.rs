use std::collections::HashMap;

use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::EpochId;
use near_primitives::views::{BlockHeaderInnerLiteView, LightClientBlockView, ValidatorStakeView};

use crate::error::Error;
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
    block_producers: &Vec<ValidatorStakeView>,
    next_block_producers: Option<Vec<ValidatorStakeView>>,
) -> Result<LightClientBlockView, Error> {
    let inner_lite = block_header.inner_lite.clone();
    let inner_lite_view = BlockHeaderInnerLiteView {
        height: inner_lite.height,
        epoch_id: inner_lite.epoch_id.0,
        next_epoch_id: inner_lite.next_epoch_id.0,
        prev_state_root: inner_lite.prev_state_root,
        outcome_root: inner_lite.outcome_root,
        timestamp: inner_lite.timestamp,
        next_bp_hash: inner_lite.next_bp_hash,
    };
    let inner_rest_hash = block_header.inner_rest.hash();

    let validator_ords = block_producers
        .iter()
        .enumerate()
        .map(|(i, v): (usize, &ValidatorStakeView)| (v.account_id.clone(), i))
        .collect::<HashMap<_, _>>();

    let /*mut*/ approvals_next = vec![None; validator_ords.len()];
    let /*mut*/ approvals_after_next = vec![None; validator_ords.len()];

    let next_block_hash = chain_store.get_next_block_hash(&block_header.hash())?.clone();
    //let next_block_header = chain_store.get_block_header(&next_block_hash)?;

    /*for approval in next_block_header.inner_rest.approvals.iter() {
        approvals_next[*validator_ords.get(&approval.account_id).expect(
            "Block can't get accepted with approvals by someone who is not a block producer",
        )] = Some(approval.signature.clone());
    }*/

    //let after_next_block_hash = chain_store.get_next_block_hash(&next_block_hash)?.clone();
    //let after_next_block_header = chain_store.get_block_header(&after_next_block_hash)?;

    /*for approval in after_next_block_header.inner_rest.approvals.iter() {
        approvals_after_next[*validator_ords.get(&approval.account_id).expect(
            "Block can't get accepted with approvals by someone who is not a block producer",
        )] = Some(approval.signature.clone());
    }*/

    Ok(LightClientBlockView {
        inner_lite: inner_lite_view,
        inner_rest_hash,
        next_block_hash,
        next_bps: next_block_producers,
        approvals_next,
        approvals_after_next,
    })
}
