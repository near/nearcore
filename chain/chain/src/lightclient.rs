use crate::error::Error;
use crate::{ChainStoreAccess, ErrorKind, RuntimeAdapter};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::EpochId;
use near_primitives::views::{
    BlockHeaderInnerLiteView, LightClientApprovalView, LightClientBlockView, ValidatorStakeView,
};
use std::collections::{HashMap, HashSet};

const MAX_LIGHT_CLIENT_FUTURE_BLOCKS: usize = 50;

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
///  * `pushed_back_commit_hash` - in case `block_header` corresponds to the last final block in
///                   some epoch, it is possible that the `last_quorum_pre_commit` for the block
///                   from which perspective `block_header` was final. In that case this argument
///                   should contain the pushed back hash (and the `block_header` must correspond
///                   to the non-pushed back block)
///  * `chain_store`
///  * `block_producers` - the ordered list of block producers in the epoch of the block that
///                   corresponds to `block_header`
///  * `next_block_producers` - the ordered list of block producers in the next epoch, if the light
///                   client should contain them (otherwise `None`)
///
/// # Requirements:
///    `block_header` must correspond to some block for which `compute_quorums` returned it as
///                   `quorum_pre_commit` for some other block. Note that it doesn't necessarily
///                   correspond to some block's `last_quorum_pre_commit` since for the last block
///                   in each epoch we push it back via `push_final_block_back_if_needed`.
///    `get_next_final_block_hash` in the chain is set for all the blocks between the block
///                   corresponding to `block_header` and the first block from which perspective
///                   such block is final
pub fn create_light_client_block_view(
    block_header: &BlockHeader,
    pushed_back_commit_hash: &CryptoHash,
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
    let prev_hash = block_header.prev_hash;

    let validator_ords = block_producers
        .iter()
        .enumerate()
        .map(|(i, v): (usize, &ValidatorStakeView)| (v.account_id.clone(), i))
        .collect::<HashMap<_, _>>();

    let mut qv_hash = None;
    let mut qv_approvals = block_producers.iter().map(|_| None).collect::<Vec<_>>();
    let mut qc_approvals = block_producers.iter().map(|_| None).collect::<Vec<_>>();
    let mut future_inner_hashes = vec![];

    let mut seen_hashes = HashSet::new();

    let mut last_block_hash = block_header.hash();
    let mut found_qv = false;
    let mut found_qc = false;

    // +1 below is because the last iteration of the loop doesn't add the hash to the
    // `future_inner_blocks` (if it does, it means `found_qc` is not set, and the error is
    // returned), and thus to cap the `future_inner_blocks` at `MAX_LIGHT_CLIENT_FUTURE_BLOCKS`,
    // we can go one iteration further
    for _i in 0..(MAX_LIGHT_CLIENT_FUTURE_BLOCKS + 1) {
        let next_block_hash = chain_store.get_next_block_hash(&last_block_hash)?.clone();
        let next_block_header = chain_store.get_block_header(&next_block_hash)?;

        for approval in next_block_header.inner_rest.approvals.iter() {
            let reference_hash = match approval.reference_hash {
                Some(reference_hash) => reference_hash,
                None => continue,
            };
            if !seen_hashes.contains(&reference_hash) {
                if let Some(validator_ord) = validator_ords.get(&approval.account_id) {
                    let light_approval = LightClientApprovalView {
                        parent_hash: approval.parent_hash,
                        reference_hash: reference_hash,
                        signature: approval.signature.clone(),
                    };
                    if found_qv {
                        qc_approvals[*validator_ord] = Some(light_approval);
                    } else {
                        qv_approvals[*validator_ord] = Some(light_approval);
                    }
                }
            }
        }

        if &next_block_header.inner_rest.last_quorum_pre_commit == pushed_back_commit_hash {
            found_qc = true;
            break;
        }

        if !found_qv && next_block_header.inner_rest.last_quorum_pre_vote == block_header.hash() {
            qv_hash = Some(next_block_header.hash());
            found_qv = true;
        }

        seen_hashes.insert(next_block_hash);
        future_inner_hashes.push(BlockHeader::compute_inner_hash(
            &next_block_header.inner_lite,
            &next_block_header.inner_rest,
        ));
        last_block_hash = next_block_hash.clone();
    }

    if !found_qc {
        return Err(ErrorKind::Other(
            "The block doesn't have quorum pre-commit within MAX_LIGHT_CLIENT_FUTURE_BLOCKS".into(),
        )
        .into());
    }

    // `qv_hash` here might be unitialized. That would only happen for final blocks that were pushed
    // back via `EpochManager::push_final_block_back_if_needed`
    let qv_hash = match qv_hash {
        Some(qv_hash) => qv_hash,
        None => {
            return Err(ErrorKind::Other(
                "The block passed to `create_light_client_block_view` doesn't have any block having it as quorum_pre_vote".into(),
            )
            .into());
        }
    };

    Ok(LightClientBlockView {
        inner_lite: inner_lite_view,
        inner_rest_hash,
        next_bps: next_block_producers,
        prev_hash,
        qv_hash,
        future_inner_hashes,
        qv_approvals,
        qc_approvals,
    })
}
