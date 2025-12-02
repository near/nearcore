use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_chain_primitives::Error;
use near_crypto::Signature;
use near_primitives::block::BlockHeader;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_sync::{
    EpochSyncProof, EpochSyncProofCurrentEpochData, EpochSyncProofEpochData,
    EpochSyncProofLastEpochData, EpochSyncProofV1, should_use_versioned_bp_hash_format,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, ApprovalStake, EpochId};
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::merkle_proof::MerkleProofAccess;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::instrument;

/// Derives an epoch sync proof using a target epoch whose last final block is given
/// (actually it's the block after that, so that we can find the approvals).
pub fn derive_epoch_sync_proof_from_last_final_block(
    store: Store,
    next_block_header_after_last_final_block_of_current_epoch: &BlockHeader,
) -> Result<EpochSyncProof, Error> {
    let chain_store = store.chain_store();
    let epoch_store = store.epoch_store();

    let last_final_block_header_in_current_epoch = chain_store
        .get_block_header(next_block_header_after_last_final_block_of_current_epoch.prev_hash())?;
    let current_epoch = *last_final_block_header_in_current_epoch.epoch_id();
    let current_epoch_info = epoch_store.get_epoch_info(&current_epoch)?;
    let next_epoch = *last_final_block_header_in_current_epoch.next_epoch_id();
    let next_epoch_info = epoch_store.get_epoch_info(&next_epoch)?;
    let genesis_epoch_info = epoch_store.get_epoch_info(&EpochId::default())?;

    // If we have an existing (possibly and likely outdated) EpochSyncProof stored on disk,
    // the last epoch we have a proof for is the "previous epoch" included in that EpochSyncProof.
    // Otherwise, the last epoch we have a "proof" for is the genesis epoch.
    let existing_epoch_sync_proof = epoch_store.get_epoch_sync_proof()?;
    let last_epoch_we_have_proof_for = existing_epoch_sync_proof
        .as_ref()
        .and_then(|existing_proof| {
            existing_proof
                .all_epochs
                .last()
                .map(|last_epoch| *last_epoch.last_final_block_header.epoch_id())
        })
        .unwrap_or_else(EpochId::default);
    let last_epoch_height_we_have_proof_for = existing_epoch_sync_proof
        .as_ref()
        .map(|existing_proof| existing_proof.last_epoch.next_epoch_info.epoch_height())
        .unwrap_or_else(|| genesis_epoch_info.epoch_height() + 1);

    // If the proof we stored is for the same epoch as current or older, then just return that.
    if current_epoch_info.epoch_height() <= last_epoch_height_we_have_proof_for {
        if let Some(existing_proof) = existing_epoch_sync_proof {
            return Ok(EpochSyncProof::V1(existing_proof));
        }
        // Corner case for if the current epoch is genesis or right after genesis.
        return Err(Error::Other("Not enough epochs after genesis to epoch sync".to_string()));
    }

    let all_epochs_since_last_proof = get_all_epoch_proofs_in_range(
        &store,
        last_epoch_we_have_proof_for,
        next_epoch,
        &last_final_block_header_in_current_epoch,
        next_block_header_after_last_final_block_of_current_epoch.approvals().to_vec(),
    )?;
    if all_epochs_since_last_proof.len() < 2 {
        return Err(Error::Other("Not enough epochs after genesis to epoch sync".to_string()));
    }
    let prev_epoch = *all_epochs_since_last_proof
        .get(all_epochs_since_last_proof.len() - 2)
        .unwrap()
        .last_final_block_header
        .epoch_id();
    let prev_epoch_info = epoch_store.get_epoch_info(&prev_epoch)?;
    let last_block_of_prev_epoch = chain_store.get_block_header(&next_epoch.0)?;
    let last_block_info_of_prev_epoch =
        epoch_store.get_block_info(last_block_of_prev_epoch.hash())?;
    let second_last_block_of_prev_epoch =
        chain_store.get_block_header(last_block_of_prev_epoch.prev_hash())?;
    let second_last_block_info_of_prev_epoch =
        epoch_store.get_block_info(last_block_of_prev_epoch.prev_hash())?;
    let first_block_info_of_prev_epoch =
        epoch_store.get_block_info(last_block_info_of_prev_epoch.epoch_first_block())?;
    let block_info_for_final_block_of_current_epoch =
        epoch_store.get_block_info(last_final_block_header_in_current_epoch.hash())?;
    let first_block_of_current_epoch = chain_store
        .get_block_header(block_info_for_final_block_of_current_epoch.epoch_first_block())?;

    let merkle_proof_for_first_block_of_current_epoch = store
        .compute_past_block_proof_in_merkle_tree_of_later_block(
            first_block_of_current_epoch.hash(),
            last_final_block_header_in_current_epoch.hash(),
        )?;
    let partial_merkle_tree_for_first_block_of_current_epoch =
        chain_store.get_block_merkle_tree(first_block_of_current_epoch.hash())?;

    let all_epochs_including_old_proof = existing_epoch_sync_proof
        .map(|proof| proof.all_epochs)
        .unwrap_or_else(Vec::new)
        .into_iter()
        .chain(all_epochs_since_last_proof.into_iter())
        .collect();
    let proof = EpochSyncProofV1 {
        all_epochs: all_epochs_including_old_proof,
        last_epoch: EpochSyncProofLastEpochData {
            epoch_info: prev_epoch_info,
            next_epoch_info: current_epoch_info,
            next_next_epoch_info: next_epoch_info,
            first_block_in_epoch: first_block_info_of_prev_epoch,
            last_block_in_epoch: last_block_info_of_prev_epoch,
            second_last_block_in_epoch: second_last_block_info_of_prev_epoch,
        },
        current_epoch: EpochSyncProofCurrentEpochData {
            first_block_header_in_epoch: <_>::clone(&first_block_of_current_epoch),
            last_block_header_in_prev_epoch: <_>::clone(&last_block_of_prev_epoch),
            second_last_block_header_in_prev_epoch: <_>::clone(&second_last_block_of_prev_epoch),
            merkle_proof_for_first_block: merkle_proof_for_first_block_of_current_epoch,
            partial_merkle_tree_for_first_block:
                partial_merkle_tree_for_first_block_of_current_epoch,
        },
    };

    Ok(EpochSyncProof::V1(proof))
}

/// Get all the past epoch data needed for epoch sync, between `after_epoch` and `next_epoch`
/// (both exclusive). `current_epoch_any_header` is any block header in the current epoch,
/// which is the epoch before `next_epoch`.
#[instrument(skip(
    store,
    current_epoch_last_final_block_header,
    current_epoch_second_last_block_approvals
))]
fn get_all_epoch_proofs_in_range(
    store: &Store,
    after_epoch: EpochId,
    next_epoch: EpochId,
    current_epoch_last_final_block_header: &BlockHeader,
    current_epoch_second_last_block_approvals: Vec<Option<Box<Signature>>>,
) -> Result<Vec<EpochSyncProofEpochData>, Error> {
    let chain_store = store.chain_store();
    let epoch_store = store.epoch_store();

    // We're going to get all the epochs and then figure out the correct chain of
    // epochs. The reason is that (1) epochs may, in very rare cases, have forks,
    // so we cannot just take all the epochs and assume their heights do not collide;
    // and (2) it is not easy to walk backwards from the last epoch; there's no
    // "give me the previous epoch" query. So instead, we use block header's
    // `next_epoch_id` to establish an epoch chain.
    let all_epoch_infos = epoch_store.iter_epoch_info().collect::<HashMap<_, _>>();

    // Collect the previous-epoch relationship based on block headers.
    // To get block headers for past epochs, we use the fact that the EpochId is the
    // same as the block hash of the last block two epochs ago. That works except for
    // the current epoch, whose last block doesn't exist yet, which is why we need
    // any arbitrary block header in the current epoch as a special case.
    let mut epoch_to_prev_epoch = HashMap::new();
    epoch_to_prev_epoch.insert(
        *current_epoch_last_final_block_header.next_epoch_id(),
        *current_epoch_last_final_block_header.epoch_id(),
    );
    for (epoch_id, _) in &all_epoch_infos {
        if let Ok(block) = chain_store.get_block_header(&epoch_id.0) {
            epoch_to_prev_epoch.insert(*block.next_epoch_id(), *block.epoch_id());
        }
    }

    // Now that we have the chain of previous epochs, walk from the last epoch backwards
    // towards the first epoch.
    let mut epoch_ids = vec![];
    let mut current_epoch = next_epoch;
    while current_epoch != after_epoch {
        let prev_epoch = epoch_to_prev_epoch.get(&current_epoch).ok_or_else(|| {
            Error::Other(format!("Could not find prev epoch for {:?}", current_epoch))
        })?;
        epoch_ids.push(current_epoch);
        current_epoch = *prev_epoch;
    }
    epoch_ids.reverse();

    // Now that we have all epochs, we can fetch the data needed for each epoch.
    let epochs = (0..epoch_ids.len() - 1)
        .into_par_iter()
        .map(|index| -> Result<EpochSyncProofEpochData, Error> {
            let next_epoch_id = epoch_ids[index + 1];
            let epoch_id = epoch_ids[index];
            let prev_epoch_id = if index == 0 { after_epoch } else { epoch_ids[index - 1] };

            let (last_final_block_header, approvals_for_last_final_block) =
                if index + 2 < epoch_ids.len() {
                    let next_next_epoch_id = epoch_ids[index + 2];
                    let last_block_header = chain_store.get_block_header(&next_next_epoch_id.0)?;
                    let second_last_block_header =
                        chain_store.get_block_header(last_block_header.prev_hash())?;
                    let third_last_block_header =
                        chain_store.get_block_header(second_last_block_header.prev_hash())?;
                    (
                        Arc::clone(&third_last_block_header),
                        second_last_block_header.approvals().to_vec(),
                    )
                } else {
                    (
                        current_epoch_last_final_block_header.clone().into(),
                        current_epoch_second_last_block_approvals.clone(),
                    )
                };
            let prev_epoch_info = all_epoch_infos.get(&prev_epoch_id).ok_or_else(|| {
                Error::Other(format!("Could not find epoch info for epoch {:?}", prev_epoch_id))
            })?;
            let epoch_info = all_epoch_infos.get(&epoch_id).ok_or_else(|| {
                Error::Other(format!("Could not find epoch info for epoch {:?}", epoch_id))
            })?;
            let next_epoch_info = all_epoch_infos.get(&next_epoch_id).ok_or_else(|| {
                Error::Other(format!("Could not find epoch info for epoch {:?}", next_epoch_id))
            })?;

            let this_epoch_block_producers = get_epoch_info_block_producers(epoch_info);
            let next_epoch_block_producers = get_epoch_info_block_producers(next_epoch_info);
            let approvals_for_this_epoch_block_producers =
                get_approvals_for_this_epoch_block_producers(
                    &approvals_for_last_final_block,
                    &this_epoch_block_producers,
                    &next_epoch_block_producers,
                );
            let use_versioned_bp_hash_format =
                should_use_versioned_bp_hash_format(prev_epoch_info.protocol_version());

            Ok(EpochSyncProofEpochData {
                block_producers: get_epoch_info_block_producers(epoch_info),
                use_versioned_bp_hash_format,
                last_final_block_header,
                this_epoch_endorsements_for_last_final_block:
                    approvals_for_this_epoch_block_producers,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(epochs)
}

/// Gets the ordered list of block producers and their stake from the EpochInfo.
pub fn get_epoch_info_block_producers(epoch_info: &EpochInfo) -> Vec<ValidatorStake> {
    // The block producers settlement can actually have duplicates.
    // The list of block producers used to compute bp_hash is the deduplicated version
    // of this list, keeping the order of first appearance.
    let mut block_producers = Vec::new();
    let mut seen_validators = HashSet::new();
    for bp_index in epoch_info.block_producers_settlement() {
        if seen_validators.insert(*bp_index) {
            block_producers.push(epoch_info.get_validator(*bp_index));
        }
    }
    block_producers
}

/// Gets the ordered list of signatures within the approvals list in a block that correspond
/// to this epoch's block producers. The given block is expected to require both the current
/// and the next epoch's signatures. The returned list has the exact same length as this
/// epoch's block producers (unlike the block's approvals list which may be shorter).
fn get_approvals_for_this_epoch_block_producers(
    approvals: &[Option<Box<Signature>>],
    this_epoch_block_producers: &[ValidatorStake],
    next_epoch_block_producers: &[ValidatorStake],
) -> Vec<Option<Box<Signature>>> {
    let approvers = get_dual_epoch_block_approvers_ordered(
        this_epoch_block_producers,
        next_epoch_block_producers,
    );
    let mut approver_to_signature = HashMap::new();
    for (index, approver) in approvers.into_iter().enumerate() {
        if let Some(Some(approval)) = approvals.get(index) {
            approver_to_signature.insert(approver.account_id, approval.clone());
        }
    }
    this_epoch_block_producers
        .iter()
        .map(|validator| approver_to_signature.get(validator.account_id()).cloned())
        .collect()
}

/// Computes the ordered list of block approvers for a block that requires signatures from both
/// the current epoch's block producers and the next epoch's block producers.
fn get_dual_epoch_block_approvers_ordered(
    current_block_producers: &[ValidatorStake],
    next_block_producers: &[ValidatorStake],
) -> Vec<ApprovalStake> {
    let mut settlement = current_block_producers.to_vec();
    let settlement_epoch_boundary = settlement.len();

    settlement.extend_from_slice(next_block_producers);

    let mut result = vec![];
    let mut validators: HashMap<AccountId, usize> = HashMap::default();
    for (ord, validator_stake) in settlement.into_iter().enumerate() {
        let account_id = validator_stake.account_id();
        match validators.get(account_id) {
            None => {
                validators.insert(account_id.clone(), result.len());
                result.push(validator_stake.get_approval_stake(ord >= settlement_epoch_boundary));
            }
            Some(old_ord) => {
                if ord >= settlement_epoch_boundary {
                    result[*old_ord].stake_next_epoch = validator_stake.stake();
                };
            }
        };
    }
    result
}
