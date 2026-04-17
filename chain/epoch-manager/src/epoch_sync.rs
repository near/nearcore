use near_chain_primitives::Error;
use near_crypto::Signature;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_sync::{
    EpochSyncProof, EpochSyncProofCurrentEpochData, EpochSyncProofEpochData,
    EpochSyncProofLastEpochData, EpochSyncProofV1, should_use_versioned_bp_hash_format,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeightDelta, EpochId};
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::epoch_store::{EpochStoreAdapter, EpochStoreUpdateAdapter};
use near_store::merkle_proof::MerkleProofAccess;
use std::collections::{HashMap, HashSet};

/// Called on the first block of epoch T with T's epoch_id.
/// Derives the epoch sync proof up to epoch T-2, because a syncing node needs at least
/// `transaction_validity_period` worth of block headers (currently ~2 epochs).
pub fn update_epoch_sync_proof(
    store: &EpochStoreAdapter,
    epoch_id: &EpochId,
) -> Result<EpochStoreUpdateAdapter<'static>, Error> {
    // epoch_id(T) is the hash of the last block of epoch T-2 (set in finalize_epoch),
    // so we can obtain last_block_hash_in_ppe without navigating through T-1.
    let last_block_hash_in_ppe = &epoch_id.0;
    let last_block_info_in_ppe = store.get_block_info(last_block_hash_in_ppe)?;

    if last_block_info_in_ppe.epoch_id() == &EpochId::default() {
        return Ok(store.store_update());
    }

    tracing::debug!(?last_block_hash_in_ppe, "updating epoch sync proof");
    let new_proof = derive_epoch_sync_proof_from_last_block(store, last_block_hash_in_ppe, true)?;
    let mut store_update = store.store_update();
    store_update.set_epoch_sync_proof(&new_proof);
    Ok(store_update)
}

/// Computes the EpochSyncProofEpochData for the epoch whose last block hash is given.
/// Derives `use_versioned_bp_hash_format` from the previous epoch's protocol version
/// via `should_use_versioned_bp_hash_format`, correctly handling both pre-V3 and post-V3 epochs.
fn get_epoch_sync_proof_epoch_data(
    store: &EpochStoreAdapter,
    last_block_hash: &CryptoHash,
) -> Result<EpochSyncProofEpochData, Error> {
    let chain_store = store.chain_store();
    let last_block_header = chain_store.get_block_header(last_block_hash)?;
    let epoch_info = store.get_epoch_info(last_block_header.epoch_id())?;
    let block_producers = get_epoch_info_block_producers(&epoch_info);

    let prev_epoch_last_block_header =
        chain_store.get_block_header(&prev_epoch_last_block_hash(&last_block_header))?;
    let prev_epoch_info = store.get_epoch_info(prev_epoch_last_block_header.epoch_id())?;
    let use_versioned_bp_hash_format =
        should_use_versioned_bp_hash_format(prev_epoch_info.protocol_version());

    let second_last_block_header = chain_store.get_block_header(last_block_header.prev_hash())?;
    let third_last_block_header =
        chain_store.get_block_header(second_last_block_header.prev_hash())?;

    let next_epoch_info = store.get_epoch_info(second_last_block_header.next_epoch_id())?;
    let next_epoch_block_producers = get_epoch_info_block_producers(&next_epoch_info);
    let this_epoch_endorsements_for_last_final_block = get_approvals_for_this_epoch_block_producers(
        second_last_block_header.approvals(),
        &block_producers,
        &next_epoch_block_producers,
    );

    Ok(EpochSyncProofEpochData {
        block_producers,
        use_versioned_bp_hash_format,
        last_final_block_header: third_last_block_header,
        this_epoch_endorsements_for_last_final_block,
    })
}

/// Figures out which target epoch we should produce a proof for, based on the current
/// state of the blockchain.
///
/// The basic requirement for picking the target epoch is that its first block must be
/// final. That's just so that we don't have to deal with any forks. Therefore, it is
/// sufficient to pick whatever epoch the current final block is in. However, there are
/// additional considerations:
///  - Because state sync also requires some previous headers to be available (depending
///    on how many chunks were missing), if we pick the most recent epoch, after the node
///    finishes epoch sync and transitions to state sync, it would not have these headers
///    before the epoch. Therefore, for this purpose, it's convenient for epoch sync to not
///    pick the most recent epoch. This would ensure that state sync has a whole epoch of
///    headers before the epoch it's syncing to.
///  - We also need to have enough block headers to check for transaction_validity_period.
///    Therefore, we need find the latest epoch for which we would have at least that many
///    headers.
///
/// This function returns the hash of the last block of the target epoch.
pub fn find_target_epoch_to_produce_proof_for(
    store: &Store,
    transaction_validity_period: BlockHeightDelta,
) -> Result<CryptoHash, Error> {
    let chain_store = store.chain_store();
    let epoch_store = store.epoch_store();

    let tip = chain_store.final_head()?;
    let current_epoch_start_height = epoch_store.get_epoch_start(&tip.epoch_id)?;
    let next_next_epoch_id = tip.next_epoch_id;
    // Last block hash of the target epoch is the same as the next next EpochId.
    // That's a general property for Near's epochs.
    let mut target_epoch_last_block_hash = next_next_epoch_id.0;
    Ok(loop {
        let block_info = epoch_store.get_block_info(&target_epoch_last_block_hash)?;
        let target_epoch_first_block_header =
            chain_store.get_block_header(block_info.epoch_first_block())?;
        // Check that we have enough headers to check for transaction_validity_period.
        // We check this against the current epoch's start height, because when we state
        // sync, we will sync against the current epoch, and starting from the point we
        // state sync is when we'll need to be able to check for transaction validity.
        if target_epoch_first_block_header.height() + transaction_validity_period
            > current_epoch_start_height
        {
            target_epoch_last_block_hash = *target_epoch_first_block_header.prev_hash();
        } else {
            break target_epoch_last_block_hash;
        }
    })
}

/// Derives an epoch sync proof using a target epoch whose last final block is given
/// (actually it's the block after that, so that we can find the approvals).
pub fn derive_epoch_sync_proof_from_last_block(
    store: &EpochStoreAdapter,
    last_block_hash: &CryptoHash,
    use_existing_proof: bool,
) -> Result<EpochSyncProof, Error> {
    let chain_store = store.chain_store();
    let last_block_header = chain_store.get_block_header(&last_block_hash)?;

    let existing_epoch_sync_proof =
        if use_existing_proof { store.get_epoch_sync_proof()? } else { None };

    // If the existing proof already covers this epoch or beyond, return it as-is.
    if let Some(existing_proof) = &existing_epoch_sync_proof {
        let epoch_info = store.get_epoch_info(last_block_header.epoch_id())?;
        if epoch_info.epoch_height() <= existing_proof.last_epoch.next_epoch_info.epoch_height() {
            return Ok(EpochSyncProof::V1(existing_epoch_sync_proof.unwrap()));
        }
    }

    if last_block_header.epoch_id() == &EpochId::default() {
        return Err(Error::Other("not enough epochs after genesis to epoch sync".to_string()));
    }

    let last_block_info = store.get_block_info(&last_block_hash)?;
    let last_block_hash_in_prev_epoch = prev_epoch_last_block_hash(&last_block_header);

    let all_epochs = derive_all_epochs_data(store, last_block_hash, existing_epoch_sync_proof)?;
    let last_epoch = get_epoch_sync_proof_last_epoch_data(store, &last_block_hash_in_prev_epoch)?;
    let current_epoch = get_epoch_sync_proof_current_epoch_data(&chain_store, &last_block_info)?;
    let proof = EpochSyncProofV1 { all_epochs, last_epoch, current_epoch };

    Ok(EpochSyncProof::V1(proof))
}

/// Derives the all_epochs data by walking backward from the target epoch to the last
/// epoch covered by the existing proof (or genesis). For each missing epoch, computes
/// the epoch data using `get_epoch_sync_proof_epoch_data`.
fn derive_all_epochs_data(
    store: &EpochStoreAdapter,
    last_block_hash: &CryptoHash,
    existing_epoch_sync_proof: Option<EpochSyncProofV1>,
) -> Result<Vec<EpochSyncProofEpochData>, Error> {
    let chain_store = store.chain_store();
    let existing_all_epochs =
        existing_epoch_sync_proof.map(|proof| proof.all_epochs).unwrap_or_default();

    // The EpochId of the last epoch already covered in existing_all_epochs.
    let stop_epoch_id = existing_all_epochs
        .last()
        .map(|e| *e.last_final_block_header.epoch_id())
        .unwrap_or_else(EpochId::default);

    // Walk backward from the target epoch, collecting the last block hash of each
    // epoch that needs to be added to the proof.
    let mut epochs_to_add = vec![];
    let mut epoch_last_block_hash = *last_block_hash;
    loop {
        let header = chain_store.get_block_header(&epoch_last_block_hash)?;
        if *header.epoch_id() == stop_epoch_id || *header.epoch_id() == EpochId::default() {
            break;
        }
        epochs_to_add.push(epoch_last_block_hash);
        epoch_last_block_hash = prev_epoch_last_block_hash(&header);
    }
    epochs_to_add.reverse();
    tracing::debug!(num_epochs = epochs_to_add.len(), "deriving epoch sync proof data");

    let new_epochs = epochs_to_add
        .into_iter()
        .map(|hash| get_epoch_sync_proof_epoch_data(store, &hash))
        .collect::<Result<Vec<_>, _>>()?;

    let all_epochs: Vec<_> = existing_all_epochs.into_iter().chain(new_epochs).collect();
    assert!(!all_epochs.is_empty(), "not enough epochs to epoch sync");
    Ok(all_epochs)
}

/// Retrieves the EpochSyncProofLastEpochData from the store given the last block hash of the epoch.
/// Note that if we are calculating the EpochSyncProof for epoch T, the last_block_hash passed to
/// this function is the last block of epoch T-1.
fn get_epoch_sync_proof_last_epoch_data(
    store: &EpochStoreAdapter,
    last_block_hash: &CryptoHash,
) -> Result<EpochSyncProofLastEpochData, Error> {
    tracing::debug!(?last_block_hash, "get_epoch_sync_proof_last_epoch_data");

    let last_block_in_epoch = store.get_block_info(last_block_hash)?;
    let second_last_block_hash = last_block_in_epoch.prev_hash();
    let second_last_block_in_epoch = store.get_block_info(second_last_block_hash)?;
    let first_block_in_epoch_hash = last_block_in_epoch.epoch_first_block();
    let first_block_in_epoch = store.get_block_info(first_block_in_epoch_hash)?;

    // The EpochId of epoch T+1 is the hash of the last block in epoch T-1
    let next_epoch_id = EpochId(*first_block_in_epoch.prev_hash());
    let next_next_epoch_id = EpochId(*last_block_hash);

    let epoch_info = store.get_epoch_info(last_block_in_epoch.epoch_id())?;
    let next_epoch_info = store.get_epoch_info(&next_epoch_id)?;
    let next_next_epoch_info = store.get_epoch_info(&next_next_epoch_id)?;

    Ok(EpochSyncProofLastEpochData {
        epoch_info,
        next_epoch_info,
        next_next_epoch_info,
        first_block_in_epoch,
        last_block_in_epoch,
        second_last_block_in_epoch,
    })
}

fn get_epoch_sync_proof_current_epoch_data(
    store: &ChainStoreAdapter,
    last_block_info: &BlockInfo,
) -> Result<EpochSyncProofCurrentEpochData, Error> {
    tracing::debug!(?last_block_info, "get_epoch_sync_proof_current_epoch_data");

    // Get necessary block headers
    let first_block_hash_in_epoch = last_block_info.epoch_first_block();
    let first_block_header_in_epoch = store.get_block_header(&first_block_hash_in_epoch)?;
    let last_block_hash_in_prev_epoch = first_block_header_in_epoch.prev_hash();
    let last_block_header_in_prev_epoch = store.get_block_header(&last_block_hash_in_prev_epoch)?;
    let second_last_block_hash_in_prev_epoch = last_block_header_in_prev_epoch.prev_hash();
    let second_last_block_header_in_prev_epoch =
        store.get_block_header(&second_last_block_hash_in_prev_epoch)?;

    // Note that for compute_past_block_proof_in_merkle_tree_of_later_block we take the head_block_hash
    // as the last final block of the epoch.
    // This is defined as the third last block in the current epoch.
    let second_last_block_hash = last_block_info.prev_hash();
    let second_last_block_header = store.get_block_header(&second_last_block_hash)?;
    let third_last_block_hash = second_last_block_header.prev_hash();
    let merkle_proof_for_first_block =
        store.store().compute_past_block_proof_in_merkle_tree_of_later_block(
            first_block_hash_in_epoch,
            third_last_block_hash,
        )?;
    let partial_merkle_tree_for_first_block =
        store.get_block_merkle_tree(first_block_hash_in_epoch)?;

    Ok(EpochSyncProofCurrentEpochData {
        first_block_header_in_epoch,
        last_block_header_in_prev_epoch,
        second_last_block_header_in_prev_epoch,
        merkle_proof_for_first_block,
        partial_merkle_tree_for_first_block,
    })
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

/// Returns the hash of the last block in the previous epoch, given a block header.
///
/// Near's epoch_id convention: epoch_id(T) = EpochId(last_block_hash(T-2)), set in
/// `finalize_epoch`. For a block in epoch T, its `next_epoch_id` field is epoch_id(T+1) =
/// EpochId(last_block_hash(T-1)). Extracting the inner hash gives us the last block hash
/// of the previous epoch without needing block_info (which may have been GC'd).
#[inline]
fn prev_epoch_last_block_hash(header: &BlockHeader) -> CryptoHash {
    header.next_epoch_id().0
}
