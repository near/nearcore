use near_chain_primitives::error::Error;
use near_crypto::Signature;
use near_primitives::{
    block_header::{Approval, ApprovalInner},
    epoch_info::EpochInfo,
    errors::EpochError,
    hash::CryptoHash,
    types::{AccountId, ApprovalStake, Balance, BlockHeight},
};
use std::{collections::HashSet, sync::Arc};

pub fn verify_approval_with_approvers_info(
    prev_block_hash: &CryptoHash,
    prev_block_height: BlockHeight,
    block_height: BlockHeight,
    approvals: &[Option<Box<near_crypto::Signature>>],
    info: Vec<ApprovalStake>,
) -> Result<bool, Error> {
    if approvals.len() > info.len() {
        return Ok(false);
    }

    let message_to_sign = Approval::get_data_for_sig(
        &if prev_block_height + 1 == block_height {
            ApprovalInner::Endorsement(*prev_block_hash)
        } else {
            ApprovalInner::Skip(prev_block_height)
        },
        block_height,
    );

    for (validator, may_be_signature) in info.into_iter().zip(approvals.iter()) {
        if let Some(signature) = may_be_signature {
            if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Verify approvals and check threshold, but ignore next epoch approvals and slashing
pub fn verify_approvals_and_threshold_orphan(
    can_approved_block_be_produced: &dyn Fn(
        &[Option<Box<Signature>>],
        &[(Balance, Balance)],
    ) -> bool,
    prev_block_hash: &CryptoHash,
    prev_block_height: BlockHeight,
    block_height: BlockHeight,
    approvals: &[Option<Box<Signature>>],
    epoch_info: Arc<EpochInfo>,
) -> Result<(), Error> {
    let block_approvers = get_heuristic_block_approvers_ordered(&epoch_info)?;
    let message_to_sign = Approval::get_data_for_sig(
        &if prev_block_height + 1 == block_height {
            ApprovalInner::Endorsement(*prev_block_hash)
        } else {
            ApprovalInner::Skip(prev_block_height)
        },
        block_height,
    );

    for (validator, may_be_signature) in block_approvers.iter().zip(approvals.iter()) {
        if let Some(signature) = may_be_signature {
            if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                return Err(Error::InvalidApprovals);
            }
        }
    }
    let stakes = block_approvers
        .iter()
        .map(|stake| (stake.stake_this_epoch, stake.stake_next_epoch))
        .collect::<Vec<_>>();
    if !can_approved_block_be_produced(approvals, &stakes) {
        Err(Error::NotEnoughApprovals)
    } else {
        Ok(())
    }
}

fn get_heuristic_block_approvers_ordered(
    epoch_info: &EpochInfo,
) -> Result<Vec<ApprovalStake>, EpochError> {
    let mut result = vec![];
    let mut validators: HashSet<AccountId> = HashSet::new();
    for validator_id in epoch_info.block_producers_settlement() {
        let validator_stake = epoch_info.get_validator(*validator_id);
        let account_id = validator_stake.account_id();
        if validators.insert(account_id.clone()) {
            result.push(validator_stake.get_approval_stake(false));
        }
    }

    Ok(result)
}
