use near_chain_primitives::error::Error;
use near_crypto::Signature;
use near_primitives::{
    block_header::{Approval, ApprovalInner},
    hash::CryptoHash,
    types::{ApprovalStake, Balance, BlockHeight},
};

pub fn verify_approval_with_approvers_info(
    prev_block_hash: &CryptoHash,
    prev_block_height: BlockHeight,
    block_height: BlockHeight,
    approvals: &[Option<Box<near_crypto::Signature>>],
    info: Vec<(ApprovalStake, bool)>,
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

    for ((validator, is_slashed), may_be_signature) in info.into_iter().zip(approvals.iter()) {
        if let Some(signature) = may_be_signature {
            if is_slashed || !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
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
        &[(Balance, Balance, bool)],
    ) -> bool,
    prev_block_hash: &CryptoHash,
    prev_block_height: BlockHeight,
    block_height: BlockHeight,
    approvals: &[Option<Box<Signature>>],
    info: Vec<ApprovalStake>,
) -> Result<(), Error> {
    let message_to_sign = Approval::get_data_for_sig(
        &if prev_block_height + 1 == block_height {
            ApprovalInner::Endorsement(*prev_block_hash)
        } else {
            ApprovalInner::Skip(prev_block_height)
        },
        block_height,
    );

    for (validator, may_be_signature) in info.iter().zip(approvals.iter()) {
        if let Some(signature) = may_be_signature {
            if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                return Err(Error::InvalidApprovals);
            }
        }
    }
    let stakes = info
        .iter()
        .map(|stake| (stake.stake_this_epoch, stake.stake_next_epoch, false))
        .collect::<Vec<_>>();
    if !can_approved_block_be_produced(approvals, &stakes) {
        Err(Error::NotEnoughApprovals)
    } else {
        Ok(())
    }
}
