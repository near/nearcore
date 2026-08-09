use crate::{Doomslug, DoomslugThresholdMode};
use near_chain_primitives::error::Error;
use near_crypto::Signature;
use near_primitives::{
    block::BlockHeader,
    block_header::{Approval, ApprovalInner},
    epoch_info::EpochInfo,
    hash::CryptoHash,
    types::{AccountId, ApprovalStake, Balance, BlockHeight, validator_stake::ValidatorStake},
};
use std::{collections::HashSet, sync::Arc};

/// All immutable inputs needed for the CPU-intensive portion of block header approval
/// verification. Epoch-manager lookups happen while preparing this value, so `verify` can safely
/// run on a computation worker without accessing `Chain`.
pub struct BlockHeaderApprovalVerification {
    header: BlockHeader,
    block_producer: ValidatorStake,
    epoch_info: Arc<EpochInfo>,
    doomslug_threshold_mode: DoomslugThresholdMode,
}

impl BlockHeaderApprovalVerification {
    pub(crate) fn new(
        header: BlockHeader,
        block_producer: ValidatorStake,
        epoch_info: Arc<EpochInfo>,
        doomslug_threshold_mode: DoomslugThresholdMode,
    ) -> Self {
        Self { header, block_producer, epoch_info, doomslug_threshold_mode }
    }

    pub fn verify(self) -> Result<(), Error> {
        if !self
            .header
            .signature()
            .verify(self.header.hash().as_ref(), self.block_producer.public_key())
        {
            return Err(Error::InvalidSignature);
        }
        let Some(prev_height) = self.header.prev_height() else {
            return Err(Error::Other("header too old to verify approvals without ancestry".into()));
        };
        verify_approvals_and_threshold_orphan(
            &|approvals, stakes| {
                Doomslug::can_approved_block_be_produced(
                    self.doomslug_threshold_mode,
                    approvals,
                    stakes,
                )
            },
            self.header.prev_hash(),
            prev_height,
            self.header.height(),
            self.header.approvals(),
            self.epoch_info,
        )
    }
}

pub fn verify_approval_with_approvers_info(
    prev_block_hash: &CryptoHash,
    prev_block_height: BlockHeight,
    block_height: BlockHeight,
    approvals: &[Option<Box<near_crypto::Signature>>],
    info: Vec<ApprovalStake>,
) -> bool {
    if approvals.len() > info.len() {
        return false;
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
                return false;
            }
        }
    }
    true
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
    let block_approvers = get_heuristic_block_approvers_ordered(&epoch_info);
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

fn get_heuristic_block_approvers_ordered(epoch_info: &EpochInfo) -> Vec<ApprovalStake> {
    let mut result = vec![];
    let mut validators: HashSet<AccountId> = HashSet::new();
    for validator_id in epoch_info.block_producers_settlement() {
        let validator_stake = epoch_info.get_validator(*validator_id);
        let account_id = validator_stake.account_id();
        if validators.insert(account_id.clone()) {
            result.push(validator_stake.get_approval_stake(false));
        }
    }

    result
}
