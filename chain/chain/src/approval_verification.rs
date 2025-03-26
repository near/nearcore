use near_chain_primitives::error::Error;
use near_primitives::{
    block_header::{Approval, ApprovalInner},
    hash::CryptoHash,
    types::{ApprovalStake, BlockHeight},
};

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
