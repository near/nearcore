use crate::error::{Error, ErrorKind};
use crate::{ChainStoreAccess, ChainStoreUpdate};
use near_primitives::block::{Approval, BlockHeader, BlockHeaderInner, Weight};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockIndex, EpochId};
use std::collections::{HashMap, HashSet};

// How many blocks back to search for a new reference hash when the chain switches and the block
//     producer cannot use the same reference hash as the last approval on chain
const REFERENCE_HASH_LOOKUP_DEPTH: usize = 10;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FinalityGadgetQuorums {
    pub last_quorum_pre_vote: CryptoHash,
    pub last_quorum_pre_commit: CryptoHash,
}

#[derive(Debug)]
pub struct ApprovalVerificationError {}

pub struct FinalityGadget {}

impl FinalityGadget {
    pub fn process_approval(
        me: &Option<AccountId>,
        approval: &Approval,
        chain_store_update: &mut ChainStoreUpdate,
    ) -> Result<(), Error> {
        if me.as_ref().map(|me| me == &approval.account_id).unwrap_or(false) {
            // First update the statistics for the current block producer if the approval is created by us
            let header = chain_store_update.get_block_header(&approval.parent_hash)?;
            let BlockHeader { inner: BlockHeaderInner { total_weight, score, .. }, .. } = header;
            let total_weight = total_weight.clone();
            let score = score.clone();

            let update_weight =
                match chain_store_update.largest_approved_weight().map(|x| x.clone()) {
                    Ok(prev_weight) => total_weight > prev_weight,
                    Err(e) => match e.kind() {
                        ErrorKind::DBNotFoundErr(_) => true,
                        _ => return Err(e),
                    },
                };

            let update_score = match chain_store_update.largest_approved_score().map(|x| x.clone())
            {
                Ok(prev_score) => score > prev_score,
                Err(e) => match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => true,
                    _ => return Err(e),
                },
            };

            if update_weight {
                chain_store_update.save_largest_approved_weight(&total_weight);
            }
            if update_score {
                chain_store_update.save_largest_approved_score(&score);
            }

            chain_store_update.save_my_last_approval(&approval.parent_hash, approval.clone());
        }

        Ok(())
    }

    pub fn verify_approval_conditions(
        _approval: &Approval,
        _chain_store: &mut dyn ChainStoreAccess,
    ) -> Result<(), ApprovalVerificationError> {
        // TODO (#1630): fill in
        Ok(())
    }

    pub fn get_my_approval_reference_hash(
        prev_hash: CryptoHash,
        chain_store: &mut dyn ChainStoreAccess,
    ) -> Option<CryptoHash> {
        let prev_prev_hash = match chain_store.get_block_header(&prev_hash) {
            Ok(header) => header.inner.prev_hash,
            Err(_) => {
                return None;
            }
        };

        let largest_weight_approved = match chain_store.largest_approved_weight() {
            Ok(largest_weight) => largest_weight.clone(),
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => return Some(prev_hash),
                _ => return None,
            },
        };

        let largest_score_approved = match chain_store.largest_approved_score() {
            Ok(largest_score) => largest_score.clone(),
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => return Some(prev_hash),
                _ => return None,
            },
        };

        let last_approval_on_chain =
            chain_store.get_my_last_approval(&prev_prev_hash).ok().cloned();

        FinalityGadget::get_my_approval_reference_hash_inner(
            prev_hash,
            last_approval_on_chain,
            largest_weight_approved,
            largest_score_approved,
            chain_store,
        )
    }

    pub fn get_my_approval_reference_hash_inner(
        prev_hash: CryptoHash,
        last_approval_on_chain: Option<Approval>,
        largest_weight_approved: Weight,
        largest_score_approved: Weight,
        chain_store: &mut dyn ChainStoreAccess,
    ) -> Option<CryptoHash> {
        let default_f = |chain_store: &mut dyn ChainStoreAccess| match chain_store
            .get_block_header(&prev_hash)
        {
            Ok(mut header) => {
                let mut candidate = None;
                // Get the reference_hash up to `REFERENCE_HASH_LOOKUP_DEPTH` blocks into the past
                for _ in 0..REFERENCE_HASH_LOOKUP_DEPTH {
                    if header.inner.total_weight > largest_weight_approved
                        && header.inner.score >= largest_score_approved
                    {
                        candidate = Some(header.hash());
                        let prev_hash = header.inner.prev_hash;
                        match chain_store.get_block_header(&prev_hash) {
                            Ok(new_header) => header = new_header,
                            Err(_) => break,
                        }
                    } else {
                        break;
                    }
                }
                return candidate;
            }
            Err(_) => None,
        };

        let last_approval_on_chain = match last_approval_on_chain {
            Some(approval) => approval,
            None => return default_f(chain_store),
        };

        let (last_weight_approved_on_chain, last_score_approved_on_chain) =
            match chain_store.get_block_header(&last_approval_on_chain.parent_hash) {
                Ok(last_header_approved_on_chain) => (
                    last_header_approved_on_chain.inner.total_weight.clone(),
                    last_header_approved_on_chain.inner.score.clone(),
                ),
                Err(_) => {
                    return default_f(chain_store);
                }
            };

        // It is impossible for an honest actor to have two approvals with the same weight for
        //    their parent hashes on two different chains, so this check is sufficient
        if last_weight_approved_on_chain == largest_weight_approved
            && last_score_approved_on_chain == largest_score_approved
        {
            Some(last_approval_on_chain.reference_hash)
        } else {
            default_f(chain_store)
        }
    }

    pub fn compute_quorums(
        mut prev_hash: CryptoHash,
        epoch_id: EpochId,
        mut height: BlockIndex,
        mut approvals: Vec<Approval>,
        chain_store: &mut dyn ChainStoreAccess,
        total_block_producers: usize,
    ) -> Result<FinalityGadgetQuorums, Error> {
        let mut quorum_pre_vote = None;
        let mut quorum_pre_commit = None;

        let mut surrounding_approvals = HashSet::new();
        let mut height_to_accounts_to_remove: HashMap<u64, HashSet<_>> = HashMap::new();
        let mut accounts_to_height_to_remove = HashMap::new();

        let mut highest_height_no_quorum = height as i64;
        let mut accounts_surrounding_no_quroum = 0;

        while quorum_pre_commit.is_none() {
            // If this is genesis, set quorum pre-vote and quorum pre-commit to the default hash
            if prev_hash == CryptoHash::default() {
                if quorum_pre_vote.is_none() {
                    quorum_pre_vote = Some(CryptoHash::default());
                }
                if quorum_pre_commit.is_none() {
                    quorum_pre_commit = Some(CryptoHash::default());
                }
                break;
            }

            // Update surrounding approvals
            for approval in approvals {
                let account_id = approval.account_id.clone();

                let was_surrounding_no_quroum =
                    if let Some(current_height) = accounts_to_height_to_remove.get(&account_id) {
                        height_to_accounts_to_remove
                            .get_mut(current_height)
                            .unwrap()
                            .remove(&account_id);

                        *current_height as i64 <= highest_height_no_quorum
                    } else {
                        false
                    };

                let reference_height =
                    chain_store.get_block_header(&approval.reference_hash)?.inner.height;

                if let Some(old_height) = accounts_to_height_to_remove.get(&account_id) {
                    if *old_height < reference_height {
                        // If the approval is fully surrounded by the previous known approval from
                        //    the same block producer, disregard it (the existence of two such approvals
                        //    is a slashable behavior, and we can safely ignore either or both here)
                        continue;
                    }
                }

                if reference_height as i64 <= highest_height_no_quorum && !was_surrounding_no_quroum
                {
                    accounts_surrounding_no_quroum += 1;
                }

                height_to_accounts_to_remove
                    .entry(reference_height)
                    .or_insert_with(|| HashSet::new())
                    .insert(account_id.clone());

                accounts_to_height_to_remove.insert(account_id.clone(), reference_height);

                surrounding_approvals.insert(account_id);
            }

            if let Some(remove_accounts) = height_to_accounts_to_remove.get(&height) {
                for account_id in remove_accounts {
                    surrounding_approvals.remove(account_id);
                    accounts_to_height_to_remove.remove(account_id);
                }
            }

            // Move the current block to the left
            let last_block_header = chain_store.get_block_header(&prev_hash)?;

            prev_hash = last_block_header.inner.prev_hash;
            approvals = last_block_header.inner.approvals.clone();
            height = last_block_header.inner.height;

            if last_block_header.inner.epoch_id != epoch_id {
                // Do not cross the epoch boundary. It is safe to get the last quorums from the last
                //     block of the previous epoch, since no approval in the current epoch could
                //     have finalized anything else in the previous epoch (they would exit here),
                //     and if anything was finalized / had a prevote in this epoch, it would have
                //     been found in previous iterations of the surrounding loop
                if quorum_pre_vote.is_none() {
                    quorum_pre_vote = Some(last_block_header.inner.last_quorum_pre_vote);
                }
                if quorum_pre_commit.is_none() {
                    quorum_pre_commit = Some(last_block_header.inner.last_quorum_pre_commit);
                }
                break;
            }

            // Move `highest_height_no_quorum` if needed
            while accounts_surrounding_no_quroum > total_block_producers * 2 / 3 {
                accounts_surrounding_no_quroum -= height_to_accounts_to_remove
                    .get(&(highest_height_no_quorum as u64))
                    .map(|v| v.len())
                    .unwrap_or(0);

                highest_height_no_quorum -= 1;
            }

            if surrounding_approvals.len() > total_block_producers * 2 / 3 {
                if quorum_pre_vote.is_none() {
                    quorum_pre_vote = Some(last_block_header.hash())
                }

                let prev_qv_hash = last_block_header.inner.last_quorum_pre_vote;
                if prev_qv_hash != CryptoHash::default() {
                    let prev_qv = chain_store.get_block_header(&prev_qv_hash)?;
                    if prev_qv.inner.height as i64 > highest_height_no_quorum {
                        quorum_pre_commit = Some(prev_qv_hash);
                    }
                }
            }
        }

        Ok(FinalityGadgetQuorums {
            last_quorum_pre_vote: quorum_pre_vote.unwrap(),
            last_quorum_pre_commit: quorum_pre_commit.unwrap(),
        })
    }
}
