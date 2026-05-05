use crate::{EpochInfo, EpochManagerAdapter, RngSeed};
use itertools::Itertools;
use near_primitives::errors::EpochError;
use near_primitives::shard_layout::{ShardInfo, ShardLayout, ShardLayoutError};
use near_primitives::types::{
    AccountId, Balance, EpochId, NumShards, ProtocolVersion, ShardId, ShardIndex,
    validator_stake::ValidatorStake,
};
use near_primitives::utils::min_heap::{MinHeap, PeekMut};
use near_primitives::version::ProtocolFeature;
use near_store::trie::ShardUId;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Local index of a chunk producer in the current epoch's chunk_producers list.
type ValidatorIdx = usize;

/// Marker struct to communicate the error where you try to assign validators to shards
/// and there are not enough to even meet the minimum per shard.
#[derive(Debug)]
pub struct NotEnoughValidators;

/// Abstraction to avoid using full validator info in tests.
pub trait HasStake {
    fn get_stake(&self) -> Balance;
}

impl HasStake for ValidatorStake {
    fn get_stake(&self) -> Balance {
        self.stake()
    }
}

/// A helper struct to maintain the shard assignment sorted by the number of
/// validators assigned to each shard.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ValidatorsFirstShardAssignmentItem {
    validators: usize,
    stake: Balance,
    shard_index: ShardIndex,
}

type ValidatorsFirstShardAssignment = MinHeap<ValidatorsFirstShardAssignmentItem>;

/// A helper struct to maintain the shard assignment sorted by the stake
/// assigned to each shard.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StakeFirstShardAssignmentItem {
    stake: Balance,
    validators: usize,
    shard_index: ShardIndex,
}

impl From<ValidatorsFirstShardAssignmentItem> for StakeFirstShardAssignmentItem {
    fn from(v: ValidatorsFirstShardAssignmentItem) -> Self {
        Self { validators: v.validators, stake: v.stake, shard_index: v.shard_index }
    }
}

fn assign_to_satisfy_shards_inner<T: HasStake + Eq, I: Iterator<Item = (usize, T)>>(
    shard_assignment: &mut ValidatorsFirstShardAssignment,
    result: &mut Vec<Vec<T>>,
    cp_iter: &mut I,
    min_validators_per_shard: usize,
) {
    let mut buffer = Vec::with_capacity(shard_assignment.len());
    // Stores (shard_index, cp_index) meaning that cp at cp_index has already been
    // added to shard shard_index.  Used to make sure we don’t add a cp to the same
    // shard multiple times.
    let seen_capacity = result.len() * min_validators_per_shard;
    let mut seen = HashSet::<(ShardIndex, usize)>::with_capacity(seen_capacity);

    while shard_assignment.peek().unwrap().validators < min_validators_per_shard {
        // cp_iter is an infinite cycle iterator so getting next value can never
        // fail.  cp_index is index of each element in the iterator but the
        // indexing is done before cycling thus the same cp always gets the same
        // cp_index.
        let (cp_index, cp) = cp_iter.next().unwrap();
        // Decide which shard to assign this chunk producer to.  We mustn’t
        // assign producers to a single shard multiple times.
        loop {
            match shard_assignment.peek_mut() {
                None => {
                    // No shards left which don’t already contain this chunk
                    // producer.  Skip it and move to another producer.
                    break;
                }
                Some(top) if top.validators >= min_validators_per_shard => {
                    // `shard_assignment` is sorted by number of chunk producers,
                    // thus all remaining shards have min_validators_per_shard
                    // producers already assigned to them.  Don’t assign current
                    // one to any shard and move to next cp.
                    break;
                }
                Some(mut top) if seen.insert((top.shard_index, cp_index)) => {
                    // Chunk producer is not yet assigned to the shard and the
                    // shard still needs more producers.  Assign `cp` to it and
                    // move to next one.
                    top.validators += 1;
                    top.stake = top.stake.checked_add(cp.get_stake()).unwrap();
                    result[top.shard_index].push(cp);
                    break;
                }
                Some(top) => {
                    // This chunk producer is already assigned to this shard.
                    // Pop the shard from the heap for now and try assigning the
                    // producer to the next shard.  (We’ll look back at the
                    // shard once we figure out what to do with current `cp`).
                    buffer.push(PeekMut::pop(top));
                }
            }
        }
        // Any shards we skipped over (because `cp` was already assigned to
        // them) need to be put back into the heap.
        shard_assignment.extend(buffer.drain(..));
    }
}

/// Assigns validators to shards to satisfy `min_validators_per_shard`
/// condition.
/// This means that validators can be repeated.
fn assign_to_satisfy_shards<T: HasStake + Eq + Clone>(
    chunk_producers: Vec<T>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
) -> Vec<Vec<T>> {
    let mut result: Vec<Vec<T>> = (0..num_shards).map(|_| Vec::new()).collect();

    // Initially, sort by number of validators first so we fill shards up.
    let mut shard_assignment: ValidatorsFirstShardAssignment = (0..num_shards)
        .map(|shard_index| shard_index as usize)
        .map(|shard_index| ValidatorsFirstShardAssignmentItem {
            validators: 0,
            stake: Balance::ZERO,
            shard_index,
        })
        .collect();

    // Distribute chunk producers until all shards have at least the
    // minimum requested number.  If there are not enough validators to satisfy
    // that requirement, assign some of the validators to multiple shards.
    let mut chunk_producers = chunk_producers.into_iter().enumerate().cycle();
    assign_to_satisfy_shards_inner(
        &mut shard_assignment,
        &mut result,
        &mut chunk_producers,
        min_validators_per_shard,
    );
    result
}

/// How to seed the chunk-producer-to-shard assignment from the previous epoch's state.
#[derive(Clone, Debug)]
pub enum AssignmentStrategy {
    /// Carry over the previous epoch's assignment by `ShardIndex`. Used when
    /// the shard layout is unchanged across the boundary.
    CarryOver,
    /// Start fresh with no prior assignment. Used pre-`StickyReshardingValidatorAssignment`
    /// on layout changes, where `ShardIndex`-based copying is meaningless.
    Fresh,
    /// Sticky-by-`ShardId` across a resharding. Unchanged shards keep their
    /// validators by `ShardId`; split children inherit a stake-balanced subset
    /// of their parent's validators via greedy bin-packing.
    ///
    /// `shard_idx_mapping[prev_idx]` is the list of new layout shard indices
    /// that descend from the prev layout shard at `prev_idx`. Single-item
    /// entry means the shard persists unchanged, multi-item entry means a
    /// split.
    StickyResharding { shard_idx_mapping: Vec<Vec<ShardIndex>> },
}

impl AssignmentStrategy {
    /// Pick the strategy for an epoch boundary. If the layouts are identical,
    /// use `CarryOver`. Otherwise, use `StickyResharding` (when the
    /// `StickyReshardingValidatorAssignment` feature is enabled and the new
    /// layout is derived from the prev layout) or `Fresh`.
    ///
    /// Falls back to `Fresh` when sticky construction fails: in production,
    /// `next_next_shard_layout()` always returns an inherited or derived
    /// layout that `sticky_resharding()` accepts, so the fallback should be
    /// unreachable there. Test setups that swap in synthetic layouts
    /// (e.g. `multi_shard`) hit this path and continue with a fresh
    /// assignment instead of halting.
    pub fn select(
        protocol_version: ProtocolVersion,
        prev_layout: &ShardLayout,
        new_layout: &ShardLayout,
    ) -> Self {
        if prev_layout == new_layout {
            return Self::CarryOver;
        }
        if !ProtocolFeature::StickyReshardingValidatorAssignment.enabled(protocol_version) {
            return Self::Fresh;
        }
        Self::sticky_resharding(new_layout, prev_layout).unwrap_or_else(|err| {
            tracing::warn!(
                target: "epoch_manager",
                ?err,
                "sticky-resharding strategy unavailable for non-derived layouts; falling back to fresh"
            );
            Self::Fresh
        })
    }

    /// Build a `StickyResharding` strategy directly. Validates that
    /// `new_layout` was derived from `prev_layout` by walking the prev
    /// layout's shards and resolving each one's children in the new layout,
    /// producing a prev->new child mapping. Errors if any prev shard has no
    /// descendants in the new layout (i.e., a merge or a dropped shard,
    /// neither yet supported).
    pub(crate) fn sticky_resharding(
        new_layout: &ShardLayout,
        prev_layout: &ShardLayout,
    ) -> Result<Self, ShardLayoutError> {
        let mut prev_to_new = vec![vec![]; prev_layout.num_shards() as usize];
        for shard_idx in prev_layout.shard_indexes() {
            let shard_id = prev_layout.get_shard_id(shard_idx)?;
            let children_ids = new_layout
                .get_children_shards_ids(shard_id)
                .ok_or(ShardLayoutError::InvalidShardId { shard_id })?;
            for child_id in children_ids {
                let child_idx = new_layout.get_shard_index(child_id)?;
                prev_to_new[shard_idx].push(child_idx);
            }
        }
        Ok(Self::StickyResharding { shard_idx_mapping: prev_to_new })
    }

    /// Whether this strategy should override the configured
    /// `shard_assignment_changes_limit` to permit more rebalancing in a single
    /// epoch. True only for `StickyResharding`, where freshly split children
    /// would otherwise be chronically under-staffed; bumping the limit lets
    /// them reach target population in one go.
    pub(crate) fn needs_changes_limit_override(&self) -> bool {
        match self {
            Self::CarryOver | Self::Fresh => false,
            Self::StickyResharding { .. } => true,
        }
    }
}

/// Copy the previous assignment over by `ShardIndex`, up to the minimum of
/// current and previous shard counts. Validators that are no longer chunk
/// producers in the new epoch are dropped. Correct only when the shard layout
/// is unchanged across the boundary (used by `AssignmentStrategy::CarryOver`).
fn carry_over_by_shard_index(
    chunk_producers: &[ValidatorStake],
    num_shards: usize,
    prev_assignment: &[Vec<ValidatorStake>],
) -> Vec<Vec<ValidatorIdx>> {
    let cp_indices = build_chunk_producer_indices(chunk_producers);
    let mut assignment = vec![vec![]; num_shards];
    let max_shards_to_copy = prev_assignment.len().min(num_shards);
    for (shard_index, validator_stakes) in
        prev_assignment.iter().take(max_shards_to_copy).enumerate()
    {
        let mut shard_validators = vec![];
        for validator_stake in validator_stakes {
            if let Some(&index) = cp_indices.get(validator_stake.account_id()) {
                shard_validators.push(index);
            }
        }
        assignment[shard_index] = shard_validators;
    }
    assignment
}

/// Per-child accumulator for `bin_pack_into_children`. The field order is
/// load-bearing: `min_by_key` over `&ChildLoad` uses the derived `Ord`, which
/// compares `stake`, then `count`, then `shard_index` — so on ties we prefer
/// the lighter shard, then the less-populated one, then the lower index.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct ChildLoad {
    stake: Balance,
    count: usize,
    shard_index: ShardIndex,
}

impl ChildLoad {
    fn new(shard_index: ShardIndex) -> Self {
        Self { stake: Balance::ZERO, count: 0, shard_index }
    }

    fn add_validator(&mut self, validator_stake: Balance) {
        self.stake = self.stake.checked_add(validator_stake).unwrap();
        self.count += 1;
    }
}

/// Greedy stake-balanced bin-pack of `validators` across the children of a
/// split shard. Walks validators desc by stake (tiebreak: lower account id
/// wins) and at each step places the next one on whichever child currently
/// has the lowest `(stake, count, shard_index)`.
///
/// Returns a `BTreeMap` with one entry per child (empty `Vec` if the child
/// got no validators). Within each child's `Vec`, validators appear in
/// placement order — highest-stake first.
///
/// `validators` are indices into `chunk_producers`; the returned `Vec`s
/// contain the same indices. `children` are `ShardIndex` values into the new
/// layout. With one child the function trivially places every survivor on
/// that child.
fn bin_pack_into_children(
    validators: Vec<ValidatorIdx>,
    children: &[ShardIndex],
    chunk_producers: &[ValidatorStake],
) -> BTreeMap<ShardIndex, Vec<ValidatorIdx>> {
    let children = children.iter().sorted().collect_vec();
    let mut child_loads = children.iter().map(|&&idx| ChildLoad::new(idx)).collect_vec();
    let mut result: BTreeMap<_, _> = children.iter().map(|&idx| (*idx, Vec::new())).collect();

    // Stakes desc, then account id asc — matches OrderedValidatorStake.
    let sorted_validators = validators.into_iter().sorted_by_key(|&i| {
        let cp = &chunk_producers[i];
        (Reverse(cp.stake()), cp.account_id().clone())
    });
    for validator_idx in sorted_validators {
        let (min_pos, _) = child_loads.iter().enumerate().min_by_key(|(_, load)| **load).unwrap();
        let load = &mut child_loads[min_pos];
        load.add_validator(chunk_producers[validator_idx].stake());
        result.get_mut(&load.shard_index).unwrap().push(validator_idx);
    }
    result
}

/// Get initial chunk producer assignment for the current epoch, given the
/// assignment for the previous epoch.
///
/// See `AssignmentStrategy` for the meaning of each variant.
///
/// PRECONDITION: Each validator appears on at most one prev shard's
/// assignment. Both `StickyResharding` and `CarryOver` propagate the prev
/// assignment per-prev-shard, so duplicates would carry into the new
/// assignment and can trip `assign_to_balance_shards`'s hard-limit assert.
fn get_initial_chunk_producer_assignment(
    chunk_producers: &[ValidatorStake],
    num_shards: usize,
    strategy: &AssignmentStrategy,
    prev_assignment: Vec<Vec<ValidatorStake>>,
) -> Vec<Vec<ValidatorIdx>> {
    match strategy {
        AssignmentStrategy::Fresh => vec![vec![]; num_shards],
        AssignmentStrategy::CarryOver => {
            carry_over_by_shard_index(chunk_producers, num_shards, &prev_assignment)
        }
        AssignmentStrategy::StickyResharding { shard_idx_mapping } => {
            sticky_by_shard_id(chunk_producers, num_shards, shard_idx_mapping, &prev_assignment)
        }
    }
}

fn build_chunk_producer_indices(
    chunk_producers: &[ValidatorStake],
) -> HashMap<AccountId, ValidatorIdx> {
    chunk_producers.iter().enumerate().map(|(i, vs)| (vs.account_id().clone(), i)).collect()
}

/// Map the prev assignment onto the new layout by `ShardId`. Used by
/// `AssignmentStrategy::StickyResharding`. Unchanged shards keep their
/// validators directly; split children get a stake-balanced subset of their
/// parent's validators via greedy bin-packing. Validators that are no longer
/// chunk producers in the new epoch are dropped.
///
/// Caller must ensure `prev_assignment` corresponds to the same number of
/// shards as `shard_idx_mapping` (i.e. one entry per prev shard).
fn sticky_by_shard_id(
    chunk_producers: &[ValidatorStake],
    num_shards: usize,
    shard_idx_mapping: &[Vec<ShardIndex>],
    prev_assignment: &[Vec<ValidatorStake>],
) -> Vec<Vec<ValidatorIdx>> {
    let cp_indices = build_chunk_producer_indices(chunk_producers);
    debug_assert_eq!(shard_idx_mapping.len(), prev_assignment.len());
    let mut assignment: Vec<Vec<ValidatorIdx>> = vec![vec![]; num_shards];
    for (prev_idx, new_indices) in shard_idx_mapping.iter().enumerate() {
        // Drop validators that are no longer chunk producers in the new epoch
        // (unstaked, kicked out, fell below threshold, role-changed).
        let surviving = prev_assignment[prev_idx]
            .iter()
            .filter_map(|v| cp_indices.get(v.account_id()).copied())
            .collect_vec();

        // Distribute survivors across the children with greedy stake-balanced
        // bin-packing. With one child this trivially places every survivor on
        // that child.
        for (child_idx, child_validators) in
            bin_pack_into_children(surviving, new_indices, chunk_producers)
        {
            assignment[child_idx] = child_validators;
        }
    }
    assignment
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone)]
/// Helper struct to maintain set of shards sorted by number of chunk producers.
struct ShardSetItem {
    shard_chunk_producer_num: usize,
    shard_index: ShardIndex,
}

/// Convert chunk producer assignment from the previous epoch to the assignment
/// for the current epoch, given the chunk producer list.
///
/// Caller must guarantee that `min_validators_per_shard` is achievable.
fn assign_to_balance_shards(
    chunk_producers: Vec<ValidatorStake>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
    shard_assignment_changes_limit: usize,
    rng_seed: RngSeed,
    strategy: &AssignmentStrategy,
    prev_chunk_producers_assignment: Vec<Vec<ValidatorStake>>,
) -> Vec<Vec<ValidatorStake>> {
    let num_chunk_producers = chunk_producers.len();
    let mut chunk_producer_assignment = get_initial_chunk_producer_assignment(
        &chunk_producers,
        num_shards as usize,
        strategy,
        prev_chunk_producers_assignment,
    );

    // Find and assign new validators first.
    let old_validators = chunk_producer_assignment.iter().flatten().collect::<HashSet<_>>();
    let new_validators =
        (0..num_chunk_producers).filter(|i| !old_validators.contains(i)).collect::<Vec<_>>();
    let mut shard_set: BTreeSet<ShardSetItem> = (0..num_shards)
        .map(|s| ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[s as usize].len(),
            shard_index: s as usize,
        })
        .collect();
    let mut new_assignments = new_validators.len();
    for validator_index in new_validators {
        let ShardSetItem { shard_index, .. } = shard_set.pop_first().unwrap();
        chunk_producer_assignment[shard_index].push(validator_index);
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[shard_index].len(),
            shard_index,
        });
    }

    // Reassign old validators to balance shards until the limit is reached.
    let rng = &mut EpochInfo::shard_assignment_rng(&rng_seed);
    let new_assignments_hard_limit = chunk_producers.len().max(shard_assignment_changes_limit);
    loop {
        let ShardSetItem {
            shard_chunk_producer_num: minimal_shard_validators_num,
            shard_index: minimal_shard,
        } = *shard_set.first().unwrap();
        let ShardSetItem {
            shard_chunk_producer_num: maximal_shard_validators_num,
            shard_index: maximal_shard,
        } = *shard_set.last().unwrap();
        let is_minimal_num_satisfied = minimal_shard_validators_num >= min_validators_per_shard;
        let is_balanced = maximal_shard_validators_num - minimal_shard_validators_num <= 1;

        if is_minimal_num_satisfied
            && (is_balanced || new_assignments >= shard_assignment_changes_limit)
        {
            break;
        }

        assert!(
            new_assignments <= new_assignments_hard_limit,
            "couldn't balance {num_shards} shards in {new_assignments_hard_limit} \
             iterations. it means that some chunk producer was selected for \
             new shard twice which shouldn't happen."
        );
        assert_ne!(
            minimal_shard,
            maximal_shard,
            "minimal shard and maximal shard are the same: {minimal_shard}. \
            either {} chunk producers are not enough to satisfy minimal number \
            {min_validators_per_shard} for {num_shards} shards, or we try to \
            balance the shard with itself.",
            chunk_producers.len(),
        );

        let minimal_shard = shard_set.pop_first().unwrap().shard_index;
        let maximal_shard = shard_set.pop_last().unwrap().shard_index;
        let validator_pos = rng.gen_range(0..chunk_producer_assignment[maximal_shard].len());
        let validator_index = chunk_producer_assignment[maximal_shard].swap_remove(validator_pos);
        chunk_producer_assignment[minimal_shard].push(validator_index);
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[minimal_shard].len(),
            shard_index: minimal_shard,
        });
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[maximal_shard].len(),
            shard_index: maximal_shard,
        });
        new_assignments += 1;
    }
    chunk_producer_assignment
        .into_iter()
        .map(|mut assignment| {
            assignment.sort();
            assignment.into_iter().map(|i| chunk_producers[i].clone()).collect()
        })
        .collect()
}

/// Assign chunk producers to shards. The i-th element of the output is the
/// list of chunk producers assigned to the i-th shard, sorted by stake.
///
/// This function guarantees that, in order of priority:
/// * every shard has at least `min_validators_per_shard` assigned to it;
/// * chunk producer repeats are completely avoided if possible;
/// * if `prev_chunk_producers_assignment` is provided, it minimizes the need
/// for chunk producers there to change shards;
/// * finally, attempts to balance number of chunk producers at shards, while
/// `shard_assignment_changes_limit` allows.
/// See discussion on #11213 for more details.
///
/// `strategy` selects the seeding behavior (see `AssignmentStrategy` for details).
///
/// NOTE: when `chunk_producers.len() < min_validators_per_shard * num_shards`,
/// `assign_to_satisfy_shards` is taken instead of `assign_to_balance_shards`,
/// and `strategy`/`prev_chunk_producers_assignment` are ignored —
/// `StickyResharding`'s stickiness-by-id property does not hold in that
/// regime.
///
/// Caller must guarantee that `chunk_producers` is sorted in non-increasing
/// order by stake.
///
/// Returns error if `chunk_producers.len() < min_validators_per_shard`.
pub(crate) fn assign_chunk_producers_to_shards(
    chunk_producers: Vec<ValidatorStake>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
    shard_assignment_changes_limit: usize,
    rng_seed: RngSeed,
    strategy: &AssignmentStrategy,
    prev_chunk_producers_assignment: Vec<Vec<ValidatorStake>>,
) -> Result<Vec<Vec<ValidatorStake>>, NotEnoughValidators> {
    debug_assert!(num_shards > 0, "expected at least one shard");

    // If there's not enough chunk producers to fill up a single shard there’s
    // nothing we can do. Return with an error.
    let num_chunk_producers = chunk_producers.len();
    if num_chunk_producers < min_validators_per_shard {
        return Err(NotEnoughValidators);
    }

    // On a resharding epoch the freshly split children come in at roughly half
    // their target population. Raise the change limit so the rebalancing loop
    // can top them up in one shot, rather than spreading the cost across many
    // epochs and leaving children chronically understaffed.
    let effective_changes_limit = if strategy.needs_changes_limit_override() {
        let target_per_shard = num_chunk_producers / (num_shards as usize);
        shard_assignment_changes_limit.max(target_per_shard)
    } else {
        shard_assignment_changes_limit
    };

    let result = if chunk_producers.len() < min_validators_per_shard * (num_shards as usize) {
        // We don't have enough chunk producers to allow assignment without
        // repeats.
        // Assign validators to satisfy only `min_validators_per_shard` condition.
        assign_to_satisfy_shards(chunk_producers, num_shards, min_validators_per_shard)
    } else {
        // We can avoid validator repeats, so we use other algorithm to balance
        // number of validators in shards.
        assign_to_balance_shards(
            chunk_producers,
            num_shards,
            min_validators_per_shard,
            effective_changes_limit,
            rng_seed,
            strategy,
            prev_chunk_producers_assignment,
        )
    };
    Ok(result)
}

/// Which shard the account belongs to in the given epoch.
pub fn account_id_to_shard_id(
    epoch_manager: &dyn EpochManagerAdapter,
    account_id: &AccountId,
    epoch_id: &EpochId,
) -> Result<ShardId, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(shard_layout.account_id_to_shard_id(account_id))
}

/// Which shard the account belongs to in the given epoch.
pub fn account_id_to_shard_info(
    epoch_manager: &dyn EpochManagerAdapter,
    account_id: &AccountId,
    epoch_id: &EpochId,
) -> Result<ShardInfo, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let shard_id = shard_layout.account_id_to_shard_id(account_id);
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    Ok(ShardInfo { shard_index, shard_uid })
}

/// Converts `ShardId` (index of shard in the *current* layout) to
/// `ShardUId` (`ShardId` + the version of shard layout itself.)
pub fn shard_id_to_uid(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_id: ShardId,
    epoch_id: &EpochId,
) -> Result<ShardUId, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
}

pub fn shard_id_to_index(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_id: ShardId,
    epoch_id: &EpochId,
) -> Result<ShardIndex, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(shard_layout.get_shard_index(shard_id)?)
}

#[cfg(test)]
mod tests {
    use crate::RngSeed;
    use crate::shard_assignment::assign_chunk_producers_to_shards as assign_inner;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, Balance, NumShards, ShardId, ShardIndex};
    use std::collections::{BTreeMap, HashMap, HashSet};

    /// Test wrapper preserving the legacy boolean-based signature used by the
    /// historical tests by mapping `use_stable_shard_assignment` to
    /// `AssignmentStrategy::{CarryOver, Fresh}`. New tests for the
    /// sticky-by-`ShardId` behavior call `assign_inner` directly.
    fn assign_chunk_producers_to_shards(
        chunk_producers: Vec<ValidatorStake>,
        num_shards: NumShards,
        min_validators_per_shard: usize,
        shard_assignment_changes_limit: usize,
        rng_seed: RngSeed,
        prev_chunk_producers_assignment: Vec<Vec<ValidatorStake>>,
        use_stable_shard_assignment: bool,
    ) -> Result<Vec<Vec<ValidatorStake>>, super::NotEnoughValidators> {
        let strategy = if use_stable_shard_assignment {
            super::AssignmentStrategy::CarryOver
        } else {
            super::AssignmentStrategy::Fresh
        };
        assign_inner(
            chunk_producers,
            num_shards,
            min_validators_per_shard,
            shard_assignment_changes_limit,
            rng_seed,
            &strategy,
            prev_chunk_producers_assignment,
        )
    }

    fn account(n: usize) -> AccountId {
        format!("test{:02}", n).parse().unwrap()
    }

    fn validator_stake_for_test(n: usize) -> ValidatorStake {
        ValidatorStake::test(account(n))
    }

    #[test]
    /// `select` falls back to `Fresh` (instead of panicking or erroring) when
    /// the new layout has no derivation relationship to the prev layout —
    /// e.g. two independent V2 layouts with no `shards_split_map`. This is
    /// the path that synthetic test layouts (`multi_shard`) take to keep
    /// unrelated tests from failing on nightly.
    fn test_select_falls_back_to_fresh_for_non_derived_layouts() {
        use near_primitives::version::PROTOCOL_VERSION;

        let prev_layout = ShardLayout::v2(
            vec!["aa".parse().unwrap()],
            vec![ShardId::new(0), ShardId::new(1)],
            None,
        );
        // Independent layout — not derived from `prev_layout`. No parent map.
        let new_layout = ShardLayout::v2(
            vec!["aa".parse().unwrap(), "bb".parse().unwrap()],
            vec![ShardId::new(2), ShardId::new(3), ShardId::new(4)],
            None,
        );
        // sticky_resharding errors on this directly (signaling the issue),
        // and select swallows the error and returns Fresh.
        assert!(super::AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).is_err());
        assert!(matches!(
            super::AssignmentStrategy::select(PROTOCOL_VERSION, &prev_layout, &new_layout),
            super::AssignmentStrategy::Fresh,
        ));
    }

    fn validator_stake_for_test_with_stake(n: usize, stake: u128) -> ValidatorStake {
        ValidatorStake::new(
            account(n),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(stake),
        )
    }

    fn assignment_for_test(assignment: Vec<Vec<usize>>) -> Vec<Vec<ValidatorStake>> {
        assignment
            .into_iter()
            .map(|ids| ids.into_iter().map(validator_stake_for_test).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }

    #[test]
    /// Tests shard assignment logic for minimal amount of validators and
    /// shards.
    fn test_shard_assignment_minimal() {
        let num_chunk_producers = 1;
        let target_assignment = assignment_for_test(vec![vec![0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            1,
            1,
            1,
            RngSeed::default(),
            vec![],
            false,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests shard assignment logic when previous chunk producer is leaving the
    /// set.
    fn test_shard_assignment_change() {
        let num_chunk_producers = 1;
        let prev_assignment = assignment_for_test(vec![vec![1]]);
        let target_assignment = assignment_for_test(vec![vec![0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            1,
            1,
            // We must assign new validator even if limit for balancing is zero.
            0,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that chunk producer repeats are supported if needed.
    fn test_shard_assignment_repeats() {
        let num_chunk_producers = 3;
        let prev_assignment =
            assignment_for_test(vec![vec![0, 1, 2], vec![0, 1, 2], vec![3, 4, 5]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1], vec![1, 0], vec![2, 0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            2,
            0,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if there are enough validators to avoid repeats, new
    /// assignment is made in stable way, by reassigning some of the old chunk
    /// producers. Repeats must not happen, like in some incorrect ideas of
    /// the assignment algorithm we had.
    fn test_shard_reassignment() {
        let num_chunk_producers = 4;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2], vec![3], vec![], vec![]]);
        let target_assignment = assignment_for_test(vec![vec![0], vec![3], vec![1], vec![2]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            4,
            1,
            // Set limit to zero, to check that it is ignored.
            0,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if chunk producers are well-balanced already, no changes are
    /// made.
    fn test_shard_assignment_is_stable() {
        let num_chunk_producers = 4;
        let prev_assignment = assignment_for_test(vec![vec![2, 3], vec![0, 1]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            2,
            1,
            // As we don't change assignment at all, zero limit for balancing is enough.
            0,
            RngSeed::default(),
            prev_assignment.clone(),
            true,
        )
        .unwrap();

        assert_eq!(assignment, prev_assignment);
    }

    #[test]
    /// Tests that limit of assignment changes is taken into account during
    /// rebalancing.
    fn test_shard_assignment_changes_limit() {
        let num_chunk_producers = 6;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2, 3], vec![4], vec![5]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1, 3], vec![2, 4], vec![5]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            1,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if there was no previous assignment and every chunk producer
    /// is new, the assignment is balanced because limit on shard changes can't
    /// be applied.
    fn test_shard_assignment_empty_start() {
        let num_chunk_producers = 10;
        let target_assignment =
            assignment_for_test(vec![vec![0, 3, 6, 9], vec![1, 4, 7], vec![2, 5, 8]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            1,
            RngSeed::default(),
            vec![],
            false,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Test case when perfect balance on number of validators is not
    /// achievable.
    fn test_shard_assignment_imperfect_balance() {
        let num_chunk_producers = 7;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2, 3, 4], vec![5], vec![6]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1, 4], vec![3, 5], vec![2, 6]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            5,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    fn validator_to_shard(assignment: &[Vec<ValidatorStake>]) -> HashMap<AccountId, ShardIndex> {
        assignment
            .iter()
            .enumerate()
            .flat_map(|(shard_index, cps)| {
                cps.iter().map(move |cp| (cp.account_id().clone(), shard_index))
            })
            .collect()
    }

    #[test]
    /// Tests that shard assignment algorithm converges to a balanced
    /// assignment, respecting the limit on shard changes.
    fn test_shard_assignment_convergence() {
        let num_chunk_producers = 15;
        let num_shards = 3;
        let mut assignment = assignment_for_test(vec![
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![11, 12, 13, 14],
            vec![],
        ]);

        let limit_per_iter = 2;
        let mut iters_left = 5;
        let mut is_balanced = false;
        while !is_balanced && iters_left > 0 {
            let new_assignment = assign_chunk_producers_to_shards(
                (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
                num_shards,
                1,
                limit_per_iter,
                RngSeed::default(),
                assignment.clone(),
                true,
            )
            .unwrap();

            let old_validator_to_shard = validator_to_shard(&assignment);
            let new_validator_to_shard = validator_to_shard(&new_assignment);
            let shard_changes = old_validator_to_shard
                .into_iter()
                .filter(|(v, s)| new_validator_to_shard.get(v) != Some(s))
                .count();
            assert!(
                shard_changes <= limit_per_iter,
                "Too many shard changes when {iters_left} iterations left"
            );

            assignment = new_assignment;
            is_balanced = assignment
                .iter()
                .all(|shard| shard.len() * (num_shards as usize) == num_chunk_producers);
            iters_left -= 1;
        }

        assert!(
            is_balanced,
            "Shard assignment didn't converge in 5 iterations, last assignment = {assignment:?}"
        );
        let original_chunk_producer_ids =
            (0..num_chunk_producers).map(account).collect::<HashSet<_>>();
        let chunk_producer_ids = assignment
            .into_iter()
            .flat_map(|shard| shard.into_iter().map(|cp| cp.account_id().clone()))
            .collect::<HashSet<_>>();
        assert_eq!(original_chunk_producer_ids, chunk_producer_ids);
    }

    #[test]
    /// Tests that shard assignment handles changing number of shards correctly.
    fn test_shard_assignment_with_changing_shard_count() {
        let num_chunk_producers = 4;
        let min_validators_per_shards = 1;

        // Previous epoch had 2 shards
        let prev_assignment = assignment_for_test(vec![vec![0, 1], vec![2, 3]]);

        // Current epoch has 3 shards
        let num_shards = 3;

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            num_shards,
            min_validators_per_shards,
            10,
            RngSeed::default(),
            prev_assignment,
            true,
        )
        .unwrap();

        // Assignments should have 3 shards
        assert_eq!(assignment.len(), 3);

        // All chunk producers should be assigned to a shard
        let assigned_producers: HashSet<_> = assignment
            .iter()
            .flat_map(|shard| shard.iter().map(|cp| cp.account_id().clone()))
            .collect();
        let expected_producers: HashSet<_> = (0..num_chunk_producers).map(account).collect();
        assert_eq!(assigned_producers, expected_producers);

        // Each shard should have at least the minimum required validators
        for shard_assignment in &assignment {
            assert!(
                !shard_assignment.is_empty(),
                "Each shard should have at least {} validator",
                min_validators_per_shards
            );
        }
    }

    impl super::HasStake for (usize, Balance) {
        fn get_stake(&self) -> Balance {
            self.1
        }
    }

    /// Builds a base V2 layout from boundary accounts and an explicit list of
    /// shard ids (so tests can pin down ids deterministically).
    fn build_base_layout(boundary_accounts: &[&str], shard_ids: &[u64]) -> ShardLayout {
        let boundaries = boundary_accounts.iter().map(|s| s.parse().unwrap()).collect();
        let ids = shard_ids.iter().map(|&id| ShardId::new(id)).collect();
        ShardLayout::v2(boundaries, ids, None)
    }

    /// Returns the shard a given validator account is assigned to in
    /// `assignment`, by `ShardId` (resolving via `layout`).
    fn assignment_by_shard_id(
        assignment: &[Vec<ValidatorStake>],
        layout: &ShardLayout,
    ) -> HashMap<ShardId, Vec<AccountId>> {
        assignment
            .iter()
            .enumerate()
            .map(|(i, vs)| {
                let shard_id = layout.get_shard_id(i).unwrap();
                let accounts = vs.iter().map(|v| v.account_id().clone()).collect();
                (shard_id, accounts)
            })
            .collect()
    }

    /// Convenience: build chunk_producers and call `bin_pack_into_children`,
    /// returning a map from shard index to the *account ids* placed there (so
    /// tests don't have to translate index → name).
    fn bin_pack_named(
        validator_stakes: &[(usize, u128)],
        children: &[ShardIndex],
    ) -> BTreeMap<ShardIndex, Vec<AccountId>> {
        let chunk_producers: Vec<ValidatorStake> = validator_stakes
            .iter()
            .map(|&(n, stake)| validator_stake_for_test_with_stake(n, stake))
            .collect();
        let validator_indices: Vec<usize> = (0..chunk_producers.len()).collect();
        super::bin_pack_into_children(validator_indices, children, &chunk_producers)
            .into_iter()
            .map(|(idx, val_indices)| {
                let accounts = val_indices
                    .into_iter()
                    .map(|i| chunk_producers[i].account_id().clone())
                    .collect();
                (idx, accounts)
            })
            .collect()
    }

    #[test]
    /// Empty input: every child gets an empty list, and the map still has one
    /// entry per child. Real case: a parent shard whose entire validator set
    /// unstakes / gets kicked out before the resharding epoch.
    fn test_bin_pack_empty_survivors() {
        let result = bin_pack_named(&[], &[10, 20]);
        assert_eq!(result.len(), 2);
        assert!(result[&10].is_empty());
        assert!(result[&20].is_empty());
    }

    #[test]
    /// One survivor across two children — one child gets it, the other is
    /// empty (and is still keyed). Real case: parent shard had only the
    /// minimum number of chunk producers (1 on some mainnet configs) before
    /// splitting.
    fn test_bin_pack_single_survivor() {
        let result = bin_pack_named(&[(0, 100)], &[10, 20]);
        // Both children start at (0,0,idx); smaller-idx wins on tiebreak.
        assert_eq!(result[&10], vec![account(0)]);
        assert!(result[&20].is_empty());
    }

    #[test]
    /// Equal-stake validators: count tiebreak makes them alternate across
    /// children. With the lexicographic tiebreak (low account id first),
    /// even-indexed validators land on the lower-index child and odd-indexed
    /// on the higher-index child. Real case: production validators often
    /// cluster around similar stake amounts.
    fn test_bin_pack_equal_stakes_round_robin() {
        let result = bin_pack_named(&[(0, 10), (1, 10), (2, 10), (3, 10)], &[10, 20]);
        assert_eq!(result[&10], vec![account(0), account(2)]);
        assert_eq!(result[&20], vec![account(1), account(3)]);
    }

    #[test]
    /// Whale + minnows: greedy bin-pack puts the whale on one child and the
    /// minnows on the other (the algorithm can't beat the lower bound set by
    /// the largest item).
    fn test_bin_pack_whale_and_minnows() {
        let result = bin_pack_named(&[(0, 100), (1, 10), (2, 10), (3, 10)], &[10, 20]);
        let stakes_on = |idx: ShardIndex| -> u128 {
            result[&idx]
                .iter()
                .map(|acct| {
                    // Reverse-lookup stake from account name; we know the
                    // pattern from the helper.
                    let n: usize = acct.as_str().trim_start_matches("test").parse().unwrap();
                    [100u128, 10, 10, 10][n]
                })
                .sum()
        };
        // Whale alone on one child; three minnows together on the other.
        let s_a = stakes_on(10);
        let s_b = stakes_on(20);
        assert_eq!(s_a + s_b, 130);
        assert_eq!(s_a.max(s_b), 100); // whale's child
        assert_eq!(s_a.min(s_b), 30); // minnows' child
        // Each child has at least one validator.
        assert!(!result[&10].is_empty());
        assert!(!result[&20].is_empty());
    }

    #[test]
    /// One shard splits into two: parent's validators are partitioned across
    /// the children with a stake-balanced bin-packing, and the unchanged
    /// shards keep their assignment by `ShardId` even though shard indices
    /// shift.
    fn test_sticky_resharding_simple_split() {
        // Old layout: 2 shards (ids 0, 1), boundary at "mm". Shard 1 will split.
        let prev_layout = build_base_layout(&["mm"], &[0, 1]);
        // Derive a new layout splitting at "tt". The shard containing "tt"
        // (shard id 1, indices [mm, ∞)) becomes the parent.
        let new_layout = ShardLayout::derive_shard_layout(&prev_layout, "tt".parse().unwrap());

        // 6 chunk producers; previous epoch placed 2 on shard 0 and 4 on shard 1.
        let chunk_producers: Vec<_> = (0..6).map(validator_stake_for_test).collect();
        let prev_assignment = assignment_for_test(vec![vec![0, 1], vec![2, 3, 4, 5]]);

        let assignment = assign_inner(
            chunk_producers,
            new_layout.num_shards() as NumShards,
            1,
            0,
            RngSeed::default(),
            &super::AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
            prev_assignment,
        )
        .unwrap();

        let accounts_by_shard_id = assignment_by_shard_id(&assignment, &new_layout);

        // Unchanged shard 0 retains its validators by id.
        assert_eq!(accounts_by_shard_id[&ShardId::new(0)], vec![account(0), account(1)],);

        // Parent shard 1 splits into children with ShardIds 2 and 3 (V2
        // assigns max+1, max+2). Bin-pack with equal stakes places
        // even-indexed parent validators on the lower-index child and
        // odd-indexed on the higher-index child.
        let children = new_layout
            .get_children_shards_ids(ShardId::new(1))
            .expect("shard 1 should have children");
        assert_eq!(children, vec![ShardId::new(2), ShardId::new(3)]);
        assert_eq!(accounts_by_shard_id[&ShardId::new(2)], vec![account(2), account(4)]);
        assert_eq!(accounts_by_shard_id[&ShardId::new(3)], vec![account(3), account(5)]);
    }

    #[test]
    /// With unequal stakes, the parent's validators should be partitioned
    /// across children to roughly balance total stake (greedy bin-packing).
    fn test_sticky_resharding_stake_balanced_split() {
        let prev_layout = build_base_layout(&["mm"], &[0, 1]);
        let new_layout = ShardLayout::derive_shard_layout(&prev_layout, "tt".parse().unwrap());

        // Validators on the parent shard (1) have stakes 100, 70, 60, 50.
        // Greedy bin-pack: 100 -> child A; 70 -> child B; 60 -> child B;
        // 50 -> child A. Totals: A=150, B=130. Difference 20, much better than
        // a naive first-half/second-half split (170 vs 110).
        let cp_shard0 = vec![validator_stake_for_test_with_stake(0, 10)];
        let cp_shard1 = vec![
            validator_stake_for_test_with_stake(1, 100),
            validator_stake_for_test_with_stake(2, 70),
            validator_stake_for_test_with_stake(3, 60),
            validator_stake_for_test_with_stake(4, 50),
        ];
        // chunk_producers must be ordered by stake desc.
        let chunk_producers: Vec<_> = vec![
            validator_stake_for_test_with_stake(1, 100),
            validator_stake_for_test_with_stake(2, 70),
            validator_stake_for_test_with_stake(3, 60),
            validator_stake_for_test_with_stake(4, 50),
            validator_stake_for_test_with_stake(0, 10),
        ];
        let prev_assignment = vec![cp_shard0, cp_shard1];

        let assignment = assign_inner(
            chunk_producers.clone(),
            new_layout.num_shards() as NumShards,
            1,
            0,
            RngSeed::default(),
            &super::AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
            prev_assignment,
        )
        .unwrap();

        let accounts_by_shard_id = assignment_by_shard_id(&assignment, &new_layout);
        let parent = ShardId::new(1);
        let children = new_layout.get_children_shards_ids(parent).unwrap();

        let stake_total = |shard_id| -> u128 {
            accounts_by_shard_id[&shard_id]
                .iter()
                .map(|acct| {
                    chunk_producers
                        .iter()
                        .find(|cp| cp.account_id() == acct)
                        .unwrap()
                        .stake()
                        .as_yoctonear()
                })
                .sum()
        };
        let stake_a = stake_total(children[0]);
        let stake_b = stake_total(children[1]);
        // Greedy bin-pack should keep the stakes within max(stake) of each
        // other. With stakes [100, 70, 60, 50] this is 100.
        let imbalance = stake_a.abs_diff(stake_b);
        assert!(
            imbalance <= 100,
            "child stakes too imbalanced: {} vs {} (diff {})",
            stake_a,
            stake_b,
            imbalance
        );
        // And both children must have inherited at least one parent validator.
        assert!(!accounts_by_shard_id[&children[0]].is_empty());
        assert!(!accounts_by_shard_id[&children[1]].is_empty());
    }

    #[test]
    /// 4-shard layout, exactly one shard splits. The other three shards keep
    /// their validators identically (by `ShardId`), even though the new layout
    /// places the split children at different `ShardIndex` positions.
    fn test_sticky_resharding_unchanged_shards_keep_validators() {
        let prev_layout = build_base_layout(&["bb", "mm", "tt"], &[0, 1, 2, 3]);
        // Split the shard whose range contains "hh" — i.e. shard 1 (range [bb, mm)).
        let new_layout = ShardLayout::derive_shard_layout(&prev_layout, "hh".parse().unwrap());

        // 8 producers, 2 per shard previously.
        let chunk_producers: Vec<_> = (0..8).map(validator_stake_for_test).collect();
        let prev_assignment =
            assignment_for_test(vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]]);

        let assignment = assign_inner(
            chunk_producers,
            new_layout.num_shards() as NumShards,
            1,
            0,
            RngSeed::default(),
            &super::AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
            prev_assignment,
        )
        .unwrap();

        let accounts_by_shard_id = assignment_by_shard_id(&assignment, &new_layout);
        let expected_unchanged_accounts = |prev_account_indices: &[usize]| -> Vec<AccountId> {
            prev_account_indices.iter().map(|&i| account(i)).collect()
        };
        // Unchanged shards 0, 2, 3 keep their original validators.
        assert_eq!(accounts_by_shard_id[&ShardId::new(0)], expected_unchanged_accounts(&[0, 1]));
        assert_eq!(accounts_by_shard_id[&ShardId::new(2)], expected_unchanged_accounts(&[4, 5]));
        assert_eq!(accounts_by_shard_id[&ShardId::new(3)], expected_unchanged_accounts(&[6, 7]));

        // The split children together cover exactly the parent's validators.
        let children = new_layout.get_children_shards_ids(ShardId::new(1)).unwrap();
        let mut combined: Vec<AccountId> =
            children.iter().flat_map(|c| accounts_by_shard_id[c].iter().cloned()).collect();
        combined.sort();
        let mut expected = vec![account(2), account(3)];
        expected.sort();
        assert_eq!(combined, expected);
    }
}
