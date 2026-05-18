//! Sticky-by-`ShardId` validator assignment across reshardings, gated behind
//! `ProtocolFeature::StickyReshardingValidatorAssignment`. Implements the
//! `AssignmentStrategy::StickyResharding` constructor, the bin-pack used to
//! distribute a parent's validators across split children, and the dispatch
//! helper called by `get_initial_chunk_producer_assignment` when the strategy
//! is `StickyResharding`. See the module-level comment in `mod.rs` for the
//! big picture.

use super::{AssignmentStrategy, ValidatorIdx, build_chunk_producer_indices};
use itertools::Itertools;
use near_primitives::shard_layout::{ShardLayout, ShardLayoutError};
use near_primitives::types::{Balance, ShardIndex, validator_stake::ValidatorStake};
use std::cmp::Reverse;
use std::collections::BTreeMap;

impl AssignmentStrategy {
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

/// Map the prev assignment onto the new layout by `ShardId`. Used by
/// `AssignmentStrategy::StickyResharding`. Unchanged shards keep their
/// validators directly; split children get a stake-balanced subset of their
/// parent's validators via greedy bin-packing. Validators that are no longer
/// chunk producers in the new epoch are dropped.
///
/// Caller must ensure `prev_assignment` corresponds to the same number of
/// shards as `shard_idx_mapping` (i.e. one entry per prev shard).
pub(super) fn sticky_by_shard_id(
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

#[cfg(test)]
mod tests {
    use super::super::AssignmentStrategy;
    use super::super::tests::{account, assignment_for_test, validator_stake_for_test};
    use super::*;
    use crate::RngSeed;
    use crate::shard_assignment::assign_chunk_producers_to_shards as assign_inner;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::{AccountId, NumShards, ShardId};
    use std::collections::HashMap;

    fn validator_stake_for_test_with_stake(n: usize, stake: u128) -> ValidatorStake {
        ValidatorStake::new(
            account(n),
            PublicKey::empty(KeyType::ED25519),
            Balance::from_yoctonear(stake),
        )
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
        bin_pack_into_children(validator_indices, children, &chunk_producers)
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
        assert!(AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).is_err());
        assert!(matches!(
            AssignmentStrategy::select(PROTOCOL_VERSION, &prev_layout, &new_layout),
            AssignmentStrategy::Fresh,
        ));
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
            &AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
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
            &AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
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
            &AssignmentStrategy::sticky_resharding(&new_layout, &prev_layout).unwrap(),
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
