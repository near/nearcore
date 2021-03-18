use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

use near_epoch_manager::test_utils::{
    hash_range, record_block_with_slashes, setup_default_epoch_manager, stake,
};
use near_epoch_manager::EpochManager;
use near_primitives::challenge::SlashedValidator;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::SlashState;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, EpochId, ValidatorKickoutReason};

const DEBUG_PRINT: bool = false;

#[test]
fn test_random_epochs() {
    run_with_seed_range(2, 10, 1000);
}

#[allow(dead_code)]
fn minimal_repro(epoch_length: u64, num_heights: u64, max_seed: u64) {
    let mut epoch_length = epoch_length;
    let mut num_heights = num_heights;
    if !std::panic::catch_unwind(move || run_with_seed_range(epoch_length, num_heights, max_seed))
        .is_err()
    {
        println!("PASS");
        return;
    }
    while epoch_length > 1
        && std::panic::catch_unwind(move || {
            run_with_seed_range(epoch_length - 1, num_heights, max_seed)
        })
        .is_err()
    {
        epoch_length -= 1;
    }
    while num_heights > 1
        && std::panic::catch_unwind(move || {
            run_with_seed_range(epoch_length, num_heights * 4 / 5, max_seed)
        })
        .is_err()
    {
        num_heights = num_heights * 4 / 5;
    }
    panic!("Found! run_with_seed_range({}, {}, {})", epoch_length, num_heights, max_seed);
}

fn run_with_seed_range(epoch_length: u64, num_heights: u64, max_seed: u64) {
    for test_seed in 0..max_seed {
        if let Err(e) =
            std::panic::catch_unwind(move || run_with_seed(epoch_length, num_heights, test_seed))
        {
            println!("Found! run_with_seed({}, {}, {})", epoch_length, num_heights, test_seed);
            std::panic::resume_unwind(e);
        }
    }
}

fn run_with_seed(epoch_length: u64, num_heights: u64, test: u64) {
    let mut seed = [0u8; 32];
    for i in 0..8 {
        seed[i] = ((test >> ((8 * i) as u64)) & 0xFF) as u8
    }
    let mut rng = StdRng::from_seed(seed);
    let do_slashes = rng.gen_bool(0.5);
    do_random_test(&mut rng, epoch_length, num_heights, do_slashes);
}

fn do_random_test<RngImpl: Rng>(
    rng: &mut RngImpl,
    epoch_length: u64,
    num_heights: u64,
    do_slashes: bool,
) {
    let stake_amount = 1_000;

    let validators =
        vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
    let mut epoch_manager = setup_default_epoch_manager(validators, epoch_length, 1, 3, 0, 90, 60);
    let h = hash_range(num_heights as usize);
    let skip_height_probability = rng.gen_range(0.0, 1.0) * rng.gen_range(0.0, 1.0);

    let heights_to_pick = (0u64..1u64)
        .chain((1u64..num_heights).filter(|_i| rng.gen_range(0.0, 1.0) >= skip_height_probability))
        .collect::<Vec<_>>();

    let mut slashes_per_block = Vec::new();
    record_block_with_slashes(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![], vec![]);
    slashes_per_block.push(vec![]);
    let mut prev_hash = h[0];
    for &height in &heights_to_pick[1..] {
        let proposals = random_proposals(rng);
        let slashes = if do_slashes { random_slashes(rng) } else { vec![] };
        slashes_per_block.push(slashes.clone());
        record_block_with_slashes(
            &mut epoch_manager,
            prev_hash,
            h[height as usize],
            height,
            proposals,
            slashes,
        );
        prev_hash = h[height as usize];
    }

    validate(&mut epoch_manager, heights_to_pick, slashes_per_block);
}

fn random_proposals<RngImpl: Rng>(rng: &mut RngImpl) -> Vec<ValidatorStake> {
    let mut proposals = Vec::new();
    let proposal_chance = 0.2;
    if rng.gen_range(0.0, 1.0) < proposal_chance {
        let account_id = format!("test{}", rng.gen_range(1, 6));
        let stake_amount = rng.gen_range(100, 2000);
        proposals.push(stake(&account_id, stake_amount));
    }
    proposals
}

fn random_slashes<RngImpl: Rng>(rng: &mut RngImpl) -> Vec<SlashedValidator> {
    let mut slashes = Vec::new();
    let slash_chance = 0.2;
    if rng.gen_range(0.0, 1.0) < slash_chance {
        let account_id = format!("test{}", rng.gen_range(1, 6));
        slashes.push(SlashedValidator::new(account_id, true));
    }
    slashes
}

fn validate(
    epoch_manager: &mut EpochManager,
    heights: Vec<u64>,
    slashes_per_block: Vec<Vec<SlashedValidator>>,
) {
    let num_blocks = heights.len();
    let height_to_hash = hash_range((heights[num_blocks - 1] + 1) as usize);
    let block_hashes =
        heights.iter().map(|&height| height_to_hash[height as usize]).collect::<Vec<_>>();
    let epoch_start_heights = block_hashes
        .iter()
        .map(|&hash| epoch_manager.get_epoch_start_height(&hash).unwrap())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let epoch_infos = block_hashes
        .iter()
        .filter_map(|hash| {
            if epoch_manager.is_next_block_epoch_start(hash).unwrap() {
                Some(epoch_manager.get_epoch_info(&EpochId(*hash)).unwrap().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        &epoch_infos[0],
        epoch_manager.get_epoch_info(&EpochId(CryptoHash::default())).unwrap(),
        "first two epoch are the same, even epoch_height"
    );
    let block_infos = block_hashes
        .iter()
        .map(|&hash| epoch_manager.get_block_info(&hash).unwrap().clone())
        .collect::<Vec<_>>();
    if DEBUG_PRINT {
        println!("Block heights: {:?}", heights);
        println!("Epoch start heights: {:?}", epoch_start_heights);
        println!("== Begin epoch infos ==");
        for info in &epoch_infos {
            println!("{:?}", info);
        }
        println!("== End epoch infos ==");
        println!("== Begin block infos ==");
        for block_info in &block_infos {
            println!(
                "height: {:?}, proposals: {:?}",
                block_info.height(),
                block_info.proposals_iter().collect::<Vec<_>>()
            );
        }
        println!("== End block infos ==");
    }

    verify_block_stats(epoch_manager, heights, &block_infos, &block_hashes);
    verify_slashes(epoch_manager, &block_infos, &slashes_per_block);
    verify_proposals(epoch_manager, &block_infos);
    verify_epochs(&epoch_infos);
}

fn verify_epochs(epoch_infos: &Vec<EpochInfo>) {
    for i in 1..epoch_infos.len() {
        let epoch_info = &epoch_infos[i];
        let prev_epoch_info = &epoch_infos[i - 1];
        assert_eq!(
            epoch_info.epoch_height(),
            prev_epoch_info.epoch_height() + 1,
            "epoch height increases by 1"
        );
        let stakes_before_change = get_stakes_map(prev_epoch_info);
        assert!(!stakes_before_change.is_empty(), "validator set cannot be empty");
        for (_, stake) in &stakes_before_change {
            assert_ne!(*stake, 0, "validators cannot have 0 stake");
        }
        let stakes_after_change = get_stakes_map(epoch_info);
        let mut stakes_with_change = stakes_before_change.clone();
        for (account_id, new_stake) in epoch_info.stake_change() {
            if *new_stake == 0 {
                if stakes_before_change.get(account_id).is_none() {
                    // Stake change from 0 to 0
                    assert!(prev_epoch_info.validator_kickout().contains_key(account_id));
                    assert!(epoch_info.validator_kickout().contains_key(account_id));
                }
                stakes_with_change.remove(account_id);
            } else {
                stakes_with_change.insert(account_id.clone(), *new_stake);
            }
        }
        assert_eq!(
            stakes_with_change,
            stakes_after_change,
            "stake change: {:?}",
            epoch_info.stake_change()
        );

        for account_id in stakes_after_change.keys() {
            assert!(
                epoch_info.validator_kickout().get(account_id).is_none(),
                "cannot be in both validator and kickout set"
            );
        }

        if is_possible_bad_epochs_case(&epoch_infos[i - 1], &epoch_infos[i]) {
            continue;
        }
        for (account_id, reason) in epoch_info.validator_kickout() {
            let was_validaror_2_ago = (i >= 2
                && epoch_infos[i - 2].account_is_validator(account_id))
                || (i == 1 && epoch_infos[0].account_is_validator(account_id));
            let in_slashes_set = reason == &ValidatorKickoutReason::Slashed;
            assert!(
                was_validaror_2_ago || in_slashes_set,
                "Kickout set can only have validators from 2 epochs ago"
            );
        }
    }
}

fn verify_proposals(epoch_manager: &mut EpochManager, block_infos: &Vec<BlockInfo>) {
    let mut proposals = BTreeMap::default();
    for i in 1..block_infos.len() {
        let prev_block_info = &block_infos[i - 1];
        let block_info = &block_infos[i];
        assert!(block_info.last_finalized_height() >= prev_block_info.last_finalized_height());
        if epoch_manager.is_next_block_epoch_start(block_infos[i].prev_hash()).unwrap() {
            assert_ne!(prev_block_info.epoch_first_block(), block_info.epoch_first_block());
            if *prev_block_info.height() == 0 {
                // special case: epochs 0 and 1
                assert_eq!(
                    prev_block_info.epoch_id(),
                    block_info.epoch_id(),
                    "first two epochs have same id"
                );
            } else {
                assert_ne!(prev_block_info.epoch_id(), block_info.epoch_id(), "epoch id changes");
            }
            let aggregator = epoch_manager
                .get_and_update_epoch_info_aggregator(
                    prev_block_info.epoch_id(),
                    block_info.prev_hash(),
                    true,
                )
                .unwrap();
            assert_eq!(aggregator.all_proposals, proposals, "Proposals do not match");
            proposals = BTreeMap::from_iter(
                block_info.proposals_iter().map(|p| (p.account_id().clone(), p)),
            );
        } else {
            proposals.extend(block_info.proposals_iter().map(|p| (p.account_id().clone(), p)));
        }
    }
}

fn verify_slashes(
    epoch_manager: &mut EpochManager,
    block_infos: &Vec<BlockInfo>,
    slashes_per_block: &Vec<Vec<SlashedValidator>>,
) {
    for i in 1..block_infos.len() {
        let prev_slashes_set = block_infos[i - 1].slashed();
        let slashes_set = block_infos[i].slashed();

        let this_block_slashes = slashes_per_block[i]
            .iter()
            .map(|sv| {
                (
                    sv.account_id.clone(),
                    if sv.is_double_sign { SlashState::DoubleSign } else { SlashState::Other },
                )
            })
            .collect::<HashMap<_, _>>();
        if epoch_manager.is_next_block_epoch_start(block_infos[i].prev_hash()).unwrap() {
            let epoch_info = epoch_manager.get_epoch_info(block_infos[i].epoch_id()).unwrap();

            // Epoch boundary.
            // DoubleSign or Other => become AlreadySlashed
            // AlreadySlashed and in stake_change => keep AlreadySlashed
            // ALreadySlashed and not in stake_change => remove
            for (account, slash_state) in prev_slashes_set {
                if let Some(slash) = this_block_slashes.get(account) {
                    assert_eq!(slashes_set.get(account), Some(slash));
                    continue;
                }
                if slash_state == &SlashState::AlreadySlashed {
                    if epoch_info.stake_change().contains_key(account) {
                        assert_eq!(slashes_set.get(account), Some(&SlashState::AlreadySlashed));
                    } else {
                        assert_eq!(slashes_set.get(account), None);
                    }
                } else {
                    assert_eq!(slashes_set.get(account), Some(&SlashState::AlreadySlashed));
                }
            }
        } else {
            // Not epoch boundary: slash state gets overwritten unless it was Other
            for (account, prev_slash_state) in prev_slashes_set {
                if let Some(state) = this_block_slashes.get(account) {
                    if prev_slash_state == &SlashState::Other {
                        assert_eq!(slashes_set.get(account), Some(&SlashState::Other))
                    } else {
                        assert_eq!(slashes_set.get(account), Some(state))
                    }
                } else {
                    assert_eq!(slashes_set.get(account), Some(prev_slash_state));
                }
            }
        }
    }
}

fn verify_block_stats(
    epoch_manager: &mut EpochManager,
    heights: Vec<u64>,
    block_infos: &Vec<BlockInfo>,
    block_hashes: &[CryptoHash],
) {
    for i in 1..block_infos.len() {
        let prev_epoch_end =
            *epoch_manager.get_block_info(block_infos[i].epoch_first_block()).unwrap().prev_hash();
        let prev_epoch_end_height =
            *epoch_manager.get_block_info(&prev_epoch_end).unwrap().height();
        let blocks_in_epoch = (i - heights.binary_search(&prev_epoch_end_height).unwrap()) as u64;
        let blocks_in_epoch_expected = heights[i] - prev_epoch_end_height;
        {
            let aggregator = epoch_manager
                .get_and_update_epoch_info_aggregator(
                    block_infos[i].epoch_id(),
                    &block_hashes[i],
                    true,
                )
                .unwrap();
            let epoch_info = epoch_manager.get_epoch_info(block_infos[i].epoch_id()).unwrap();
            for key in aggregator.block_tracker.keys().copied() {
                assert!(key < epoch_info.validators_iter().len() as u64);
            }
            for shard_stats in aggregator.shard_tracker.values() {
                for key in shard_stats.keys().copied() {
                    assert!(key < epoch_info.validators_iter().len() as u64);
                }
            }
            let sum_produced =
                aggregator.block_tracker.values().map(|value| value.produced).sum::<u64>();
            let sum_expected =
                aggregator.block_tracker.values().map(|value| value.expected).sum::<u64>();
            assert_eq!(sum_produced, blocks_in_epoch);
            assert_eq!(sum_expected, blocks_in_epoch_expected);
            for shard_id in 0..(aggregator.shard_tracker.len() as u64) {
                let sum_produced = aggregator
                    .shard_tracker
                    .get(&shard_id)
                    .unwrap()
                    .values()
                    .map(|value| value.produced)
                    .sum::<u64>();
                let sum_expected = aggregator
                    .shard_tracker
                    .get(&shard_id)
                    .unwrap()
                    .values()
                    .map(|value| value.expected)
                    .sum::<u64>();
                assert_eq!(sum_produced, blocks_in_epoch);
                assert_eq!(sum_expected, blocks_in_epoch_expected);
            }
        }
    }
}

// Bad epoch case: All validators are kicked out.
fn is_possible_bad_epochs_case(prev: &EpochInfo, curr: &EpochInfo) -> bool {
    let mut copy = prev.clone();
    *copy.epoch_height_mut() += 1;
    &copy == curr
}

fn get_stakes_map(epoch_info: &EpochInfo) -> HashMap<AccountId, Balance> {
    epoch_info
        .validators_iter()
        .chain(epoch_info.fishermen_iter())
        .map(|stake| stake.account_and_stake())
        .collect::<HashMap<_, _>>()
}
