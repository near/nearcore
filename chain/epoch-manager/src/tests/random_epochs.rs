use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::EpochManager;
use crate::test_utils::{hash_range, record_block, setup_default_epoch_manager, stake};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, EpochId};

const DEBUG_PRINT: bool = false;

#[test]
fn test_random_epochs() {
    run_with_seed_range(2, 10, 1000);
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
    do_random_test(&mut rng, epoch_length, num_heights);
}

fn do_random_test<RngImpl: Rng>(rng: &mut RngImpl, epoch_length: u64, num_heights: u64) {
    let stake_amount = 1_000;

    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, epoch_length, 1, 3, 90, 60);
    let h = hash_range(num_heights as usize);
    let skip_height_probability = rng.gen_range(0.0..1.0) * rng.gen_range(0.0..1.0);

    let heights_to_pick = (0u64..1u64)
        .chain((1u64..num_heights).filter(|_i| rng.gen_range(0.0..1.0) >= skip_height_probability))
        .collect::<Vec<_>>();

    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let mut prev_hash = h[0];
    for &height in &heights_to_pick[1..] {
        let proposals = random_proposals(rng);
        record_block(&mut epoch_manager, prev_hash, h[height as usize], height, proposals);
        prev_hash = h[height as usize];
    }

    validate(&mut epoch_manager, heights_to_pick);
}

fn random_proposals<RngImpl: Rng>(rng: &mut RngImpl) -> Vec<ValidatorStake> {
    let mut proposals = Vec::new();
    let proposal_chance = 0.2;
    if rng.gen_range(0.0..1.0) < proposal_chance {
        let account_id = AccountId::try_from(format!("test{}", rng.gen_range(1..6))).unwrap();
        let stake_amount = rng.gen_range(100..2000);
        proposals.push(stake(account_id, stake_amount));
    }
    proposals
}

fn validate(epoch_manager: &EpochManager, heights: Vec<u64>) {
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
                Some(epoch_manager.get_epoch_info(&EpochId(*hash)).unwrap())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        &epoch_infos[0],
        &epoch_manager.get_epoch_info(&EpochId(CryptoHash::default())).unwrap(),
        "first two epoch are the same, even epoch_height"
    );
    let block_infos = block_hashes
        .iter()
        .map(|&hash| epoch_manager.get_block_info(&hash).unwrap())
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
    verify_proposals(epoch_manager, &block_infos);
    verify_epochs(&epoch_infos);
}

fn verify_epochs(epoch_infos: &[Arc<EpochInfo>]) {
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
                if !stakes_before_change.contains_key(account_id) {
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
        for account_id in epoch_info.validator_kickout().keys() {
            let was_validator_2_ago = (i >= 2
                && epoch_infos[i - 2].account_is_validator(account_id))
                || (i == 1 && epoch_infos[0].account_is_validator(account_id));
            assert!(was_validator_2_ago, "Kickout set can only have validators from 2 epochs ago");
        }
    }
}

fn verify_proposals(epoch_manager: &EpochManager, block_infos: &[Arc<BlockInfo>]) {
    let mut proposals = BTreeMap::default();
    for i in 1..block_infos.len() {
        let prev_block_info = &block_infos[i - 1];
        let block_info = &block_infos[i];
        assert!(block_info.last_finalized_height() >= prev_block_info.last_finalized_height());
        if epoch_manager.is_next_block_epoch_start(block_infos[i].prev_hash()).unwrap() {
            assert_ne!(prev_block_info.epoch_first_block(), block_info.epoch_first_block());
            if prev_block_info.height() == 0 {
                // special case: epochs 0 and 1
                assert_eq!(
                    prev_block_info.epoch_id(),
                    block_info.epoch_id(),
                    "first two epochs have same id"
                );
            } else {
                assert_ne!(prev_block_info.epoch_id(), block_info.epoch_id(), "epoch id changes");
            }
            let aggregator =
                epoch_manager.get_epoch_info_aggregator_upto_last(block_info.prev_hash()).unwrap();
            assert_eq!(aggregator.all_proposals, proposals, "Proposals do not match");
            proposals = BTreeMap::from_iter(
                block_info.proposals_iter().map(|p| (p.account_id().clone(), p)),
            );
        } else {
            proposals.extend(block_info.proposals_iter().map(|p| (p.account_id().clone(), p)));
        }
    }
}

fn verify_block_stats(
    epoch_manager: &EpochManager,
    heights: Vec<u64>,
    block_infos: &[Arc<BlockInfo>],
    block_hashes: &[CryptoHash],
) {
    for i in 1..block_infos.len() {
        let prev_epoch_end =
            *epoch_manager.get_block_info(block_infos[i].epoch_first_block()).unwrap().prev_hash();
        let prev_epoch_end_height = epoch_manager.get_block_info(&prev_epoch_end).unwrap().height();
        let blocks_in_epoch = (i - heights.binary_search(&prev_epoch_end_height).unwrap()) as u64;
        let blocks_in_epoch_expected = heights[i] - prev_epoch_end_height;
        {
            let aggregator =
                epoch_manager.get_epoch_info_aggregator_upto_last(&block_hashes[i]).unwrap();
            let epoch_id = block_infos[i].epoch_id();
            let epoch_info = epoch_manager.get_epoch_info(epoch_id).unwrap();
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
            // TODO: The following sophisticated check doesn't do anything. The
            // shard tracker is empty because the chunk mask in all block infos
            // is empty.
            for &shard_id in aggregator.shard_tracker.keys() {
                let sum_produced = aggregator
                    .shard_tracker
                    .get(&shard_id)
                    .unwrap()
                    .values()
                    .map(|value| value.produced())
                    .sum::<u64>();
                let sum_expected = aggregator
                    .shard_tracker
                    .get(&shard_id)
                    .unwrap()
                    .values()
                    .map(|value| value.expected())
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
