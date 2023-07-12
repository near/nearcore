mod random_epochs;

use super::*;
use crate::reward_calculator::NUM_NS_IN_SECOND;
use crate::test_utils::{
    block_info, change_stake, default_reward_calculator, epoch_config,
    epoch_config_with_production_config, epoch_info, epoch_info_with_num_seats, hash_range,
    record_block, record_block_with_final_block_hash, record_block_with_slashes,
    record_with_block_info, reward, setup_default_epoch_manager, setup_epoch_manager, stake,
    DEFAULT_TOTAL_SUPPLY,
};
use near_primitives::challenge::SlashedValidator;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::hash::hash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::ValidatorKickoutReason::{NotEnoughBlocks, NotEnoughChunks};
use near_primitives::version::ProtocolFeature::SimpleNightshade;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;
use num_rational::Ratio;

impl EpochManager {
    /// Returns number of produced and expected blocks by given validator.
    fn get_num_validator_blocks(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<ValidatorStats, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let validator_id = *epoch_info
            .get_validator_id(account_id)
            .ok_or_else(|| EpochError::NotAValidator(account_id.clone(), epoch_id.clone()))?;
        let aggregator = self.get_epoch_info_aggregator_upto_last(last_known_block_hash)?;
        Ok(aggregator
            .block_tracker
            .get(&validator_id)
            .unwrap_or_else(|| &ValidatorStats { produced: 0, expected: 0 })
            .clone())
    }
}

#[test]
fn test_stake_validator() {
    let amount_staked = 1_000_000;
    let validators = vec![("test1".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 1, 1, 2, 2, 90, 60);

    let h = hash_range(4);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

    let expected0 = epoch_info_with_num_seats(
        1,
        vec![("test1".parse().unwrap(), amount_staked)],
        vec![0, 0],
        vec![vec![0, 0]],
        vec![],
        vec![],
        change_stake(vec![("test1".parse().unwrap(), amount_staked)]),
        vec![],
        reward(vec![("near".parse().unwrap(), 0)]),
        0,
        4,
    );
    let compare_epoch_infos = |a: &EpochInfo, b: &EpochInfo| -> bool {
        a.validators_iter().eq(b.validators_iter())
            && a.fishermen_iter().eq(b.fishermen_iter())
            && a.stake_change() == b.stake_change()
            && a.validator_kickout() == b.validator_kickout()
            && a.validator_reward() == b.validator_reward()
    };
    let epoch0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch0).unwrap(), &expected0));

    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![stake("test2".parse().unwrap(), amount_staked)],
    );
    let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch1).unwrap(), &expected0));
    assert_eq!(epoch_manager.get_epoch_id(&h[2]), Err(EpochError::MissingBlock(h[2])));

    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    // test2 staked in epoch 1 and therefore should be included in epoch 3.
    let epoch2 = epoch_manager.get_epoch_id(&h[2]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch2).unwrap(), &expected0));

    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

    let expected3 = epoch_info_with_num_seats(
        2,
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)],
        vec![0, 1],
        vec![vec![0, 1]],
        vec![],
        vec![],
        change_stake(vec![
            ("test1".parse().unwrap(), amount_staked),
            ("test2".parse().unwrap(), amount_staked),
        ]),
        vec![],
        // only the validator who produced the block in this epoch gets the reward since epoch length is 1
        reward(vec![("test1".parse().unwrap(), 0), ("near".parse().unwrap(), 0)]),
        0,
        4,
    );
    // no validator change in the last epoch
    let epoch3 = epoch_manager.get_epoch_id(&h[3]).unwrap();
    assert!(compare_epoch_infos(&epoch_manager.get_epoch_info(&epoch3).unwrap(), &expected3));

    // Start another epoch manager from the same store to check that it saved the state.
    let epoch_manager2 = EpochManager::new(
        epoch_manager.store.clone(),
        epoch_manager.config.clone(),
        PROTOCOL_VERSION,
        epoch_manager.reward_calculator,
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap();
    assert!(compare_epoch_infos(&epoch_manager2.get_epoch_info(&epoch3).unwrap(), &expected3));
}

#[test]
fn test_validator_change_of_stake() {
    let amount_staked = 1_000_000;
    let fishermen_threshold = 100;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_epoch_manager(
        validators,
        2,
        1,
        2,
        0,
        90,
        60,
        fishermen_threshold,
        default_reward_calculator(),
    );

    let h = hash_range(4);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 10)]);
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    // New epoch starts here.
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), 0), ("test2".parse().unwrap(), amount_staked)],
    );
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );
    matches!(
        epoch_info.validator_kickout().get("test1"),
        Some(ValidatorKickoutReason::NotEnoughStake { stake: 10, .. })
    );
}

/// Test handling forks across the epoch finalization.
/// Fork with where one BP produces blocks in one chain and 2 BPs are in another chain.
///     |   | /--1---4------|--7---10------|---13---
///   x-|-0-|-
///     |   | \--2---3---5--|---6---8---9--|----11---12--
/// In upper fork, only test2 left + new validator test4.
/// In lower fork, test1 and test3 are left.
#[test]
fn test_fork_finalization() {
    let amount_staked = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), amount_staked),
        ("test2".parse().unwrap(), amount_staked),
        ("test3".parse().unwrap(), amount_staked),
    ];
    let epoch_length = 20;
    let mut epoch_manager =
        setup_default_epoch_manager(validators.clone(), epoch_length, 1, 3, 0, 90, 60);

    let h = hash_range((5 * epoch_length - 1) as usize);
    // Have an alternate set of hashes to use on the other branch to avoid collisions.
    let h2: Vec<CryptoHash> = h.iter().map(|x| hash(x.as_ref())).collect();

    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

    let build_branch = |epoch_manager: &mut EpochManager,
                        base_block: CryptoHash,
                        hashes: &[CryptoHash],
                        validator_accounts: &[&str]|
     -> Vec<CryptoHash> {
        let mut prev_block = base_block;
        let mut branch_blocks = Vec::new();
        for (i, curr_block) in hashes.iter().enumerate().skip(2) {
            let height = i as u64;
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
            let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
            let block_producer_id = EpochManager::block_producer_from_info(&epoch_info, height);
            let block_producer = epoch_info.get_validator(block_producer_id);
            let account_id = block_producer.account_id();
            if validator_accounts.iter().any(|v| *v == account_id.as_ref()) {
                record_block(epoch_manager, prev_block, *curr_block, height, vec![]);
                prev_block = *curr_block;
                branch_blocks.push(*curr_block);
            }
        }
        branch_blocks
    };

    // build test2/test4 fork
    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![stake("test4".parse().unwrap(), amount_staked)],
    );
    let blocks_test2 = build_branch(&mut epoch_manager, h[1], &h, &["test2", "test4"]);

    // build test1/test3 fork
    let blocks_test1 = build_branch(&mut epoch_manager, h[0], &h2, &["test1", "test3"]);

    let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    let mut bps = epoch_manager
        .get_all_block_producers_ordered(&epoch1, &h[1])
        .unwrap()
        .iter()
        .map(|x| (x.0.account_id().clone(), x.1))
        .collect::<Vec<_>>();
    bps.sort_unstable();
    assert_eq!(
        bps,
        vec![
            ("test1".parse().unwrap(), false),
            ("test2".parse().unwrap(), false),
            ("test3".parse().unwrap(), false)
        ]
    );

    let last_block = blocks_test2.last().unwrap();
    let epoch2_1 = epoch_manager.get_epoch_id(last_block).unwrap();
    assert_eq!(
        epoch_manager
            .get_all_block_producers_ordered(&epoch2_1, &h[1])
            .unwrap()
            .iter()
            .map(|x| (x.0.account_id().clone(), x.1))
            .collect::<Vec<_>>(),
        vec![("test2".parse().unwrap(), false), ("test4".parse().unwrap(), false)]
    );

    let last_block = blocks_test1.last().unwrap();
    let epoch2_2 = epoch_manager.get_epoch_id(last_block).unwrap();
    assert_eq!(
        epoch_manager
            .get_all_block_producers_ordered(&epoch2_2, &h[1])
            .unwrap()
            .iter()
            .map(|x| (x.0.account_id().clone(), x.1))
            .collect::<Vec<_>>(),
        vec![("test1".parse().unwrap(), false), ("test3".parse().unwrap(), false),]
    );

    // Check that if we have a different epoch manager and apply only second branch we get the same results.
    let mut epoch_manager2 = setup_default_epoch_manager(validators, epoch_length, 1, 3, 0, 90, 60);
    record_block(&mut epoch_manager2, CryptoHash::default(), h[0], 0, vec![]);
    build_branch(&mut epoch_manager2, h[0], &h2, &["test1", "test3"]);
    assert_eq!(epoch_manager.get_epoch_info(&epoch2_2), epoch_manager2.get_epoch_info(&epoch2_2));
}

/// In the case where there is only one validator and the
/// number of blocks produced by the validator is under the
/// threshold for some given epoch, the validator should not
/// be kicked out
#[test]
fn test_one_validator_kickout() {
    let amount_staked = 1_000;
    let mut epoch_manager = setup_default_epoch_manager(
        vec![("test1".parse().unwrap(), amount_staked)],
        2,
        1,
        1,
        0,
        90,
        60,
    );

    let h = hash_range(6);
    // this validator only produces one block every epoch whereas they should have produced 2. However, since
    // this is the only validator left, we still keep them as validator.
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test1", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_kickout(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test1".parse().unwrap(), amount_staked)]);
}

/// When computing validator kickout, we should not kickout validators such that the union
/// of kickout for this epoch and last epoch equals the entire validator set.
#[test]
fn test_validator_kickout() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let epoch_length = 10;
    let mut epoch_manager = setup_default_epoch_manager(validators, epoch_length, 1, 2, 0, 90, 60);
    let h = hash_range((3 * epoch_length) as usize);

    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let mut prev_block = h[0];
    let mut test2_expected_blocks = 0;
    let init_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in h.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let block_producer = epoch_manager.get_block_producer_info(&epoch_id, height).unwrap();
        if block_producer.account_id().as_ref() == "test2" && epoch_id == init_epoch_id {
            // test2 skips its blocks in the first epoch
            test2_expected_blocks += 1;
        } else if block_producer.account_id().as_ref() == "test1" && epoch_id != init_epoch_id {
            // test1 skips its blocks in subsequent epochs
            ()
        } else {
            record_block(&mut epoch_manager, prev_block, *curr_block, height, vec![]);
            prev_block = *curr_block;
        }
    }
    let epoch_infos: Vec<_> =
        h.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).collect();
    check_kickout(
        &epoch_infos[1],
        &[(
            "test2",
            ValidatorKickoutReason::NotEnoughBlocks {
                produced: 0,
                expected: test2_expected_blocks,
            },
        )],
    );
    let epoch_info = &epoch_infos[2];
    check_validators(epoch_info, &[("test1", amount_staked)]);
    check_fishermen(epoch_info, &[]);
    check_stake_change(epoch_info, vec![("test1".parse().unwrap(), amount_staked)]);
    check_kickout(epoch_info, &[]);
    check_reward(
        epoch_info,
        vec![
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
            ("test1".parse().unwrap(), 0),
        ],
    );
}

#[test]
fn test_validator_unstake() {
    let store = create_test_store();
    let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, PROTOCOL_VERSION, default_reward_calculator(), validators)
            .unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), 0), ("test2".parse().unwrap(), amount_staked)],
    );
    check_kickout(&epoch_info, &[("test1", ValidatorKickoutReason::Unstaked)]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );

    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test2".parse().unwrap(), amount_staked)]);
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );

    record_block(&mut epoch_manager, h[5], h[6], 6, vec![]);
    record_block(&mut epoch_manager, h[6], h[7], 7, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[7]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(&epoch_info, vec![("test2".parse().unwrap(), amount_staked)]);
    check_kickout(&epoch_info, &[]);
    check_reward(&epoch_info, vec![("test2".parse().unwrap(), 0), ("near".parse().unwrap(), 0)]);
}

#[test]
fn test_slashing() {
    let store = create_test_store();
    let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, PROTOCOL_VERSION, default_reward_calculator(), validators)
            .unwrap();

    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

    // Slash test1
    let mut slashed = HashMap::new();
    slashed.insert("test1".parse::<AccountId>().unwrap(), SlashState::Other);
    record_block_with_slashes(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), false)],
    );

    let epoch_id = epoch_manager.get_epoch_id(&h[1]).unwrap();
    let mut bps = epoch_manager
        .get_all_block_producers_ordered(&epoch_id, &h[1])
        .unwrap()
        .iter()
        .map(|x| (x.0.account_id().clone(), x.1))
        .collect::<Vec<_>>();
    bps.sort_unstable();
    assert_eq!(bps, vec![("test1".parse().unwrap(), true), ("test2".parse().unwrap(), false)]);

    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    // Epoch 3 -> defined by proposals/slashes in h[1].
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);

    let epoch_id = epoch_manager.get_epoch_id(&h[5]).unwrap();
    assert_eq!(epoch_id.0, h[2]);
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), 0), ("test2".parse().unwrap(), amount_staked)],
    );
    check_kickout(&epoch_info, &[("test1", ValidatorKickoutReason::Slashed)]);

    let slashed1: Vec<_> =
        epoch_manager.get_block_info(&h[2]).unwrap().slashed().clone().into_iter().collect();
    let slashed2: Vec<_> =
        epoch_manager.get_block_info(&h[3]).unwrap().slashed().clone().into_iter().collect();
    let slashed3: Vec<_> =
        epoch_manager.get_block_info(&h[5]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed1, vec![("test1".parse().unwrap(), SlashState::Other)]);
    assert_eq!(slashed2, vec![("test1".parse().unwrap(), SlashState::AlreadySlashed)]);
    assert_eq!(slashed3, vec![("test1".parse().unwrap(), SlashState::AlreadySlashed)]);
}

/// Test that double sign interacts with other challenges in the correct way.
#[test]
fn test_double_sign_slashing1() {
    let store = create_test_store();
    let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, PROTOCOL_VERSION, default_reward_calculator(), validators)
            .unwrap();

    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![],
        vec![
            SlashedValidator::new("test1".parse().unwrap(), true),
            SlashedValidator::new("test1".parse().unwrap(), false),
        ],
    );
    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[2]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::Other)]);
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    // new epoch
    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[3]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::AlreadySlashed)]);
    // slash test1 for double sign
    record_block_with_slashes(
        &mut epoch_manager,
        h[3],
        h[4],
        4,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), true)],
    );

    // Epoch 3 -> defined by proposals/slashes in h[1].
    record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
    let epoch_id = epoch_manager.get_epoch_id(&h[5]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    assert_eq!(
        epoch_info
            .validators_iter()
            .map(|v| (v.account_id().clone(), v.stake()))
            .collect::<Vec<_>>(),
        vec![("test2".parse().unwrap(), amount_staked)],
    );
    assert_eq!(
        epoch_info.validator_kickout(),
        &[("test1".parse().unwrap(), ValidatorKickoutReason::Slashed)]
            .into_iter()
            .collect::<HashMap<_, _>>()
    );
    assert_eq!(
        epoch_info.stake_change(),
        &change_stake(vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), amount_staked)
        ]),
    );

    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[5]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::AlreadySlashed)]);
}

/// Test that two double sign challenge in two epochs works
#[test]
fn test_double_sign_slashing2() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);

    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), true)],
    );

    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[1]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::DoubleSign)]);

    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[2]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::DoubleSign)]);
    // new epoch
    record_block_with_slashes(
        &mut epoch_manager,
        h[2],
        h[3],
        3,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), true)],
    );
    let slashed: Vec<_> =
        epoch_manager.get_block_info(&h[3]).unwrap().slashed().clone().into_iter().collect();
    assert_eq!(slashed, vec![("test1".parse().unwrap(), SlashState::DoubleSign)]);
}

/// If all current validator try to unstake, we disallow that.
#[test]
fn test_all_validators_unstake() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // all validators are trying to unstake.
    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![
            stake("test1".parse().unwrap(), 0),
            stake("test2".parse().unwrap(), 0),
            stake("test3".parse().unwrap(), 0),
        ],
    );
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    let next_epoch = epoch_manager.get_next_epoch_id(&h[2]).unwrap();
    assert_eq!(
        epoch_manager.get_epoch_info(&next_epoch).unwrap().validators_iter().collect::<Vec<_>>(),
        vec![
            stake("test1".parse().unwrap(), stake_amount),
            stake("test2".parse().unwrap(), stake_amount),
            stake("test3".parse().unwrap(), stake_amount)
        ],
    );
}

#[test]
fn test_validator_reward_one_validator() {
    let stake_amount = 1_000_000;
    let test1_stake_amount = 110;
    let validators = vec![
        ("test1".parse().unwrap(), test1_stake_amount),
        ("test2".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 2;
    let total_supply = validators.iter().map(|(_, stake)| stake).sum();
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 50,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        num_seconds_per_year: 50,
    };
    let mut epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        1,
        0,
        90,
        60,
        100,
        reward_calculator.clone(),
    );
    let rng_seed = [0; 32];
    let h = hash_range(5);

    epoch_manager
        .record_block_info(
            block_info(
                h[0],
                0,
                0,
                Default::default(),
                Default::default(),
                h[0],
                vec![true],
                total_supply,
            ),
            rng_seed,
        )
        .unwrap();
    epoch_manager
        .record_block_info(
            block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
            rng_seed,
        )
        .unwrap();
    epoch_manager
        .record_block_info(
            block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
            rng_seed,
        )
        .unwrap();
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ValidatorStats { produced: 1, expected: 1 },
        },
    );
    let mut validator_stakes = HashMap::new();
    validator_stakes.insert("test2".parse().unwrap(), stake_amount);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validator_stakes,
        total_supply,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
    );
    let test2_reward = *validator_reward.get("test2").unwrap();
    let protocol_reward = *validator_reward.get("near").unwrap();

    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(&epoch_info, &[("test2", stake_amount + test2_reward)]);
    check_fishermen(&epoch_info, &[("test1", test1_stake_amount)]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), test1_stake_amount),
            ("test2".parse().unwrap(), stake_amount + test2_reward),
        ],
    );
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![("test2".parse().unwrap(), test2_reward), ("near".parse().unwrap(), protocol_reward)],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_validator_reward_weight_by_stake() {
    let stake_amount1 = 1_000_000;
    let stake_amount2 = 500_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount1), ("test2".parse().unwrap(), stake_amount2)];
    let epoch_length = 2;
    let total_supply = (stake_amount1 + stake_amount2) * validators.len() as u128;
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 50,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        num_seconds_per_year: 50,
    };
    let mut epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        90,
        60,
        100,
        reward_calculator.clone(),
    );
    let h = hash_range(5);
    record_with_block_info(
        &mut epoch_manager,
        block_info(
            h[0],
            0,
            0,
            Default::default(),
            Default::default(),
            h[0],
            vec![true],
            total_supply,
        ),
    );
    record_with_block_info(
        &mut epoch_manager,
        block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
    );
    record_with_block_info(
        &mut epoch_manager,
        block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
    );
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test1".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ValidatorStats { produced: 1, expected: 1 },
        },
    );
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ValidatorStats { produced: 1, expected: 1 },
        },
    );
    let mut validators_stakes = HashMap::new();
    validators_stakes.insert("test1".parse().unwrap(), stake_amount1);
    validators_stakes.insert("test2".parse().unwrap(), stake_amount2);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validators_stakes,
        total_supply,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
    );
    let test1_reward = *validator_reward.get("test1").unwrap();
    let test2_reward = *validator_reward.get("test2").unwrap();
    assert_eq!(test1_reward, test2_reward * 2);
    let protocol_reward = *validator_reward.get("near").unwrap();

    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(
        &epoch_info,
        &[("test1", stake_amount1 + test1_reward), ("test2", stake_amount2 + test2_reward)],
    );
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount1 + test1_reward),
            ("test2".parse().unwrap(), stake_amount2 + test2_reward),
        ],
    );
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), test1_reward),
            ("test2".parse().unwrap(), test2_reward),
            ("near".parse().unwrap(), protocol_reward),
        ],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_reward_multiple_shards() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let reward_calculator = RewardCalculator {
        max_inflation_rate: Ratio::new(5, 100),
        num_blocks_per_year: 1_000_000,
        epoch_length,
        protocol_reward_rate: Ratio::new(1, 10),
        protocol_treasury_account: "near".parse().unwrap(),
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        num_seconds_per_year: 1_000_000,
    };
    let num_shards = 2;
    let mut epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        num_shards,
        2,
        0,
        90,
        60,
        0,
        reward_calculator.clone(),
    );
    let h = hash_range((2 * epoch_length + 1) as usize);
    record_with_block_info(
        &mut epoch_manager,
        block_info(
            h[0],
            0,
            0,
            Default::default(),
            Default::default(),
            h[0],
            vec![true],
            total_supply,
        ),
    );
    let mut expected_chunks = 0;
    let init_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&h[0]).unwrap();
    for height in 1..(2 * epoch_length) {
        let i = height as usize;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&h[i - 1]).unwrap();
        // test1 skips its chunks in the first epoch
        let chunk_mask = (0..num_shards)
            .map(|shard_index| {
                let expected_chunk_producer = epoch_manager
                    .get_chunk_producer_info(&epoch_id, height, shard_index as u64)
                    .unwrap();
                if expected_chunk_producer.account_id().as_ref() == "test1"
                    && epoch_id == init_epoch_id
                {
                    expected_chunks += 1;
                    false
                } else {
                    true
                }
            })
            .collect();
        record_with_block_info(
            &mut epoch_manager,
            block_info(h[i], height, height, h[i - 1], h[i - 1], h[i], chunk_mask, total_supply),
        );
    }
    let mut validator_online_ratio = HashMap::new();
    validator_online_ratio.insert(
        "test2".parse().unwrap(),
        BlockChunkValidatorStats {
            block_stats: ValidatorStats { produced: 1, expected: 1 },
            chunk_stats: ValidatorStats { produced: 1, expected: 1 },
        },
    );
    let mut validators_stakes = HashMap::new();
    validators_stakes.insert("test1".parse().unwrap(), stake_amount);
    validators_stakes.insert("test2".parse().unwrap(), stake_amount);
    let (validator_reward, inflation) = reward_calculator.calculate_reward(
        validator_online_ratio,
        &validators_stakes,
        total_supply,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        epoch_length * NUM_NS_IN_SECOND,
    );
    let test2_reward = *validator_reward.get("test2").unwrap();
    let protocol_reward = *validator_reward.get("near").unwrap();
    let epoch_infos: Vec<_> =
        h.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).collect();
    let epoch_info = &epoch_infos[1];
    check_validators(epoch_info, &[("test2", stake_amount + test2_reward)]);
    check_fishermen(epoch_info, &[]);
    check_stake_change(
        epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), stake_amount + test2_reward),
        ],
    );
    check_kickout(
        epoch_info,
        &[(
            "test1",
            ValidatorKickoutReason::NotEnoughChunks { produced: 0, expected: expected_chunks },
        )],
    );
    check_reward(
        epoch_info,
        vec![("test2".parse().unwrap(), test2_reward), ("near".parse().unwrap(), protocol_reward)],
    );
    assert_eq!(epoch_info.minted_amount(), inflation);
}

#[test]
fn test_unstake_and_then_change_stake() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![stake("test1".parse().unwrap(), amount_staked)],
    );
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    assert_eq!(epoch_id, EpochId(h[2]));
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    check_validators(&epoch_info, &[("test1", amount_staked), ("test2", amount_staked)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)],
    );
    check_kickout(&epoch_info, &[]);
    check_reward(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), 0),
            ("near".parse().unwrap(), 0),
        ],
    );
}

/// When a block producer fails to produce a block, check that other chunk producers who produce
/// chunks for that block are not kicked out because of it.
#[test]
fn test_expected_chunks() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 20;
    let total_supply = stake_amount * validators.len() as u128;
    let mut epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        3,
        3,
        0,
        90,
        60,
        0,
        default_reward_calculator(),
    );
    let rng_seed = [0; 32];
    let hashes = hash_range((2 * epoch_length) as usize);
    record_block(&mut epoch_manager, Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    let mut prev_block = hashes[0];
    let initial_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let block_producer = EpochManager::block_producer_from_info(&epoch_info, height);
        // test1 does not produce blocks during first epoch
        if block_producer == 0 && epoch_id == initial_epoch_id {
            expected += 1;
        } else {
            epoch_manager
                .record_block_info(
                    block_info(
                        *curr_block,
                        height,
                        height,
                        prev_block,
                        prev_block,
                        epoch_id.0,
                        vec![true, true, true],
                        total_supply,
                    ),
                    rng_seed,
                )
                .unwrap()
                .commit()
                .unwrap();
            prev_block = *curr_block;
        }
        if epoch_id != initial_epoch_id {
            break;
        }
    }
    let epoch_info = hashes
        .iter()
        .filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok())
        .last()
        .unwrap();
    assert_eq!(
        epoch_info.validator_kickout(),
        &[(
            "test1".parse::<AccountId>().unwrap(),
            ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected }
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
    );
}

#[test]
fn test_expected_chunks_prev_block_not_produced() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let epoch_length = 50;
    let total_supply = stake_amount * validators.len() as u128;
    let mut epoch_manager = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        3,
        0,
        90,
        90,
        0,
        default_reward_calculator(),
    );
    let rng_seed = [0; 32];
    let hashes = hash_range((2 * epoch_length) as usize);
    record_block(&mut epoch_manager, Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    let mut prev_block = hashes[0];
    let initial_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
    for (i, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = i as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let block_producer = EpochManager::block_producer_from_info(&epoch_info, height);
        let prev_block_info = epoch_manager.get_block_info(&prev_block).unwrap();
        let prev_height = prev_block_info.height();
        let expected_chunk_producer =
            EpochManager::chunk_producer_from_info(&epoch_info, prev_height + 1, 0);
        // test1 does not produce blocks during first epoch
        if block_producer == 0 && epoch_id == initial_epoch_id {
            expected += 1;
        } else {
            // test1 also misses all their chunks
            let should_produce_chunk = expected_chunk_producer != 0;
            epoch_manager
                .record_block_info(
                    block_info(
                        *curr_block,
                        height,
                        height,
                        prev_block,
                        prev_block,
                        epoch_id.0,
                        vec![should_produce_chunk],
                        total_supply,
                    ),
                    rng_seed,
                )
                .unwrap()
                .commit()
                .unwrap();
            prev_block = *curr_block;
        }
        if epoch_id != initial_epoch_id {
            break;
        }
    }
    let epoch_info = hashes
        .iter()
        .filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok())
        .last()
        .unwrap();
    assert_eq!(
        epoch_info.validator_kickout(),
        &[(
            "test1".parse().unwrap(),
            ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected }
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
    );
}

fn update_tracker(
    epoch_info: &EpochInfo,
    heights: std::ops::Range<BlockHeight>,
    produced_heights: &[BlockHeight],
    tracker: &mut HashMap<ValidatorId, ValidatorStats>,
) {
    for h in heights {
        let block_producer = EpochManager::block_producer_from_info(epoch_info, h);
        let entry =
            tracker.entry(block_producer).or_insert(ValidatorStats { produced: 0, expected: 0 });
        if produced_heights.contains(&h) {
            entry.produced += 1;
        }
        entry.expected += 1;
    }
}

#[test]
fn test_epoch_info_aggregator() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 5;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        10,
        10,
        0,
        default_reward_calculator(),
    );
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
    record_block_with_final_block_hash(&mut em, h[1], h[3], h[0], 3, vec![]);
    assert_eq!(h[0], em.epoch_info_aggregator.last_block_hash);
    let epoch_id = em.get_epoch_id(&h[3]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();

    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..4, &[1, 3], &mut tracker);

    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[3]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    // get_epoch_info_aggregator_upto_last does not change
    // epoch_info_aggregator
    assert_eq!(h[0], em.epoch_info_aggregator.last_block_hash);

    record_block_with_final_block_hash(&mut em, h[3], h[5], h[1], 5, vec![]);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);

    update_tracker(&epoch_info, 4..6, &[5], &mut tracker);

    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);
}

/// If the node stops and restarts, the aggregator should be able to recover
#[test]
fn test_epoch_info_aggregator_data_loss() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 5;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        10,
        10,
        0,
        default_reward_calculator(),
    );
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block(&mut em, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), stake_amount - 10)]);
    record_block(&mut em, h[1], h[3], 3, vec![stake("test2".parse().unwrap(), stake_amount + 10)]);
    assert_eq!(h[1], em.epoch_info_aggregator.last_block_hash);
    em.epoch_info_aggregator = EpochInfoAggregator::default();
    record_block(&mut em, h[3], h[5], 5, vec![stake("test1".parse().unwrap(), stake_amount - 1)]);
    assert_eq!(h[3], em.epoch_info_aggregator.last_block_hash);
    let epoch_id = em.get_epoch_id(&h[5]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..6, &[1, 3, 5], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert_eq!(
        aggregator.all_proposals,
        vec![
            stake("test1".parse().unwrap(), stake_amount - 1),
            stake("test2".parse().unwrap(), stake_amount + 10)
        ]
        .into_iter()
        .map(|p| (p.account_id().clone(), p))
        .collect::<BTreeMap<_, _>>()
    );
}

/// Aggregator should still work even if there is a reorg past the last final block.
#[test]
fn test_epoch_info_aggregator_reorg_past_final_block() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 6;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        10,
        10,
        0,
        default_reward_calculator(),
    );
    let h = hash_range(6);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
    record_block_with_final_block_hash(&mut em, h[1], h[2], h[0], 2, vec![]);
    record_block_with_final_block_hash(
        &mut em,
        h[2],
        h[3],
        h[1],
        3,
        vec![stake("test1".parse().unwrap(), stake_amount - 1)],
    );
    record_block_with_final_block_hash(&mut em, h[3], h[4], h[3], 4, vec![]);
    record_block_with_final_block_hash(&mut em, h[2], h[5], h[1], 5, vec![]);
    let epoch_id = em.get_epoch_id(&h[5]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 1..6, &[1, 2, 5], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[5]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert!(aggregator.all_proposals.is_empty());
}

#[test]
fn test_epoch_info_aggregator_reorg_beginning_of_epoch() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 4;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        10,
        10,
        0,
        default_reward_calculator(),
    );
    let h = hash_range(10);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    for i in 1..5 {
        record_block(&mut em, h[i - 1], h[i], i as u64, vec![]);
    }
    record_block(&mut em, h[4], h[5], 5, vec![stake("test1".parse().unwrap(), stake_amount - 1)]);
    record_block_with_final_block_hash(
        &mut em,
        h[5],
        h[6],
        h[4],
        6,
        vec![stake("test2".parse().unwrap(), stake_amount - 100)],
    );
    // reorg
    record_block(&mut em, h[4], h[7], 7, vec![]);
    let epoch_id = em.get_epoch_id(&h[7]).unwrap();
    let epoch_info = em.get_epoch_info(&epoch_id).unwrap();
    let mut tracker = HashMap::new();
    update_tracker(&epoch_info, 5..8, &[7], &mut tracker);
    let aggregator = em.get_epoch_info_aggregator_upto_last(&h[7]).unwrap();
    assert_eq!(aggregator.block_tracker, tracker);
    assert!(aggregator.all_proposals.is_empty());
}

fn count_missing_blocks(
    epoch_manager: &mut EpochManager,
    epoch_id: &EpochId,
    height_range: std::ops::Range<u64>,
    produced_heights: &[u64],
    validator: &str,
) -> ValidatorStats {
    let mut result = ValidatorStats { produced: 0, expected: 0 };
    for h in height_range {
        let block_producer = epoch_manager.get_block_producer_info(epoch_id, h).unwrap();
        if validator == block_producer.account_id().as_ref() {
            if produced_heights.contains(&h) {
                result.produced += 1;
            }
            result.expected += 1;
        }
    }
    result
}

#[test]
fn test_num_missing_blocks() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 2;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        2,
        0,
        10,
        10,
        0,
        default_reward_calculator(),
    );
    let h = hash_range(8);
    record_block(&mut em, Default::default(), h[0], 0, vec![]);
    record_block(&mut em, h[0], h[1], 1, vec![]);
    record_block(&mut em, h[1], h[3], 3, vec![]);
    let epoch_id = em.get_epoch_id(&h[1]).unwrap();
    assert_eq!(
        em.get_num_validator_blocks(&epoch_id, &h[3], &"test1".parse().unwrap()).unwrap(),
        count_missing_blocks(&mut em, &epoch_id, 1..4, &[1, 3], "test1"),
    );
    assert_eq!(
        em.get_num_validator_blocks(&epoch_id, &h[3], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&mut em, &epoch_id, 1..4, &[1, 3], "test2"),
    );

    // Build chain 0 <- x <- x <- x <- ( 4 <- 5 ) <- x <- 7
    record_block(&mut em, h[0], h[4], 4, vec![]);
    let epoch_id = em.get_epoch_id(&h[4]).unwrap();
    // Block 4 is first block after genesis and starts new epoch, but we actually count how many missed blocks have happened since block 0.
    assert_eq!(
        em.get_num_validator_blocks(&epoch_id, &h[4], &"test1".parse().unwrap()).unwrap(),
        count_missing_blocks(&mut em, &epoch_id, 1..5, &[4], "test1"),
    );
    assert_eq!(
        em.get_num_validator_blocks(&epoch_id, &h[4], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&mut em, &epoch_id, 1..5, &[4], "test2"),
    );
    record_block(&mut em, h[4], h[5], 5, vec![]);
    record_block(&mut em, h[5], h[7], 7, vec![]);
    let epoch_id = em.get_epoch_id(&h[7]).unwrap();
    // The next epoch started after 5 with 6, and test2 missed their slot from perspective of block 7.
    assert_eq!(
        em.get_num_validator_blocks(&epoch_id, &h[7], &"test2".parse().unwrap()).unwrap(),
        count_missing_blocks(&mut em, &epoch_id, 6..8, &[7], "test2"),
    );
}

/// Test when blocks are all produced, validators can be kicked out because of not producing
/// enough chunks
#[test]
fn test_chunk_validator_kickout() {
    let stake_amount = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let epoch_length = 10;
    let total_supply = stake_amount * validators.len() as u128;
    let mut em = setup_epoch_manager(
        validators,
        epoch_length,
        4,
        2,
        0,
        90,
        70,
        0,
        default_reward_calculator(),
    );
    let rng_seed = [0; 32];
    let hashes = hash_range((epoch_length + 2) as usize);
    record_block(&mut em, Default::default(), hashes[0], 0, vec![]);
    let mut expected = 0;
    for (prev_block, (height, curr_block)) in hashes.iter().zip(hashes.iter().enumerate().skip(1)) {
        let height = height as u64;
        let epoch_id = em.get_epoch_id_from_prev_block(prev_block).unwrap();
        let epoch_info = em.get_epoch_info(&epoch_id).unwrap().clone();
        if height < epoch_length {
            let chunk_mask = (0..4)
                .map(|shard_id| {
                    let chunk_producer = EpochManager::chunk_producer_from_info(
                        &epoch_info,
                        height,
                        shard_id as u64,
                    );
                    // test1 skips chunks
                    if chunk_producer == 0 {
                        expected += 1;
                        false
                    } else {
                        true
                    }
                })
                .collect();
            em.record_block_info(
                block_info(
                    *curr_block,
                    height,
                    height - 1,
                    *prev_block,
                    *prev_block,
                    epoch_id.0,
                    chunk_mask,
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
        } else {
            em.record_block_info(
                block_info(
                    *curr_block,
                    height,
                    height - 1,
                    *prev_block,
                    *prev_block,
                    epoch_id.0,
                    vec![true, true, true, true],
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
        }
    }

    let last_epoch_info = hashes.iter().filter_map(|x| em.get_epoch_info(&EpochId(*x)).ok()).last();
    assert_eq!(
        last_epoch_info.unwrap().validator_kickout(),
        &[(
            "test1".parse().unwrap(),
            ValidatorKickoutReason::NotEnoughChunks { produced: 0, expected }
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    );
}

#[test]
fn test_compare_epoch_id() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![stake("test1".parse().unwrap(), amount_staked)],
    );
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    let epoch_id0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
    let epoch_id1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
    let epoch_id2 = epoch_manager.get_next_epoch_id(&h[1]).unwrap();
    let epoch_id3 = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id0, &epoch_id1), Ok(Ordering::Equal));
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id2, &epoch_id3), Ok(Ordering::Less));
    assert_eq!(epoch_manager.compare_epoch_id(&epoch_id3, &epoch_id1), Ok(Ordering::Greater));
    let random_epoch_id = EpochId(hash(&[100]));
    assert!(epoch_manager.compare_epoch_id(&epoch_id3, &random_epoch_id).is_err());
}

#[test]
fn test_fishermen() {
    let stake_amount = 1_000_000;
    let fishermen_threshold = 100;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), fishermen_threshold),
        ("test4".parse().unwrap(), fishermen_threshold / 2),
    ];
    let epoch_length = 4;
    let em = setup_epoch_manager(
        validators,
        epoch_length,
        1,
        4,
        0,
        90,
        70,
        fishermen_threshold,
        default_reward_calculator(),
    );
    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount), ("test2", stake_amount)]);
    check_fishermen(&epoch_info, &[("test3", fishermen_threshold)]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), stake_amount),
            ("test3".parse().unwrap(), fishermen_threshold),
            ("test4".parse().unwrap(), 0),
        ],
    );
    check_kickout(&epoch_info, &[]);
}

#[test]
fn test_fishermen_unstake() {
    let stake_amount = 1_000_000;
    let fishermen_threshold = 100;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), fishermen_threshold),
        ("test3".parse().unwrap(), fishermen_threshold),
    ];
    let mut em = setup_epoch_manager(
        validators,
        2,
        1,
        1,
        0,
        90,
        70,
        fishermen_threshold,
        default_reward_calculator(),
    );
    let h = hash_range(5);
    record_block(&mut em, CryptoHash::default(), h[0], 0, vec![]);
    // fishermen unstake
    record_block(&mut em, h[0], h[1], 1, vec![stake("test2".parse().unwrap(), 0)]);
    record_block(&mut em, h[1], h[2], 2, vec![stake("test3".parse().unwrap(), 1)]);

    let epoch_info = em.get_epoch_info(&EpochId(h[2])).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount)]);
    check_fishermen(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), 0),
            ("test3".parse().unwrap(), 0),
        ],
    );
    let kickout = epoch_info.validator_kickout();
    assert_eq!(kickout.get("test2").unwrap(), &ValidatorKickoutReason::Unstaked);
    matches!(kickout.get("test3"), Some(ValidatorKickoutReason::NotEnoughStake { .. }));
}

#[test]
fn test_validator_consistency() {
    let stake_amount = 1_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 1, 0, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = epoch_manager.get_epoch_id(&h[0]).unwrap();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let mut actual_block_producers = HashSet::new();
    for index in epoch_info.block_producers_settlement().into_iter() {
        let bp = epoch_info.validator_account_id(*index).clone();
        actual_block_producers.insert(bp);
    }
    for index in epoch_info.chunk_producers_settlement().into_iter().flatten() {
        let bp = epoch_info.validator_account_id(*index).clone();
        actual_block_producers.insert(bp);
    }
    for bp in actual_block_producers {
        assert!(epoch_info.account_is_validator(&bp))
    }
}

/// Test that when epoch length is larger than the cache size of block info cache, there is
/// no unexpected error.
#[test]
fn test_finalize_epoch_large_epoch_length() {
    let stake_amount = 1_000;
    let validators =
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)];
    let mut epoch_manager =
        setup_default_epoch_manager(validators, (BLOCK_CACHE_SIZE + 1) as u64, 1, 2, 0, 90, 60);
    let h = hash_range(BLOCK_CACHE_SIZE + 2);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..=(BLOCK_CACHE_SIZE + 1) {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[BLOCK_CACHE_SIZE + 1])).unwrap();
    assert_eq!(
        epoch_info.validators_iter().map(|v| v.account_and_stake()).collect::<Vec<_>>(),
        vec![("test1".parse().unwrap(), stake_amount), ("test2".parse().unwrap(), stake_amount)],
    );
    assert_eq!(
        epoch_info.stake_change(),
        &change_stake(vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), stake_amount)
        ]),
    );
    assert_eq!(
        BLOCK_CACHE_SIZE + 2,
        epoch_manager.epoch_info_aggregator_loop_counter.load(std::sync::atomic::Ordering::SeqCst),
        "Expected every block to be visited exactly once"
    );
}

#[test]
fn test_kickout_set() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), 0),
        ("test3".parse().unwrap(), 10),
    ];
    // have two seats to that 500 would be the threshold
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![stake("test2".parse().unwrap(), stake_amount)],
    );
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test2".parse().unwrap(), 0)]);
    let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    assert_eq!(
        epoch_info1.validators_iter().map(|r| r.account_id().clone()).collect::<Vec<_>>(),
        vec!["test1".parse().unwrap()]
    );
    assert_eq!(
        epoch_info1.stake_change().clone(),
        change_stake(vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), 0),
            ("test3".parse().unwrap(), 10)
        ])
    );
    assert!(epoch_info1.validator_kickout().is_empty());
    record_block(
        &mut epoch_manager,
        h[2],
        h[3],
        3,
        vec![stake("test2".parse().unwrap(), stake_amount)],
    );
    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    let epoch_info = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    check_validators(&epoch_info, &[("test1", stake_amount), ("test2", stake_amount)]);
    check_fishermen(&epoch_info, &[("test3", 10)]);
    check_kickout(&epoch_info, &[]);
    check_stake_change(
        &epoch_info,
        vec![
            ("test1".parse().unwrap(), stake_amount),
            ("test2".parse().unwrap(), stake_amount),
            ("test3".parse().unwrap(), 10),
        ],
    );
}

#[test]
fn test_epoch_height_increase() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(5);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[2], 2, vec![stake("test1".parse().unwrap(), 223)]);
    record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);

    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    assert_ne!(epoch_info2.epoch_height(), epoch_info3.epoch_height());
}

#[test]
/// Slashed after unstaking: slashed for 2 epochs
fn test_unstake_slash() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(9);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), false)],
    );
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    record_block(
        &mut epoch_manager,
        h[3],
        h[4],
        4,
        vec![stake("test1".parse().unwrap(), stake_amount)],
    );

    let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap();
    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap();
    let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    assert_eq!(
        epoch_info1.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Unstaked)
    );
    assert_eq!(
        epoch_info2.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert_eq!(
        epoch_info3.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert!(epoch_info4.validator_kickout().is_empty());
    assert!(epoch_info4.account_is_validator(&"test1".parse().unwrap()));
}

#[test]
/// Slashed with no unstake in previous epoch: slashed for 3 epochs
fn test_no_unstake_slash() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(9);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), false)],
    );
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    record_block(
        &mut epoch_manager,
        h[3],
        h[4],
        4,
        vec![stake("test1".parse().unwrap(), stake_amount)],
    );

    let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap();
    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap();
    let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    assert_eq!(
        epoch_info1.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert_eq!(
        epoch_info2.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert_eq!(
        epoch_info3.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert!(epoch_info4.validator_kickout().is_empty());
    assert!(epoch_info4.account_is_validator(&"test1".parse().unwrap()));
}

#[test]
/// Slashed right after validator rotated out
fn test_slash_non_validator() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(9);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 0)]);
    record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[2],
        h[3],
        3,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), false)],
    );
    record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
    record_block(
        &mut epoch_manager,
        h[4],
        h[5],
        5,
        vec![stake("test1".parse().unwrap(), stake_amount)],
    );

    let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap(); // Unstaked
    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(); // -
    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap(); // Slashed
    let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap(); // Slashed
    let epoch_info5 = epoch_manager.get_epoch_info(&EpochId(h[5])).unwrap(); // Ok
    assert_eq!(
        epoch_info1.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Unstaked)
    );
    assert!(epoch_info2.validator_kickout().is_empty());
    assert_eq!(
        epoch_info3.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert_eq!(
        epoch_info4.validator_kickout().get("test1"),
        Some(&ValidatorKickoutReason::Slashed)
    );
    assert!(epoch_info5.validator_kickout().is_empty());
    assert!(epoch_info5.account_is_validator(&"test1".parse().unwrap()));
}

#[test]
/// Slashed and attempt to restake: proposal gets ignored
fn test_slash_restake() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(9);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block_with_slashes(
        &mut epoch_manager,
        h[0],
        h[1],
        1,
        vec![],
        vec![SlashedValidator::new("test1".parse().unwrap(), false)],
    );
    record_block(
        &mut epoch_manager,
        h[1],
        h[2],
        2,
        vec![stake("test1".parse().unwrap(), stake_amount)],
    );
    record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
    record_block(
        &mut epoch_manager,
        h[3],
        h[4],
        4,
        vec![stake("test1".parse().unwrap(), stake_amount)],
    );
    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
    assert!(epoch_info2.stake_change().get("test1").is_none());
    let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap();
    assert!(epoch_info4.stake_change().get("test1").is_some());
}

#[test]
fn test_all_kickout_edge_case() {
    let stake_amount = 1_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    const EPOCH_LENGTH: u64 = 10;
    let mut epoch_manager = setup_default_epoch_manager(validators, EPOCH_LENGTH, 1, 3, 0, 90, 60);
    let hashes = hash_range((8 * EPOCH_LENGTH + 1) as usize);

    record_block(&mut epoch_manager, CryptoHash::default(), hashes[0], 0, vec![]);
    let mut prev_block = hashes[0];
    for (height, curr_block) in hashes.iter().enumerate().skip(1) {
        let height = height as u64;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap().clone();
        let block_producer = EpochManager::block_producer_from_info(&epoch_info, height);
        let block_producer = epoch_info.validator_account_id(block_producer);
        if height < EPOCH_LENGTH {
            // kickout test2 during first epoch
            if block_producer.as_ref() == "test1" || block_producer.as_ref() == "test3" {
                record_block(&mut epoch_manager, prev_block, *curr_block, height, Vec::new());
                prev_block = *curr_block;
            }
        } else if height < 2 * EPOCH_LENGTH {
            // produce blocks as normal during the second epoch
            record_block(&mut epoch_manager, prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        } else if height < 5 * EPOCH_LENGTH {
            // no one produces blocks during epochs 3, 4, 5
            // (but only 2 get kicked out because we can't kickout all)
            ()
        } else if height < 6 * EPOCH_LENGTH {
            // produce blocks normally during epoch 6
            record_block(&mut epoch_manager, prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        } else if height < 7 * EPOCH_LENGTH {
            // the validator which was not kicked out in epoch 6 stops producing blocks,
            // but cannot be kicked out now because they are the last validator
            if block_producer != epoch_info.validator_account_id(0) {
                record_block(&mut epoch_manager, prev_block, *curr_block, height, Vec::new());
                prev_block = *curr_block;
            }
        } else {
            // produce blocks normally again
            record_block(&mut epoch_manager, prev_block, *curr_block, height, Vec::new());
            prev_block = *curr_block;
        }
    }

    let last_epoch_info =
        hashes.iter().filter_map(|x| epoch_manager.get_epoch_info(&EpochId(*x)).ok()).last();
    assert_eq!(last_epoch_info.unwrap().validator_kickout(), &HashMap::default());
}

fn check_validators(epoch_info: &EpochInfo, expected_validators: &[(&str, u128)]) {
    for (v, (account_id, stake)) in
        epoch_info.validators_iter().zip(expected_validators.into_iter())
    {
        assert_eq!(v.account_id().as_ref(), *account_id);
        assert_eq!(v.stake(), *stake);
    }
}

fn check_fishermen(epoch_info: &EpochInfo, expected_fishermen: &[(&str, u128)]) {
    for (v, (account_id, stake)) in epoch_info.fishermen_iter().zip(expected_fishermen.into_iter())
    {
        assert_eq!(v.account_id().as_ref(), *account_id);
        assert_eq!(v.stake(), *stake);
    }
}

fn check_stake_change(epoch_info: &EpochInfo, changes: Vec<(AccountId, u128)>) {
    assert_eq!(epoch_info.stake_change(), &change_stake(changes));
}

fn check_reward(epoch_info: &EpochInfo, changes: Vec<(AccountId, u128)>) {
    assert_eq!(epoch_info.validator_reward(), &reward(changes));
}

fn check_kickout(epoch_info: &EpochInfo, reasons: &[(&str, ValidatorKickoutReason)]) {
    let kickout = reasons
        .into_iter()
        .map(|(account, reason)| (account.parse().unwrap(), reason.clone()))
        .collect::<HashMap<_, _>>();
    assert_eq!(epoch_info.validator_kickout(), &kickout);
}

#[test]
fn test_fisherman_kickout() {
    let stake_amount = 1_000_000;
    let validators = vec![
        ("test1".parse().unwrap(), stake_amount),
        ("test2".parse().unwrap(), stake_amount),
        ("test3".parse().unwrap(), stake_amount),
    ];
    let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
    let h = hash_range(6);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1".parse().unwrap(), 148)]);
    // test1 starts as validator,
    // - reduces stake in epoch T, will be fisherman in epoch T+2
    // - Misses a block in epoch T+1, will be kicked out in epoch T+3
    // - Finalize epoch T+1 => T+3 kicks test1 as fisherman without a record in stake_change
    record_block(&mut epoch_manager, h[1], h[3], 3, vec![]);

    let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap();
    check_validators(&epoch_info2, &[("test2", stake_amount), ("test3", stake_amount)]);
    check_fishermen(&epoch_info2, &[("test1", 148)]);
    check_stake_change(
        &epoch_info2,
        vec![
            ("test1".parse().unwrap(), 148),
            ("test2".parse().unwrap(), stake_amount),
            ("test3".parse().unwrap(), stake_amount),
        ],
    );
    check_kickout(&epoch_info2, &[]);

    let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap();
    check_validators(&epoch_info3, &[("test2", stake_amount), ("test3", stake_amount)]);
    check_fishermen(&epoch_info3, &[]);
    check_stake_change(
        &epoch_info3,
        vec![
            ("test1".parse().unwrap(), 0),
            ("test2".parse().unwrap(), stake_amount),
            ("test3".parse().unwrap(), stake_amount),
        ],
    );
    check_kickout(&epoch_info3, &[("test1", NotEnoughBlocks { produced: 0, expected: 1 })]);
}

fn set_block_info_protocol_version(info: &mut BlockInfo, protocol_version: ProtocolVersion) {
    match info {
        BlockInfo::V1(v1) => v1.latest_protocol_version = protocol_version,
        BlockInfo::V2(v2) => v2.latest_protocol_version = protocol_version,
    }
}

#[test]
fn test_protocol_version_switch() {
    let store = create_test_store();
    let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, 0, default_reward_calculator(), validators).unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let mut block_info1 = block_info(h[1], 1, 1, h[0], h[0], h[0], vec![], DEFAULT_TOTAL_SUPPLY);
    set_block_info_protocol_version(&mut block_info1, 0);
    epoch_manager.record_block_info(block_info1, [0; 32]).unwrap();
    for i in 2..6 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    assert_eq!(epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().protocol_version(), 0);
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
}

#[test]
fn test_protocol_version_switch_with_shard_layout_change() {
    let store = create_test_store();
    let config = epoch_config_with_production_config(2, 1, 2, 0, 90, 60, 0, true);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let new_protocol_version = SimpleNightshade.protocol_version();
    let mut epoch_manager = EpochManager::new(
        store,
        config,
        new_protocol_version - 1,
        default_reward_calculator(),
        validators,
    )
    .unwrap();
    let h = hash_range(8);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..8 {
        let mut block_info = block_info(
            h[i],
            i as u64,
            i as u64 - 1,
            h[i - 1],
            h[i - 1],
            h[0],
            vec![],
            DEFAULT_TOTAL_SUPPLY,
        );
        if i == 1 {
            set_block_info_protocol_version(&mut block_info, new_protocol_version - 1);
        } else {
            set_block_info_protocol_version(&mut block_info, new_protocol_version);
        }
        epoch_manager.record_block_info(block_info, [0; 32]).unwrap();
    }
    let epochs = vec![EpochId::default(), EpochId(h[2]), EpochId(h[4])];
    assert_eq!(
        epoch_manager.get_epoch_info(&epochs[1]).unwrap().protocol_version(),
        new_protocol_version - 1
    );
    assert_eq!(epoch_manager.get_shard_layout(&epochs[1]).unwrap(), ShardLayout::v0_single_shard(),);
    assert_eq!(
        epoch_manager.get_epoch_info(&epochs[2]).unwrap().protocol_version(),
        new_protocol_version
    );
    assert_eq!(
        epoch_manager.get_shard_layout(&epochs[2]).unwrap(),
        ShardLayout::get_simple_nightshade_layout()
    );

    // Check split shards
    // h[5] is the first block of epoch epochs[1] and shard layout will change at epochs[2]
    assert_eq!(epoch_manager.will_shard_layout_change(&h[3]).unwrap(), false);
    for i in 4..=5 {
        assert_eq!(epoch_manager.will_shard_layout_change(&h[i]).unwrap(), true);
    }
    assert_eq!(epoch_manager.will_shard_layout_change(&h[6]).unwrap(), false);
}

#[test]
fn test_protocol_version_switch_with_many_seats() {
    let store = create_test_store();
    let num_block_producer_seats_per_shard = vec![10];
    let epoch_config = EpochConfig {
        epoch_length: 10,
        num_block_producer_seats: 4,
        num_block_producer_seats_per_shard,
        avg_hidden_validator_seats_per_shard: Vec::from([0]),
        block_producer_kickout_threshold: 90,
        chunk_producer_kickout_threshold: 60,
        fishermen_threshold: 0,
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        protocol_upgrade_stake_threshold: Ratio::new(80, 100),
        minimum_stake_divisor: 1,
        shard_layout: ShardLayout::v0_single_shard(),
        validator_selection_config: Default::default(),
        validator_max_kickout_stake_perc: 100,
    };
    let config = AllEpochConfig::new(false, epoch_config);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked / 5),
    ];
    let mut epoch_manager =
        EpochManager::new(store, config, 0, default_reward_calculator(), validators).unwrap();
    let h = hash_range(50);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    let mut block_info1 = block_info(h[1], 1, 1, h[0], h[0], h[0], vec![], DEFAULT_TOTAL_SUPPLY);
    set_block_info_protocol_version(&mut block_info1, 0);
    epoch_manager.record_block_info(block_info1, [0; 32]).unwrap();
    for i in 2..32 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[10])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
    assert_eq!(
        epoch_manager.get_epoch_info(&EpochId(h[20])).unwrap().protocol_version(),
        PROTOCOL_VERSION
    );
}

#[test]
fn test_protocol_version_switch_after_switch() {
    let store = create_test_store();
    let epoch_length: usize = 10;
    let config = epoch_config(epoch_length as u64, 1, 2, 0, 90, 60, 0);
    let amount_staked = 1_000_000;
    let validators = vec![
        stake("test1".parse().unwrap(), amount_staked),
        stake("test2".parse().unwrap(), amount_staked),
    ];
    let mut epoch_manager = EpochManager::new(
        store,
        config,
        UPGRADABILITY_FIX_PROTOCOL_VERSION,
        default_reward_calculator(),
        validators,
    )
    .unwrap();
    let h = hash_range(5 * epoch_length);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..(2 * epoch_length + 1) {
        let mut block_info = block_info(
            h[i],
            i as u64,
            i as u64 - 1,
            h[i - 1],
            h[i - 1],
            h[0],
            vec![],
            DEFAULT_TOTAL_SUPPLY,
        );
        if i != 2 * epoch_length {
            set_block_info_protocol_version(
                &mut block_info,
                UPGRADABILITY_FIX_PROTOCOL_VERSION + 1,
            );
        } else {
            set_block_info_protocol_version(&mut block_info, UPGRADABILITY_FIX_PROTOCOL_VERSION);
        }
        epoch_manager.record_block_info(block_info, [0; 32]).unwrap();
    }

    let get_epoch_infos = |em: &mut EpochManager| -> Vec<Arc<EpochInfo>> {
        h.iter().filter_map(|x| em.get_epoch_info(&EpochId(*x)).ok()).collect()
    };

    let epoch_infos = get_epoch_infos(&mut epoch_manager);

    assert_eq!(epoch_infos[1].protocol_version(), UPGRADABILITY_FIX_PROTOCOL_VERSION + 1);

    assert_eq!(epoch_infos[2].protocol_version(), UPGRADABILITY_FIX_PROTOCOL_VERSION + 1);

    // if there are enough votes to use the old version, it should be allowed
    for i in (2 * epoch_length + 1)..(4 * epoch_length - 1) {
        let mut block_info = block_info(
            h[i],
            i as u64,
            i as u64 - 1,
            h[i - 1],
            h[i - 1],
            h[0],
            vec![],
            DEFAULT_TOTAL_SUPPLY,
        );
        set_block_info_protocol_version(&mut block_info, UPGRADABILITY_FIX_PROTOCOL_VERSION);
        epoch_manager.record_block_info(block_info, [0; 32]).unwrap();
    }

    let epoch_infos = get_epoch_infos(&mut epoch_manager);

    assert_eq!(epoch_infos[3].protocol_version(), UPGRADABILITY_FIX_PROTOCOL_VERSION);
}

/// Epoch aggregator should not need to be recomputed under the following scenario
///                      /-----------h+2
/// h-2 ---- h-1 ------ h
///                      \------h+1
/// even though from the perspective of h+2 the last final block is h-2.
#[test]
fn test_final_block_consistency() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 10, 1, 3, 0, 90, 60);

    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..5 {
        record_block_with_final_block_hash(
            &mut epoch_manager,
            h[i - 1],
            h[i],
            if i == 1 { CryptoHash::default() } else { h[i - 2] },
            i as u64,
            vec![],
        );
    }

    let epoch_aggregator_final_hash = epoch_manager.epoch_info_aggregator.last_block_hash;

    epoch_manager
        .record_block_info(
            block_info(h[5], 5, 1, h[1], h[2], h[1], vec![], DEFAULT_TOTAL_SUPPLY),
            [0; 32],
        )
        .unwrap()
        .commit()
        .unwrap();
    let new_epoch_aggregator_final_hash = epoch_manager.epoch_info_aggregator.last_block_hash;
    assert_eq!(epoch_aggregator_final_hash, new_epoch_aggregator_final_hash);
}

#[test]
fn test_epoch_validators_cache() {
    let amount_staked = 1_000_000;
    let validators =
        vec![("test1".parse().unwrap(), amount_staked), ("test2".parse().unwrap(), amount_staked)];
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 10, 0, 90, 60);
    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..4 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }
    assert_eq!(epoch_manager.epoch_validators_ordered.len(), 0);

    let epoch_id = EpochId(h[2]);
    let epoch_validators =
        epoch_manager.get_all_block_producers_settlement(&epoch_id, &h[3]).unwrap().to_vec();
    assert_eq!(epoch_manager.epoch_validators_ordered.len(), 1);
    let epoch_validators_in_cache = epoch_manager.epoch_validators_ordered.get(&epoch_id).unwrap();
    assert_eq!(*epoch_validators, *epoch_validators_in_cache);

    assert_eq!(epoch_manager.epoch_validators_ordered_unique.len(), 0);
    let epoch_validators_unique =
        epoch_manager.get_all_block_producers_ordered(&epoch_id, &h[3]).unwrap().to_vec();
    let epoch_validators_unique_in_cache =
        epoch_manager.epoch_validators_ordered_unique.get(&epoch_id).unwrap();
    assert_eq!(*epoch_validators_unique, *epoch_validators_unique_in_cache);
}

#[test]
fn test_chunk_producers() {
    let amount_staked = 1_000_000;
    // Make sure that last validator has at least 160/1'000'000  / num_shards of stake.
    // We're running with 2 shards and test1 + test2 has 2'000'000 tokens - so chunk_only should have over 160.
    let validators = vec![
        ("test1".parse().unwrap(), amount_staked),
        ("test2".parse().unwrap(), amount_staked),
        ("chunk_only".parse().unwrap(), 200),
        ("not_enough_producer".parse().unwrap(), 100),
    ];

    // There are 2 shards, and 2 block producers seats.
    // So test1 and test2 should become block producers, and chunk_only should become chunk only producer.
    let mut epoch_manager = setup_default_epoch_manager(validators, 2, 2, 2, 0, 90, 60);
    let h = hash_range(10);
    record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
    for i in 1..=4 {
        record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
    }

    let epoch_id = EpochId(h[2]);

    let block_producers = epoch_manager
        .get_all_block_producers_settlement(&epoch_id, &h[4])
        .unwrap()
        .iter()
        .map(|(stake, _)| stake.account_id().to_string())
        .collect::<Vec<_>>();
    assert_eq!(vec!(String::from("test1"), String::from("test2")), block_producers);

    let mut chunk_producers = epoch_manager
        .get_all_chunk_producers(&epoch_id)
        .unwrap()
        .to_vec()
        .iter()
        .map(|stake| stake.account_id().to_string())
        .collect::<Vec<_>>();
    chunk_producers.sort();

    assert_eq!(
        vec!(String::from("chunk_only"), String::from("test1"), String::from("test2")),
        chunk_producers
    );
}

/// A sanity test for the compute_kickout_info function, tests that
/// the validators that don't meet the block/chunk producer kickout threshold is kicked out
#[test]
fn test_validator_kickout_sanity() {
    let epoch_config = epoch_config(5, 2, 4, 0, 90, 80, 0).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 500),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1, 2], vec![0, 1, 3, 4]],
        vec![],
        vec![],
        BTreeMap::new(),
        vec![],
        HashMap::new(),
        0,
    );
    let (kickouts, validator_stats) = EpochManager::compute_kickout_info(
        &epoch_config,
        &epoch_info,
        &HashMap::from([
            (0, ValidatorStats { produced: 100, expected: 100 }),
            (1, ValidatorStats { produced: 90, expected: 100 }),
            (2, ValidatorStats { produced: 100, expected: 100 }),
            // test3 will be kicked out
            (3, ValidatorStats { produced: 89, expected: 100 }),
        ]),
        &HashMap::from([
            (
                0,
                HashMap::from([
                    (0, ValidatorStats { produced: 100, expected: 100 }),
                    (1, ValidatorStats { produced: 80, expected: 100 }),
                    (2, ValidatorStats { produced: 70, expected: 100 }),
                ]),
            ),
            (
                1,
                HashMap::from([
                    (0, ValidatorStats { produced: 70, expected: 100 }),
                    (1, ValidatorStats { produced: 79, expected: 100 }),
                    (3, ValidatorStats { produced: 100, expected: 100 }),
                    (4, ValidatorStats { produced: 100, expected: 100 }),
                ]),
            ),
        ]),
        &HashMap::new(),
        &HashMap::new(),
    );
    assert_eq!(
        kickouts,
        HashMap::from([
            ("test1".parse().unwrap(), NotEnoughChunks { produced: 159, expected: 200 }),
            ("test2".parse().unwrap(), NotEnoughChunks { produced: 70, expected: 100 }),
            ("test3".parse().unwrap(), NotEnoughBlocks { produced: 89, expected: 100 }),
        ])
    );
    assert_eq!(
        validator_stats,
        HashMap::from([
            (
                "test0".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 100, expected: 100 },
                    chunk_stats: ValidatorStats { produced: 170, expected: 200 }
                }
            ),
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ValidatorStats { produced: 100, expected: 100 }
                }
            ),
        ])
    );
}

#[test]
/// Test that the stake of validators kicked out in an epoch doesn't exceed the max_kickout_stake_ratio
fn test_max_kickout_stake_ratio() {
    #[allow(unused_mut)]
    let mut epoch_config =
        epoch_config(5, 2, 4, 0, 90, 80, 0).for_protocol_version(PROTOCOL_VERSION);
    let accounts = vec![
        ("test0".parse().unwrap(), 1000),
        ("test1".parse().unwrap(), 1000),
        ("test2".parse().unwrap(), 1000),
        ("test3".parse().unwrap(), 1000),
        ("test4".parse().unwrap(), 1000),
    ];
    let epoch_info = epoch_info(
        0,
        accounts,
        vec![0, 1, 2, 3],
        vec![vec![0, 1], vec![2, 4]],
        vec![],
        vec![],
        BTreeMap::new(),
        vec![],
        HashMap::new(),
        0,
    );
    let block_stats = HashMap::from([
        (0, ValidatorStats { produced: 50, expected: 100 }),
        // here both test1 and test2 produced the most number of blocks, we made that intentionally
        // to test the algorithm to pick one deterministically to save in this case.
        (1, ValidatorStats { produced: 70, expected: 100 }),
        (2, ValidatorStats { produced: 70, expected: 100 }),
        // validator 3 doesn't need to produce any block or chunk
        (3, ValidatorStats { produced: 0, expected: 0 }),
    ]);
    let chunk_stats = HashMap::from([
        (
            0,
            HashMap::from([
                (0, ValidatorStats { produced: 0, expected: 100 }),
                (1, ValidatorStats { produced: 0, expected: 100 }),
            ]),
        ),
        (
            1,
            HashMap::from([
                (2, ValidatorStats { produced: 100, expected: 100 }),
                (4, ValidatorStats { produced: 50, expected: 100 }),
            ]),
        ),
    ]);
    let prev_validator_kickout =
        HashMap::from([("test3".parse().unwrap(), ValidatorKickoutReason::Unstaked)]);
    let (kickouts, validator_stats) = EpochManager::compute_kickout_info(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats,
        &HashMap::new(),
        &prev_validator_kickout,
    );
    assert_eq!(
        kickouts,
        // We would have kicked out test0, test1, test2 and test4, but test3 was kicked out
        // last epoch. To avoid kicking out all validators in two epochs, we saved test1 because
        // it produced the most blocks (test1 and test2 produced the same number of blocks, but test1
        // is listed before test2 in the validators list).
        HashMap::from([
            ("test0".parse().unwrap(), NotEnoughBlocks { produced: 50, expected: 100 }),
            ("test2".parse().unwrap(), NotEnoughBlocks { produced: 70, expected: 100 }),
            ("test4".parse().unwrap(), NotEnoughChunks { produced: 50, expected: 100 }),
        ])
    );
    assert_eq!(
        validator_stats,
        HashMap::from([
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 0 }
                }
            ),
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 70, expected: 100 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 100 }
                }
            ),
        ])
    );
    // At most 50% of total stake can be kicked out
    epoch_config.validator_max_kickout_stake_perc = 40;
    let (kickouts, validator_stats) = EpochManager::compute_kickout_info(
        &epoch_config,
        &epoch_info,
        &block_stats,
        &chunk_stats,
        &HashMap::new(),
        &prev_validator_kickout,
    );
    assert_eq!(
        kickouts,
        // We would have kicked out test0, test1, test2 and test4, but
        // test1, test2, and test4 are exempted. Note that test3 can't be exempted because it
        // is in prev_validator_kickout.
        HashMap::from([(
            "test0".parse().unwrap(),
            NotEnoughBlocks { produced: 50, expected: 100 }
        ),])
    );
    assert_eq!(
        validator_stats,
        HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 70, expected: 100 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 100 }
                }
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 70, expected: 100 },
                    chunk_stats: ValidatorStats { produced: 100, expected: 100 }
                }
            ),
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 0 }
                }
            ),
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ValidatorStats { produced: 50, expected: 100 }
                }
            ),
        ])
    );
}
