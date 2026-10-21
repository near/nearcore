//! Tests for crossing the pre-spice -> spice activation boundary while shard tracking
//! and the validator set change at activation.

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_account_id, create_validator_ids};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, StakeAction};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::test_utils::{create_test_signer, pre_spice_protocol_version};
use near_primitives::types::{AccountId, AccountInfo, Balance, NumSeats, ShardId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use std::collections::HashSet;

/// The upgrade with chunk-producer shard assignments shuffled every epoch, so shard
/// tracking rotates exactly at the boundary: the producers of the first spice epoch
/// bootstrap the last pre-spice block's results for shards they did not track before.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_shard_rotation() {
    init_test_logger();

    let epoch_length = 10;
    let num_producers: NumSeats = 4;
    let accounts = create_validator_ids(num_producers as usize);
    let validators: Vec<AccountInfo> = accounts
        .iter()
        .map(|account_id| AccountInfo {
            public_key: create_test_signer(account_id.as_str()).public_key(),
            account_id: account_id.clone(),
            amount: Balance::from_near(100),
        })
        .collect();
    let validators_spec =
        ValidatorsSpec::raw(validators, num_producers, num_producers, num_producers);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(ShardLayout::multi_shard(2, 0))
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .protocol_version(pre_spice_protocol_version())
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .build();

    // Cross the boundary, then locate the last pre-spice block under the head.
    env.node_runner(0).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(120));
    let last_pre_spice = env.node(0).last_pre_spice_block_header();
    let boundary_height = last_pre_spice.height();

    // The shuffle must actually rotate tracking at the boundary, or this test shows
    // nothing: some shard's chunk-producer set has to change across it.
    {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        let pre_spice_epoch_id = last_pre_spice.epoch_id();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(pre_spice_epoch_id).unwrap();
        let rotated = shard_layout.shard_ids().any(|shard_id| {
            epoch_manager.get_epoch_chunk_producers_for_shard(pre_spice_epoch_id, shard_id).unwrap()
                != epoch_manager
                    .get_epoch_chunk_producers_for_shard(&spice_epoch_id, shard_id)
                    .unwrap()
        });
        assert!(rotated, "the shuffle left every shard's chunk-producer set unchanged");
    }

    // The first spice epoch must complete. An epoch cannot end before its first block
    // certifies, and executing that block needs the last pre-spice block's results,
    // which the rotated-in producers bootstrap and distribute.
    env.node_runner(0).run_until_new_epoch();
    assert!(
        env.node(0).last_certified_block_header().height() > boundary_height,
        "certification must cross the boundary",
    );
}

/// The upgrade with validators that start tracking a shard exactly at activation, and
/// catch-up too slow to land the last pre-spice block before its boundary bootstrap runs.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_validators_tracking_a_shard_from_activation() {
    init_test_logger();

    let epoch_length = 10;
    let num_producers = 2;
    let num_late_trackers = 2;
    // Spice activates two epochs after genesis under an immediate upgrade vote.
    let spice_epoch_height = 2;
    let late_shard_id = ShardId::new(1);
    // Indexed by epoch height: nothing tracked before activation, the shard from then on.
    let schedule: Vec<Vec<ShardId>> = (0..20)
        .map(
            |epoch_height| {
                if epoch_height < spice_epoch_height { vec![] } else { vec![late_shard_id] }
            },
        )
        .collect();
    let mut env = TestLoopBuilder::new()
        .validators(num_producers, num_late_trackers)
        .num_shards(2)
        .epoch_length(epoch_length)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .config_modifier(move |config, index| {
            // Catch-up advances one step per period: state sync takes a few steps and
            // then each block of the epoch takes one. Steps this far apart keep the late
            // trackers catching up well past the epoch's last block.
            config.catchup_step_period = Duration::seconds(5);
            if index >= num_producers {
                config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
            }
        })
        .build();
    let late_trackers: Vec<usize> = (num_producers..num_producers + num_late_trackers).collect();

    // Run until the last pre-spice block is the head.
    env.node_runner(0).run_until(
        |node| {
            let head_block_hash = node.head().last_block_hash;
            is_last_pre_spice_block(node.client().epoch_manager.as_ref(), &head_block_hash).unwrap()
        },
        Duration::seconds(600),
    );
    let last_pre_spice = env.node(0).head_block();
    let boundary_height = last_pre_spice.header().height();
    let late_shard_uid = {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        assert_eq!(
            epoch_manager.get_epoch_height_from_prev_block(last_pre_spice.hash()).unwrap(),
            spice_epoch_height,
            "the schedule must start tracking the shard in the first spice epoch",
        );
        let shard_layout =
            epoch_manager.get_shard_layout(last_pre_spice.header().epoch_id()).unwrap();
        ShardUId::from_shard_id_and_layout(late_shard_id, &shard_layout)
    };

    // The late trackers' endorsements of the boundary chunk must be needed for it to
    // certify, or their loss goes unnoticed.
    {
        let node = env.node(0);
        let designated = node
            .client()
            .epoch_manager
            .get_chunk_validator_assignments(
                last_pre_spice.header().epoch_id(),
                late_shard_id,
                boundary_height,
            )
            .unwrap();
        let late_accounts: HashSet<AccountId> =
            late_trackers.iter().map(|index| env.node_datas[*index].account_id.clone()).collect();
        for account_id in &late_accounts {
            assert!(
                designated.contains(account_id),
                "{account_id} must validate the boundary chunk"
            );
        }
        let others: HashSet<AccountId> = designated
            .assignments()
            .iter()
            .map(|(account_id, _)| account_id.clone())
            .filter(|account_id| !late_accounts.contains(account_id))
            .collect();
        assert!(
            !designated.is_endorsed(&others),
            "the boundary chunk must not certify without the late trackers' endorsements",
        );
    }

    // Each late tracker processes the last pre-spice block without applying the shard
    // it starts tracking; that apply is still ahead of it, in catch-up. The block's
    // postprocessing has already triggered the boundary bootstrap by now.
    for &node_index in &late_trackers {
        env.node_runner(node_index)
            .run_until(|node| node.head().height >= boundary_height, Duration::seconds(60));
        let node = env.node(node_index);
        assert_eq!(
            &node.head().last_block_hash,
            last_pre_spice.hash(),
            "node {node_index} advanced past the last pre-spice block before catch-up landed it",
        );
        assert!(
            node.client()
                .chain
                .chain_store
                .get_chunk_extra(last_pre_spice.hash(), &late_shard_uid)
                .is_err(),
            "node {node_index} had applied {late_shard_uid} of the last pre-spice block on \
             processing it; the catch-up is not slow enough for the test to show anything",
        );
    }

    // The first spice epoch must still complete: the late trackers are designated
    // validators of the boundary chunk and can validate its witness without having
    // caught up, so their endorsements must not depend on catch-up timing. The epoch
    // itself stretches: the late trackers also validate the spice chunks of the shard
    // they now track, and those they apply themselves, which waits on the slow catch-up.
    env.node_runner(0).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(60));
    let first_spice_epoch_id = env.node(0).head().epoch_id;
    env.node_runner(0)
        .run_until(|node| node.head().epoch_id != first_spice_epoch_id, Duration::seconds(600));
    assert!(
        env.node(0).last_certified_block_header().height() > boundary_height,
        "certification must cross the boundary",
    );
}

/// The upgrade with the validator set changing exactly at activation: one validator
/// joins in the first spice epoch and one leaves after the last pre-spice epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_validator_set_change_at_activation() {
    init_test_logger();
    let epoch_length = 20;
    let num_producers = 4;
    let validators = create_validator_ids(num_producers);
    let joiner = create_account_id("joiner");
    let leaver = validators[num_producers - 1].clone();
    let mut env = TestLoopBuilder::new()
        .validators(num_producers, 0)
        .enable_rpc()
        .num_shards(2)
        .epoch_length(epoch_length)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .add_non_validator_client(&joiner)
        .add_user_account(&joiner, Balance::from_near(20_000))
        // The leaver's account needs a user access key to sign its unstake.
        .add_user_account(&leaver, Balance::from_near(1_000))
        .build();

    // Stake changes in the genesis epoch define the validator set of the epoch after
    // next, which under the immediate vote is the first spice epoch.
    let leaver_stake = {
        let node = env.node(0);
        let genesis_epoch_id = node.head().epoch_id;
        node.client()
            .epoch_manager
            .get_epoch_all_validators(&genesis_epoch_id)
            .unwrap()
            .into_iter()
            .find(|stake| stake.account_id() == &leaver)
            .expect("leaver must validate at genesis")
            .stake()
    };
    let stake_tx = |account_id: &AccountId, stake: Balance| {
        let public_key = create_test_signer(account_id.as_str()).public_key();
        env.rpc_node().tx_from_actions(
            account_id,
            account_id,
            vec![Action::Stake(Box::new(StakeAction { stake, public_key }))],
        )
    };
    let txs = vec![stake_tx(&joiner, leaver_stake), stake_tx(&leaver, Balance::ZERO)];
    for tx in txs {
        env.rpc_node().submit_tx(tx);
    }

    // Cross the boundary, then locate the last pre-spice block under the head.
    env.node_runner(0).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(120));
    let last_pre_spice = env.node(0).last_pre_spice_block_header();
    let boundary_height = last_pre_spice.height();

    // The change must land exactly at activation, or the test shows nothing.
    let spice_epoch_id = {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        let pre_spice_epoch_id = last_pre_spice.epoch_id();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let validates_in = |epoch_id, account_id: &AccountId| {
            epoch_manager
                .get_epoch_all_validators(epoch_id)
                .unwrap()
                .iter()
                .any(|stake| stake.account_id() == account_id)
        };
        assert!(!validates_in(pre_spice_epoch_id, &joiner), "joiner must not validate pre-spice");
        assert!(validates_in(&spice_epoch_id, &joiner), "joiner must validate under spice");
        assert!(validates_in(pre_spice_epoch_id, &leaver), "leaver must validate pre-spice");
        assert!(!validates_in(&spice_epoch_id, &leaver), "leaver must be gone under spice");
        spice_epoch_id
    };

    // The first spice epoch must complete under the new set, which needs the boundary
    // certified under the old one, with the leaver following as a non-validator.
    env.node_runner(0).run_until_new_epoch();
    let target_height = env.node(0).head().height;
    env.runner_for_account(&joiner).run_until_head_height(target_height);
    env.runner_for_account(&leaver).run_until_head_height(target_height);

    // Past the first couple of spice blocks, the new set produces every chunk, the
    // joiner among its chunk producers.
    {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        let shard_layout = epoch_manager.get_shard_layout(&spice_epoch_id).unwrap();
        let joiner_produces = shard_layout.shard_ids().any(|shard_id| {
            epoch_manager
                .get_epoch_chunk_producers_for_shard(&spice_epoch_id, shard_id)
                .unwrap()
                .contains(&joiner)
        });
        assert!(joiner_produces, "joiner must produce chunks under spice");
        let mut block = node.head_block();
        while block.header().height() > boundary_height + 2 {
            assert!(
                block.header().chunk_mask().iter().all(|present| *present),
                "chunk missing at height {} under the new validator set",
                block.header().height(),
            );
            block = node.client().chain.get_block(block.header().prev_hash()).unwrap();
        }
    }
}
