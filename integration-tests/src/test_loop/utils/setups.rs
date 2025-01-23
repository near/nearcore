//! This file contains standard setups for test loop tests.
//! Using TestLoopBuilder gives a lot of flexibility, but sometimes you just need some basic blockchain.

use itertools::Itertools;
use near_chain_configs::test_genesis::{
    build_genesis_and_epoch_config_store, GenesisAndEpochConfigParams, ValidatorsSpec,
};
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::logic::ProtocolVersion;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;

/// 2 producers, 2 validators, 1 rpc node, 4 shards, 20 accounts (account{i}) with 10k NEAR each.
pub fn standard_setup_1() -> TestLoopEnv {
    let num_clients = 5;
    let num_producers = 2;
    let num_validators = 2;
    let num_rpc = 1;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(num_producers);
    let (validators, tmp) = tmp.split_at(num_validators);
    let (rpcs, tmp) = tmp.split_at(num_rpc);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let [_rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

    let epoch_length = 10;
    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let validators_spec = ValidatorsSpec::desired_roles(&producers, &validators);

    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
            shard_layout,
            validators_spec,
            accounts: &accounts,
        },
        |genesis_builder| genesis_builder.genesis_height(10000).transaction_validity_period(1000),
        |epoch_config_builder| {
            epoch_config_builder.shuffle_shard_assignment_for_chunk_producers(true)
        },
    );

    TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
}

pub fn derive_new_epoch_config_from_boundary(
    base_epoch_config: &EpochConfig,
    boundary_account: &AccountId,
) -> EpochConfig {
    let base_shard_layout = &base_epoch_config.shard_layout;
    let mut epoch_config = base_epoch_config.clone();
    epoch_config.shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, boundary_account.clone());
    tracing::info!(target: "test", ?base_shard_layout, new_shard_layout=?epoch_config.shard_layout, "shard layout");
    epoch_config
}

/// Two protocol upgrades would happen as soon as possible,
/// usually in two consecutive epochs, unless upgrade voting decides differently.
pub fn two_upgrades_voting_schedule(
    target_protocol_version: ProtocolVersion,
) -> ProtocolUpgradeVotingSchedule {
    let past_datetime_1 =
        ProtocolUpgradeVotingSchedule::parse_datetime("1970-01-01 00:00:00").unwrap();
    let past_datetime_2 =
        ProtocolUpgradeVotingSchedule::parse_datetime("1970-01-02 00:00:00").unwrap();
    let voting_schedule = vec![
        (past_datetime_1, target_protocol_version - 1),
        (past_datetime_2, target_protocol_version),
    ];
    ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
        target_protocol_version,
        voting_schedule,
    )
    .unwrap()
}
