//! Helpers for test loop tests related to epoch configuration and protocol upgrades.
//! Provides utilities for deriving new epoch configs/shard layouts and constructing upgrade voting schedules.

use near_primitives::epoch_manager::EpochConfig;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_vm_runner::logic::ProtocolVersion;

pub fn derive_new_epoch_config_from_boundary(
    base_epoch_config: &EpochConfig,
    boundary_account: &AccountId,
) -> (EpochConfig, ShardLayout) {
    let base_shard_layout = &base_epoch_config.static_shard_layout().unwrap();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, boundary_account.clone());
    tracing::info!(target: "test", ?base_shard_layout, ?new_shard_layout, "shard layout");
    let epoch_config = base_epoch_config.clone().with_shard_layout(new_shard_layout.clone());
    (epoch_config, new_shard_layout)
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
        target_protocol_version - 2,
        target_protocol_version,
        voting_schedule,
    )
    .unwrap()
}
