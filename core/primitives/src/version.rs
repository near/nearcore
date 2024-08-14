use crate::types::Balance;
use once_cell::sync::Lazy;

/// Data structure for semver version and github tag or commit.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
    #[serde(default)]
    pub rustc_version: String,
}

use crate::upgrade_schedule::ProtocolUpgradeVotingSchedule;

/// near_primitives_core re-exports
pub use near_primitives_core::checked_feature;
pub use near_primitives_core::types::ProtocolVersion;
pub use near_primitives_core::version::ProtocolFeature;
pub use near_primitives_core::version::PEER_MIN_ALLOWED_PROTOCOL_VERSION;
pub use near_primitives_core::version::PROTOCOL_VERSION;

/// Minimum gas price proposed in NEP 92 and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92: Balance = 1_000_000_000;
pub const MIN_PROTOCOL_VERSION_NEP_92: ProtocolVersion = 31;

/// Minimum gas price proposed in NEP 92 (fixed) and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92_FIX: Balance = 100_000_000;
pub const MIN_PROTOCOL_VERSION_NEP_92_FIX: ProtocolVersion = 32;

pub const CORRECT_RANDOM_VALUE_PROTOCOL_VERSION: ProtocolVersion = 33;

/// The protocol version that enables reward on mainnet.
pub const ENABLE_INFLATION_PROTOCOL_VERSION: ProtocolVersion = 36;

/// Fix upgrade to use the latest voted protocol version instead of the current epoch protocol
/// version when there is no new change in protocol version.
pub const UPGRADABILITY_FIX_PROTOCOL_VERSION: ProtocolVersion = 37;

/// Updates the way receipt ID, data ID and random seeds are constructed.
pub const CREATE_HASH_PROTOCOL_VERSION: ProtocolVersion = 38;

/// Fix the storage usage of the delete key action.
pub const DELETE_KEY_STORAGE_USAGE_PROTOCOL_VERSION: ProtocolVersion = 40;

pub const SHARD_CHUNK_HEADER_UPGRADE_VERSION: ProtocolVersion = 41;

/// Updates the way receipt ID is constructed to use current block hash instead of last block hash.
pub const CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION: ProtocolVersion = 42;

/// Pessimistic gas price estimation uses a fixed value of `minimum_new_receipt_gas` to stop being
/// tied to the function call base cost.
pub const FIXED_MINIMUM_NEW_RECEIPT_GAS_VERSION: ProtocolVersion = 66;

/// The points in time after which the voting for the latest protocol version
/// should start.
///
/// In non-release builds this is typically a date in the far past (meaning that
/// nightly builds will vote for new protocols immediately).  On release builds
/// it’s set according to the schedule for that protocol upgrade.  Release
/// candidates usually have separate schedule to final releases.
pub const PROTOCOL_UPGRADE_SCHEDULE: Lazy<ProtocolUpgradeVotingSchedule> = Lazy::new(|| {
    // Update according to the schedule when making a release. Keep in mind that
    // the protocol upgrade will happen 1-2 epochs (15h-30h) after the set date.
    // Ideally that should be during working hours.
    //
    // Multiple protocol version upgrades can be scheduled. Please make sure
    // that they are ordered by datetime and version, the last one is the
    // PROTOCOL_VERSION and that there is enough time between subsequent
    // upgrades.
    //
    // At most one protocol version upgrade vote can happen per epoch. If, by any
    // chance, two or more votes get scheduled on the same epoch, the latest upgrades
    // will be postponed.

    // e.g.
    // let v1_protocol_version = 50;
    // let v2_protocol_version = 51;
    // let v1_datetime = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-01 15:00:00").unwrap();
    // let v2_datetime = ProtocolUpgradeVotingSchedule::parse_datetime("2000-01-10 15:00:00").unwrap();
    //
    // let schedule = vec![(v1_datetime, v1_protocol_version), (v2_datetime, v2_protocol_version)];
    // ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(PROTOCOL_VERSION, schedule).unwrap()

    let mut schedule = vec![];

    // 2.1.0
    // Tuesday
    let protocol_version = 70;
    let datetime = ProtocolUpgradeVotingSchedule::parse_datetime("2024-08-06 10:00:00").unwrap();
    schedule.push((datetime, protocol_version));

    // FixMinStakeRatio
    let protocol_version = 71;
    let datetime = ProtocolUpgradeVotingSchedule::parse_datetime("2024-08-18 10:00:00").unwrap();
    schedule.push((datetime, protocol_version));

    ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(PROTOCOL_VERSION, schedule).unwrap()
});

/// Gives new clients an option to upgrade without announcing that they support
/// the new version.  This gives non-validator nodes time to upgrade.  See
/// <https://github.com/near/NEPs/issues/205>
#[cfg(feature = "clock")]
pub fn get_protocol_version(
    next_epoch_protocol_version: ProtocolVersion,
    clock: near_time::Clock,
) -> ProtocolVersion {
    let now = clock.now_utc();
    let chrono = chrono::DateTime::from_timestamp(now.unix_timestamp(), now.nanosecond());
    PROTOCOL_UPGRADE_SCHEDULE
        .get_protocol_version(chrono.unwrap_or_default(), next_epoch_protocol_version)
}
