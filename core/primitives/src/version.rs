use crate::types::Balance;
use std::sync::LazyLock;

/// Data structure for semver version and github tag or commit.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
    pub commit: String,
    #[serde(default)]
    pub rustc_version: String,
}

use crate::upgrade_schedule::ProtocolUpgradeVotingSchedule;

/// near_primitives_core re-exports
pub use near_primitives_core::checked_feature;
pub use near_primitives_core::types::ProtocolVersion;
pub use near_primitives_core::version::MIN_SUPPORTED_PROTOCOL_VERSION;
pub use near_primitives_core::version::PEER_MIN_ALLOWED_PROTOCOL_VERSION;
pub use near_primitives_core::version::PROTOCOL_VERSION;
pub use near_primitives_core::version::ProtocolFeature;

/// Minimum gas price proposed in NEP 92 and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92: Balance = 1_000_000_000;

/// Minimum gas price proposed in NEP 92 (fixed) and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92_FIX: Balance = 100_000_000;

/// The points in time after which the voting for the latest protocol version
/// should start.
///
/// In non-release builds this is typically a date in the far past (meaning that
/// nightly builds will vote for new protocols immediately).  On release builds
/// it’s set according to the schedule for that protocol upgrade.  Release
/// candidates usually have separate schedule to final releases.
pub const PROTOCOL_UPGRADE_SCHEDULE: LazyLock<ProtocolUpgradeVotingSchedule> =
    LazyLock::new(|| {
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

        ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(PROTOCOL_VERSION, vec![]).unwrap()
    });
