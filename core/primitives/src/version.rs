use crate::types::Balance;
use near_primitives_core::chains::{MAINNET, TESTNET};

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
pub use near_primitives_core::types::ProtocolVersion;
pub use near_primitives_core::version::MIN_SUPPORTED_PROTOCOL_VERSION;
pub use near_primitives_core::version::PEER_MIN_ALLOWED_PROTOCOL_VERSION;
pub use near_primitives_core::version::PROD_GENESIS_PROTOCOL_VERSION;
pub use near_primitives_core::version::PROTOCOL_VERSION;
pub use near_primitives_core::version::ProtocolFeature;

/// Minimum gas price proposed in NEP 92 and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92: Balance = 1_000_000_000;

/// Minimum gas price proposed in NEP 92 (fixed) and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92_FIX: Balance = 100_000_000;

/// The protocol version for the block header v3. This is also when we started using
/// versioned block producer hash format.
pub const BLOCK_HEADER_V3_PROTOCOL_VERSION: ProtocolVersion = 49;

/// Returns the protocol upgrade schedule for chain_id or an empty schedule if
/// chain_id is not recognized.
pub fn get_protocol_upgrade_schedule(chain_id: &str) -> ProtocolUpgradeVotingSchedule {
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
    let schedule = match chain_id {
        MAINNET => {
            let schedule = vec![];
            schedule
        }
        TESTNET => {
            let schedule = vec![];
            schedule
        }
        _ => {
            let schedule = vec![];
            schedule
        }
    };
    ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
        MIN_SUPPORTED_PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        schedule,
    )
    .unwrap()
}
