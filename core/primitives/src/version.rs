use crate::types::Balance;
use serde::{Deserialize, Serialize};

/// Data structure for semver version and github tag or commit.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
}

/// Database version.
pub type DbVersion = u32;

/// Current version of the database.
pub const DB_VERSION: DbVersion = 5;

/// Protocol version type.
pub type ProtocolVersion = u32;

/// Current latest version of the protocol.
pub const PROTOCOL_VERSION: ProtocolVersion = 31;

pub const FIRST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION: ProtocolVersion = 29;

/// Minimum gas price proposed in NEP 92 and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92: Balance = 1_000_000_000;
pub const MIN_PROTOCOL_VERSION_NEP_92: ProtocolVersion = 31;
