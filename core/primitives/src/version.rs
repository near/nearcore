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
pub const DB_VERSION: DbVersion = 2;

/// Protocol version type.
pub type ProtocolVersion = u32;

/// Current latest version of the protocol.
pub const PROTOCOL_VERSION: ProtocolVersion = 27;

pub const FIRST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION: ProtocolVersion = PROTOCOL_VERSION;
