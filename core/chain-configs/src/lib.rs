mod client_config;
mod genesis_config;

pub use client_config::ClientConfig;
pub use genesis_config::GenesisConfig;
pub use genesis_config::CONFIG_VERSION as GENESIS_CONFIG_VERSION;

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 4;
