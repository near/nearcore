pub mod block;
pub mod epoch;
pub mod external;
pub mod handler;
pub mod header;
pub mod state;

/// Whether the V2 sync pipeline is enabled.
/// When true, the sync handler uses the linear V2 pipeline
/// (EpochSync → HeaderSync → StateSync → BlockSync) instead of the
/// legacy V1 logic.
pub const SYNC_V2_ENABLED: bool = true;
