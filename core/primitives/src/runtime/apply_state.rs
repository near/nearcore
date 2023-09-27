use crate::runtime::migration_data::{MigrationData, MigrationFlags};
use crate::{
    hash::CryptoHash,
    runtime::config::RuntimeConfig,
    types::{Balance, BlockHeight, EpochHeight, EpochId, Gas},
    version::ProtocolVersion,
};
use near_vm_runner::logic::CompiledContractCache;
use std::sync::Arc;

#[derive(Debug)]
pub struct ApplyState {
    /// Currently building block height.
    pub block_height: BlockHeight,
    /// Prev block hash
    pub prev_block_hash: CryptoHash,
    /// Current block hash
    pub block_hash: CryptoHash,
    /// Current epoch id
    pub epoch_id: EpochId,
    /// Current epoch height
    pub epoch_height: EpochHeight,
    /// Price for the gas.
    pub gas_price: Balance,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// Gas limit for a given chunk.
    /// If None is given, assumes there is no gas limit.
    pub gas_limit: Option<Gas>,
    /// Current random seed (from current block vrf output).
    pub random_seed: CryptoHash,
    /// Current Protocol version when we apply the state transition
    pub current_protocol_version: ProtocolVersion,
    /// The Runtime config to use for the current transition.
    pub config: Arc<RuntimeConfig>,
    /// Cache for compiled contracts.
    pub cache: Option<Box<dyn CompiledContractCache>>,
    /// Whether the chunk being applied is new.
    pub is_new_chunk: bool,
    /// Data for migrations that may need to be applied at the start of an epoch when protocol
    /// version changes
    pub migration_data: Arc<MigrationData>,
    /// Flags for migrations indicating whether they can be applied at this block
    pub migration_flags: MigrationFlags,
}
