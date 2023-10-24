//! This module contains helper functions for initialization of genesis data in store
//! We first check if store has the genesis hash and state_roots, if not, we go ahead with initialization

mod initialization;
mod state_applier;

pub use initialization::initialize_genesis_state;
pub use state_applier::compute_genesis_storage_usage;
pub use state_applier::compute_storage_usage;
pub use state_applier::GenesisStateApplier;
