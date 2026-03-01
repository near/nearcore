pub mod chunk_endorsement;
pub mod chunk_validation_actor;
pub mod chunk_validator;
pub mod partial_witness;
mod shadow_validate;
mod state_witness_producer;
pub mod state_witness_tracker;
mod validate;

use std::sync::Arc;

use near_parameters::RuntimeConfig;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_vm_runner::ContractRuntimeCache;

/// Checks whether the compiled contract cache contains a compiled version of the given contract.
/// Used by both SPICE and non-SPICE validators to determine which contracts need to be requested
/// from chunk producers.
pub fn contracts_cache_contains_contract(
    cache: &dyn ContractRuntimeCache,
    contract_hash: &CodeHash,
    runtime_config: &RuntimeConfig,
) -> bool {
    near_vm_runner::contract_cached(Arc::clone(&runtime_config.wasm_config), cache, contract_hash.0)
        .is_ok_and(|b| b)
}
