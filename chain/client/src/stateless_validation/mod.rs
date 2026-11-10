pub mod chunk_endorsement;
pub mod chunk_validation_actor;
pub mod chunk_validator;
pub mod partial_witness;
mod shadow_validate;
mod state_witness_producer;
pub mod state_witness_tracker;
mod validate;
#[cfg(test)]
mod validate_tests;

use near_chain::types::RuntimeAdapter;
use near_parameters::RuntimeConfig;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use std::sync::Arc;

/// Whether this node can run the contract without fetching its code from a chunk producer: it is
/// in the compiled-contract cache under the config of the epoch, or its code is in the node's
/// contract code cache and gets compiled locally when the chunk is applied.
pub fn contract_available_locally(
    runtime: &dyn RuntimeAdapter,
    contract_hash: &CodeHash,
    runtime_config: &RuntimeConfig,
) -> bool {
    let is_compiled = near_vm_runner::contract_cached(
        Arc::clone(&runtime_config.wasm_config),
        runtime.compiled_contract_cache(),
        contract_hash.0,
    )
    .is_ok_and(|cached| cached);
    let has_code = runtime.contract_code_cache().has(&contract_hash.0);
    is_compiled || has_code
}
