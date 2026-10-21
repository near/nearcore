//! Code for working with consensus compilation status [`ContractCompilationStatus`].
//!
//! Current implementation:
//! - Only tracks if a compilation result has been reached at the current VM generation.
//! - Once accepted, the consensus level status remains `Cached` even if it gets
//!   evicted from the actual cache.
//!
//! Planned, TODO(async-compilation):
//! - Actually limit receipt execution based on the status.
//! - Protocol-level cache eviction, based on WASM size and likely also access
//!   frequency. Also handles clean up for re-deployments and resharding.
//! - Clean up state after a protocol upgrade.
//! - Rejected status, for contracts that consensus decided can't be compiled.
//! - Pending contract status while the code is compiling in the background.

use crate::ApplyState;
use near_primitives::errors::{IntegerOverflowError, RuntimeError};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{ContractCompilationStatus, StateChangeCause};
use near_store::{TrieUpdate, get, set};

/// Persist compilation status and return the total compiled bytes.
pub(crate) fn update_contract_compilation_status(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
) -> Result<u64, RuntimeError> {
    let mut cold_bytes = 0u64;
    for (code_hash, code_len) in state_update.take_contract_compilations() {
        let key = TrieKey::ContractCompilationStatus {
            vm_generation: apply_state.vm_generation,
            code_hash,
        };
        let status: Option<ContractCompilationStatus> = get(state_update, &key)?;
        if status.is_none() {
            cold_bytes = cold_bytes.checked_add(code_len).ok_or(IntegerOverflowError)?;
            set(state_update, key, &ContractCompilationStatus::Cached);
        }
    }
    state_update.commit(StateChangeCause::ContractCompilationStatusUpdate);
    Ok(cold_bytes)
}
