//! Code for working with consensus compilation status [`ContractCompilationStatus`].
//!
//! Current implementation:
//! - Tracks if a compilation result has been reached at the current VM generation.
//! - Limits cold compilation bytes per chunk. Receipt preflight conservatively
//!   admits contracts before executing any actions, retaining partial compilation
//!   progress across chunks even when the receipt cannot execute yet.
//! - Once accepted, the consensus level status remains `Cached` even if it gets
//!   evicted from the actual cache.
//! - Stale VM generations are overwritten on compilation, keeping one entry per code hash.
//!
//! Planned, TODO(async-compilation):
//! - Protocol-level cache eviction, based on WASM size and likely also access
//!   frequency. Also handles clean up for re-deployments and resharding.
//! - Rejected status, for contracts that consensus decided can't be compiled.
//! - Pending contract status while the code is compiling in the background.

use crate::ApplyState;
use near_primitives::errors::RuntimeError;
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{ContractCompilationStatus, StateChangeCause};
use near_store::{ContractCompilationAdmission, TrieUpdate, get, set};

/// Check whether compilation is already cached or admitted in this chunk.
pub(crate) fn contract_compilation_is_admitted(
    state_update: &TrieUpdate,
    apply_state: &ApplyState,
    code_hash: CryptoHash,
) -> Result<bool, RuntimeError> {
    if state_update.is_contract_compilation_admitted(&code_hash) {
        return Ok(true);
    }
    let key = TrieKey::ContractCompilationStatus { code_hash };
    let status: Option<ContractCompilationStatus> = get(state_update, &key)?;
    Ok(status
        == Some(ContractCompilationStatus::Cached { vm_generation: apply_state.vm_generation }))
}

pub(crate) fn try_admit_contract_compilation(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    code_hash: CryptoHash,
    code_len: u64,
) -> Result<ContractCompilationAdmission, RuntimeError> {
    if contract_compilation_is_admitted(state_update, apply_state, code_hash)? {
        Ok(ContractCompilationAdmission::Admitted)
    } else {
        Ok(state_update.try_admit_contract_compilation(
            code_hash,
            code_len,
            apply_state.config.max_cold_contract_bytes_per_chunk,
        ))
    }
}

/// Persist compilation status without further status reads and return the total cold bytes.
pub(crate) fn update_contract_compilation_status(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
) -> Result<u64, RuntimeError> {
    let (compilations, cold_bytes) = state_update.take_contract_compilations();
    for code_hash in compilations {
        let key = TrieKey::ContractCompilationStatus { code_hash };
        set(
            state_update,
            key,
            &ContractCompilationStatus::Cached { vm_generation: apply_state.vm_generation },
        );
    }
    state_update.commit(StateChangeCause::ContractCompilationStatusUpdate);
    Ok(cold_bytes)
}
