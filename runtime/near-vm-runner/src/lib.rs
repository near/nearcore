#[cfg(test)]
mod tests;

mod cache;
mod errors;
mod imports;
#[cfg(feature = "wasmer0_vm")]
mod memory;
mod vm_kind;

mod preload;
pub mod prepare;
mod runner;

#[cfg(feature = "wasmer0_vm")]
mod wasmer_runner;

#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

#[cfg(feature = "wasmer1_vm")]
mod wasmer1_runner;

use near_primitives::checked_feature;
use near_primitives::version::is_implicit_account_creation_enabled;
use near_vm_logic::{ProtocolVersion, VMLogicProtocolFeatures};

pub use near_vm_errors::VMError;
pub use preload::{ContractCallPrepareRequest, ContractCallPrepareResult, ContractCaller};
pub use runner::compile_module;
pub use runner::run;

pub use near_vm_logic::with_ext_cost_counter;

pub use cache::precompile_contract;
pub use cache::precompile_contract_vm;
pub use cache::MockCompiledContractCache;

// These two are public for the standalone runner, but are an implementation
// detail of `near-vm-runner`. Public API like `run` should not expose VMKind.
pub use runner::run_vm;
pub use vm_kind::VMKind;

/// This is a bit of an odd place to convert from [`ProtocolVersion`] to [`VMLogicProtocolFeatures`].
/// Ideally, we'd let the caller to pass [`VMLogicProtocolFeatures`] in. We, however, do need
/// to know about [`ProtocolVersion`] anyway to create the appropriate subset of host function
/// imports, so we might as well decide about [`VMLogicProtocolFeatures`].
fn vm_logic_protocol_features(protocol_version: ProtocolVersion) -> VMLogicProtocolFeatures {
    VMLogicProtocolFeatures {
        implicit_account_creation: is_implicit_account_creation_enabled(protocol_version),
        allow_create_account_on_delete: checked_feature!(
            "protocol_feature_allow_create_account_on_delete",
            AllowCreateAccountOnDelete,
            protocol_version
        ),
    }
}
