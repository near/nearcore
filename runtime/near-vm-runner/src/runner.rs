use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{CacheError, CompilationError, VMRunnerError};
use crate::logic::types::PromiseResult;
use crate::logic::{External, VMContext, VMOutcome};
use crate::{ContractCode, ContractRuntimeCache};
use near_parameters::vm::{Config, VMKind};
use near_parameters::RuntimeFeesConfig;
use std::sync::Arc;

/// Returned by VM::run method.
///
/// `VMRunnerError` means nearcore is buggy or the data base has been corrupted.
/// We are unable to produce a deterministic result. The correct action usually
/// is to crash or maybe ban a peer and/or send a challenge.
///
/// A `VMOutcome` is a graceful completion of a VM execution. It can also contain
/// an guest error message in the `aborted` field. But these are not errors in
/// the real sense, those are just reasons why execution failed at some point.
/// Such as when a smart contract code panics.
/// Note that the fact that `VMOutcome` contains is tracked on the blockchain.
/// All validators must produce an error deterministically or all should succeed.
/// (See also `PartialExecutionStatus`.)
/// Similarly, the gas values on `VMOutcome` must be the exact same on all
/// validators, even when a guest error occurs, or else their state will diverge.
pub(crate) type VMResult<T = VMOutcome> = Result<T, VMRunnerError>;

/// Validate and run the specified contract.
///
/// This is the entry point for executing a NEAR protocol contract. Before the
/// entry point (as specified by the `method_name` argument) of the contract
/// code is executed, the contract will be validated (see
/// [`crate::prepare::prepare_contract`]), instrumented (e.g. for gas
/// accounting), and linked with the externs specified via the `ext` argument.
///
/// [`VMContext::input`] will be passed to the contract entrypoint as an
/// argument.
///
/// The contract will be executed with the default VM implementation for the
/// current protocol version.
///
/// The gas cost for contract preparation will be subtracted by the VM
/// implementation.
#[tracing::instrument(target = "vm", level = "debug", "run", skip_all, fields(
    code.hash = %ext.code_hash(),
    method_name,
    vm_kind = ?wasm_config.vm_kind,
    burnt_gas = tracing::field::Empty,
    compute_usage = tracing::field::Empty,
))]
pub fn run(
    method_name: &str,
    ext: &mut (dyn External + Send),
    context: &VMContext,
    wasm_config: Arc<Config>,
    fees_config: Arc<RuntimeFeesConfig>,
    promise_results: std::sync::Arc<[PromiseResult]>,
    cache: Option<&dyn ContractRuntimeCache>,
) -> VMResult {
    let span = tracing::Span::current();
    let vm_kind = wasm_config.vm_kind;
    let runtime = vm_kind
        .runtime(wasm_config)
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    let outcome = runtime.run(method_name, ext, context, fees_config, promise_results, cache);
    let outcome = match outcome {
        Ok(o) => o,
        e @ Err(_) => return e,
    };

    span.record("burnt_gas", outcome.burnt_gas);
    span.record("compute_usage", outcome.compute_usage);
    Ok(outcome)
}

pub trait VM {
    /// Validate and run the specified contract.
    ///
    /// This is the entry point for executing a NEAR protocol contract. Before
    /// the entry point (as specified by the `method_name` argument) of the
    /// contract code is executed, the contract will be validated (see
    /// [`crate::prepare::prepare_contract`]), instrumented (e.g. for gas
    /// accounting), and linked with the externs specified via the `ext`
    /// argument.
    ///
    /// [`VMContext::input`] will be passed to the contract entrypoint as an
    /// argument.
    ///
    /// The gas cost for contract preparation will be subtracted by the VM
    /// implementation.
    fn run(
        &self,
        method_name: &str,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        promise_results: std::sync::Arc<[PromiseResult]>,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> VMResult;

    /// Precompile a WASM contract to a VM specific format and store the result
    /// into the `cache`.
    ///
    /// Further calls to [`Self::run`] or [`Self::precompile`] with the same
    /// `code`, `cache` and [`Config`] may reuse the results of this
    /// precompilation step.
    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError>;
}

pub trait VMKindExt {
    fn is_available(&self) -> bool;
    /// Make a [`VM`] for this [`VMKind`].
    ///
    /// This is not intended to be used by code other than internal tools like
    /// the estimator.
    fn runtime(&self, config: std::sync::Arc<Config>) -> Option<Box<dyn VM>>;
}

impl VMKindExt for VMKind {
    fn is_available(&self) -> bool {
        match self {
            Self::Wasmer0 => cfg!(all(feature = "wasmer0_vm", target_arch = "x86_64")),
            Self::Wasmtime => cfg!(feature = "wasmtime_vm"),
            Self::Wasmer2 => cfg!(all(feature = "wasmer2_vm", target_arch = "x86_64")),
            Self::NearVm => cfg!(all(feature = "near_vm", target_arch = "x86_64")),
        }
    }
    fn runtime(&self, config: std::sync::Arc<Config>) -> Option<Box<dyn VM>> {
        match self {
            #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
            Self::Wasmer0 => Some(Box::new(crate::wasmer_runner::Wasmer0VM::new(config))),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime => Some(Box::new(crate::wasmtime_runner::WasmtimeVM::new(config))),
            #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
            Self::Wasmer2 => Some(Box::new(crate::wasmer2_runner::Wasmer2VM::new(config))),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm => Some(Box::new(crate::near_vm_runner::NearVM::new(config))),
            #[allow(unreachable_patterns)] // reachable when some of the VMs are disabled.
            _ => {
                let _ = config;
                None
            }
        }
    }
}
