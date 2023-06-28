use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{CacheError, CompilationError, VMRunnerError};
use crate::logic::types::PromiseResult;
use crate::logic::{CompiledContractCache, External, VMContext, VMOutcome};
use crate::vm_kind::VMKind;
use near_primitives_core::config::VMConfig;
use near_primitives_core::contract::ContractCode;
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_primitives_core::types::ProtocolVersion;

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
pub fn run(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &VMConfig,
    fees_config: &RuntimeFeesConfig,
    promise_results: &[PromiseResult],
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
) -> VMResult {
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);
    let span = tracing::debug_span!(
        target: "vm",
        "run",
        "code.len" = code.code().len(),
        %method_name,
        ?vm_kind,
        burnt_gas = tracing::field::Empty,
        %current_protocol_version,
    )
    .entered();

    let runtime = vm_kind
        .runtime(wasm_config.clone())
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));

    let outcome = runtime.run(
        code,
        method_name,
        ext,
        context,
        fees_config,
        promise_results,
        current_protocol_version,
        cache,
    )?;

    span.record("burnt_gas", &outcome.burnt_gas);
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
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        current_protocol_version: ProtocolVersion,
        cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult;

    /// Precompile a WASM contract to a VM specific format and store the result
    /// into the `cache`.
    ///
    /// Further calls to [`Self::run`] or [`Self::precompile`] with the same
    /// `code`, `cache` and [`VMConfig`] may reuse the results of this
    /// precompilation step.
    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn CompiledContractCache,
    ) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError>;
}

impl VMKind {
    /// Make a [`VM`] for this [`VMKind`].
    ///
    /// This is not intended to be used by code other than internal tools like
    /// the estimator.
    pub fn runtime(&self, config: VMConfig) -> Option<Box<dyn VM>> {
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
            _ => None,
        }
    }
}
