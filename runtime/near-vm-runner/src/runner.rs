use crate::vm_kind::VMKind;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{FunctionCallError, VMRunnerError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMOutcome};

/// Returned by VM::run method.
///
/// `VMOutcome` is a graceful completion of a VM execution, which may or
/// may not have failed. `VMRunnerError` contains fatal errors that usually
/// are panicked on by the caller.
type VMResult = Result<VMOutcome, VMRunnerError>;

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
    )
    .entered();

    let runtime = vm_kind.runtime(wasm_config.clone()).unwrap_or_else(|| {
        panic!("the {:?} runtime has not been enabled at compile time", vm_kind)
    });

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
        code: &[u8],
        code_hash: &CryptoHash,
        cache: &dyn CompiledContractCache,
    ) -> Option<Result<FunctionCallError, VMRunnerError>>;

    /// Verify the `code` contract can be compiled with this `VM`.
    ///
    /// This is intended primarily for testing purposes.
    fn check_compile(&self, code: &[u8]) -> bool;
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
            #[allow(unreachable_patterns)] // reachable when some of the VMs are disabled.
            _ => None,
        }
    }
}
