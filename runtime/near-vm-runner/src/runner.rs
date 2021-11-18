use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::{config::VMConfig, types::CompiledContractCache, version::ProtocolVersion};
use near_vm_errors::{CompilationError, FunctionCallError, VMError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMOutcome};

use crate::cache::into_vm_result;
use crate::VMKind;

/// Validate and run the specified contract.
///
/// This is the entry point for executing a NEAR protocol contract. Before the entry point (as
/// specified by the `method_name` argument) of the contract code is executed, the contract will be
/// validated (see [`prepare::prepare_contract`]), instrumented (e.g. for gas accounting), and
/// linked with the externs specified via the `ext` argument.
///
/// [`VMContext::input`] will be passed to the contract entrypoint as an argument.
///
/// The contract will be executed with the default VM implementation for the current protocol
/// version. In order to specify a different VM implementation call [`run_vm`] instead.
///
/// The gas cost for contract preparation will be subtracted by the VM implementation.
pub fn run<'a>(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    current_protocol_version: ProtocolVersion,
    cache: Option<&'a dyn CompiledContractCache>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);
    run_vm(
        code,
        method_name,
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        vm_kind,
        current_protocol_version,
        cache,
    )
}

/// Validate and run the specified contract with a specific VM implementation.
///
/// This is the entry point for executing a NEAR protocol contract. Before the entry point (as
/// specified by the `method_name` argument) of the contract code is executed, the contract will be
/// validated (see [`prepare::prepare_contract`]), instrumented (e.g. for gas accounting), and
/// linked with the externs specified via the `ext` argument.
///
/// [`VMContext::input`] will be passed to the contract entrypoint as an argument.
///
/// The gas cost for contract preparation will be subtracted by the VM implementation.
///
/// # Panics
///
/// Calling this function with a `VMKind` that has not been enabled at the compile time will cause
/// this function to panic.
pub fn run_vm(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &VMConfig,
    fees_config: &RuntimeFeesConfig,
    promise_results: &[PromiseResult],
    vm_kind: VMKind,
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let _span = tracing::debug_span!(target: "vm", "run_vm", ?vm_kind).entered();

    #[cfg(feature = "wasmer0_vm")]
    use crate::wasmer_runner::run_wasmer;

    #[cfg(feature = "wasmtime_vm")]
    use crate::wasmtime_runner::wasmtime_runner::run_wasmtime;

    #[cfg(feature = "wasmer2_vm")]
    use crate::wasmer2_runner::run_wasmer2;

    let (outcome, error) = match vm_kind {
        #[cfg(feature = "wasmer0_vm")]
        VMKind::Wasmer0 => run_wasmer(
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmer0_vm"))]
        VMKind::Wasmer0 => panic!("Wasmer0 is not supported, compile with '--features wasmer0_vm'"),
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => run_wasmtime(
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
        #[cfg(feature = "wasmer2_vm")]
        VMKind::Wasmer2 => run_wasmer2(
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmer2_vm"))]
        VMKind::Wasmer2 => panic!("Wasmer2 is not supported, compile with '--features wasmer2_vm'"),
    };
    (outcome, error)
}

/// Precompile a WASM contract to a VM specific format and store the result into the `cache`.
///
/// Repeated execution of the `code` with the same cache will reuse the cached VM code.
#[allow(dead_code)]
pub fn precompile<'a>(
    code: &[u8],
    code_hash: &CryptoHash,
    // This `VMConfig` is required because it influences the codegen decisions such as whether gas
    // metering instrumentation is required. These influences will be accounted for when computing
    // the cache key.
    wasm_config: &'a VMConfig,
    cache: &'a dyn CompiledContractCache,
    vm_kind: VMKind,
) -> Option<VMError> {
    match vm_kind {
        #[cfg(not(feature = "wasmer0_vm"))]
        VMKind::Wasmer0 => panic!("Wasmer0 is not supported, compile with '--features wasmer0_vm'"),
        #[cfg(feature = "wasmer0_vm")]
        VMKind::Wasmer0 => {
            let result = crate::cache::wasmer0_cache::compile_and_serialize_wasmer(
                code,
                wasm_config,
                code_hash,
                cache,
            );
            into_vm_result(result).err()
        }
        #[cfg(feature = "wasmer2_vm")]
        VMKind::Wasmer2 => {
            let compiler = wasmer_compiler_singlepass::Singlepass::new();
            let engine = wasmer::Universal::new(compiler).engine();
            let store = wasmer::Store::new(&engine);
            let result = crate::cache::wasmer2_cache::compile_and_serialize_wasmer2(
                code,
                code_hash,
                wasm_config,
                cache,
                &store,
            );
            into_vm_result(result).err()
        }
        #[cfg(not(feature = "wasmer2_vm"))]
        VMKind::Wasmer2 => panic!("Wasmer2 is not supported, compile with '--features wasmer2_vm'"),
        VMKind::Wasmtime => Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::UnsupportedCompiler {
                msg: "Precompilation not supported in Wasmtime yet".to_string(),
            },
        ))),
    }
}

/// Verify the `code` contract can be compiled with the specified `VMKind`.
///
/// This is intended primarily for testing purposes.
pub fn compile_module(vm_kind: VMKind, code: &Vec<u8>) -> bool {
    match vm_kind {
        #[cfg(feature = "wasmer0_vm")]
        VMKind::Wasmer0 => {
            use crate::wasmer_runner::compile_wasmer0_module;
            compile_wasmer0_module(code)
        }
        #[cfg(not(feature = "wasmer0_vm"))]
        VMKind::Wasmer0 => panic!("Wasmer0 is not supported, compile with '--features wasmer0_vm'"),
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => {
            use crate::wasmtime_runner::compile_module;
            compile_module(code)
        }
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
        #[cfg(feature = "wasmer2_vm")]
        VMKind::Wasmer2 => {
            use crate::wasmer2_runner::compile_wasmer2_module;
            compile_wasmer2_module(code)
        }
        #[cfg(not(feature = "wasmer2_vm"))]
        VMKind::Wasmer2 => panic!("Wasmer2 is not supported, compile with '--features wasmer2_vm'"),
    };
    false
}
