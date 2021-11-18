use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::{config::VMConfig, types::CompiledContractCache, version::ProtocolVersion};
use near_vm_errors::{CompilationError, FunctionCallError, VMError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMOutcome};

use crate::cache::into_vm_result;
use crate::VMKind;

/// `run` does the following:
/// - deserializes and validate the `code` binary (see `prepare::prepare_contract`)
/// - injects gas counting into
/// - adds fee to VMLogic's GasCounter for size of contract
/// - instantiates (links) `VMLogic` externs with the imports of the binary
/// - calls the `method_name` with `context.input`
///   - updates `ext` with new receipts, created during the execution
///   - counts burnt and used gas
///   - counts how accounts storage usage increased by the call
///   - collects logs
///   - sets the return data
///  returns result as `VMOutcome`
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

/// `precompile` compiles WASM contract to a VM specific format and stores result into the `cache`.
/// Further execution with the same cache will result in compilation avoidance and reusing cached
/// result. `wasm_config` is required as during compilation we decide if gas metering shall be
/// embedded in the native code, and so we take that into account when computing database key.
#[allow(dead_code)]
pub fn precompile<'a>(
    code: &[u8],
    code_hash: &CryptoHash,
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
            let store = crate::wasmer2_runner::default_wasmer2_store();
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

/// Used for testing cost of compiling a module
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
