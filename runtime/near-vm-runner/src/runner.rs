use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::{
    config::VMConfig, profile::ProfileData, types::CompiledContractCache, version::ProtocolVersion,
};
use near_vm_errors::{CompilationError, FunctionCallError, VMError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMOutcome};

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
    profile: &ProfileData,
) -> (Option<VMOutcome>, Option<VMError>) {
    run_vm(
        code,
        method_name,
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        VMKind::default(),
        current_protocol_version,
        cache,
        profile.clone(),
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
    profile: ProfileData,
) -> (Option<VMOutcome>, Option<VMError>) {
    let _span = tracing::debug_span!("run_vm").entered();

    #[cfg(feature = "wasmer0_vm")]
    use crate::wasmer_runner::run_wasmer;

    #[cfg(feature = "wasmtime_vm")]
    use crate::wasmtime_runner::wasmtime_runner::run_wasmtime;

    #[cfg(feature = "wasmer1_vm")]
    use crate::wasmer1_runner::run_wasmer1;

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
            profile.clone(),
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
            profile.clone(),
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
        #[cfg(feature = "wasmer1_vm")]
        VMKind::Wasmer1 => run_wasmer1(
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            profile.clone(),
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmer1_vm"))]
        VMKind::Wasmer1 => panic!("Wasmer1 is not supported, compile with '--features wasmer1_vm'"),
    };
    if let Some(VMOutcome { burnt_gas, .. }) = &outcome {
        profile.set_burnt_gas(*burnt_gas)
    }
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
            result.err()
        }
        #[cfg(feature = "wasmer1_vm")]
        VMKind::Wasmer1 => {
            let engine =
                wasmer::JIT::new(wasmer_compiler_singlepass::Singlepass::default()).engine();
            let store = wasmer::Store::new(&engine);
            let result = crate::cache::wasmer1_cache::compile_and_serialize_wasmer1(
                code,
                code_hash,
                wasm_config,
                cache,
                &store,
            );
            result.err()
        }
        #[cfg(not(feature = "wasmer1_vm"))]
        VMKind::Wasmer1 => panic!("Wasmer1 is not supported, compile with '--features wasmer1_vm'"),
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
        #[cfg(feature = "wasmer1_vm")]
        VMKind::Wasmer1 => {
            use crate::wasmer1_runner::compile_wasmer1_module;
            compile_wasmer1_module(code)
        }
        #[cfg(not(feature = "wasmer1_vm"))]
        VMKind::Wasmer1 => panic!("Wasmer1 is not supported, compile with '--features wasmer1_vm'"),
    };
    false
}
