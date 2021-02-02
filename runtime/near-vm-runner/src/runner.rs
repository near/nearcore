use near_primitives::hash::CryptoHash;
use near_primitives::{
    config::VMConfig, profile::ProfileData, types::CompiledContractCache, version::ProtocolVersion,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::{CompilationError, FunctionCallError, VMError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMKind, VMOutcome};

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
    code_hash: Vec<u8>,
    code: &[u8],
    method_name: &[u8],
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    current_protocol_version: ProtocolVersion,
    cache: Option<&'a dyn CompiledContractCache>,
    #[cfg(feature = "costs_counting")] profile: &Option<ProfileData>,
) -> (Option<VMOutcome>, Option<VMError>) {
    #[cfg(feature = "costs_counting")]
    match profile {
        Some(profile) => run_vm_profiled(
            code_hash,
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            VMKind::default(),
            profile.clone(),
            current_protocol_version,
            cache,
        ),
        _ => run_vm(
            code_hash,
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
        ),
    }
    #[cfg(not(feature = "costs_counting"))]
    run_vm(
        code_hash,
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
    )
}
pub fn run_vm<'a>(
    code_hash: Vec<u8>,
    code: &[u8],
    method_name: &[u8],
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    vm_kind: VMKind,
    current_protocol_version: ProtocolVersion,
    cache: Option<&'a dyn CompiledContractCache>,
) -> (Option<VMOutcome>, Option<VMError>) {
    use crate::wasmer_runner::run_wasmer;
    #[cfg(feature = "wasmtime_vm")]
    use crate::wasmtime_runner::wasmtime_runner::run_wasmtime;
    match vm_kind {
        VMKind::Wasmer => run_wasmer(
            code_hash,
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            None,
            current_protocol_version,
            cache,
        ),
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => run_wasmtime(
            code_hash,
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            None,
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
    }
}

pub fn run_vm_profiled<'a>(
    code_hash: Vec<u8>,
    code: &[u8],
    method_name: &[u8],
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    vm_kind: VMKind,
    profile: ProfileData,
    current_protocol_version: ProtocolVersion,
    cache: Option<&'a dyn CompiledContractCache>,
) -> (Option<VMOutcome>, Option<VMError>) {
    use crate::wasmer_runner::run_wasmer;
    #[cfg(feature = "wasmtime_vm")]
    use crate::wasmtime_runner::wasmtime_runner::run_wasmtime;
    let (outcome, error) = match vm_kind {
        VMKind::Wasmer => run_wasmer(
            code_hash,
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            Some(profile.clone()),
            current_protocol_version,
            cache,
        ),
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => run_wasmtime(
            code_hash,
            code,
            method_name,
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            Some(profile.clone()),
            current_protocol_version,
            cache,
        ),
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
    };
    match &outcome {
        Some(VMOutcome { burnt_gas, .. }) => profile.set_burnt_gas(*burnt_gas),
        _ => (),
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
    use crate::cache::compile_and_serialize_wasmer;
    match vm_kind {
        VMKind::Wasmer => {
            let result = compile_and_serialize_wasmer(code, wasm_config, code_hash, cache);
            result.err()
        }
        VMKind::Wasmtime => Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::UnsupportedCompiler {
                msg: "Precompilation not supported in Wasmtime yet".to_string(),
            },
        ))),
    }
}

pub fn with_vm_variants(runner: fn(VMKind) -> ()) {
    runner(VMKind::Wasmer);
    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);
}

/// Used for testing cost of compiling a module
pub fn compile_module(vm_kind: VMKind, code: &Vec<u8>) -> bool {
    match vm_kind {
        VMKind::Wasmer => {
            use crate::wasmer_runner::compile_module;
            compile_module(code)
        }
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => {
            use crate::wasmtime_runner::compile_module;
            compile_module(code)
        }
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => {
            panic!("Wasmtime is not supported, compile with '--features wasmtime_vm'")
        }
    };
    false
}
