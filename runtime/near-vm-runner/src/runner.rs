use crate::errors::IntoVMError;
use crate::memory::WasmerMemory;
use crate::{cache, imports};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::{FunctionCallError, MethodResolveError, VMError};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMConfig, VMContext, VMLogic, VMOutcome};
use wasmer_runtime::Module;

fn check_method(module: &Module, method_name: &str) -> Result<(), VMError> {
    let info = module.info();
    use wasmer_runtime_core::module::ExportIndex::Func;
    if let Some(Func(index)) = info.exports.get(method_name) {
        let func = info.func_assoc.get(index.clone()).unwrap();
        let sig = info.signatures.get(func.clone()).unwrap();
        if sig.params().is_empty() && sig.returns().is_empty() {
            Ok(())
        } else {
            Err(VMError::FunctionCallError(FunctionCallError::ResolveError(
                MethodResolveError::MethodInvalidSignature,
            )))
        }
    } else {
        Err(VMError::FunctionCallError(FunctionCallError::ResolveError(
            MethodResolveError::MethodNotFound,
        )))
    }
}

/// `run` does the following:
/// - deserializes and validate the `code` binary (see `prepare::prepare_contract`)
/// - injects gas counting into
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
) -> (Option<VMOutcome>, Option<VMError>) {
    if !cfg!(target_arch = "x86") && !cfg!(target_arch = "x86_64") {
        // TODO(#1940): Remove once NaN is standardized by the VM.
        panic!(
            "Execution of smart contracts is only supported for x86 and x86_64 CPU architectures."
        );
    }
    if method_name.is_empty() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::ResolveError(
                MethodResolveError::MethodEmptyName,
            ))),
        );
    }

    let module = match cache::compile_module(code_hash, code, wasm_config) {
        Ok(x) => x,
        Err(err) => return (None, Some(err)),
    };
    let mut memory = match WasmerMemory::new(wasm_config) {
        Ok(x) => x,
        Err(_err) => panic!("Cannot create memory for a contract call"),
    };
    let memory_copy = memory.clone();

    let mut logic =
        VMLogic::new(ext, context, wasm_config, fees_config, promise_results, &mut memory);

    let import_object = imports::build(memory_copy, &mut logic);

    let method_name = match std::str::from_utf8(method_name) {
        Ok(x) => x,
        Err(_) => {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::ResolveError(
                    MethodResolveError::MethodUTF8Error,
                ))),
            )
        }
    };
    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    match module.instantiate(&import_object) {
        Ok(instance) => match instance.call(&method_name, &[]) {
            Ok(_) => (Some(logic.outcome()), None),
            Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
        },
        Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
    }
}
