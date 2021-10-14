use crate::cache::into_vm_result;
use crate::errors::IntoVMError;
use crate::memory::WasmerMemory;
use crate::{cache, imports};
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::{config::VMConfig, types::CompiledContractCache, version::ProtocolVersion};
use near_vm_errors::{CompilationError, FunctionCallError, MethodResolveError, VMError, WasmTrap};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMLogic, VMLogicError, VMOutcome};
use wasmer_runtime::{ImportObject, Module};

fn check_method(module: &Module, method_name: &str) -> Result<(), VMError> {
    let info = module.info();
    use wasmer_runtime_core::module::ExportIndex::Func;
    if let Some(Func(index)) = info.exports.map.get(method_name) {
        let func = info.func_assoc.get(index.clone()).unwrap();
        let sig = info.signatures.get(func.clone()).unwrap();
        if sig.params().is_empty() && sig.returns().is_empty() {
            Ok(())
        } else {
            Err(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodInvalidSignature,
            )))
        }
    } else {
        Err(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
            MethodResolveError::MethodNotFound,
        )))
    }
}

impl IntoVMError for wasmer_runtime::error::Error {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::Error;
        match self {
            Error::CompileError(err) => {
                VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::WasmerCompileError { msg: err.to_string() },
                ))
            }
            Error::LinkError(err) => VMError::FunctionCallError(FunctionCallError::LinkError {
                msg: format!("{:.500}", Error::LinkError(err).to_string()),
            }),
            Error::RuntimeError(err) => err.into_vm_error(),
            Error::ResolveError(err) => err.into_vm_error(),
            Error::CallError(err) => err.into_vm_error(),
            Error::CreationError(err) => panic!("{}", err),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::CallError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::CallError;
        match self {
            CallError::Resolve(err) => err.into_vm_error(),
            CallError::Runtime(err) => err.into_vm_error(),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::ResolveError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::ResolveError as WasmerResolveError;
        match self {
            WasmerResolveError::Signature { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature),
            ),
            WasmerResolveError::ExportNotFound { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
            ),
            WasmerResolveError::ExportWrongType { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
            ),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::RuntimeError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::InvokeError;
        use wasmer_runtime::error::RuntimeError;
        match &self {
            RuntimeError::InvokeError(invoke_error) => match invoke_error {
                // Indicates an exceptional circumstance such as a bug in Wasmer
                // or a hardware failure.
                // As of 0.17.0, thrown when stack unwinder fails, or when
                // invoke returns false and doesn't fill error info what Singlepass BE doesn't.
                // Failed unwinder may happen in the case of deep recursion/stack overflow.
                // Also can be thrown on unreachable instruction, which is quite unfortunate.
                //
                // See https://github.com/near/wasmer/blob/0.17.2/lib/runtime-core/src/fault.rs#L285
                InvokeError::FailedWithNoError => {
                    tracing::error!(target: "vm", "Got FailedWithNoError from wasmer0");
                    VMError::FunctionCallError(
                        // XXX: Initially, we treated this error case as
                        // deterministic (so, we stored this error in our state,
                        // etc.)
                        //
                        // Then, in
                        // https://github.com/near/nearcore/pull/4181#discussion_r606267838
                        // we reasoned that this error actually happens
                        // non-deterministically, so it's better to panic in this
                        // case.
                        //
                        // However, when rolling this out, we noticed that this
                        // error happens deterministically for at least one
                        // contract. So here we roll this back to a previous
                        // behavior and emit some deterministic error, which won't
                        // cause the node to panic.
                        //
                        // So far, we are unable to reproduce this deterministic
                        // failure though.
                        FunctionCallError::WasmTrap(WasmTrap::Unreachable),
                    )
                }
                // Indicates that a trap occurred that is not known to Wasmer.
                // As of 0.17.0, thrown only from Cranelift BE.
                InvokeError::UnknownTrap { address, signal } => {
                    panic!(
                        "Impossible UnknownTrap error (Cranelift only): signal {} at {}",
                        signal.to_string(),
                        address
                    );
                }
                // A trap that Wasmer knows about occurred.
                InvokeError::TrapCode { code, srcloc: _ } => {
                    VMError::FunctionCallError(match code {
                        wasmer_runtime::ExceptionCode::Unreachable => {
                            FunctionCallError::WasmTrap(WasmTrap::Unreachable)
                        }
                        wasmer_runtime::ExceptionCode::IncorrectCallIndirectSignature => {
                            FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature)
                        }
                        wasmer_runtime::ExceptionCode::MemoryOutOfBounds => {
                            FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
                        }
                        wasmer_runtime::ExceptionCode::CallIndirectOOB => {
                            FunctionCallError::WasmTrap(WasmTrap::CallIndirectOOB)
                        }
                        wasmer_runtime::ExceptionCode::IllegalArithmetic => {
                            FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
                        }
                        wasmer_runtime::ExceptionCode::MisalignedAtomicAccess => {
                            FunctionCallError::WasmTrap(WasmTrap::MisalignedAtomicAccess)
                        }
                    })
                }
                // A trap occurred that Wasmer knows about but it had a trap code that
                // we weren't expecting or that we do not handle.
                // As of 0.17.0, thrown only from Cranelift BE.
                InvokeError::UnknownTrapCode { trap_code, srcloc } => {
                    panic!(
                        "Impossible UnknownTrapCode error (Cranelift only): trap {} at {}",
                        trap_code, srcloc
                    );
                }
                // An "early trap" occurred.
                // As of 0.17.0, thrown only from Cranelift BE.
                InvokeError::EarlyTrap(_) => {
                    panic!("Impossible EarlyTrap error (Cranelift only)");
                }
                // Indicates that a breakpoint was hit. The inner value is dependent
                // upon the middleware or backend being used.
                // As of 0.17.0, thrown only from Singlepass BE and wraps RuntimeError
                // instance.
                InvokeError::Breakpoint(_) => unreachable!("Wasmer breakpoint"),
            },
            // A metering triggered error value.
            // As of 0.17.0, thrown only from Singlepass BE, and as we do not rely
            // on Wasmer metering system cannot be returned to us. Whenever we will
            // shall be rechecked.
            RuntimeError::Metering(_) => {
                panic!("Support metering errors properly");
            }
            // A frozen state of Wasm used to pause and resume execution.
            // As of 0.17.0, can be activated when special memory page
            // (see get_wasm_interrupt_signal_mem()) is accessed.
            // This address is passed via InternalCtx.interrupt_signal_mem
            // to the runtime, and is triggered only from do_optimize().
            // do_optimize() is only called if backend is mentioned in
            // Run.optimized_backends option, and we don't.
            RuntimeError::InstanceImage(_) => {
                panic!("Support instance image errors properly");
            }
            RuntimeError::User(data) => {
                if let Some(err) = data.downcast_ref::<VMLogicError>() {
                    err.into()
                } else {
                    panic!(
                        "Bad error case! Output is non-deterministic {:?} {:?}",
                        data.type_id(),
                        self.to_string()
                    );
                }
            }
        }
    }
}

pub fn run_wasmer<'a>(
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
    let _span = tracing::debug_span!(target: "vm", "run_wasmer").entered();

    if !cfg!(target_arch = "x86") && !cfg!(target_arch = "x86_64") {
        // TODO(#1940): Remove once NaN is standardized by the VM.
        panic!(
            "Execution of smart contracts is only supported for x86 and x86_64 CPU architectures."
        );
    }
    #[cfg(not(feature = "no_cpu_compatibility_checks"))]
    if !is_x86_feature_detected!("avx") {
        panic!("AVX support is required in order to run Wasmer VM Singlepass backend.");
    }
    if method_name.is_empty() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName,
            ))),
        );
    }

    // TODO: consider using get_module() here, once we'll go via deployment path.
    let module = cache::wasmer0_cache::compile_module_cached_wasmer0(code, wasm_config, cache);
    let module = match into_vm_result(module) {
        Ok(x) => x,
        Err(err) => return (None, Some(err)),
    };
    let mut memory = WasmerMemory::new(
        wasm_config.limit_config.initial_memory_pages,
        wasm_config.limit_config.max_memory_pages,
    )
    .expect("Cannot create memory for a contract call");
    // Note that we don't clone the actual backing memory, just increase the RC.
    let memory_copy = memory.clone();

    let mut logic = VMLogic::new_with_protocol_version(
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        &mut memory,
        current_protocol_version,
    );

    // TODO: remove, as those costs are incorrectly computed, and we shall account it on deployment.
    if logic.add_contract_compile_fee(code.code().len() as u64).is_err() {
        return (
            Some(logic.outcome()),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(
                near_vm_errors::HostError::GasExceeded,
            ))),
        );
    }

    let import_object = imports::build_wasmer(memory_copy, &mut logic, current_protocol_version);

    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    let err = run_method(&module, &import_object, method_name).err();
    (Some(logic.outcome()), err)
}

fn run_method(module: &Module, import: &ImportObject, method_name: &str) -> Result<(), VMError> {
    let _span = tracing::debug_span!(target: "vm", "run_method").entered();

    let instance = {
        let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
        module.instantiate(import).map_err(|err| err.into_vm_error())?
    };

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
        instance.call(&method_name, &[]).map_err(|err| err.into_vm_error())?;
    }

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/drop_instance").entered();
        drop(instance)
    }

    Ok(())
}

pub(crate) fn run_wasmer0_module<'a>(
    module: Module,
    memory: &mut WasmerMemory,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    current_protocol_version: ProtocolVersion,
) -> (Option<VMOutcome>, Option<VMError>) {
    if method_name.is_empty() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName,
            ))),
        );
    }
    // Note that we don't clone the actual backing memory, just increase the RC.
    let memory_copy = memory.clone();

    let mut logic = VMLogic::new_with_protocol_version(
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        memory,
        current_protocol_version,
    );

    let import_object = imports::build_wasmer(memory_copy, &mut logic, current_protocol_version);

    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    let err = run_method(&module, &import_object, method_name).err();
    (Some(logic.outcome()), err)
}

pub fn compile_wasmer0_module(code: &[u8]) -> bool {
    wasmer_runtime::compile(code).is_ok()
}

pub(crate) fn wasmer0_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    42
}
