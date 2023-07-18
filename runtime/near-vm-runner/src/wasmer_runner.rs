use crate::errors::{ContractPrecompilatonResult, IntoVMError};
use crate::internal::VMKind;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMRunnerError, WasmTrap,
};
use crate::logic::types::PromiseResult;
use crate::logic::{
    CompiledContract, CompiledContractCache, External, VMContext, VMLogic, VMLogicError, VMOutcome,
};
use crate::memory::WasmerMemory;
use crate::prepare;
use crate::runner::VMResult;
use crate::{get_contract_cache_key, imports};
use near_primitives_core::config::VMConfig;
use near_primitives_core::contract::ContractCode;
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_primitives_core::types::ProtocolVersion;
use wasmer_runtime::{ImportObject, Module};

fn check_method(module: &Module, method_name: &str) -> Result<(), FunctionCallError> {
    let info = module.info();
    use wasmer_runtime_core::module::ExportIndex::Func;
    if let Some(Func(index)) = info.exports.map.get(method_name) {
        let func = info.func_assoc.get(*index).unwrap();
        let sig = info.signatures.get(*func).unwrap();
        if sig.params().is_empty() && sig.returns().is_empty() {
            Ok(())
        } else {
            Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature))
        }
    } else {
        Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound))
    }
}

impl IntoVMError for wasmer_runtime::error::Error {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        use wasmer_runtime::error::Error;
        match self {
            Error::CompileError(err) => {
                Ok(FunctionCallError::CompilationError(CompilationError::WasmerCompileError {
                    msg: err.to_string(),
                }))
            }
            Error::LinkError(err) => Ok(FunctionCallError::LinkError {
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
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        use wasmer_runtime::error::CallError;
        match self {
            CallError::Resolve(err) => err.into_vm_error(),
            CallError::Runtime(err) => err.into_vm_error(),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::ResolveError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        use wasmer_runtime::error::ResolveError as WasmerResolveError;
        Ok(match self {
            WasmerResolveError::Signature { .. } => {
                FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature)
            }
            WasmerResolveError::ExportNotFound { .. } => {
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound)
            }
            WasmerResolveError::ExportWrongType { .. } => {
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound)
            }
        })
    }
}

impl IntoVMError for wasmer_runtime::error::RuntimeError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        use wasmer_runtime::error::{InvokeError, RuntimeError};
        match self {
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
                    Ok(FunctionCallError::WasmTrap(WasmTrap::Unreachable))
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
                InvokeError::TrapCode { code, srcloc: _ } => Ok(match code {
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
                }),
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
            RuntimeError::User(data) => match data.downcast::<VMLogicError>() {
                Ok(err) => (*err).try_into(),
                Err(data) => panic!(
                    "Bad error case! Output is non-deterministic {:?} {:?}",
                    data.type_id(),
                    RuntimeError::User(data).to_string()
                ),
            },
        }
    }
}

fn run_method(
    module: &Module,
    import: &ImportObject,
    method_name: &str,
) -> Result<Result<(), FunctionCallError>, VMRunnerError> {
    let _span = tracing::debug_span!(target: "vm", "run_method").entered();

    let instance = {
        let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
        match module.instantiate(import) {
            Ok(instance) => instance,
            Err(err) => {
                let guest_aborted = err.into_vm_error()?;
                return Ok(Err(guest_aborted));
            }
        }
    };

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
        if let Err(err) = instance.call(method_name, &[]) {
            let guest_aborted = err.into_vm_error()?;
            return Ok(Err(guest_aborted));
        }
    }

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/drop_instance").entered();
        drop(instance)
    }

    Ok(Ok(()))
}

pub(crate) fn wasmer0_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    42
}

pub(crate) struct Wasmer0VM {
    config: VMConfig,
}

impl Wasmer0VM {
    pub(crate) fn new(config: VMConfig) -> Self {
        Self { config }
    }

    pub(crate) fn compile_uncached(
        &self,
        code: &ContractCode,
    ) -> Result<wasmer_runtime::Module, CompilationError> {
        let _span = tracing::debug_span!(target: "vm", "Wasmer0VM::compile_uncached").entered();
        let prepared_code = prepare::prepare_contract(code.code(), &self.config, VMKind::Wasmer0)
            .map_err(CompilationError::PrepareError)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| match err {
            wasmer_runtime::error::CompileError::ValidationError { .. } => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            // NOTE: Despite the `InternalError` name, this failure occurs if
            // the input `code` is invalid wasm.
            wasmer_runtime::error::CompileError::InternalError { .. } => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
        })
    }

    pub(crate) fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let module_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(code, VMKind::Wasmer0, &self.config);

        if let Some(cache) = cache {
            let record = match &module_or_error {
                Ok(module) => {
                    let code = module
                        .cache()
                        .and_then(|it| it.serialize())
                        .map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
                    CompiledContract::Code(code)
                }
                Err(err) => CompiledContract::CompileModuleError(err.clone()),
            };
            cache.put(&key, record).map_err(CacheError::WriteError)?;
        }

        Ok(module_or_error)
    }

    pub(crate) fn compile_and_load(
        &self,
        code: &ContractCode,
        cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult<Result<wasmer_runtime::Module, CompilationError>> {
        let _span = tracing::debug_span!(target: "vm", "Wasmer0VM::compile_and_load").entered();

        let key = get_contract_cache_key(code, VMKind::Wasmer0, &self.config);

        let compile_or_read_from_cache =
            || -> VMResult<Result<wasmer_runtime::Module, CompilationError>> {
                let _span =
                    tracing::debug_span!(target: "vm", "Wasmer0VM::compile_or_read_from_cache")
                        .entered();

                let cache_record = cache
                    .map(|cache| cache.get(&key))
                    .transpose()
                    .map_err(CacheError::ReadError)?
                    .flatten();

                let stored_module: Option<wasmer_runtime::Module> = match cache_record {
                    None => None,
                    Some(CompiledContract::CompileModuleError(err)) => return Ok(Err(err)),
                    Some(CompiledContract::Code(serialized_module)) => {
                        let _span =
                            tracing::debug_span!(target: "vm", "Wasmer0VM::read_from_cache")
                                .entered();
                        let artifact = wasmer_runtime_core::cache::Artifact::deserialize(
                            serialized_module.as_slice(),
                        )
                        .map_err(|_e| CacheError::DeserializationError)?;
                        unsafe {
                            let compiler = wasmer_runtime::compiler_for_backend(
                                wasmer_runtime::Backend::Singlepass,
                            )
                            .unwrap();
                            let module = wasmer_runtime_core::load_cache_with(
                                artifact,
                                compiler.as_ref(),
                            )
                            .map_err(|err| VMRunnerError::LoadingError(format!("{err:?}")))?;
                            Some(module)
                        }
                    }
                };

                Ok(match stored_module {
                    Some(it) => Ok(it),
                    None => self.compile_and_cache(code, cache)?,
                })
            };

        return compile_or_read_from_cache();
    }
}

impl crate::runner::VM for Wasmer0VM {
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
    ) -> Result<VMOutcome, VMRunnerError> {
        if !cfg!(target_arch = "x86") && !cfg!(target_arch = "x86_64") {
            // TODO(#1940): Remove once NaN is standardized by the VM.
            panic!(
                "Execution of smart contracts is only supported for x86 and x86_64 CPU \
                 architectures."
            );
        }
        #[cfg(not(feature = "no_cpu_compatibility_checks"))]
        if !is_x86_feature_detected!("avx") {
            panic!("AVX support is required in order to run Wasmer VM Singlepass backend.");
        }

        let mut memory = WasmerMemory::new(
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        );
        // Note that we don't clone the actual backing memory, just increase the RC.
        let memory_copy = memory.clone();

        let mut logic = VMLogic::new_with_protocol_version(
            ext,
            context,
            &self.config,
            fees_config,
            promise_results,
            &mut memory,
            current_protocol_version,
        );

        let result = logic.before_loading_executable(
            method_name,
            current_protocol_version,
            code.code().len(),
        );
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        // TODO: consider using get_module() here, once we'll go via deployment path.
        let module = self.compile_and_load(code, cache)?;
        let module = match module {
            Ok(x) => x,
            // Note on backwards-compatibility: This error used to be an error
            // without result, later refactored to NOP outcome. Now this returns
            // an actual outcome, including gas costs that occurred before this
            // point. This is compatible with earlier versions because those
            // version do not have gas costs before reaching this code. (Also
            // see `test_old_fn_loading_behavior_preserved` for a test that
            // verifies future changes do not counteract this assumption.)
            Err(err) => {
                return Ok(VMOutcome::abort(logic, FunctionCallError::CompilationError(err)))
            }
        };

        let result = logic.after_loading_executable(current_protocol_version, code.code().len());
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        let import_object =
            imports::wasmer::build(memory_copy, &mut logic, current_protocol_version);

        if let Err(e) = check_method(&module, method_name) {
            return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                logic,
                e,
                current_protocol_version,
            ));
        }

        match run_method(&module, &import_object, method_name)? {
            Ok(()) => Ok(VMOutcome::ok(logic)),
            Err(err) => Ok(VMOutcome::abort(logic, err)),
        }
    }

    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn CompiledContractCache,
    ) -> Result<
        Result<ContractPrecompilatonResult, CompilationError>,
        crate::logic::errors::CacheError,
    > {
        Ok(self
            .compile_and_cache(code, Some(cache))?
            .map(|_| ContractPrecompilatonResult::ContractCompiled))
    }
}
