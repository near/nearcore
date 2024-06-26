use crate::cache::{CompiledContract, CompiledContractInfo, ContractRuntimeCache};
use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMRunnerError, WasmTrap,
};
use crate::logic::types::PromiseResult;
use crate::logic::{ExecutionResultState, External, VMContext, VMLogic, VMLogicError, VMOutcome};
use crate::logic::{MemSlice, MemoryLike};
use crate::prepare;
use crate::runner::VMResult;
use crate::{get_contract_cache_key, imports, ContractCode};
use near_parameters::vm::{Config, VMKind};
use near_parameters::RuntimeFeesConfig;
use std::borrow::Cow;
use std::ffi::c_void;
use std::sync::Arc;
use wasmer_runtime::units::Pages;
use wasmer_runtime::wasm::MemoryDescriptor;
use wasmer_runtime::Memory;
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

trait IntoVMError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError>;
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
                        signal, address
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

pub struct WasmerMemory(Memory);

impl WasmerMemory {
    pub fn new(initial_memory_pages: u32, max_memory_pages: u32) -> Self {
        WasmerMemory(
            Memory::new(
                MemoryDescriptor::new(
                    Pages(initial_memory_pages),
                    Some(Pages(max_memory_pages)),
                    false,
                )
                .unwrap(),
            )
            .expect("TODO creating memory cannot fail"),
        )
    }

    pub fn clone(&self) -> Memory {
        self.0.clone()
    }
}

impl WasmerMemory {
    fn with_memory<F, T>(&self, offset: u64, len: usize, func: F) -> Result<T, ()>
    where
        F: FnOnce(core::slice::Iter<'_, std::cell::Cell<u8>>) -> T,
    {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(len).ok_or(())?;
        self.0.view().get(start..end).map(|mem| func(mem.iter())).ok_or(())
    }
}

impl MemoryLike for WasmerMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        self.with_memory(slice.ptr, slice.len()?, |_| ())
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        self.with_memory(slice.ptr, slice.len()?, |mem| {
            Cow::Owned(mem.map(core::cell::Cell::get).collect())
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        self.with_memory(offset, buffer.len(), |mem| {
            buffer.iter_mut().zip(mem).for_each(|(dst, src)| *dst = src.get());
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        self.with_memory(offset, buffer.len(), |mem| {
            mem.zip(buffer.iter()).for_each(|(dst, src)| dst.set(*src));
        })
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
    config: Arc<Config>,
}

impl Wasmer0VM {
    pub(crate) fn new(config: Arc<Config>) -> Self {
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
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let module_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(*code.hash(), &self.config);

        if let Some(cache) = cache {
            let record = CompiledContractInfo {
                wasm_bytes: code.code().len() as u64,
                compiled: match &module_or_error {
                    Ok(module) => {
                        let code = module
                            .cache()
                            .and_then(|it| it.serialize())
                            .map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
                        CompiledContract::Code(code)
                    }
                    Err(err) => CompiledContract::CompileModuleError(err.clone()),
                },
            };
            cache.put(&key, record).map_err(CacheError::WriteError)?;
        }

        Ok(module_or_error)
    }

    pub(crate) fn compile_and_load(
        &self,
        code: &ContractCode,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> VMResult<Result<wasmer_runtime::Module, CompilationError>> {
        let _span = tracing::debug_span!(target: "vm", "Wasmer0VM::compile_and_load").entered();

        let key = get_contract_cache_key(*code.hash(), &self.config);

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
                    Some(CompiledContractInfo {
                        compiled: CompiledContract::CompileModuleError(err),
                        ..
                    }) => return Ok(Err(err)),
                    Some(CompiledContractInfo {
                        compiled: CompiledContract::Code(serialized_module),
                        ..
                    }) => {
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
        method_name: &str,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        promise_results: std::sync::Arc<[PromiseResult]>,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Result<VMOutcome, VMRunnerError> {
        let Some(code) = ext.get_contract() else {
            return Err(VMRunnerError::ContractCodeNotPresent);
        };
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

        let mut execution_state = ExecutionResultState::new(&context, Arc::clone(&self.config));
        let result =
            execution_state.before_loading_executable(method_name, code.code().len() as u64);
        if let Err(e) = result {
            return Ok(VMOutcome::abort(execution_state, e));
        }

        // TODO: consider using get_module() here, once we'll go via deployment path.
        let module = self.compile_and_load(&code, cache)?;
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
                return Ok(VMOutcome::abort(
                    execution_state,
                    FunctionCallError::CompilationError(err),
                ))
            }
        };

        let result = execution_state.after_loading_executable(code.code().len() as u64);
        if let Err(e) = result {
            return Ok(VMOutcome::abort(execution_state, e));
        }
        if let Err(e) = check_method(&module, method_name) {
            return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(execution_state, e));
        }

        let memory = WasmerMemory::new(
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        );
        // Note that we don't clone the actual backing memory, just increase the RC.
        let memory_copy = memory.clone();
        let mut logic =
            VMLogic::new(ext, context, fees_config, promise_results, execution_state, memory);
        let import_object = build_imports(memory_copy, &self.config, &mut logic);
        match run_method(&module, &import_object, method_name)? {
            Ok(()) => Ok(VMOutcome::ok(logic.result_state)),
            Err(err) => Ok(VMOutcome::abort(logic.result_state, err)),
        }
    }

    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<
        Result<ContractPrecompilatonResult, CompilationError>,
        crate::logic::errors::CacheError,
    > {
        Ok(self
            .compile_and_cache(code, Some(cache))?
            .map(|_| ContractPrecompilatonResult::ContractCompiled))
    }
}

#[derive(Clone, Copy)]
struct ImportReference(pub *mut c_void);
unsafe impl Send for ImportReference {}
unsafe impl Sync for ImportReference {}

pub(crate) fn build_imports(
    memory: wasmer_runtime::memory::Memory,
    config: &Config,
    logic: &mut VMLogic<'_>,
) -> wasmer_runtime::ImportObject {
    let raw_ptr = logic as *mut _ as *mut c_void;
    let import_reference = ImportReference(raw_ptr);
    let mut import_object = wasmer_runtime::ImportObject::new_with_data(move || {
        let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
        ({ import_reference }.0, dtor)
    });

    let mut ns_internal = wasmer_runtime_core::import::Namespace::new();
    let mut ns_env = wasmer_runtime_core::import::Namespace::new();
    ns_env.insert("memory", memory);

    macro_rules! add_import {
            (
              $mod:ident / $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
            ) => {
                #[allow(unused_parens)]
                fn $name( ctx: &mut wasmer_runtime::Ctx, $( $arg_name: $arg_type ),* ) -> Result<($( $returns ),*), VMLogicError> {
                    const TRACE: bool = $crate::imports::should_trace_host_function(stringify!($name));
                    let _span = TRACE.then(|| {
                        tracing::trace_span!(target: "vm::host_function", stringify!($name)).entered()
                    });
                    let logic: &mut VMLogic<'_> = unsafe { &mut *(ctx.data as *mut VMLogic<'_>) };
                    logic.$func( $( $arg_name, )* )
                }

                match stringify!($mod) {
                    "env" => ns_env.insert(stringify!($name), wasmer_runtime::func!($name)),
                    "internal" => ns_internal.insert(stringify!($name), wasmer_runtime::func!($name)),
                    _ => unimplemented!(),
                }
            };
        }
    imports::for_each_available_import!(config, add_import);

    import_object.register("env", ns_env);
    import_object.register("internal", ns_internal);
    import_object
}

#[test]
fn test_memory_like() {
    crate::logic::test_utils::test_memory_like(|| Box::new(WasmerMemory::new(1, 1)));
}
