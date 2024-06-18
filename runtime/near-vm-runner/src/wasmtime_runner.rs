use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMLogicError,
    VMRunnerError, WasmTrap,
};
use crate::logic::types::PromiseResult;
use crate::logic::Config;
use crate::logic::{External, MemSlice, MemoryLike, VMContext, VMLogic, VMOutcome};
use crate::runner::VMResult;
use crate::{
    get_contract_cache_key, imports, prepare, CompiledContract, CompiledContractInfo, ContractCode,
    ContractRuntimeCache, NoContractRuntimeCache,
};
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;
use std::borrow::Cow;
use std::cell::{RefCell, UnsafeCell};
use std::ffi::c_void;
use wasmtime::ExternType::Func;
use wasmtime::{Engine, Linker, Memory, MemoryType, Module, Store};

type Caller = wasmtime::Caller<'static, ()>;
thread_local! {
    pub(crate) static CALLER: RefCell<Option<Caller>> = const { RefCell::new(None) };
}
pub struct WasmtimeMemory(Memory);

impl WasmtimeMemory {
    pub fn new(
        store: &mut Store<()>,
        initial_memory_bytes: u32,
        max_memory_bytes: u32,
    ) -> Result<Self, FunctionCallError> {
        Ok(WasmtimeMemory(
            Memory::new(store, MemoryType::new(initial_memory_bytes, Some(max_memory_bytes)))
                .map_err(|_| PrepareError::Memory)?,
        ))
    }
}

fn with_caller<T>(func: impl FnOnce(&mut Caller) -> T) -> T {
    CALLER.with(|caller| func(caller.borrow_mut().as_mut().unwrap()))
}

impl MemoryLike for WasmtimeMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        let end = slice.end::<usize>()?;
        if end <= with_caller(|caller| self.0.data_size(caller)) {
            Ok(())
        } else {
            Err(())
        }
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        let range = slice.range::<usize>()?;
        with_caller(|caller| {
            self.0.data(caller).get(range).map(|slice| Cow::Owned(slice.to_vec())).ok_or(())
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        with_caller(|caller| {
            let memory = self.0.data(caller).get(start..end).ok_or(())?;
            buffer.copy_from_slice(memory);
            Ok(())
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        with_caller(|caller| {
            let memory = self.0.data_mut(caller).get_mut(start..end).ok_or(())?;
            memory.copy_from_slice(buffer);
            Ok(())
        })
    }
}

trait IntoVMError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError>;
}

impl IntoVMError for anyhow::Error {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        let cause = self.root_cause();
        if let Some(container) = cause.downcast_ref::<ErrorContainer>() {
            use {VMLogicError as LE, VMRunnerError as RE};
            return match container.take() {
                Some(LE::HostError(h)) => Ok(FunctionCallError::HostError(h)),
                Some(LE::ExternalError(s)) => Err(RE::ExternalError(s)),
                Some(LE::InconsistentStateError(e)) => Err(RE::InconsistentStateError(e)),
                None => panic!("error has already been taken out of the container?!"),
            };
        }
        if let Some(trap) = cause.downcast_ref::<wasmtime::Trap>() {
            use wasmtime::Trap as T;
            let nondeterministic_message = 'nondet: {
                return Ok(FunctionCallError::WasmTrap(match *trap {
                    T::StackOverflow => WasmTrap::StackOverflow,
                    T::MemoryOutOfBounds => WasmTrap::MemoryOutOfBounds,
                    T::TableOutOfBounds => WasmTrap::MemoryOutOfBounds,
                    T::IndirectCallToNull => WasmTrap::IndirectCallToNull,
                    T::BadSignature => WasmTrap::IncorrectCallIndirectSignature,
                    T::IntegerOverflow => WasmTrap::IllegalArithmetic,
                    T::IntegerDivisionByZero => WasmTrap::IllegalArithmetic,
                    T::BadConversionToInteger => WasmTrap::IllegalArithmetic,
                    T::UnreachableCodeReached => WasmTrap::Unreachable,
                    T::Interrupt => break 'nondet "interrupt",
                    T::HeapMisaligned => break 'nondet "heap misaligned",
                    t => {
                        return Err(VMRunnerError::WasmUnknownError {
                            debug_message: format!("unhandled trap type: {:?}", t),
                        })
                    }
                }));
            };
            return Err(VMRunnerError::Nondeterministic(nondeterministic_message.into()));
        }
        Ok(FunctionCallError::LinkError { msg: format!("{:#?}", cause) })
    }
}

#[allow(clippy::needless_pass_by_ref_mut)]
pub fn get_engine(config: &wasmtime::Config) -> Engine {
    Engine::new(config).unwrap()
}

pub(crate) fn default_wasmtime_config(config: &Config) -> wasmtime::Config {
    let features =
        crate::features::WasmFeatures::from(config.limit_config.contract_prepare_version);
    let mut config = wasmtime::Config::from(features);
    config.max_wasm_stack(1024 * 1024 * 1024); // wasm stack metering is implemented by instrumentation, we don't want wasmtime to trap before that
    config
}

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}

pub(crate) struct WasmtimeVM {
    config: Config,
    engine: wasmtime::Engine,
}

impl WasmtimeVM {
    pub(crate) fn new(config: Config) -> Self {
        Self { engine: get_engine(&default_wasmtime_config(&config)), config }
    }

    #[tracing::instrument(target = "vm", level = "debug", "WasmtimeVM::compile_uncached", skip_all)]
    fn compile_uncached(&self, code: &ContractCode) -> Result<Vec<u8>, CompilationError> {
        let start = std::time::Instant::now();
        let prepared_code = prepare::prepare_contract(code.code(), &self.config, VMKind::Wasmtime)
            .map_err(CompilationError::PrepareError)?;
        let serialized = self.engine.precompile_module(&prepared_code).map_err(|err| {
            tracing::error!(?err, "wasmtime failed to compile the prepared code (this is defense-in-depth, the error was recovered from but should be reported to the developers)");
            CompilationError::WasmtimeCompileError { msg: err.to_string() }
        });
        crate::metrics::compilation_duration(VMKind::Wasmtime, start.elapsed());
        serialized
    }

    fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<Result<Vec<u8>, CompilationError>, CacheError> {
        let serialized_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(*code.hash(), &self.config);
        let record = CompiledContractInfo {
            wasm_bytes: code.code().len() as u64,
            compiled: match &serialized_or_error {
                Ok(serialized) => CompiledContract::Code(serialized.clone()),
                Err(err) => CompiledContract::CompileModuleError(err.clone()),
            },
        };
        cache.put(&key, record).map_err(CacheError::WriteError)?;
        Ok(serialized_or_error)
    }

    fn with_compiled_and_loaded(
        &self,
        cache: &dyn ContractRuntimeCache,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        method_name: &str,
        closure: impl FnOnce(VMLogic, Memory, Store<()>, Module) -> Result<VMOutcome, VMRunnerError>,
    ) -> VMResult<VMOutcome> {
        type MemoryCacheType = (u64, Result<Module, CompilationError>);
        let to_any = |v: MemoryCacheType| -> Box<dyn std::any::Any + Send> { Box::new(v) };
        let (wasm_bytes, module_result) = cache.memory_cache().try_lookup(
            code_hash,
            || match code {
                None => {
                    let key = get_contract_cache_key(code_hash, &self.config);
                    let cache_record = cache.get(&key).map_err(CacheError::ReadError)?;
                    let Some(code) = cache_record else {
                        return Err(VMRunnerError::CacheError(CacheError::ReadError(
                            std::io::Error::from(std::io::ErrorKind::NotFound),
                        )));
                    };
                    match &code.compiled {
                        CompiledContract::CompileModuleError(err) => {
                            Ok::<_, VMRunnerError>(to_any((code.wasm_bytes, Err(err.clone()))))
                        }
                        CompiledContract::Code(serialized_module) => {
                            unsafe {
                                // (UN-)SAFETY: the `serialized_module` must have been produced by
                                // a prior call to `serialize`.
                                //
                                // In practice this is not necessarily true. One could have
                                // forgotten to change the cache key when upgrading the version of
                                // the near_vm library or the database could have had its data
                                // corrupted while at rest.
                                //
                                // There should definitely be some validation in near_vm to ensure
                                // we load what we think we load.
                                let module = Module::deserialize(&self.engine, &serialized_module)
                                    .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
                                Ok(to_any((code.wasm_bytes, Ok(module))))
                            }
                        }
                    }
                }
                Some(code) => Ok(to_any((
                    code.code().len() as u64,
                    match self.compile_and_cache(code, cache)? {
                        Ok(serialized_module) => Ok(unsafe {
                            Module::deserialize(&self.engine, serialized_module)
                                .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?
                        }),
                        Err(err) => Err(err),
                    },
                ))),
            },
            move |value| {
                let &(wasm_bytes, ref downcast) = value
                    .downcast_ref::<MemoryCacheType>()
                    .expect("downcast should always succeed");

                (wasm_bytes, downcast.clone())
            },
        )?;

        let mut store = Store::new(&self.engine, ());
        let mut memory = WasmtimeMemory::new(
            &mut store,
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        )
        .unwrap();
        let memory_copy = memory.0;
        let mut logic =
            VMLogic::new(ext, context, &self.config, fees_config, promise_results, &mut memory);
        let result = logic.before_loading_executable(method_name, wasm_bytes);
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }
        match module_result {
            Ok(module) => {
                let result = logic.after_loading_executable(wasm_bytes);
                if let Err(e) = result {
                    return Ok(VMOutcome::abort(logic, e));
                }
                closure(logic, memory_copy, store, module)
            }
            Err(e) => Ok(VMOutcome::abort(logic, FunctionCallError::CompilationError(e))),
        }
    }
}

impl crate::runner::VM for WasmtimeVM {
    fn run(
        &self,
        code_hash: CryptoHash,
        code: Option<&ContractCode>,
        method_name: &str,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Result<VMOutcome, VMRunnerError> {
        let cache = cache.unwrap_or(&NoContractRuntimeCache);
        self.with_compiled_and_loaded(
            code_hash,
            code,
            cache,
            ext,
            context,
            fees_config,
            promise_results,
            method_name,
            |mut logic, memory, mut store, module| {
                let mut linker = Linker::new(&(&self.engine));
                link(&mut linker, memory, &store, &mut logic);
                match module.get_export(method_name) {
                    Some(export) => match export {
                        Func(func_type) => {
                            if func_type.params().len() != 0 || func_type.results().len() != 0 {
                                let err = FunctionCallError::MethodResolveError(
                                    MethodResolveError::MethodInvalidSignature,
                                );
                                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                                    logic, err,
                                ));
                            }
                        }
                        _ => {
                            return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                                logic,
                                FunctionCallError::MethodResolveError(
                                    MethodResolveError::MethodNotFound,
                                ),
                            ));
                        }
                    },
                    None => {
                        return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                            logic,
                            FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodNotFound,
                            ),
                        ));
                    }
                }
                match linker.instantiate(&mut store, &module) {
                    Ok(instance) => match instance.get_func(&mut store, method_name) {
                        Some(func) => match func.typed::<(), ()>(&mut store) {
                            Ok(run) => match run.call(&mut store, ()) {
                                Ok(_) => Ok(VMOutcome::ok(logic)),
                                Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
                            },
                            Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
                        },
                        None => {
                            return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                                logic,
                                FunctionCallError::MethodResolveError(
                                    MethodResolveError::MethodNotFound,
                                ),
                            ));
                        }
                    },
                    Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
                }
            },
        )
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
            .compile_and_cache(code, cache)?
            .map(|_| ContractPrecompilatonResult::ContractCompiled))
    }
}

/// This is a container from which an error can be taken out by value. This is necessary as
/// `anyhow` does not really give any opportunity to grab causes by value and the VM Logic
/// errors end up a couple layers deep in a causal chain.
#[derive(Debug)]
pub(crate) struct ErrorContainer(std::sync::Mutex<Option<VMLogicError>>);
impl ErrorContainer {
    pub(crate) fn take(&self) -> Option<VMLogicError> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        guard.take()
    }
}
impl std::error::Error for ErrorContainer {}
impl std::fmt::Display for ErrorContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VMLogic error occurred and is now stored in an opaque storage container")
    }
}

thread_local! {
    static CALLER_CONTEXT: UnsafeCell<*mut c_void> = const { UnsafeCell::new(core::ptr::null_mut()) };
}

fn link<'a, 'b>(
    linker: &mut wasmtime::Linker<()>,
    memory: wasmtime::Memory,
    store: &wasmtime::Store<()>,
    logic: &'a mut VMLogic<'b>,
) {
    // Unfortunately, due to the Wasmtime implementation we have to do tricks with the
    // lifetimes of the logic instance and pass raw pointers here.
    // FIXME(nagisa): I believe this is no longer required, we just need to look at this code
    // again.
    let raw_logic = logic as *mut _ as *mut c_void;
    CALLER_CONTEXT.with(|caller_context| unsafe { *caller_context.get() = raw_logic });
    linker.define(store, "env", "memory", memory).expect("cannot define memory");

    macro_rules! add_import {
        (
          $mod:ident / $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
        ) => {
            #[allow(unused_parens)]
            fn $name(caller: wasmtime::Caller<'_, ()>, $( $arg_name: $arg_type ),* ) -> anyhow::Result<($( $returns ),*)> {
                const TRACE: bool = imports::should_trace_host_function(stringify!($name));
                let _span = TRACE.then(|| {
                    tracing::trace_span!(target: "vm::host_function", stringify!($name)).entered()
                });
                // the below is bad. don't do this at home. it probably works thanks to the exact way the system is setup.
                // Thanksfully, this doesn't run in production, and hopefully should be possible to remove before we even
                // consider doing so.
                let data = CALLER_CONTEXT.with(|caller_context| {
                    unsafe {
                        *caller_context.get()
                    }
                });
                unsafe {
                    // Transmute the lifetime of caller so it's possible to put it in a thread-local.
                    #[allow(clippy::missing_transmute_annotations)]
                    crate::wasmtime_runner::CALLER.with(|runner_caller| *runner_caller.borrow_mut() = std::mem::transmute(caller));
                }
                let logic: &mut VMLogic<'_> = unsafe { &mut *(data as *mut VMLogic<'_>) };
                match logic.$func( $( $arg_name as $arg_type, )* ) {
                    Ok(result) => Ok(result as ($( $returns ),* ) ),
                    Err(err) => {
                        Err(ErrorContainer(std::sync::Mutex::new(Some(err))).into())
                    }
                }
            }

            linker.func_wrap(stringify!($mod), stringify!($name), $name).expect("cannot link external");
        };
    }
    imports::for_each_available_import!(logic.config, add_import);
}
