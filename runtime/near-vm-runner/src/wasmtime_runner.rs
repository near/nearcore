use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, PrepareError,
    VMLogicError, VMRunnerError, WasmTrap,
};
use crate::logic::{Config, ExecutionResultState, GasCounter};
use crate::logic::{External, MemSlice, MemoryLike, VMContext, VMLogic, VMOutcome};
use crate::runner::VMResult;
use crate::{
    CompiledContract, CompiledContractInfo, Contract, ContractCode, ContractRuntimeCache,
    NoContractRuntimeCache, get_contract_cache_key, imports, lazy_drop, prepare,
};
use core::mem::transmute;
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::Arc;
use wasmtime::{Engine, ExternType, Instance, Linker, Memory, MemoryType, Module, Store, Strategy};

type Caller = wasmtime::Caller<'static, Ctx>;

#[derive(Default)]
pub struct Ctx {
    logic: Option<VMLogic<'static>>,
    caller: Arc<RefCell<Option<Caller>>>,
}

#[derive(Clone)]
pub struct WasmtimeMemory {
    memory: Memory,
    caller: Arc<RefCell<Option<Caller>>>,
}

impl WasmtimeMemory {
    pub fn new(
        caller: Arc<RefCell<Option<Caller>>>,
        store: &mut Store<Ctx>,
        initial_memory_bytes: u32,
        max_memory_bytes: u32,
    ) -> Result<Self, FunctionCallError> {
        Ok(WasmtimeMemory {
            memory: Memory::new(
                store,
                MemoryType::new(initial_memory_bytes, Some(max_memory_bytes)),
            )
            .map_err(|_| PrepareError::Memory)?,
            caller,
        })
    }
}

impl WasmtimeMemory {
    fn with_caller<T>(&self, func: impl FnOnce(&mut Caller) -> T) -> T {
        let mut caller = self.caller.borrow_mut();
        func(caller.as_mut().expect("caller missing"))
    }
}

impl MemoryLike for WasmtimeMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        let end = slice.end::<usize>()?;
        if end <= self.with_caller(|caller| self.memory.data_size(caller)) {
            Ok(())
        } else {
            Err(())
        }
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        let range = slice.range::<usize>()?;
        self.with_caller(|caller| {
            self.memory.data(caller).get(range).map(|slice| Cow::Owned(slice.to_vec())).ok_or(())
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        self.with_caller(|caller| {
            let memory = self.memory.data(caller).get(start..end).ok_or(())?;
            buffer.copy_from_slice(memory);
            Ok(())
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        self.with_caller(|caller| {
            let memory = self.memory.data_mut(caller).get_mut(start..end).ok_or(())?;
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
                        });
                    }
                }));
            };
            return Err(VMRunnerError::Nondeterministic(nondeterministic_message.into()));
        }
        // FIXME: this can blow up in size and would get stored in the storage in case this was a
        // production runtime. Something more proper should be done here.
        Ok(FunctionCallError::LinkError { msg: format!("{:?}", cause) })
    }
}

#[allow(clippy::needless_pass_by_ref_mut)]
pub fn get_engine(config: &wasmtime::Config) -> Engine {
    Engine::new(config).expect("failed to construct engine")
}

pub(crate) fn default_wasmtime_config(c: &Config) -> wasmtime::Config {
    let features = crate::features::WasmFeatures::new(c);

    // NOTE: Configuration values are based on:
    // - https://docs.wasmtime.dev/examples-fast-execution.html
    // - https://docs.wasmtime.dev/examples-fast-instantiation.html

    let mut config = wasmtime::Config::from(features);
    config
        // From official documentation:
        // > Note that systems loading many modules may wish to disable this
        // > configuration option instead of leaving it on-by-default.
        // > Some platforms exhibit quadratic behavior when registering/unregistering
        // > unwinding information which can greatly slow down the module loading/unloading process.
        // https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html#method.native_unwind_info
        .native_unwind_info(false)
        .wasm_backtrace(false)
        // Enable copy-on-write heap images.
        .memory_init_cow(true)
        // wasm stack metering is implemented by instrumentation, we don't want wasmtime to trap before that
        .max_wasm_stack(1024 * 1024 * 1024)
        // enable the Cranelift optimizing compiler.
        .strategy(Strategy::Cranelift)
        // Enable signals-based traps. This is required to elide explicit bounds-checking.
        .signals_based_traps(true)
        // Configure linear memories such that explicit bounds-checking can be elided.
        .memory_reservation(1 << 32)
        .memory_guard_size(1 << 32)
        .cranelift_nan_canonicalization(true);
    config
}

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}

pub(crate) struct WasmtimeVM {
    config: Arc<Config>,
    engine: wasmtime::Engine,
}

impl WasmtimeVM {
    pub(crate) fn new(config: Arc<Config>) -> Self {
        Self { engine: get_engine(&default_wasmtime_config(&config)), config }
    }

    #[tracing::instrument(target = "vm", level = "debug", "WasmtimeVM::compile_uncached", skip_all)]
    pub(crate) fn compile_uncached(
        &self,
        code: &ContractCode,
    ) -> Result<Vec<u8>, CompilationError> {
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
        contract: &dyn Contract,
        mut gas_counter: GasCounter,
        method: &str,
        closure: impl FnOnce(GasCounter, Module) -> VMResult<PreparedContract>,
    ) -> VMResult<PreparedContract> {
        type MemoryCacheType = (u64, Result<Module, CompilationError>);
        let to_any = |v: MemoryCacheType| -> Box<dyn std::any::Any + Send> { Box::new(v) };
        let key = get_contract_cache_key(contract.hash(), &self.config);
        let (wasm_bytes, module_result) = cache.memory_cache().try_lookup(
            key,
            || {
                let cache_record = cache.get(&key).map_err(CacheError::ReadError)?;
                let Some(compiled_contract_info) = cache_record else {
                    let Some(code) = contract.get_code() else {
                        return Err(VMRunnerError::ContractCodeNotPresent);
                    };
                    return Ok(to_any((
                        code.code().len() as u64,
                        match self.compile_and_cache(&code, cache)? {
                            Ok(serialized_module) => Ok(unsafe {
                                Module::deserialize(&self.engine, serialized_module)
                                    .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?
                            }),
                            Err(err) => Err(err),
                        },
                    )));
                };
                match &compiled_contract_info.compiled {
                    CompiledContract::CompileModuleError(err) => Ok::<_, VMRunnerError>(to_any((
                        compiled_contract_info.wasm_bytes,
                        Err(err.clone()),
                    ))),
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
                            Ok(to_any((compiled_contract_info.wasm_bytes, Ok(module))))
                        }
                    }
                }
            },
            move |value| {
                let &(wasm_bytes, ref downcast) = value
                    .downcast_ref::<MemoryCacheType>()
                    .expect("downcast should always succeed");

                (wasm_bytes, downcast.clone())
            },
        )?;

        let config = Arc::clone(&self.config);
        let result = gas_counter.before_loading_executable(&config, &method, wasm_bytes);
        if let Err(e) = result {
            let result = PreparationResult::OutcomeAbort(e);
            return Ok(PreparedContract { config, gas_counter, result });
        }
        match module_result {
            Ok(module) => {
                let result = gas_counter.after_loading_executable(&config, wasm_bytes);
                if let Err(e) = result {
                    let result = PreparationResult::OutcomeAbort(e);
                    return Ok(PreparedContract { config, gas_counter, result });
                }
                closure(gas_counter, module)
            }
            Err(e) => {
                let result =
                    PreparationResult::OutcomeAbort(FunctionCallError::CompilationError(e));
                return Ok(PreparedContract { config, gas_counter, result });
            }
        }
    }
}

impl crate::runner::VM for WasmtimeVM {
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

    fn prepare(
        self: Box<Self>,
        code: &dyn Contract,
        cache: Option<&dyn ContractRuntimeCache>,
        gas_counter: GasCounter,
        method: &str,
    ) -> Box<dyn crate::PreparedContract> {
        let cache = cache.unwrap_or(&NoContractRuntimeCache);
        let prepd = self.with_compiled_and_loaded(
            cache,
            code,
            gas_counter,
            method,
            |gas_counter, module| {
                let config = Arc::clone(&self.config);
                let Some(ExternType::Func(func_type)) = module.get_export(method) else {
                    let e =
                        FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound);
                    let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                    return Ok(PreparedContract { config, gas_counter, result });
                };
                if func_type.params().len() != 0 || func_type.results().len() != 0 {
                    let e = FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodInvalidSignature,
                    );
                    let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                    return Ok(PreparedContract { config, gas_counter, result });
                }

                let result =
                    PreparationResult::Ready(ReadyContract { module, method: method.into() });
                Ok(PreparedContract { config, gas_counter, result })
            },
        );
        Box::new(prepd)
    }
}

struct ReadyContract {
    module: Module,
    method: Box<str>,
}

struct PreparedContract {
    config: Arc<Config>,
    gas_counter: GasCounter,
    result: PreparationResult,
}

#[allow(clippy::large_enum_variant)]
enum PreparationResult {
    OutcomeAbortButNopInOldProtocol(FunctionCallError),
    OutcomeAbort(FunctionCallError),
    Ready(ReadyContract),
}

/// This enum allows us to replicate the various [`VMOutcome`] states without moving [`VMLogic`]
///
/// If function like [`call`] where to rely on [`VMOutcome::ok`], for example, it would require
/// ownership of [`VMLogic`], to acquire the inner [`ExecutionResultState`].
/// `run`, however, owns [`VMLogic`] and creates a mutable borrow,
/// which is then stored in a thread-local static as a raw pointer.
/// This means that we need to be very careful to ensure that the reference created is only dropped
/// after the module method call has returned.
/// Moving the [`VMLogic`] would break this assertion.
enum RunOutcome {
    Ok,
    AbortNop(FunctionCallError),
    Abort(FunctionCallError),
}

fn call(
    mut store: &mut Store<Ctx>,
    instance: Instance,
    method: &str,
) -> Result<RunOutcome, VMRunnerError> {
    let Some(func) = instance.get_func(&mut store, method) else {
        return Ok(RunOutcome::AbortNop(FunctionCallError::MethodResolveError(
            MethodResolveError::MethodNotFound,
        )));
    };
    match func.typed(&mut store) {
        Ok(run) => match run.call(store, ()) {
            Ok(()) => Ok(RunOutcome::Ok),
            Err(err) => err.into_vm_error().map(RunOutcome::Abort),
        },
        Err(err) => err.into_vm_error().map(RunOutcome::Abort),
    }
}

fn instantiate_and_call(
    mut store: &mut Store<Ctx>,
    linker: &Linker<Ctx>,
    module: &Module,
    method: &str,
) -> Result<RunOutcome, VMRunnerError> {
    match linker.instantiate(&mut store, module) {
        Ok(instance) => call(store, instance, method),
        Err(err) => err.into_vm_error().map(RunOutcome::Abort),
    }
}

impl crate::PreparedContract for VMResult<PreparedContract> {
    fn run(
        self: Box<Self>,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
    ) -> VMResult {
        let PreparedContract { config, gas_counter, result } = (*self)?;
        let result_state = ExecutionResultState::new(&context, gas_counter, config);
        let ReadyContract { module, method } = match result {
            PreparationResult::Ready(r) => r,
            PreparationResult::OutcomeAbortButNopInOldProtocol(e) => {
                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, e));
            }
            PreparationResult::OutcomeAbort(e) => {
                return Ok(VMOutcome::abort(result_state, e));
            }
        };

        let engine = module.engine();

        let config = Arc::clone(&result_state.config);

        let ctx = Ctx::default();

        // Clone the caller for future access outside of the store's context
        let caller = Arc::clone(&ctx.caller);
        let mut store = Store::<Ctx>::new(engine, ctx);
        let mut memory = WasmtimeMemory::new(
            caller,
            &mut store,
            config.limit_config.initial_memory_pages,
            config.limit_config.max_memory_pages,
        )
        .expect("failed to construct wasmtime memory");
        let logic = VMLogic::new(ext, context, fees_config, result_state, &mut memory);
        // SAFETY:
        // Although the 'static here is a lie, we are pretty confident that the `VMLogic` here
        // only lives for the duration of the contract method call (which is covered by the original
        // lifetime).
        store
            .data_mut()
            .logic
            .replace(unsafe { transmute::<VMLogic<'_>, VMLogic<'static>>(logic) });

        let mut linker = Linker::new(engine);
        // TODO: config could be accessed through `logic.result_state`, without this code having to
        // figure it out...
        link(&mut linker, memory.memory, &store, &config);
        let res = instantiate_and_call(&mut store, &linker, &module, &method);
        let logic = store.data_mut().logic.take().expect("logic missing");
        lazy_drop(Box::new((linker, module)));
        match res? {
            RunOutcome::Ok => Ok(VMOutcome::ok(logic.result_state)),
            RunOutcome::AbortNop(error) => {
                Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(logic.result_state, error))
            }
            RunOutcome::Abort(error) => Ok(VMOutcome::abort(logic.result_state, error)),
        }
    }
}

/// This is a container from which an error can be taken out by value. This is necessary as
/// `anyhow` does not really give any opportunity to grab causes by value and the VM Logic
/// errors end up a couple layers deep in a causal chain.
#[derive(Debug)]
pub(crate) struct ErrorContainer(parking_lot::Mutex<Option<VMLogicError>>);
impl ErrorContainer {
    pub(crate) fn take(&self) -> Option<VMLogicError> {
        self.0.lock().take()
    }
}
impl std::error::Error for ErrorContainer {}
impl std::fmt::Display for ErrorContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VMLogic error occurred and is now stored in an opaque storage container")
    }
}

fn link(
    linker: &mut wasmtime::Linker<Ctx>,
    memory: wasmtime::Memory,
    store: &wasmtime::Store<Ctx>,
    config: &Config,
) {
    linker.define(store, "env", "memory", memory).expect("cannot define memory");

    macro_rules! add_import {
        (
          $mod:ident / $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
        ) => {
            #[allow(unused_parens)]
            fn $name(mut caller: wasmtime::Caller<'_, Ctx>, $( $arg_name: $arg_type ),* ) -> anyhow::Result<($( $returns ),*)> {
                const TRACE: bool = imports::should_trace_host_function(stringify!($name));
                let _span = TRACE.then(|| {
                    tracing::trace_span!(target: "vm::host_function", stringify!($name)).entered()
                });

                let ctx = caller.data_mut();
                let mut logic = ctx.logic.take().expect("logic missing");
                let memory_caller = Arc::clone(&ctx.caller);
                // SAFETY:
                // Although the 'static here is a lie, we are pretty confident that the `Caller` here
                // only lives for the duration of the contract method call (which is covered by the original
                // lifetime), and we're doing `memory_caller.take()` just below, which should be safe.
                memory_caller.replace(Some(unsafe {transmute::<wasmtime::Caller<'_, Ctx>, wasmtime::Caller<'static, Ctx>>(caller)}));
                let res = match logic.$func( $( $arg_name as $arg_type, )* ) {
                    Ok(result) => Ok(result as ($( $returns ),* ) ),
                    Err(err) => {
                        Err(ErrorContainer(parking_lot::Mutex::new(Some(err))).into())
                    }
                };
                let mut caller = memory_caller.take().expect("caller missing");
                caller.data_mut().logic.replace(logic);
                res
            }

            linker.func_wrap(stringify!($mod), stringify!($name), $name).expect("cannot link external");
        };
    }
    imports::for_each_available_import!(config, add_import);
}
