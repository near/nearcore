use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMLogicError,
    VMRunnerError, WasmTrap,
};
use crate::logic::{
    Config, ExecutionResultState, External, GasCounter, MemSlice, MemoryLike, VMContext, VMLogic,
    VMOutcome,
};
use crate::runner::VMResult;
use crate::{
    CompiledContract, CompiledContractInfo, Contract, ContractCode, ContractRuntimeCache,
    MEMORY_EXPORT, NoContractRuntimeCache, get_contract_cache_key, imports, prepare,
};
use core::mem::transmute;
use core::ops::Deref;
use core::sync::atomic::{AtomicU64, Ordering};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::{LimitConfig, VMKind};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tracing::warn;
use wasmtime::{
    Engine, Extern, ExternType, Instance, InstanceAllocationStrategy, InstancePre, Linker, Module,
    ModuleExport, PoolingAllocationConfig, ResourcesRequired, Store, StoreLimits,
    StoreLimitsBuilder, Strategy,
};

type Caller = wasmtime::Caller<'static, Ctx>;

/// The maximum amount of concurrent calls this engine can handle.
/// If this limit is reached, invocations will block until an execution slot is available.
///
/// Wasmtime will use this value to pre-allocate and pool resources internally.
/// Wasmtime defaults to `1_000`
const MAX_CONCURRENCY: u32 = 1_000;

/// The default maximum amount of tables per module.
///
/// This value is used if `max_tables_per_contract` is not set.
///
/// Wasmtime defaults to `1`
const DEFAULT_MAX_TABLES_PER_MODULE: u32 = 1;

/// The default maximum amount of elements in a single table.
///
/// This value is used if `max_elements_per_contract_table` is not set.
///
/// Wasmtime defaults to `20_000`
const DEFAULT_MAX_ELEMENTS_PER_TABLE: usize = 10_000;

/// Guest page size, in bytes
const GUEST_PAGE_SIZE: usize = 1 << 16;

static VMS: LazyLock<parking_lot::RwLock<HashMap<Arc<Config>, WasmtimeVM>>> =
    LazyLock::new(parking_lot::RwLock::default);

fn guest_memory_size(pages: u32) -> Option<usize> {
    let pages = usize::try_from(pages).ok()?;
    pages.checked_mul(GUEST_PAGE_SIZE)
}

struct InstancePermit<'a> {
    instances: &'a AtomicU64,
    tables: &'a AtomicU64,
    num_tables: u32,
    release_notify: &'a parking_lot::Condvar,
    release_mutex: &'a parking_lot::Mutex<()>,
}

impl Drop for InstancePermit<'_> {
    fn drop(&mut self) {
        self.instances.fetch_sub(1, Ordering::Release);
        if self.num_tables != 0 {
            self.tables.fetch_sub(self.num_tables.into(), Ordering::Release);
        }
        let _guard = self.release_mutex.lock();
        self.release_notify.notify_all();
    }
}

/// State stored in [ConcurrencySemaphore]
struct ConcurrencySemaphoreState {
    max_tables: u32,
    instances: AtomicU64,
    tables: AtomicU64,
    release_notify: parking_lot::Condvar,
    release_mutex: parking_lot::Mutex<()>,
}

/// A simple semaphore, which is not expected to be contended often.
#[derive(Clone)]
struct ConcurrencySemaphore(Arc<ConcurrencySemaphoreState>);

impl ConcurrencySemaphore {
    /// Constructs a new [ConcurrencySemaphore].
    ///
    /// `max_tables` is the maximum amount of tables that can concurrently exist.
    pub fn new(max_tables: u32) -> Self {
        Self(Arc::new(ConcurrencySemaphoreState {
            max_tables,
            instances: AtomicU64::default(),
            tables: AtomicU64::default(),
            release_notify: parking_lot::Condvar::default(),
            release_mutex: parking_lot::Mutex::default(),
        }))
    }
}

impl Deref for ConcurrencySemaphore {
    type Target = ConcurrencySemaphoreState;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ConcurrencySemaphore {
    /// Attempts to reserve a single instance slot, returns `true` on success
    fn try_reserve_instance(&self) -> bool {
        let prev = self.instances.fetch_add(1, Ordering::Acquire);
        prev.checked_add(1).is_some_and(|n| n <= MAX_CONCURRENCY.into())
    }

    /// Attempts to reserve `n` table slots, returns `true` on success
    fn try_reserve_tables(&self, n: u32) -> bool {
        let prev = self.tables.fetch_add(n.into(), Ordering::Acquire);
        prev.checked_add(n.into()).is_some_and(|n| n <= self.max_tables.into())
    }

    /// Releases a single instance slot returning the amount of previously active instances
    fn release_instance(&self) -> u64 {
        self.instances.fetch_sub(1, Ordering::Release)
    }

    /// Releases `n` table slots returning the amount of previously active tables
    fn release_tables(&self, n: u32) -> u64 {
        self.tables.fetch_sub(n.into(), Ordering::Release)
    }

    /// Attempt to acquire the semaphore using the specified number of tables
    fn try_acquire(&self, num_tables: u32) -> Option<InstancePermit<'_>> {
        debug_assert!(num_tables <= self.max_tables);

        // At most [`u16::MAX`] iterations per lock
        let mut iterations: u16 = 0;

        if num_tables != 0 {
            if !self.try_reserve_tables(num_tables) {
                let active = self.release_tables(num_tables).checked_sub(num_tables.into())?;
                warn!(active, requested = num_tables, "table lock contended");
                let mut guard = self.release_mutex.lock();
                while !self.try_reserve_tables(num_tables) {
                    iterations = iterations.checked_add(1)?;
                    if self.release_tables(num_tables) <= self.max_tables.into() {
                        continue;
                    }
                    self.release_notify.wait(&mut guard);
                }
            }
        }

        // reset iteration counter
        iterations = 0;
        if !self.try_reserve_instance() {
            let active = self.release_instance().checked_sub(1)?;
            warn!(active, "instance lock contended");
            let mut guard = self.release_mutex.lock();
            while !self.try_reserve_instance() {
                iterations = iterations.checked_add(1)?;
                if self.release_instance() <= MAX_CONCURRENCY.into() {
                    continue;
                }
                self.release_notify.wait(&mut guard);
            }
        }
        return Some(InstancePermit {
            instances: &self.instances,
            tables: &self.tables,
            num_tables,
            release_notify: &self.release_notify,
            release_mutex: &self.release_mutex,
        });
    }
}

pub struct Ctx {
    logic: Option<VMLogic<'static>>,
    caller: Arc<RefCell<Option<Caller>>>,
    limits: StoreLimits,
}

impl Ctx {
    fn new(logic: VMLogic<'static>, caller: Arc<RefCell<Option<Caller>>>) -> Self {
        let LimitConfig {
            max_memory_pages,
            max_tables_per_contract,
            max_elements_per_contract_table,
            ..
        } = logic.result_state.config.limit_config;
        let max_tables_per_contract =
            max_tables_per_contract.unwrap_or(DEFAULT_MAX_TABLES_PER_MODULE);
        let max_elements_per_contract_table =
            max_elements_per_contract_table.unwrap_or(DEFAULT_MAX_ELEMENTS_PER_TABLE);
        let max_memory_size = guest_memory_size(max_memory_pages).unwrap_or(usize::MAX);

        let limits = StoreLimitsBuilder::new()
            .instances(1)
            .memories(1)
            .memory_size(max_memory_size)
            .tables(max_tables_per_contract.try_into().unwrap_or(usize::MAX))
            .table_elements(max_elements_per_contract_table)
            .build();
        Self { logic: Some(logic), caller, limits }
    }
}

/// Implementation of [MemoryLike] in terms of [wasmtime::Memory]
#[derive(Clone)]
pub struct WasmtimeMemory {
    memory: ModuleExport,
    caller: Arc<RefCell<Option<Caller>>>,
}

impl WasmtimeMemory {
    fn with<T>(
        &self,
        func: impl FnOnce(&mut Caller, wasmtime::Memory) -> Result<T, ()>,
    ) -> Result<T, ()> {
        let mut caller = self.caller.borrow_mut();
        let caller = caller.as_mut().expect("caller missing");
        let Some(Extern::Memory(memory)) = caller.get_module_export(&self.memory) else {
            return Err(());
        };
        func(caller, memory)
    }
}

impl MemoryLike for WasmtimeMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        let end = slice.end::<usize>()?;
        self.with(|caller, memory| if end <= memory.data_size(caller) { Ok(()) } else { Err(()) })
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        let range = slice.range::<usize>()?;
        self.with(|caller, memory| {
            memory.data(caller).get(range).map(|slice| Cow::Owned(slice.to_vec())).ok_or(())
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        self.with(|caller, memory| {
            let memory = memory.data(caller).get(start..end).ok_or(())?;
            buffer.copy_from_slice(memory);
            Ok(())
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        self.with(|caller, memory| {
            let memory = memory.data_mut(caller).get_mut(start..end).ok_or(())?;
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

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}

#[derive(Clone)]
pub(crate) struct WasmtimeVM {
    config: Arc<Config>,
    engine: wasmtime::Engine,
    concurrency: ConcurrencySemaphore,
}

#[derive(Clone)]
struct PreparedModule {
    pre: InstancePre<Ctx>,
    memory: ModuleExport,
    num_tables: u32,
}

impl WasmtimeVM {
    pub(crate) fn new(config: Arc<Config>) -> Self {
        {
            if let Some(vm) = VMS.read().get(&config) {
                return vm.clone();
            }
        }
        VMS.write()
            .entry(config)
            .or_insert_with_key(|config| {
                let features = crate::features::WasmFeatures::new(config);

                // NOTE: Configuration values are based on:
                // - https://docs.wasmtime.dev/examples-fast-execution.html
                // - https://docs.wasmtime.dev/examples-fast-instantiation.html

                let LimitConfig {
                    max_memory_pages,
                    max_tables_per_contract,
                    max_elements_per_contract_table,
                    ..
                } = config.limit_config;

                let max_memory_size = guest_memory_size(max_memory_pages).unwrap_or(usize::MAX);
                let max_tables_per_contract =
                    max_tables_per_contract.unwrap_or(DEFAULT_MAX_TABLES_PER_MODULE);
                let max_elements_per_contract_table =
                    max_elements_per_contract_table.unwrap_or(DEFAULT_MAX_ELEMENTS_PER_TABLE);
                let max_tables = MAX_CONCURRENCY.saturating_mul(max_tables_per_contract);

                let mut pooling_config = PoolingAllocationConfig::default();
                pooling_config
                    .max_memory_size(max_memory_size)
                    .table_elements(max_elements_per_contract_table)
                    .total_component_instances(0)
                    .total_core_instances(MAX_CONCURRENCY)
                    .total_memories(MAX_CONCURRENCY)
                    .total_tables(max_tables)
                    .max_memories_per_module(1)
                    .max_tables_per_module(max_tables_per_contract);

                let mut engine_config = wasmtime::Config::from(features);
                engine_config
                    .allocation_strategy(InstanceAllocationStrategy::Pooling(pooling_config))
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

                let config = Arc::clone(config);
                let engine =
                    Engine::new(&engine_config).expect("failed to construct Wasmtime engine");
                let concurrency = ConcurrencySemaphore::new(max_tables);
                Self { config, engine, concurrency }
            })
            .clone()
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
        closure: impl FnOnce(
            GasCounter,
            Result<PreparedModule, FunctionCallError>,
        ) -> VMResult<PreparedContract>,
    ) -> VMResult<PreparedContract> {
        type MemoryCacheType =
            (u64, Result<Result<PreparedModule, FunctionCallError>, CompilationError>);
        let to_any = |v: MemoryCacheType| -> Box<dyn std::any::Any + Send> { Box::new(v) };
        let key = get_contract_cache_key(contract.hash(), &self.config);
        let (wasm_bytes, pre_result) = cache.memory_cache().try_lookup(
            key,
            || {
                let cache_record = cache.get(&key).map_err(CacheError::ReadError)?;
                let (wasm_bytes, module) =
                    if let Some(CompiledContractInfo { wasm_bytes, compiled }) = cache_record {
                        match compiled {
                            CompiledContract::CompileModuleError(err) => {
                                return Ok(to_any((wasm_bytes, Err(err))));
                            }
                            CompiledContract::Code(module) => (wasm_bytes, module),
                        }
                    } else {
                        let Some(code) = contract.get_code() else {
                            return Err(VMRunnerError::ContractCodeNotPresent);
                        };
                        let wasm_bytes = code.code().len() as u64;
                        match self.compile_and_cache(&code, cache)? {
                            Err(err) => return Ok(to_any((wasm_bytes, Err(err)))),
                            Ok(module) => (wasm_bytes, module),
                        }
                    };
                // (UN-)SAFETY: the `module` must have been produced by
                // a prior call to `serialize`.
                //
                // In practice this is not necessarily true. One could have
                // forgotten to change the cache key when upgrading the version of
                // the near_vm library or the database could have had its data
                // corrupted while at rest.
                //
                // There should definitely be some validation in near_vm to ensure
                // we load what we think we load.
                let module = unsafe { Module::deserialize(&self.engine, &module) }
                    .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
                let Some(memory) = module.get_export_index(MEMORY_EXPORT) else {
                    return Ok(to_any((
                        wasm_bytes,
                        Ok(Err(FunctionCallError::LinkError {
                            msg: "memory export missing".into(),
                        })),
                    )));
                };
                let mut linker = Linker::new(&self.engine);
                link(&mut linker, &self.config);
                match linker.instantiate_pre(&module) {
                    Err(err) => {
                        let err = err.into_vm_error()?;
                        Ok(to_any((wasm_bytes, Ok(Err(err)))))
                    }
                    Ok(pre) => {
                        let ResourcesRequired { num_tables, .. } = module.resources_required();
                        Ok(to_any((wasm_bytes, Ok(Ok(PreparedModule { pre, memory, num_tables })))))
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
        match pre_result {
            Ok(res) => {
                let result = gas_counter.after_loading_executable(&config, wasm_bytes);
                if let Err(e) = result {
                    let result = PreparationResult::OutcomeAbort(e);
                    return Ok(PreparedContract { config, gas_counter, result });
                }
                closure(gas_counter, res)
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
        let prepd =
            self.with_compiled_and_loaded(cache, code, gas_counter, method, |gas_counter, pre| {
                let config = Arc::clone(&self.config);
                match pre {
                    Ok(PreparedModule { pre, memory, num_tables }) => {
                        let Some(ExternType::Func(func_type)) = pre.module().get_export(method)
                        else {
                            let e = FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodNotFound,
                            );
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

                        let result = PreparationResult::Ready(ReadyContract {
                            pre,
                            memory,
                            num_tables,
                            method: method.into(),
                            concurrency: self.concurrency.clone(),
                        });
                        Ok(PreparedContract { config, gas_counter, result })
                    }
                    Err(err) => {
                        let result = PreparationResult::OutcomeAbort(err);
                        Ok(PreparedContract { config, gas_counter, result })
                    }
                }
            });
        Box::new(prepd)
    }
}

struct ReadyContract {
    pre: InstancePre<Ctx>,
    memory: ModuleExport,
    num_tables: u32,
    method: Box<str>,
    concurrency: ConcurrencySemaphore,
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

impl crate::PreparedContract for VMResult<PreparedContract> {
    fn run(
        self: Box<Self>,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
    ) -> VMResult {
        let PreparedContract { config, gas_counter, result } = (*self)?;
        let result_state = ExecutionResultState::new(&context, gas_counter, config);
        let ReadyContract { pre, memory, method, num_tables, concurrency } = match result {
            PreparationResult::Ready(r) => r,
            PreparationResult::OutcomeAbortButNopInOldProtocol(e) => {
                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, e));
            }
            PreparationResult::OutcomeAbort(e) => {
                return Ok(VMOutcome::abort(result_state, e));
            }
        };

        let caller = Arc::default();
        let mut memory = WasmtimeMemory { caller: Arc::clone(&caller), memory };
        let logic = VMLogic::new(ext, context, fees_config, result_state, &mut memory);
        // SAFETY:
        // Although the 'static here is a lie, we are pretty confident that the `VMLogic` here
        // only lives for the duration of the contract method call (which is covered by the original
        // lifetime).
        let logic = unsafe { transmute::<VMLogic<'_>, VMLogic<'static>>(logic) };
        let ctx = Ctx::new(logic, caller);

        let mut store = Store::<Ctx>::new(pre.module().engine(), ctx);
        store.limiter(|ctx| &mut ctx.limits);
        let Some(_permit) = concurrency.try_acquire(num_tables) else {
            let logic = store.data_mut().logic.take().expect("logic missing");
            return Ok(VMOutcome::abort(
                logic.result_state,
                FunctionCallError::LinkError { msg: "failed to acquire execution slot".into() },
            ));
        };
        let instance = match pre.instantiate(&mut store) {
            Ok(instance) => instance,
            Err(err) => {
                let err = err.into_vm_error()?;
                let logic = store.data_mut().logic.take().expect("logic missing");
                return Ok(VMOutcome::abort(logic.result_state, err));
            }
        };

        let res = call(&mut store, instance, &method);
        let logic = store.data_mut().logic.take().expect("logic missing");
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

fn link(linker: &mut wasmtime::Linker<Ctx>, config: &Config) {
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

#[cfg(test)]
mod tests {
    use core::array;
    use std::thread::{scope, spawn, yield_now};

    use super::*;

    #[test]
    fn test_semaphore() {
        const MAX_TABLES: u32 = 5 * MAX_CONCURRENCY;

        let concurrency = ConcurrencySemaphore::new(MAX_TABLES);
        let permit = concurrency.try_acquire(MAX_TABLES).expect("failed to acquire permit");
        let thread = spawn({
            let concurrency = concurrency.clone();
            move || concurrency.try_acquire(1).is_some()
        });
        yield_now();
        assert!(!thread.is_finished());
        drop(permit);
        let acquired = thread.join().expect("failed to join thread");
        assert!(acquired);

        assert!(concurrency.try_reserve_tables(MAX_TABLES));
        assert!(!concurrency.try_reserve_tables(1));
        assert_eq!(concurrency.release_tables(MAX_TABLES), u64::from(MAX_TABLES + 1));
        assert_eq!(concurrency.release_tables(1), 1);
        assert!(concurrency.try_reserve_tables(2));
        assert_eq!(concurrency.release_tables(1), 2);
        assert_eq!(concurrency.release_tables(1), 1);

        #[expect(clippy::large_stack_frames)]
        scope(|scope| {
            let permits: [_; MAX_CONCURRENCY as _] = array::from_fn(|_| {
                scope.spawn(|| concurrency.try_acquire(MAX_TABLES / MAX_CONCURRENCY))
            });
            let permits = permits.map(|thread| {
                thread.join().expect("failed to join thread").expect("failed to acquire permit")
            });
            assert!(!concurrency.try_reserve_instance());
            assert_eq!(concurrency.release_instance(), u64::from(MAX_CONCURRENCY + 1));

            let thread = spawn({
                let concurrency = concurrency.clone();
                move || concurrency.try_acquire(MAX_TABLES).is_some()
            });
            let mut permits = Vec::from(permits);
            let permit = permits.pop().expect("last permit missing");
            for permit in permits {
                drop(permit);
                yield_now();
                assert!(!thread.is_finished());
            }

            yield_now();
            assert!(!thread.is_finished());
            drop(permit);

            let acquired = thread.join().expect("failed to join thread");
            assert!(acquired);
        });

        #[expect(clippy::large_stack_frames)]
        scope(|scope| {
            let permits: [_; MAX_CONCURRENCY as _] =
                array::from_fn(|_| scope.spawn(|| concurrency.try_acquire(0)));
            let permits = permits.map(|thread| {
                thread.join().expect("failed to join thread").expect("failed to acquire permit")
            });
            assert!(!concurrency.try_reserve_instance());
            assert_eq!(concurrency.release_instance(), u64::from(MAX_CONCURRENCY + 1));

            let thread = spawn({
                let concurrency = concurrency.clone();
                move || concurrency.try_acquire(0).is_some()
            });

            yield_now();
            assert!(!thread.is_finished());
            drop(permits);

            let acquired = thread.join().expect("failed to join thread");
            assert!(acquired);
        });
    }
}
