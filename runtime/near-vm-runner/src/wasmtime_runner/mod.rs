use crate::cache::get_contract_cache_key;
use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMLogicError,
    VMRunnerError, WasmTrap,
};
use crate::logic::{Config, ExecutionResultState, External, GasCounter, VMContext, VMOutcome};
use crate::runner::VMResult;
use crate::{
    CompiledContract, CompiledContractInfo, Contract, ContractCode, ContractRuntimeCache,
    EXPORT_PREFIX, NoContractRuntimeCache, prepare,
};
use core::mem::transmute;
use core::ops::Deref;
use core::sync::atomic::{AtomicU64, Ordering};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::{LimitConfig, VMKind};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};
use tracing::warn;
use wasmtime::component::types::ComponentItem;
use wasmtime::{
    CodeBuilder, Engine, ExternType, InstanceAllocationStrategy, PoolingAllocationConfig, Strategy,
    WasmBacktraceDetails,
};

mod component;
mod module;

/// The maximum amount of concurrent calls this engine can handle.
/// If this limit is reached, invocations will block until an execution slot is available.
///
/// Wasmtime will use this value to pre-allocate and pool resources internally.
/// Wasmtime defaults to `1_000`
const MAX_CONCURRENCY: u32 = 1_000;

/// Value used for [PoolingAllocationConfig::decommit_batch_size]
///
/// Wasmtime defaults to `1`
const DECOMMIT_BATCH_SIZE: usize = MAX_CONCURRENCY as usize / 2;

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

#[derive(Hash, PartialEq, Eq)]
struct VMKey {
    config: Arc<Config>,
    target: Option<String>,
}

static VMS: LazyLock<parking_lot::RwLock<HashMap<VMKey, WasmtimeVM>>> =
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

#[derive(Clone)]
pub(crate) struct WasmtimeVM {
    config: Arc<Config>,
    engine: wasmtime::Engine,
    concurrency: ConcurrencySemaphore,
}

#[derive(Clone)]
enum PreparedExecutable {
    Module(module::Prepared),
    Component(component::Prepared),
}

impl WasmtimeVM {
    pub(crate) fn new(config: Arc<Config>) -> Self {
        Self::new_for_target(config, None)
            .expect("construction without target specified cannot fail")
    }

    pub(crate) fn new_for_target(
        config: Arc<Config>,
        target: Option<String>,
    ) -> wasmtime::Result<Self> {
        let vm_key = VMKey { config: Arc::clone(&config), target: target.clone() };
        {
            if let Some(vm) = VMS.read().get(&vm_key) {
                return Ok(vm.clone());
            }
        }

        let features = crate::features::WasmFeatures::new(&config);
        let mut engine_config = wasmtime::Config::from(features);
        if let Some(target) = &target {
            engine_config.target(&target)?;
        }
        let mut guard = VMS.write();
        let vm = guard.entry(vm_key).or_insert_with_key(|vm_key| {
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
                .decommit_batch_size(DECOMMIT_BATCH_SIZE)
                .max_memory_size(max_memory_size)
                .table_elements(max_elements_per_contract_table)
                .total_component_instances(MAX_CONCURRENCY)
                .total_core_instances(MAX_CONCURRENCY)
                .total_memories(MAX_CONCURRENCY)
                .total_tables(max_tables)
                .max_memories_per_module(1)
                .max_tables_per_module(max_tables_per_contract)
                .table_keep_resident(max_elements_per_contract_table);

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
                .wasm_backtrace_details(WasmBacktraceDetails::Disable)
                // Enable copy-on-write heap images.
                .memory_init_cow(true)
                // Wasm stack metering is implemented by instrumentation, we don't want wasmtime to trap before that
                .max_wasm_stack(1024 * 1024 * 1024)
                // Enable the Cranelift optimizing compiler.
                .strategy(Strategy::Cranelift)
                // Enable signals-based traps. This is required to elide explicit bounds-checking.
                .signals_based_traps(true)
                // Configure linear memories such that explicit bounds-checking can be elided.
                .force_memory_init_memfd(true)
                .memory_guaranteed_dense_image_size(max_memory_size.try_into().unwrap_or(u64::MAX))
                .guard_before_linear_memory(false)
                .memory_guard_size(0)
                .memory_may_move(false)
                .memory_reservation(max_memory_size.try_into().unwrap_or(u64::MAX))
                .memory_reservation_for_growth(0)
                .compiler_inlining(true)
                .cranelift_nan_canonicalization(true)
                .wasm_wide_arithmetic(true);

            let config = Arc::clone(&vm_key.config);
            let engine = Engine::new(&engine_config).expect("failed to construct Wasmtime engine");
            let concurrency = ConcurrencySemaphore::new(max_tables);
            Self { config, engine, concurrency }
        });
        Ok(vm.clone())
    }

    pub(crate) fn vm_hash(&self) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        self.engine.precompile_compatibility_hash().hash(&mut hasher);
        hasher.write_u16(65); // increment the 65 or something when making modifications that affect
        // the artifact compatibility.
        hasher.finish()
    }

    #[tracing::instrument(target = "vm", level = "debug", "WasmtimeVM::compile_uncached", skip_all)]
    pub fn compile_uncached(
        &self,
        code: &ContractCode,
    ) -> Result<(Vec<u8>, bool), CompilationError> {
        let start = std::time::Instant::now();
        let prepared_code = prepare::prepare_contract(code.code(), &self.config, VMKind::Wasmtime)
            .map_err(CompilationError::PrepareError)?;

        let is_component = wasmparser_236::Parser::is_component(&prepared_code);
        let serialized = if is_component && self.config.component_model {
            let mut builder = CodeBuilder::new(&self.engine);
            let builder = builder.wasm_binary(&prepared_code, None).unwrap();
            unsafe { builder.expose_unsafe_intrinsics("unsafe-intrinsics") }.compile_component_serialized()
        } else {
            self.engine.precompile_module(&prepared_code)
        }
        .map(|code| (code, is_component))
        .map_err(|err| {
            tracing::error!(
                ?err,
                "wasmtime failed to compile the prepared code (this is defense-in-depth, the error was recovered from but should be reported to the developers)",
            );
            CompilationError::WasmtimeCompileError { msg: err.to_string() }
        });
        crate::metrics::compilation_duration(VMKind::Wasmtime, start.elapsed());
        serialized
    }

    fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<Result<(Vec<u8>, bool), CompilationError>, CacheError> {
        let serialized_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(*code.hash(), &self.config, self.vm_hash());
        let wasm_bytes = code.code().len() as u64;
        let record = match serialized_or_error {
            Ok((ref serialized, is_component)) => CompiledContractInfo {
                wasm_bytes,
                is_component,
                compiled: CompiledContract::Code(serialized.clone()),
            },
            Err(ref err) => CompiledContractInfo {
                wasm_bytes,
                is_component: wasmparser_236::Parser::is_component(code.code()),
                compiled: CompiledContract::CompileError(err.clone()),
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
            Result<PreparedExecutable, FunctionCallError>,
        ) -> VMResult<PreparedContract>,
    ) -> VMResult<PreparedContract> {
        type MemoryCacheType =
            (u64, Result<Result<PreparedExecutable, FunctionCallError>, CompilationError>);
        let to_any = |v: MemoryCacheType| -> Box<dyn std::any::Any + Send> { Box::new(v) };
        let key = get_contract_cache_key(contract.hash(), &self.config, self.vm_hash());
        let (wasm_bytes, pre_result) = cache.memory_cache().try_lookup(
            key,
            || {
                let cache_record = cache.get(&key).map_err(CacheError::ReadError)?;
                let (wasm_bytes, code, is_component) =
                    if let Some(CompiledContractInfo { wasm_bytes, compiled, is_component }) =
                        cache_record
                    {
                        match compiled {
                            CompiledContract::CompileError(err) => {
                                return Ok(to_any((wasm_bytes, Err(err))));
                            }
                            CompiledContract::Code(code) => (wasm_bytes, code, is_component),
                        }
                    } else {
                        let Some(code) = contract.get_code() else {
                            return Err(VMRunnerError::ContractCodeNotPresent);
                        };
                        let wasm_bytes = code.code().len() as u64;

                        match self.compile_and_cache(&code, cache)? {
                            Err(err) => return Ok(to_any((wasm_bytes, Err(err)))),
                            Ok((code, is_component)) => (wasm_bytes, code, is_component),
                        }
                    };
                // (UN-)SAFETY: the `code` must have been produced by
                // a prior call to [`Engine::precompile_component`] or
                // [`Engine::precompile_module`].
                //
                // In practice this is not necessarily true. One could have
                // forgotten to change the cache key when upgrading the version of
                // the near_vm library or the database could have had its data
                // corrupted while at rest.
                //
                // There should definitely be some validation in near_vm to ensure
                // we load what we think we load.
                let exec = if is_component {
                    unsafe { component::Prepared::load(&self.engine, &self.config, &code)? }
                        .map(PreparedExecutable::Component)
                } else {
                    unsafe { module::Prepared::load(&self.engine, &self.config, &code)? }
                        .map(PreparedExecutable::Module)
                };
                Ok(to_any((wasm_bytes, Ok(exec))))
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
    fn contract_cached(
        &self,
        cache: &dyn ContractRuntimeCache,
        hash: near_primitives_core::hash::CryptoHash,
    ) -> Result<bool, crate::logic::errors::CacheError> {
        let key = get_contract_cache_key(hash, &self.config, self.vm_hash());
        // Check if we already cached with such a key.
        cache.has(&key).map_err(CacheError::ReadError)
    }

    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<
        Result<ContractPrecompilatonResult, CompilationError>,
        crate::logic::errors::CacheError,
    > {
        if self.contract_cached(cache, *code.hash())? {
            return Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
        }
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
                let (exec, num_tables) = match pre {
                    Ok(PreparedExecutable::Component(component::Prepared { pre, num_tables })) => {
                        let Some((ComponentItem::ComponentFunc(ty), method)) =
                            pre.component().get_export(None, method)
                        else {
                            let e = FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodNotFound,
                            );
                            let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                            return Ok(PreparedContract { config, gas_counter, result });
                        };
                        if ty.params().len() != 0 || ty.results().len() != 0 {
                            let e = FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodInvalidSignature,
                            );
                            let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                            return Ok(PreparedContract { config, gas_counter, result });
                        }
                        (Executable::Component(component::Executable { pre, method }), num_tables)
                    }
                    Ok(PreparedExecutable::Module(module::Prepared {
                        pre,
                        memory,
                        remaining_gas,
                        start,
                        num_tables,
                    })) => {
                        let method = format!("{EXPORT_PREFIX}{method}");
                        let Some(ExternType::Func(ty)) = pre.module().get_export(&method) else {
                            let e = FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodNotFound,
                            );
                            let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                            return Ok(PreparedContract { config, gas_counter, result });
                        };
                        if ty.params().len() != 0 || ty.results().len() != 0 {
                            let e = FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodInvalidSignature,
                            );
                            let result = PreparationResult::OutcomeAbortButNopInOldProtocol(e);
                            return Ok(PreparedContract { config, gas_counter, result });
                        }
                        (
                            Executable::Module(module::Executable {
                                pre,
                                memory,
                                remaining_gas,
                                start,
                                method: method.into(),
                            }),
                            num_tables,
                        )
                    }
                    Err(err) => {
                        let result = PreparationResult::OutcomeAbort(err);
                        return Ok(PreparedContract { config, gas_counter, result });
                    }
                };
                let result = PreparationResult::Ready(ReadyContract {
                    exec,
                    num_tables,
                    concurrency: self.concurrency.clone(),
                });
                Ok(PreparedContract { config, gas_counter, result })
            });
        Box::new(prepd)
    }
}

struct ReadyContract {
    exec: Executable,
    num_tables: u32,
    concurrency: ConcurrencySemaphore,
}

enum Executable {
    Module(module::Executable),
    Component(component::Executable),
}

impl Executable {
    fn run(
        self,
        result_state: ExecutionResultState,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        num_tables: u32,
        concurrency: ConcurrencySemaphore,
    ) -> VMResult {
        // SAFETY:
        // Although the 'static here is a lie, we are pretty confident that the `External` and
        // VMContext here only live for the duration of the contract method call
        // (which is covered by the original lifetime).
        let ext = unsafe { transmute::<&mut dyn External, &'static mut dyn External>(ext) };
        let context = unsafe { transmute::<&VMContext, &'static VMContext>(context) };
        let Some(permit) = concurrency.try_acquire(num_tables) else {
            return Ok(VMOutcome::abort(
                result_state,
                FunctionCallError::LinkError { msg: "failed to acquire execution slot".into() },
            ));
        };
        match self {
            Executable::Module(exec) => exec.run(result_state, ext, context, fees_config, permit),
            Executable::Component(exec) => {
                exec.run(result_state, ext, context, fees_config, permit)
            }
        }
    }
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
#[derive(Debug)]
enum RunOutcome {
    Ok,
    AbortNop(FunctionCallError),
    Abort(FunctionCallError),
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
        let ReadyContract { exec, num_tables, concurrency } = match result {
            PreparationResult::Ready(r) => r,
            PreparationResult::OutcomeAbortButNopInOldProtocol(e) => {
                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, e));
            }
            PreparationResult::OutcomeAbort(e) => {
                return Ok(VMOutcome::abort(result_state, e));
            }
        };
        exec.run(result_state, ext, context, fees_config, num_tables, concurrency)
    }
}

/// This is a container from which an error can be taken out by value. This is necessary as
/// `anyhow` does not really give any opportunity to grab causes by value and the VM Logic
/// errors end up a couple layers deep in a causal chain.
#[derive(Debug)]
struct ErrorContainer(parking_lot::Mutex<Option<VMLogicError>>);
impl ErrorContainer {
    fn take(&self) -> Option<VMLogicError> {
        self.0.lock().take()
    }

    fn new(err: impl Into<VMLogicError>) -> Self {
        Self(parking_lot::Mutex::new(Some(err.into())))
    }
}
impl std::error::Error for ErrorContainer {}
impl std::fmt::Display for ErrorContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VMLogic error occurred and is now stored in an opaque storage container")
    }
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
