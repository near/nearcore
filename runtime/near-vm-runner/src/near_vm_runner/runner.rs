use super::{NearVmMemory, VM_CONFIG};
use crate::cache::CompiledContractInfo;
use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMRunnerError, WasmTrap,
};
use crate::logic::gas_counter::FastGasCounter;
use crate::logic::{Config, ExecutionResultState, External, VMContext, VMLogic, VMOutcome};
use crate::near_vm_runner::{NearVmCompiler, NearVmEngine};
use crate::runner::VMResult;
use crate::{
    get_contract_cache_key, imports, CompiledContract, ContractCode, ContractRuntimeCache,
};
use crate::{prepare, NoContractRuntimeCache};
use memoffset::offset_of;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;
use near_vm_compiler_singlepass::Singlepass;
use near_vm_engine::universal::{
    MemoryPool, Universal, UniversalArtifact, UniversalEngine, UniversalExecutable,
    UniversalExecutableRef,
};
use near_vm_types::{FunctionIndex, InstanceConfig, MemoryType, Pages, WASM_PAGE_SIZE};
use near_vm_vm::{
    Artifact, ExportFunction, ExportFunctionMetadata, Instantiatable, LinearMemory, LinearTable,
    MemoryStyle, Resolver, TrapCode, VMFunction, VMFunctionKind, VMMemory,
};
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

type VMArtifact = Arc<UniversalArtifact>;

fn get_entrypoint_index(
    artifact: &UniversalArtifact,
    method_name: &str,
) -> Result<FunctionIndex, FunctionCallError> {
    if method_name.is_empty() {
        // Do we really need this code?
        return Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodEmptyName));
    }
    if let Some(near_vm_types::ExportIndex::Function(index)) = artifact.export_field(method_name) {
        let signature = artifact.function_signature(index).expect("index should produce signature");
        let signature =
            artifact.engine().lookup_signature(signature).expect("signature store invlidated?");
        if signature.params().is_empty() && signature.results().is_empty() {
            Ok(index)
        } else {
            Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature))
        }
    } else {
        Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound))
    }
}

fn translate_runtime_error(
    error: near_vm_engine::RuntimeError,
    logic: &mut VMLogic,
) -> Result<FunctionCallError, VMRunnerError> {
    // Errors produced by host function calls also become `RuntimeError`s that wrap a dynamic
    // instance of `VMLogicError` internally. See the implementation of `NearVmImports`.
    let error = match error.downcast::<crate::logic::VMLogicError>() {
        Ok(vm_logic) => {
            return vm_logic.try_into();
        }
        Err(original) => original,
    };
    let msg = error.message();
    let trap_code = error.to_trap().unwrap_or_else(|| {
        panic!("runtime error is not a trap: {}", msg);
    });
    Ok(match trap_code {
        TrapCode::GasExceeded => FunctionCallError::HostError(logic.process_gas_limit()),
        TrapCode::StackOverflow => FunctionCallError::WasmTrap(WasmTrap::StackOverflow),
        TrapCode::HeapAccessOutOfBounds => FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds),
        TrapCode::HeapMisaligned => FunctionCallError::WasmTrap(WasmTrap::MisalignedAtomicAccess),
        TrapCode::TableAccessOutOfBounds => {
            FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
        }
        TrapCode::OutOfBounds => FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds),
        TrapCode::IndirectCallToNull => FunctionCallError::WasmTrap(WasmTrap::IndirectCallToNull),
        TrapCode::BadSignature => {
            FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature)
        }
        TrapCode::IntegerOverflow => FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
        TrapCode::IntegerDivisionByZero => FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
        TrapCode::BadConversionToInteger => {
            FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
        }
        TrapCode::UnreachableCodeReached => FunctionCallError::WasmTrap(WasmTrap::Unreachable),
        TrapCode::UnalignedAtomic => FunctionCallError::WasmTrap(WasmTrap::MisalignedAtomicAccess),
    })
}

pub(crate) struct NearVM {
    pub(crate) config: Arc<Config>,
    pub(crate) engine: UniversalEngine,
}

impl NearVM {
    pub(crate) fn new_for_target(config: Arc<Config>, target: near_vm_compiler::Target) -> Self {
        // We only support singlepass compiler at the moment.
        assert_eq!(VM_CONFIG.compiler, NearVmCompiler::Singlepass);
        let mut compiler = Singlepass::new();
        compiler.set_9393_fix(!config.disable_9393_fix);
        // We only support universal engine at the moment.
        assert_eq!(VM_CONFIG.engine, NearVmEngine::Universal);

        static CODE_MEMORY_POOL_CELL: OnceLock<MemoryPool> = OnceLock::new();
        let code_memory_pool = CODE_MEMORY_POOL_CELL
            .get_or_init(|| {
                // NOTE: 8MiB is a best guess as to what the maximum size a loaded artifact can
                // plausibly be. This is not necessarily true – there may be WebAssembly
                // instructions that expand by more than 4 times in terms of instruction size after
                // a conversion to x86_64, In that case a re-allocation will occur and executing
                // that particular function call will be slower. Not to mention there isn't a
                // strong guarantee on the upper bound of the memory that the contract runtime may
                // require.
                // NOTE: 128 is not the upper limit on the number of maps that may be allocated at
                // once. This number may grow depending on the size of the in-memory VMArtifact
                // cache, which is configurable by the operator.
                MemoryPool::new(128, 8 * 1024 * 1024).unwrap_or_else(|e| {
                    panic!("could not pre-allocate resources for the runtime: {e}");
                })
            })
            .clone();

        let features =
            crate::features::WasmFeatures::from(config.limit_config.contract_prepare_version);
        Self {
            config,
            engine: Universal::new(compiler)
                .target(target)
                .features(features.into())
                .code_memory_pool(code_memory_pool)
                .engine(),
        }
    }

    pub(crate) fn new(config: Arc<Config>) -> Self {
        use near_vm_compiler::{CpuFeature, Target, Triple};
        let target_features = if cfg!(feature = "no_cpu_compatibility_checks") {
            let mut fs = CpuFeature::set();
            // These features should be sufficient to run the single pass compiler.
            fs.insert(CpuFeature::SSE2);
            fs.insert(CpuFeature::SSE3);
            fs.insert(CpuFeature::SSSE3);
            fs.insert(CpuFeature::SSE41);
            fs.insert(CpuFeature::SSE42);
            fs.insert(CpuFeature::POPCNT);
            fs.insert(CpuFeature::AVX);
            fs
        } else {
            CpuFeature::for_host()
        };
        Self::new_for_target(config, Target::new(Triple::host(), target_features))
    }

    pub(crate) fn compile_uncached(
        &self,
        code: &ContractCode,
    ) -> Result<UniversalExecutable, CompilationError> {
        let _span = tracing::debug_span!(target: "vm", "NearVM::compile_uncached").entered();
        let start = std::time::Instant::now();
        let prepared_code = prepare::prepare_contract(code.code(), &self.config, VMKind::NearVm)
            .map_err(CompilationError::PrepareError)?;

        debug_assert!(
            matches!(self.engine.validate(&prepared_code), Ok(_)),
            "near_vm failed to validate the prepared code"
        );
        let executable = self
            .engine
            .compile_universal(&prepared_code, &self)
            .map_err(|err| {
                tracing::error!(?err, "near_vm failed to compile the prepared code (this is defense-in-depth, the error was recovered from but should be reported to the developers)");
                CompilationError::WasmerCompileError { msg: err.to_string() }
            })?;
        crate::metrics::compilation_duration(VMKind::NearVm, start.elapsed());
        Ok(executable)
    }

    fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: &dyn ContractRuntimeCache,
    ) -> Result<Result<UniversalExecutable, CompilationError>, CacheError> {
        let executable_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(*code.hash(), &self.config);
        let record = CompiledContractInfo {
            wasm_bytes: code.code().len() as u64,
            compiled: match &executable_or_error {
                Ok(executable) => {
                    let code = executable
                        .serialize()
                        .map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
                    CompiledContract::Code(code)
                }
                Err(err) => CompiledContract::CompileModuleError(err.clone()),
            },
        };
        cache.put(&key, record).map_err(CacheError::WriteError)?;
        Ok(executable_or_error)
    }

    #[tracing::instrument(
        level = "debug",
        target = "vm",
        name = "NearVM::with_compiled_and_loaded",
        skip_all
    )]
    fn with_compiled_and_loaded(
        self: Box<Self>,
        cache: &dyn ContractRuntimeCache,
        ext: &dyn External,
        context: &VMContext,
        closure: impl FnOnce(ExecutionResultState, &VMArtifact, Box<Self>) -> VMResult<PreparedContract>,
    ) -> VMResult<PreparedContract> {
        // (wasm code size, compilation result)
        type MemoryCacheType = (u64, Result<VMArtifact, CompilationError>);
        let to_any = |v: MemoryCacheType| -> Box<dyn std::any::Any + Send> { Box::new(v) };
        // To identify a cache hit from either in-memory and on-disk cache correctly, we first assume that we have a cache hit here,
        // and then we set it to false when we fail to find any entry and decide to compile (by calling compile_and_cache below).
        let mut is_cache_hit = true;
        let code_hash = ext.code_hash();
        let (wasm_bytes, artifact_result) = cache.memory_cache().try_lookup(
            code_hash,
            || {
                // `cache` stores compiled machine code in the database
                //
                // Caches also cache _compilation_ errors, so that we don't have to
                // re-parse invalid code (invalid code, in a sense, is a normal
                // outcome). And `cache`, being a database, can fail with an `io::Error`.
                let _span =
                    tracing::debug_span!(target: "vm", "NearVM::fetch_from_cache").entered();
                let key = get_contract_cache_key(code_hash, &self.config);
                let cache_record = cache.get(&key).map_err(CacheError::ReadError)?;
                let Some(compiled_contract_info) = cache_record else {
                    let Some(code) = ext.get_contract() else {
                        return Err(VMRunnerError::ContractCodeNotPresent);
                    };
                    let _span =
                        tracing::debug_span!(target: "vm", "NearVM::build_from_source").entered();
                    is_cache_hit = false;
                    return Ok(to_any((
                        code.code().len() as u64,
                        match self.compile_and_cache(&code, cache)? {
                            Ok(executable) => Ok(self
                                .engine
                                .load_universal_executable(&executable)
                                .map(Arc::new)
                                .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?),
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
                        let _span =
                            tracing::debug_span!(target: "vm", "NearVM::load_from_fs_cache")
                                .entered();
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
                            let executable =
                                UniversalExecutableRef::deserialize(&serialized_module)
                                    .map_err(|_| CacheError::DeserializationError)?;
                            let artifact = self
                                .engine
                                .load_universal_executable_ref(&executable)
                                .map(Arc::new)
                                .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
                            Ok(to_any((compiled_contract_info.wasm_bytes, Ok(artifact))))
                        }
                    }
                }
            },
            move |value| {
                let _span =
                    tracing::debug_span!(target: "vm", "NearVM::load_from_mem_cache").entered();
                let &(wasm_bytes, ref downcast) = value
                    .downcast_ref::<MemoryCacheType>()
                    .expect("downcast should always succeed");

                (wasm_bytes, downcast.clone())
            },
        )?;

        crate::metrics::record_compiled_contract_cache_lookup(is_cache_hit);

        let mut result_state = ExecutionResultState::new(&context, Arc::clone(&self.config));
        let result = result_state.before_loading_executable(&context.method, wasm_bytes);
        if let Err(e) = result {
            return Ok(PreparedContract::Outcome(VMOutcome::abort(result_state, e)));
        }
        match artifact_result {
            Ok(artifact) => {
                let result = result_state.after_loading_executable(wasm_bytes);
                if let Err(e) = result {
                    return Ok(PreparedContract::Outcome(VMOutcome::abort(result_state, e)));
                }
                closure(result_state, &artifact, self)
            }
            Err(e) => Ok(PreparedContract::Outcome(VMOutcome::abort(
                result_state,
                FunctionCallError::CompilationError(e),
            ))),
        }
    }

    fn run_method(
        &self,
        artifact: &VMArtifact,
        mut import: NearVmImports<'_, '_, '_>,
        entrypoint: FunctionIndex,
    ) -> Result<Result<(), FunctionCallError>, VMRunnerError> {
        let _span = tracing::debug_span!(target: "vm", "run_method").entered();

        // FastGasCounter in Nearcore must be reinterpret_cast-able to the one in NearVm.
        assert_eq!(
            size_of::<FastGasCounter>(),
            size_of::<near_vm_types::FastGasCounter>() + size_of::<u64>()
        );
        assert_eq!(
            offset_of!(FastGasCounter, burnt_gas),
            offset_of!(near_vm_types::FastGasCounter, burnt_gas)
        );
        assert_eq!(
            offset_of!(FastGasCounter, gas_limit),
            offset_of!(near_vm_types::FastGasCounter, gas_limit)
        );
        let gas = import.vmlogic.gas_counter_pointer() as *mut near_vm_types::FastGasCounter;
        unsafe {
            let instance = {
                let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
                // An important caveat is that the `'static` lifetime here refers to the lifetime
                // of `VMLogic` reference to which is retained by the `InstanceHandle` we create.
                // However this `InstanceHandle` only lives during the execution of this body, so
                // we can be sure that `VMLogic` remains live and valid at any time.
                // SAFETY: we ensure that the tables are valid during the lifetime of this instance
                // by retaining an instance to `UniversalEngine` which holds the allocations.
                let maybe_handle = Arc::clone(artifact).instantiate(
                    &self,
                    &mut import,
                    Box::new(()),
                    // SAFETY: We have verified that the `FastGasCounter` layout matches the
                    // expected layout. `gas` remains dereferenceable throughout this function
                    // by the virtue of it being contained within `import` which lives for the
                    // entirety of this function.
                    InstanceConfig::with_stack_limit(self.config.limit_config.max_stack_height)
                        .with_counter(gas),
                );
                let handle = match maybe_handle {
                    Ok(handle) => handle,
                    Err(err) => {
                        use near_vm_engine::InstantiationError::*;
                        let abort = match err {
                            Start(err) => translate_runtime_error(err, import.vmlogic)?,
                            Link(e) => FunctionCallError::LinkError { msg: e.to_string() },
                            CpuFeature(e) => panic!(
                                "host doesn't support the CPU features needed to run contracts: {}",
                                e
                            ),
                        };
                        return Ok(Err(abort));
                    }
                };
                // SAFETY: being called immediately after instantiation.
                match handle.finish_instantiation() {
                    Ok(handle) => handle,
                    Err(trap) => {
                        let abort = translate_runtime_error(
                            near_vm_engine::RuntimeError::from_trap(trap),
                            import.vmlogic,
                        )?;
                        return Ok(Err(abort));
                    }
                };
                handle
            };
            if let Some(function) = instance.function_by_index(entrypoint) {
                let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
                // Signature for the entry point should be `() -> ()`. This is only a sanity check
                // – this should've been already checked by `get_entrypoint_index`.
                let signature = artifact
                    .engine()
                    .lookup_signature(function.signature)
                    .expect("extern type should refer to valid signature");
                if signature.params().is_empty() && signature.results().is_empty() {
                    let trampoline =
                        function.call_trampoline.expect("externs always have a trampoline");
                    // SAFETY: we double-checked the signature, and all of the remaining arguments
                    // come from an exported function definition which must be valid since it comes
                    // from near_vm itself.
                    let res = instance.invoke_function(
                        function.vmctx,
                        trampoline,
                        function.address,
                        [].as_mut_ptr() as *mut _,
                    );
                    if let Err(trap) = res {
                        let abort = translate_runtime_error(
                            near_vm_engine::RuntimeError::from_trap(trap),
                            import.vmlogic,
                        )?;
                        return Ok(Err(abort));
                    }
                } else {
                    panic!("signature should've already been checked by `get_entrypoint_index`")
                }
            } else {
                panic!("signature should've already been checked by `get_entrypoint_index`")
            }

            {
                let _span =
                    tracing::debug_span!(target: "vm", "run_method/drop_instance").entered();
                drop(instance)
            }
        }

        Ok(Ok(()))
    }
}

impl near_vm_vm::Tunables for &NearVM {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        MemoryStyle::Static {
            bound: memory.maximum.unwrap_or(Pages(self.config.limit_config.max_memory_pages)),
            offset_guard_size: WASM_PAGE_SIZE as u64,
        }
    }

    fn table_style(&self, _table: &near_vm_types::TableType) -> near_vm_vm::TableStyle {
        near_vm_vm::TableStyle::CallerChecksSignature
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        _style: &MemoryStyle,
    ) -> Result<std::sync::Arc<LinearMemory>, near_vm_vm::MemoryError> {
        // We do not support arbitrary Host memories. The only memory contracts may use is the
        // memory imported via `env.memory`.
        Err(near_vm_vm::MemoryError::CouldNotGrow {
            current: Pages(0),
            attempted_delta: ty.minimum,
        })
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        _style: &MemoryStyle,
        _vm_definition_location: std::ptr::NonNull<near_vm_vm::VMMemoryDefinition>,
    ) -> Result<std::sync::Arc<LinearMemory>, near_vm_vm::MemoryError> {
        // We do not support VM memories. The only memory contracts may use is the memory imported
        // via `env.memory`.
        Err(near_vm_vm::MemoryError::CouldNotGrow {
            current: Pages(0),
            attempted_delta: ty.minimum,
        })
    }

    fn create_host_table(
        &self,
        _ty: &near_vm_types::TableType,
        _style: &near_vm_vm::TableStyle,
    ) -> Result<std::sync::Arc<dyn near_vm_vm::Table>, String> {
        panic!("should never be called")
    }

    unsafe fn create_vm_table(
        &self,
        ty: &near_vm_types::TableType,
        style: &near_vm_vm::TableStyle,
        vm_definition_location: std::ptr::NonNull<near_vm_vm::VMTableDefinition>,
    ) -> Result<std::sync::Arc<dyn near_vm_vm::Table>, String> {
        // This is called when instantiating a module.
        Ok(Arc::new(LinearTable::from_definition(&ty, &style, vm_definition_location)?))
    }

    fn stack_init_gas_cost(&self, stack_size: u64) -> u64 {
        u64::from(self.config.regular_op_cost).saturating_mul((stack_size + 7) / 8)
    }

    /// Instrumentation configuration: stack limiter config
    fn stack_limiter_cfg(&self) -> Box<dyn finite_wasm::max_stack::SizeConfig> {
        Box::new(MaxStackCfg)
    }

    /// Instrumentation configuration: gas accounting config
    fn gas_cfg(&self) -> Box<dyn finite_wasm::wasmparser::VisitOperator<Output = u64>> {
        Box::new(GasCostCfg(u64::from(self.config.regular_op_cost)))
    }
}

struct MaxStackCfg;

impl finite_wasm::max_stack::SizeConfig for MaxStackCfg {
    fn size_of_value(&self, ty: finite_wasm::wasmparser::ValType) -> u8 {
        use finite_wasm::wasmparser::ValType;
        match ty {
            ValType::I32 => 4,
            ValType::I64 => 8,
            ValType::F32 => 4,
            ValType::F64 => 8,
            ValType::V128 => 16,
            ValType::Ref(_) => 8,
        }
    }
    fn size_of_function_activation(
        &self,
        locals: &prefix_sum_vec::PrefixSumVec<finite_wasm::wasmparser::ValType, u32>,
    ) -> u64 {
        let mut res = 64_u64; // Rough accounting for rip, rbp and some registers spilled. Not exact.
        let mut last_idx_plus_one = 0_u64;
        for (idx, local) in locals {
            let idx = u64::from(*idx);
            res = res.saturating_add(
                idx.checked_sub(last_idx_plus_one)
                    .expect("prefix-sum-vec indices went backwards")
                    .saturating_add(1)
                    .saturating_mul(u64::from(self.size_of_value(*local))),
            );
            last_idx_plus_one = idx.saturating_add(1);
        }
        res
    }
}

struct GasCostCfg(u64);

macro_rules! gas_cost {
    ($( @$proposal:ident $op:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident)*) => {
        $(
            fn $visit(&mut self $($(, $arg: $argty)*)?) -> u64 {
                gas_cost!(@@$proposal $op self $({ $($arg: $argty),* })? => $visit)
            }
        )*
    };

    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_block) => {
        0
    };
    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_end) => {
        0
    };
    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_else) => {
        0
    };
    (@@$_proposal:ident $_op:ident $self:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident) => {
        $self.0
    };
}

impl<'a> finite_wasm::wasmparser::VisitOperator<'a> for GasCostCfg {
    type Output = u64;
    finite_wasm::wasmparser::for_each_operator!(gas_cost);
}

impl crate::runner::VM for NearVM {
    fn prepare(
        self: Box<Self>,
        ext: &dyn External,
        context: &VMContext,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Box<dyn crate::PreparedContract> {
        let cache = cache.unwrap_or(&NoContractRuntimeCache);
        let prepd =
            self.with_compiled_and_loaded(cache, ext, context, |result_state, artifact, vm| {
                let memory = NearVmMemory::new(
                    vm.config.limit_config.initial_memory_pages,
                    vm.config.limit_config.max_memory_pages,
                )
                .expect("Cannot create memory for a contract call");
                let entrypoint = match get_entrypoint_index(&*artifact, &context.method) {
                    Ok(index) => index,
                    Err(e) => {
                        return Ok(PreparedContract::Outcome(
                            VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, e),
                        ))
                    }
                };
                Ok(PreparedContract::Ready(ReadyContract {
                    memory,
                    result_state,
                    entrypoint,
                    artifact: Arc::clone(artifact),
                    vm,
                }))
            });
        Box::new(prepd)
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

struct ReadyContract {
    memory: NearVmMemory,
    result_state: ExecutionResultState,
    entrypoint: FunctionIndex,
    artifact: VMArtifact,
    vm: Box<NearVM>,
}

#[allow(clippy::large_enum_variant)]
enum PreparedContract {
    Outcome(VMOutcome),
    Ready(ReadyContract),
}

impl crate::PreparedContract for VMResult<PreparedContract> {
    fn run(
        self: Box<Self>,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
    ) -> VMResult {
        let ReadyContract { memory, result_state, entrypoint, artifact, vm } = match (*self)? {
            PreparedContract::Outcome(outcome) => return Ok(outcome),
            PreparedContract::Ready(r) => r,
        };
        let config = Arc::clone(&result_state.config);
        let vmmemory = memory.vm();
        let mut logic = VMLogic::new(ext, context, fees_config, result_state, memory);
        let import = build_imports(vmmemory, &mut logic, config, artifact.engine());
        match vm.run_method(&artifact, import, entrypoint)? {
            Ok(()) => Ok(VMOutcome::ok(logic.result_state)),
            Err(err) => Ok(VMOutcome::abort(logic.result_state, err)),
        }
    }
}

pub(crate) struct NearVmImports<'engine, 'vmlogic, 'vmlogic_refs> {
    pub(crate) memory: VMMemory,
    config: Arc<Config>,
    // Note: this same object is also referenced by the `metadata` field!
    pub(crate) vmlogic: &'vmlogic mut VMLogic<'vmlogic_refs>,
    pub(crate) metadata: Arc<ExportFunctionMetadata>,
    pub(crate) engine: &'engine UniversalEngine,
}

trait NearVmType {
    type NearVm;
    fn to_near_vm(self) -> Self::NearVm;
    fn ty() -> near_vm_types::Type;
}
macro_rules! near_vm_types {
        ($($native:ty as $near_vm:ty => $type_expr:expr;)*) => {
            $(impl NearVmType for $native {
                type NearVm = $near_vm;
                fn to_near_vm(self) -> $near_vm {
                    self as _
                }
                fn ty() -> near_vm_types::Type {
                    $type_expr
                }
            })*
        }
    }
near_vm_types! {
    u32 as i32 => near_vm_types::Type::I32;
    u64 as i64 => near_vm_types::Type::I64;
}

macro_rules! return_ty {
        ($return_type: ident = [ ]) => {
            type $return_type = ();
            fn make_ret() -> () {}
        };
        ($return_type: ident = [ $($returns: ident),* ]) => {
            #[repr(C)]
            struct $return_type($(<$returns as NearVmType>::NearVm),*);
            fn make_ret($($returns: $returns),*) -> Ret { Ret($($returns.to_near_vm()),*) }
        }
    }

impl<'e, 'l, 'lr> Resolver for NearVmImports<'e, 'l, 'lr> {
    fn resolve(&self, _index: u32, module: &str, field: &str) -> Option<near_vm_vm::Export> {
        if module == "env" && field == "memory" {
            return Some(near_vm_vm::Export::Memory(self.memory.clone()));
        }

        macro_rules! add_import {
                (
                  $mod:ident / $name:ident : $func:ident <
                    [ $( $arg_name:ident : $arg_type:ident ),* ]
                    -> [ $( $returns:ident ),* ]
                  >
                ) => {
                    return_ty!(Ret = [ $($returns),* ]);

                    extern "C" fn $name(env: *mut VMLogic<'_>, $( $arg_name: $arg_type ),* )
                    -> Ret {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            const TRACE: bool = $crate::imports::should_trace_host_function(stringify!($name));
                            let _span = TRACE.then(|| {
                                tracing::trace_span!(target: "vm::host_function", stringify!($name)).entered()
                            });

                            // SAFETY: This code should only be executable within `'vmlogic`
                            // lifetime and so it is safe to dereference the `env` pointer which is
                            // known to be derived from a valid `&'vmlogic mut VMLogic<'_>` in the
                            // first place.
                            unsafe { (*env).$func( $( $arg_name, )* ) }
                        }));
                        // We want to ensure that the only kind of error that host function calls
                        // return are VMLogicError. This is important because we later attempt to
                        // downcast the `RuntimeError`s into `VMLogicError`.
                        let result: Result<Result<_, crate::logic::VMLogicError>, _>  = result;
                        #[allow(unused_parens)]
                        match result {
                            Ok(Ok(($($returns),*))) => make_ret($($returns),*),
                            Ok(Err(trap)) => unsafe {
                                // SAFETY: this can only be called by a WASM contract, so all the
                                // necessary hooks are known to be in place.
                                near_vm_vm::raise_user_trap(Box::new(trap))
                            },
                            Err(e) => unsafe {
                                // SAFETY: this can only be called by a WASM contract, so all the
                                // necessary hooks are known to be in place.
                                near_vm_vm::resume_panic(e)
                            },
                        }
                    }
                    // TODO: a phf hashmap would probably work better here.
                    if module == stringify!($mod) && field == stringify!($name) {
                        let args = [$(<$arg_type as NearVmType>::ty()),*];
                        let rets = [$(<$returns as NearVmType>::ty()),*];
                        let signature = near_vm_types::FunctionType::new(&args[..], &rets[..]);
                        let signature = self.engine.register_signature(signature);
                        return Some(near_vm_vm::Export::Function(ExportFunction {
                            vm_function: VMFunction {
                                address: $name as *const _,
                                // SAFETY: here we erase the lifetime of the `vmlogic` reference,
                                // but we believe that the lifetimes on `NearVmImports` enforce
                                // sufficiently that it isn't possible to call this exported
                                // function when vmlogic is no loger live.
                                vmctx: near_vm_vm::VMFunctionEnvironment {
                                    host_env: self.vmlogic as *const _ as *mut _
                                },
                                signature,
                                kind: VMFunctionKind::Static,
                                call_trampoline: None,
                                instance_ref: None,
                            },
                            metadata: Some(Arc::clone(&self.metadata)),
                        }));
                    }
                };
            }
        imports::for_each_available_import!(self.config, add_import);
        return None;
    }
}

pub(crate) fn build_imports<'e, 'a, 'b>(
    memory: VMMemory,
    logic: &'a mut VMLogic<'b>,
    config: Arc<Config>,
    engine: &'e UniversalEngine,
) -> NearVmImports<'e, 'a, 'b> {
    let metadata = unsafe {
        // SAFETY: the functions here are thread-safe. We ensure that the lifetime of `VMLogic`
        // is sufficiently long by tying the lifetime of VMLogic to the return type which
        // contains this metadata.
        ExportFunctionMetadata::new(logic as *mut _ as *mut _, None, |ptr| ptr, |_| {})
    };
    NearVmImports { memory, config, vmlogic: logic, metadata: Arc::new(metadata), engine }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_memory_like() {
        crate::logic::test_utils::test_memory_like(|| {
            Box::new(super::NearVmMemory::new(1, 1).unwrap())
        });
    }
}
