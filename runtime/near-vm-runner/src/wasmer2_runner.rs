use crate::cache::{CompiledContract, CompiledContractInfo, ContractRuntimeCache};
use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMRunnerError, WasmTrap,
};
use crate::logic::gas_counter::FastGasCounter;
use crate::logic::{
    Config, ExecutionResultState, External, MemSlice, MemoryLike, VMContext, VMLogic, VMOutcome,
};
use crate::prepare;
use crate::runner::VMResult;
use crate::{get_contract_cache_key, imports, ContractCode};
use memoffset::offset_of;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;
use std::borrow::Cow;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine::{Engine, Executable};
use wasmer_engine_universal::{
    Universal, UniversalEngine, UniversalExecutable, UniversalExecutableRef,
};
use wasmer_types::{FunctionIndex, InstanceConfig, MemoryType, Pages, WASM_PAGE_SIZE};
use wasmer_vm::{
    Artifact, ExportFunction, ExportFunctionMetadata, Instantiatable, LinearMemory, LinearTable,
    Memory, MemoryStyle, Resolver, TrapCode, VMFunction, VMFunctionKind, VMMemory,
};

#[derive(Clone)]
pub struct Wasmer2Memory(Arc<LinearMemory>);

impl Wasmer2Memory {
    fn new(
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, wasmer_vm::MemoryError> {
        let max_pages = Pages(max_memory_pages);
        Ok(Wasmer2Memory(Arc::new(LinearMemory::new(
            &MemoryType::new(Pages(initial_memory_pages), Some(max_pages), false),
            &MemoryStyle::Static {
                bound: max_pages,
                offset_guard_size: wasmer_types::WASM_PAGE_SIZE as u64,
            },
        )?)))
    }

    /// Returns pointer to memory at the specified offset provided that there’s
    /// enough space in the buffer starting at the returned pointer.
    ///
    /// Safety: Caller must guarantee that the returned pointer is not used
    /// after guest memory mapping is changed (e.g. grown).
    unsafe fn get_ptr(&self, offset: u64, len: usize) -> Result<*mut u8, ()> {
        let offset = usize::try_from(offset).map_err(|_| ())?;
        // SAFETY: Caller promisses memory mapping won’t change.
        let vmmem = unsafe { self.0.vmmemory().as_ref() };
        // `checked_sub` here verifies that offsetting the buffer by offset
        // still lands us in-bounds of the allocated object.
        let remaining = vmmem.current_length.checked_sub(offset).ok_or(())?;
        if len <= remaining {
            Ok(vmmem.base.add(offset))
        } else {
            Err(())
        }
    }

    /// Returns shared reference to slice in guest memory at given offset.
    ///
    /// Safety: Caller must guarantee that guest memory mapping is not changed
    /// (e.g. grown) while the slice is held.
    unsafe fn get(&self, offset: u64, len: usize) -> Result<&[u8], ()> {
        // SAFETY: Caller promisses memory mapping won’t change.
        let ptr = unsafe { self.get_ptr(offset, len)? };
        // SAFETY: get_ptr verifies that [ptr, ptr+len) is valid slice.
        Ok(unsafe { core::slice::from_raw_parts(ptr, len) })
    }

    /// Returns shared reference to slice in guest memory at given offset.
    ///
    /// Safety: Caller must guarantee that guest memory mapping is not changed
    /// (e.g. grown) while the slice is held.
    unsafe fn get_mut(&mut self, offset: u64, len: usize) -> Result<&mut [u8], ()> {
        // SAFETY: Caller promisses memory mapping won’t change.
        let ptr = unsafe { self.get_ptr(offset, len)? };
        // SAFETY: get_ptr verifies that [ptr, ptr+len) is valid slice and since
        // we’re holding exclusive self reference another mut reference won’t be
        // created
        Ok(unsafe { core::slice::from_raw_parts_mut(ptr, len) })
    }

    pub(crate) fn vm(&self) -> VMMemory {
        VMMemory { from: self.0.clone(), instance_ref: None }
    }
}

impl MemoryLike for Wasmer2Memory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        unsafe { self.get_ptr(slice.ptr, slice.len()?) }.map(|_| ())
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        // SAFETY: Firstly, contracts are executed on a single thread thus we
        // know no one will change guest memory mapping under us.  Secondly, the
        // way MemoryLike interface is used we know the memory mapping won’t be
        // changed by the caller while it holds the slice reference.
        unsafe { self.get(slice.ptr, slice.len()?) }.map(Cow::Borrowed)
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        Ok(buffer.copy_from_slice(unsafe { self.get(offset, buffer.len())? }))
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        Ok(unsafe { self.get_mut(offset, buffer.len())? }.copy_from_slice(buffer))
    }
}

fn get_entrypoint_index(
    artifact: &wasmer_engine_universal::UniversalArtifact,
    method_name: &str,
) -> Result<FunctionIndex, FunctionCallError> {
    if method_name.is_empty() {
        // Do we really need this code?
        return Err(FunctionCallError::MethodResolveError(MethodResolveError::MethodEmptyName));
    }
    if let Some(wasmer_types::ExportIndex::Function(index)) = artifact.export_field(method_name) {
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
    error: wasmer_engine::RuntimeError,
    logic: &mut VMLogic,
) -> Result<FunctionCallError, VMRunnerError> {
    // Errors produced by host function calls also become `RuntimeError`s that wrap a dynamic
    // instance of `VMLogicError` internally. See the implementation of `Wasmer2Imports`.
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

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum WasmerEngine {
    Universal = 1,
    StaticLib = 2,
    DynamicLib = 3,
}

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum WasmerCompiler {
    Singlepass = 1,
    Cranelift = 2,
    Llvm = 3,
}

#[derive(Hash)]
struct Wasmer2Config {
    seed: u32,
    engine: WasmerEngine,
    compiler: WasmerCompiler,
}

impl Wasmer2Config {
    fn config_hash(self: Self) -> u64 {
        crate::utils::stable_hash(&self)
    }
}

// We use following scheme for the bits forming seed:
//  kind << 29, kind is 1 for Wasmer2
//  major version << 6
//  minor version
const WASMER2_CONFIG: Wasmer2Config = Wasmer2Config {
    seed: (1 << 29) | (12 << 6) | 1,
    engine: WasmerEngine::Universal,
    compiler: WasmerCompiler::Singlepass,
};

pub(crate) fn wasmer2_vm_hash() -> u64 {
    WASMER2_CONFIG.config_hash()
}

pub(crate) type VMArtifact = Arc<wasmer_engine_universal::UniversalArtifact>;

pub(crate) struct Wasmer2VM {
    pub(crate) config: Arc<Config>,
    pub(crate) engine: UniversalEngine,
}

impl Wasmer2VM {
    pub(crate) fn new_for_target(config: Arc<Config>, target: wasmer_compiler::Target) -> Self {
        // We only support singlepass compiler at the moment.
        assert_eq!(WASMER2_CONFIG.compiler, WasmerCompiler::Singlepass);
        let compiler = Singlepass::new();
        // We only support universal engine at the moment.
        assert_eq!(WASMER2_CONFIG.engine, WasmerEngine::Universal);
        let features =
            crate::features::WasmFeatures::from(config.limit_config.contract_prepare_version);
        Self {
            config,
            engine: Universal::new(compiler).target(target).features(features.into()).engine(),
        }
    }

    pub(crate) fn new(config: Arc<Config>) -> Self {
        use wasmer_compiler::{CpuFeature, Target, Triple};
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
        let _span = tracing::debug_span!(target: "vm", "Wasmer2VM::compile_uncached").entered();
        let prepared_code = prepare::prepare_contract(code.code(), &self.config, VMKind::Wasmer2)
            .map_err(CompilationError::PrepareError)?;

        debug_assert!(
            matches!(self.engine.validate(&prepared_code), Ok(_)),
            "wasmer failed to validate the prepared code"
        );
        let executable = self
            .engine
            .compile_universal(&prepared_code, &self)
            .map_err(|err| {
                tracing::error!(?err, "wasmer failed to compile the prepared code (this is defense-in-depth, the error was recovered from but should be reported to pagoda)");
                CompilationError::WasmerCompileError { msg: err.to_string() }
            })?;
        Ok(executable)
    }

    fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Result<Result<UniversalExecutable, CompilationError>, CacheError> {
        let executable_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(*code.hash(), &self.config);

        if let Some(cache) = cache {
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
        }

        Ok(executable_or_error)
    }

    fn compile_and_load(
        &self,
        code: &ContractCode,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> VMResult<Result<VMArtifact, CompilationError>> {
        // A bit of a tricky logic ahead! We need to deal with two levels of
        // caching:
        //   * `cache` stores compiled machine code in the database
        //   * `MEM_CACHE` below holds in-memory cache of loaded contracts
        //
        // Caches also cache _compilation_ errors, so that we don't have to
        // re-parse invalid code (invalid code, in a sense, is a normal
        // outcome). And `cache`, being a database, can fail with an `io::Error`.
        let _span = tracing::debug_span!(target: "vm", "Wasmer2VM::compile_and_load").entered();

        let key = get_contract_cache_key(*code.hash(), &self.config);

        let compile_or_read_from_cache = || -> VMResult<Result<VMArtifact, CompilationError>> {
            let _span = tracing::debug_span!(target: "vm", "Wasmer2VM::compile_or_read_from_cache")
                .entered();
            let cache_record = cache
                .map(|cache| cache.get(&key))
                .transpose()
                .map_err(CacheError::ReadError)?
                .flatten();

            let stored_artifact: Option<VMArtifact> = match cache_record {
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
                        tracing::debug_span!(target: "vm", "Wasmer2VM::read_from_cache").entered();
                    unsafe {
                        // (UN-)SAFETY: the `serialized_module` must have been produced by a prior call to
                        // `serialize`.
                        //
                        // In practice this is not necessarily true. One could have forgotten to change the
                        // cache key when upgrading the version of the wasmer library or the database could
                        // have had its data corrupted while at rest.
                        //
                        // There should definitely be some validation in wasmer to ensure we load what we think
                        // we load.
                        let executable = UniversalExecutableRef::deserialize(&serialized_module)
                            .map_err(|_| CacheError::DeserializationError)?;
                        let artifact = self
                            .engine
                            .load_universal_executable_ref(&executable)
                            .map(Arc::new)
                            .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
                        Some(artifact)
                    }
                }
            };

            Ok(if let Some(it) = stored_artifact {
                Ok(it)
            } else {
                match self.compile_and_cache(code, cache)? {
                    Ok(executable) => Ok(self
                        .engine
                        .load_universal_executable(&executable)
                        .map(Arc::new)
                        .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?),
                    Err(err) => Err(err),
                }
            })
        };

        return compile_or_read_from_cache();
    }

    fn run_method(
        &self,
        artifact: &VMArtifact,
        mut import: Wasmer2Imports<'_, '_, '_>,
        entrypoint: FunctionIndex,
    ) -> Result<Result<(), FunctionCallError>, VMRunnerError> {
        let _span = tracing::debug_span!(target: "vm", "run_method").entered();

        // FastGasCounter in Nearcore and Wasmer must match in layout.
        assert_eq!(size_of::<FastGasCounter>(), size_of::<wasmer_types::FastGasCounter>());
        assert_eq!(
            offset_of!(FastGasCounter, burnt_gas),
            offset_of!(wasmer_types::FastGasCounter, burnt_gas)
        );
        assert_eq!(
            offset_of!(FastGasCounter, gas_limit),
            offset_of!(wasmer_types::FastGasCounter, gas_limit)
        );
        assert_eq!(
            offset_of!(FastGasCounter, opcode_cost),
            offset_of!(wasmer_types::FastGasCounter, opcode_cost)
        );
        let gas = import.vmlogic.gas_counter_pointer() as *mut wasmer_types::FastGasCounter;
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
                    InstanceConfig::default()
                        .with_counter(gas)
                        .with_stack_limit(self.config.limit_config.wasmer2_stack_limit),
                );
                let handle = match maybe_handle {
                    Ok(handle) => handle,
                    Err(err) => {
                        use wasmer_engine::InstantiationError::*;
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
                            wasmer_engine::RuntimeError::from_trap(trap),
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
                    // from wasmer itself.
                    let res = instance.invoke_function(
                        function.vmctx,
                        trampoline,
                        function.address,
                        [].as_mut_ptr() as *mut _,
                    );
                    if let Err(trap) = res {
                        let abort = translate_runtime_error(
                            wasmer_engine::RuntimeError::from_trap(trap),
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

impl wasmer_vm::Tunables for &Wasmer2VM {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        MemoryStyle::Static {
            bound: memory.maximum.unwrap_or(Pages(self.config.limit_config.max_memory_pages)),
            offset_guard_size: WASM_PAGE_SIZE as u64,
        }
    }

    fn table_style(&self, _table: &wasmer_types::TableType) -> wasmer_vm::TableStyle {
        wasmer_vm::TableStyle::CallerChecksSignature
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        _style: &MemoryStyle,
    ) -> Result<std::sync::Arc<dyn Memory>, wasmer_vm::MemoryError> {
        // We do not support arbitrary Host memories. The only memory contracts may use is the
        // memory imported via `env.memory`.
        Err(wasmer_vm::MemoryError::CouldNotGrow { current: Pages(0), attempted_delta: ty.minimum })
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        _style: &MemoryStyle,
        _vm_definition_location: std::ptr::NonNull<wasmer_vm::VMMemoryDefinition>,
    ) -> Result<std::sync::Arc<dyn Memory>, wasmer_vm::MemoryError> {
        // We do not support VM memories. The only memory contracts may use is the memory imported
        // via `env.memory`.
        Err(wasmer_vm::MemoryError::CouldNotGrow { current: Pages(0), attempted_delta: ty.minimum })
    }

    fn create_host_table(
        &self,
        _ty: &wasmer_types::TableType,
        _style: &wasmer_vm::TableStyle,
    ) -> Result<std::sync::Arc<dyn wasmer_vm::Table>, String> {
        panic!("should never be called")
    }

    unsafe fn create_vm_table(
        &self,
        ty: &wasmer_types::TableType,
        style: &wasmer_vm::TableStyle,
        vm_definition_location: std::ptr::NonNull<wasmer_vm::VMTableDefinition>,
    ) -> Result<std::sync::Arc<dyn wasmer_vm::Table>, String> {
        // This is called when instantiating a module.
        Ok(Arc::new(LinearTable::from_definition(&ty, &style, vm_definition_location)?))
    }
}

impl crate::runner::VM for Wasmer2VM {
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

    fn prepare(
        self: Box<Self>,
        ext: &dyn External,
        context: &VMContext,
        cache: Option<&dyn ContractRuntimeCache>,
    ) -> Box<dyn crate::PreparedContract> {
        type Result = VMResult<PreparedContract>;
        let Some(code) = ext.get_contract() else {
            return Box::new(Result::Err(VMRunnerError::ContractCodeNotPresent));
        };
        let mut result_state = ExecutionResultState::new(&context, Arc::clone(&self.config));
        let result =
            result_state.before_loading_executable(&context.method, code.code().len() as u64);
        if let Err(e) = result {
            return Box::new(Ok(PreparedContract::Outcome(VMOutcome::abort(result_state, e))));
        }
        let artifact = match self.compile_and_load(&code, cache) {
            Ok(Ok(it)) => it,
            Ok(Err(err)) => {
                return Box::new(Ok(PreparedContract::Outcome(VMOutcome::abort(
                    result_state,
                    FunctionCallError::CompilationError(err),
                ))));
            }
            Err(err) => {
                return Box::new(Result::Err(err));
            }
        };
        let result = result_state.after_loading_executable(code.code().len() as u64);
        if let Err(e) = result {
            return Box::new(Ok(PreparedContract::Outcome(VMOutcome::abort(result_state, e))));
        }
        let entrypoint = match get_entrypoint_index(&*artifact, &context.method) {
            Ok(index) => index,
            Err(e) => {
                return Box::new(Ok(PreparedContract::Outcome(
                    VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, e),
                )))
            }
        };

        let memory = Wasmer2Memory::new(
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        )
        .expect("Cannot create memory for a contract call");
        Box::new(Ok(PreparedContract::Ready {
            vm: self,
            memory,
            result_state,
            entrypoint,
            artifact,
        }))
    }
}

pub(crate) enum PreparedContract {
    Outcome(VMOutcome),
    Ready {
        vm: Box<Wasmer2VM>,
        memory: Wasmer2Memory,
        result_state: ExecutionResultState,
        entrypoint: FunctionIndex,
        artifact: VMArtifact,
    },
}

impl crate::PreparedContract for VMResult<PreparedContract> {
    fn run(
        self: Box<Self>,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
    ) -> VMResult {
        let (vm, memory, result_state, entrypoint, artifact) = match (*self)? {
            PreparedContract::Outcome(outcome) => return Ok(outcome),
            PreparedContract::Ready { vm, memory, result_state, entrypoint, artifact } => {
                (vm, memory, result_state, entrypoint, artifact)
            }
        };
        // FIXME: this mostly duplicates the `run_module` method.
        // Note that we don't clone the actual backing memory, just increase the RC.
        let vmmemory = memory.vm();
        let mut logic = VMLogic::new(ext, context, fees_config, result_state, memory);
        let import = build_imports(vmmemory, &mut logic, Arc::clone(&vm.config), artifact.engine());
        match vm.run_method(&artifact, import, entrypoint)? {
            Ok(()) => Ok(VMOutcome::ok(logic.result_state)),
            Err(err) => Ok(VMOutcome::abort(logic.result_state, err)),
        }
    }
}

pub(crate) struct Wasmer2Imports<'engine, 'vmlogic, 'vmlogic_refs> {
    pub(crate) memory: VMMemory,
    config: Arc<Config>,
    // Note: this same object is also referenced by the `metadata` field!
    pub(crate) vmlogic: &'vmlogic mut VMLogic<'vmlogic_refs>,
    pub(crate) metadata: Arc<ExportFunctionMetadata>,
    pub(crate) engine: &'engine UniversalEngine,
}

trait Wasmer2Type {
    type Wasmer;
    fn to_wasmer(self) -> Self::Wasmer;
    fn ty() -> wasmer_types::Type;
}
macro_rules! wasmer_types {
    ($($native:ty as $wasmer:ty => $type_expr:expr;)*) => {
        $(impl Wasmer2Type for $native {
            type Wasmer = $wasmer;
            fn to_wasmer(self) -> $wasmer {
                self as _
            }
            fn ty() -> wasmer_types::Type {
                $type_expr
            }
        })*
    }
}
wasmer_types! {
    u32 as i32 => wasmer_types::Type::I32;
    u64 as i64 => wasmer_types::Type::I64;
}

macro_rules! return_ty {
    ($return_type: ident = [ ]) => {
        type $return_type = ();
        fn make_ret() -> () {}
    };
    ($return_type: ident = [ $($returns: ident),* ]) => {
        #[repr(C)]
        struct $return_type($(<$returns as Wasmer2Type>::Wasmer),*);
        fn make_ret($($returns: $returns),*) -> Ret { Ret($($returns.to_wasmer()),*) }
    }
}

impl<'e, 'l, 'lr> Resolver for Wasmer2Imports<'e, 'l, 'lr> {
    fn resolve(&self, _index: u32, module: &str, field: &str) -> Option<wasmer_vm::Export> {
        if module == "env" && field == "memory" {
            return Some(wasmer_vm::Export::Memory(self.memory.clone()));
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
                            wasmer_vm::raise_user_trap(Box::new(trap))
                        },
                        Err(e) => unsafe {
                            // SAFETY: this can only be called by a WASM contract, so all the
                            // necessary hooks are known to be in place.
                            wasmer_vm::resume_panic(e)
                        },
                    }
                }
                // TODO: a phf hashmap would probably work better here.
                if module == stringify!($mod) && field == stringify!($name) {
                    let args = [$(<$arg_type as Wasmer2Type>::ty()),*];
                    let rets = [$(<$returns as Wasmer2Type>::ty()),*];
                    let signature = wasmer_types::FunctionTypeRef::new(&args[..], &rets[..]);
                    let signature = self.engine.register_signature(signature);
                    return Some(wasmer_vm::Export::Function(ExportFunction {
                        vm_function: VMFunction {
                            address: $name as *const _,
                            // SAFETY: here we erase the lifetime of the `vmlogic` reference,
                            // but we believe that the lifetimes on `Wasmer2Imports` enforce
                            // sufficiently that it isn't possible to call this exported
                            // function when vmlogic is no loger live.
                            vmctx: wasmer_vm::VMFunctionEnvironment {
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
) -> Wasmer2Imports<'e, 'a, 'b> {
    let metadata = unsafe {
        // SAFETY: the functions here are thread-safe. We ensure that the lifetime of `VMLogic`
        // is sufficiently long by tying the lifetime of VMLogic to the return type which
        // contains this metadata.
        ExportFunctionMetadata::new(logic as *mut _ as *mut _, None, |ptr| ptr, |_| {})
    };
    Wasmer2Imports { memory, config, vmlogic: logic, metadata: Arc::new(metadata), engine }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_memory_like() {
        crate::logic::test_utils::test_memory_like(|| {
            Box::new(super::Wasmer2Memory::new(1, 1).unwrap())
        });
    }
}
