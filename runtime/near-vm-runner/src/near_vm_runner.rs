use crate::errors::ContractPrecompilatonResult;
use crate::imports::near_vm::NearVmImports;
use crate::internal::VMKind;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, VMRunnerError, WasmTrap,
};
use crate::logic::gas_counter::FastGasCounter;
use crate::logic::types::{PromiseResult, ProtocolVersion};
use crate::logic::{
    CompiledContract, CompiledContractCache, External, MemSlice, MemoryLike, VMConfig, VMContext,
    VMLogic, VMOutcome,
};
use crate::prepare;
use crate::runner::VMResult;
use crate::{get_contract_cache_key, imports};
use memoffset::offset_of;
use near_primitives_core::contract::ContractCode;
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_vm_compiler_singlepass::Singlepass;
use near_vm_engine::universal::{
    LimitedMemoryPool, Universal, UniversalEngine, UniversalExecutable, UniversalExecutableRef,
};
use near_vm_types::{FunctionIndex, InstanceConfig, MemoryType, Pages, WASM_PAGE_SIZE};
use near_vm_vm::{
    Artifact, Instantiatable, LinearMemory, LinearTable, Memory, MemoryStyle, TrapCode, VMMemory,
};
use std::borrow::Cow;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
pub struct NearVmMemory(Arc<LinearMemory>);

impl NearVmMemory {
    fn new(
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, near_vm_vm::MemoryError> {
        let max_pages = Pages(max_memory_pages);
        Ok(NearVmMemory(Arc::new(LinearMemory::new(
            &MemoryType::new(Pages(initial_memory_pages), Some(max_pages), false),
            &MemoryStyle::Static {
                bound: max_pages,
                offset_guard_size: near_vm_types::WASM_PAGE_SIZE as u64,
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

impl MemoryLike for NearVmMemory {
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
    artifact: &near_vm_engine::universal::UniversalArtifact,
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

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum NearVmEngine {
    Universal = 1,
    StaticLib = 2,
    DynamicLib = 3,
}

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum NearVmCompiler {
    Singlepass = 1,
    Cranelift = 2,
    Llvm = 3,
}

#[derive(Hash)]
struct NearVmConfig {
    seed: u32,
    engine: NearVmEngine,
    compiler: NearVmCompiler,
}

impl NearVmConfig {
    fn config_hash(self: Self) -> u64 {
        crate::utils::stable_hash(&self)
    }
}

// We use following scheme for the bits forming seed:
//  kind << 29, kind 2 is for NearVm
//  major version << 6
//  minor version
const VM_CONFIG: NearVmConfig = NearVmConfig {
    seed: (2 << 29) | (2 << 6) | 0,
    engine: NearVmEngine::Universal,
    compiler: NearVmCompiler::Singlepass,
};

pub(crate) fn near_vm_vm_hash() -> u64 {
    VM_CONFIG.config_hash()
}

pub(crate) type VMArtifact = Arc<near_vm_engine::universal::UniversalArtifact>;

pub(crate) struct NearVM {
    pub(crate) config: VMConfig,
    pub(crate) engine: UniversalEngine,
}

impl NearVM {
    pub(crate) fn new_for_target(config: VMConfig, target: near_vm_compiler::Target) -> Self {
        // We only support singlepass compiler at the moment.
        assert_eq!(VM_CONFIG.compiler, NearVmCompiler::Singlepass);
        let compiler = Singlepass::new();
        // We only support universal engine at the moment.
        assert_eq!(VM_CONFIG.engine, NearVmEngine::Universal);

        static CODE_MEMORY_POOL_CELL: OnceLock<LimitedMemoryPool> = OnceLock::new();
        let code_memory_pool = CODE_MEMORY_POOL_CELL
            .get_or_init(|| {
                // FIXME: should have as many code memories as there are possible parallel
                // invocations of the runtime… How do we determine that? Should we make it
                // configurable for the node operators, perhaps, so that they can make an informed
                // choice based on the amount of memory they have and shards they track? Should we
                // actually use some sort of semaphore to enforce a parallelism limit?
                //
                // NB: 64MiB is a best guess as to what the maximum size a loaded artifact can
                // plausibly be. This is not necessarily true – there may be WebAssembly
                // instructions that expand by more than 4 times in terms of instruction size after
                // a conversion to x86_64, In that case a re-allocation will occur and executing
                // that particular function call will be slower. Not to mention there isn't a
                // strong guarantee on the upper bound of the memory that the contract runtime may
                // require.
                LimitedMemoryPool::new(8, 64 * 1024 * 1024).unwrap_or_else(|e| {
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

    pub(crate) fn new(config: VMConfig) -> Self {
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
                tracing::error!(?err, "near_vm failed to compile the prepared code (this is defense-in-depth, the error was recovered from but should be reported to pagoda)");
                CompilationError::WasmerCompileError { msg: err.to_string() }
            })?;
        Ok(executable)
    }

    fn compile_and_cache(
        &self,
        code: &ContractCode,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<UniversalExecutable, CompilationError>, CacheError> {
        let executable_or_error = self.compile_uncached(code);
        let key = get_contract_cache_key(code, VMKind::NearVm, &self.config);

        if let Some(cache) = cache {
            let record = match &executable_or_error {
                Ok(executable) => {
                    let code = executable
                        .serialize()
                        .map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
                    CompiledContract::Code(code)
                }
                Err(err) => CompiledContract::CompileModuleError(err.clone()),
            };
            cache.put(&key, record).map_err(CacheError::WriteError)?;
        }

        Ok(executable_or_error)
    }

    fn compile_and_load(
        &self,
        code: &ContractCode,
        cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult<Result<VMArtifact, CompilationError>> {
        // `cache` stores compiled machine code in the database
        //
        // Caches also cache _compilation_ errors, so that we don't have to
        // re-parse invalid code (invalid code, in a sense, is a normal
        // outcome). And `cache`, being a database, can fail with an `io::Error`.
        let _span = tracing::debug_span!(target: "vm", "NearVM::compile_and_load").entered();
        let key = get_contract_cache_key(code, VMKind::NearVm, &self.config);
        let cache_record = cache
            .map(|cache| cache.get(&key))
            .transpose()
            .map_err(CacheError::ReadError)?
            .flatten();

        let stored_artifact: Option<VMArtifact> = match cache_record {
            None => None,
            Some(CompiledContract::CompileModuleError(err)) => return Ok(Err(err)),
            Some(CompiledContract::Code(serialized_module)) => {
                let _span = tracing::debug_span!(target: "vm", "NearVM::read_from_cache").entered();
                unsafe {
                    // (UN-)SAFETY: the `serialized_module` must have been produced by a prior call to
                    // `serialize`.
                    //
                    // In practice this is not necessarily true. One could have forgotten to change the
                    // cache key when upgrading the version of the near_vm library or the database could
                    // have had its data corrupted while at rest.
                    //
                    // There should definitely be some validation in near_vm to ensure we load what we think
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
    }

    fn run_method(
        &self,
        artifact: &VMArtifact,
        mut import: NearVmImports<'_, '_, '_>,
        method_name: &str,
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
        let entrypoint = match get_entrypoint_index(&*artifact, method_name) {
            Ok(index) => index,
            Err(abort) => return Ok(Err(abort)),
        };
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
    ) -> Result<std::sync::Arc<dyn Memory>, near_vm_vm::MemoryError> {
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
    ) -> Result<std::sync::Arc<dyn Memory>, near_vm_vm::MemoryError> {
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
        let mut memory = NearVmMemory::new(
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        )
        .expect("Cannot create memory for a contract call");

        // FIXME: this mostly duplicates the `run_module` method.
        // Note that we don't clone the actual backing memory, just increase the RC.
        let vmmemory = memory.vm();
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

        let artifact = self.compile_and_load(code, cache)?;
        let artifact = match artifact {
            Ok(it) => it,
            Err(err) => {
                return Ok(VMOutcome::abort(logic, FunctionCallError::CompilationError(err)));
            }
        };

        let result = logic.after_loading_executable(current_protocol_version, code.code().len());
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }
        let import = imports::near_vm::build(
            vmmemory,
            &mut logic,
            current_protocol_version,
            artifact.engine(),
        );
        if let Err(e) = get_entrypoint_index(&*artifact, method_name) {
            return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                logic,
                e,
                current_protocol_version,
            ));
        }
        match self.run_method(&artifact, import, method_name)? {
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

#[test]
fn test_memory_like() {
    crate::logic::test_utils::test_memory_like(|| Box::new(NearVmMemory::new(1, 1).unwrap()));
}
