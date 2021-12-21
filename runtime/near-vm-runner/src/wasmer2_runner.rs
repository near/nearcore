use crate::cache::into_vm_result;
use crate::errors::IntoVMError;
use crate::imports::wasmer2::Wasmer2Imports;
use crate::prepare::WASM_FEATURES;
use crate::{cache, imports};
use memoffset::offset_of;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_stable_hasher::StableHasher;
use near_vm_errors::{
    CompilationError, FunctionCallError, HostError, MethodResolveError, VMError, WasmTrap,
};
use near_vm_logic::gas_counter::FastGasCounter;
use near_vm_logic::types::{PromiseResult, ProtocolVersion};
use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMLogicError, VMOutcome};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine::{Artifact, DeserializeError, Engine, InstantiationError, RuntimeError};
use wasmer_engine_universal::{Universal, UniversalEngine};
use wasmer_types::{
    Bytes, ExportIndex, Features, FunctionIndex, InstanceConfig, MemoryType, MemoryView, Pages,
};
use wasmer_vm::{LinearMemory, LinearTable, Memory, MemoryStyle, TrapCode, VMExtern, VMMemory};

const WASMER_FEATURES: Features = Features {
    threads: WASM_FEATURES.threads,
    reference_types: WASM_FEATURES.reference_types,
    simd: WASM_FEATURES.simd,
    bulk_memory: WASM_FEATURES.bulk_memory,
    multi_value: WASM_FEATURES.multi_value,
    tail_call: WASM_FEATURES.tail_call,
    module_linking: WASM_FEATURES.module_linking,
    multi_memory: WASM_FEATURES.multi_memory,
    memory64: WASM_FEATURES.memory64,
    exceptions: WASM_FEATURES.exceptions,
    // singlepass does not support signals.
    signal_less: true,
};

#[derive(Clone)]
pub struct Wasmer2Memory(Arc<LinearMemory>);

impl Wasmer2Memory {
    pub(crate) fn new(initial_memory_pages: u32, max_memory_pages: u32) -> Result<Self, VMError> {
        let max_pages = Pages(max_memory_pages);
        Ok(Wasmer2Memory(Arc::new(
            LinearMemory::new(
                &MemoryType::new(Pages(initial_memory_pages), Some(max_pages), false),
                &MemoryStyle::Static { bound: max_pages, offset_guard_size: 0x1000 },
            )
            .expect("creating memory must not fail"),
        )))
    }

    fn view(&self) -> MemoryView<u8> {
        let size = u32::try_from(self.0.size().bytes().0).expect("memory size must fit into u32");
        let base = self.0.vmmemory();
        unsafe {
            // SAFETY: We have successfully created this as host memory during construction, so the
            // base pointer must be a valid owned value of `VMMemoryDefinition`.
            let base = base.as_ref().base;
            // SAFETY: the `base` pointer is valid, since it has been read from an initialized
            // `VMMemoryDefinition`.
            MemoryView::new(base, size)
        }
    }

    pub(crate) fn vm(&self) -> VMMemory {
        VMMemory { from: self.0.clone(), instance_ref: None }
    }
}

impl MemoryLike for Wasmer2Memory {
    fn fits_memory(&self, offset: u64, len: u64) -> bool {
        match offset.checked_add(len) {
            None => false,
            Some(end) => self.0.size().bytes() >= Bytes(end as usize),
        }
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
        let offset = offset as usize;
        for (i, cell) in self.view()[offset..(offset + buffer.len())].iter().enumerate() {
            buffer[i] = cell.get();
        }
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        self.view()[offset as usize].get()
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        let offset = offset as usize;
        self.view()[offset..(offset + buffer.len())]
            .iter()
            .zip(buffer.iter())
            .for_each(|(cell, v)| cell.set(*v));
    }
}

impl IntoVMError for InstantiationError {
    fn into_vm_error(self) -> VMError {
        match self {
            InstantiationError::Link(e) => {
                VMError::FunctionCallError(FunctionCallError::LinkError { msg: e.to_string() })
            }
            InstantiationError::CpuFeature(e) => {
                panic!("host does not support the CPU features required to run contracts: {}", e)
            }
            InstantiationError::Start(e) => e.into_vm_error(),
        }
    }
}

impl IntoVMError for RuntimeError {
    fn into_vm_error(self) -> VMError {
        // These vars are not used in every cases, however, downcast below use Arc::try_unwrap
        // so we cannot clone self
        let error_msg = self.message();
        let trap_code = self.clone().to_trap();
        if let Ok(e) = self.downcast::<VMLogicError>() {
            return e.into();
        }
        // If we panic here - it means we encountered an issue in Wasmer.
        let trap_code = trap_code.unwrap_or_else(|| panic!("Unknown error: {}", error_msg));
        let error = match trap_code {
            TrapCode::StackOverflow => FunctionCallError::WasmTrap(WasmTrap::StackOverflow),
            TrapCode::HeapAccessOutOfBounds => {
                FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
            }
            TrapCode::HeapMisaligned => {
                FunctionCallError::WasmTrap(WasmTrap::MisalignedAtomicAccess)
            }
            TrapCode::TableAccessOutOfBounds => {
                FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
            }
            TrapCode::OutOfBounds => FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds),
            TrapCode::IndirectCallToNull => {
                FunctionCallError::WasmTrap(WasmTrap::IndirectCallToNull)
            }
            TrapCode::BadSignature => {
                FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature)
            }
            TrapCode::IntegerOverflow => FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
            TrapCode::IntegerDivisionByZero => {
                FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
            }
            TrapCode::BadConversionToInteger => {
                FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
            }
            TrapCode::UnreachableCodeReached => FunctionCallError::WasmTrap(WasmTrap::Unreachable),
            TrapCode::UnalignedAtomic => {
                FunctionCallError::WasmTrap(WasmTrap::MisalignedAtomicAccess)
            }
            TrapCode::GasExceeded => FunctionCallError::HostError(HostError::GasExceeded),
        };
        VMError::FunctionCallError(error)
    }
}

fn get_entrypoint_index(
    artifact: &dyn Artifact,
    method_name: &str,
) -> Result<FunctionIndex, VMError> {
    if method_name.is_empty() {
        // Do we really need this code?
        return Err(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
            MethodResolveError::MethodEmptyName,
        )));
    }
    let module = artifact.module_ref();
    if let Some(ExportIndex::Function(index)) = module.exports.get(method_name) {
        let func = module.functions.get(index.clone()).unwrap();
        let sig = module.signatures.get(func.clone()).unwrap();
        if sig.params().is_empty() && sig.results().is_empty() {
            Ok(*index)
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

fn translate_instantiation_error(err: InstantiationError, logic: &mut VMLogic) -> VMError {
    match err {
        InstantiationError::Start(err) => translate_runtime_error(err, logic),
        _ => err.into_vm_error(),
    }
}

fn translate_runtime_error(err: RuntimeError, logic: &mut VMLogic) -> VMError {
    match err.clone().to_trap() {
        Some(TrapCode::GasExceeded) => {
            VMError::FunctionCallError(FunctionCallError::HostError(logic.process_gas_limit()))
        }
        _ => err.into_vm_error(),
    }
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
        let mut s = StableHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

// We use following scheme for the bits forming seed:
//  kind << 10, kind is 1 for Wasmer2
//  major version << 6
//  minor version
const WASMER2_CONFIG: Wasmer2Config = Wasmer2Config {
    seed: (1 << 10) | (4 << 6) | 0,
    engine: WasmerEngine::Universal,
    compiler: WasmerCompiler::Singlepass,
};

pub(crate) fn wasmer2_vm_hash() -> u64 {
    WASMER2_CONFIG.config_hash()
}

#[derive(Clone)]
pub(crate) struct VMArtifact {
    artifact: Arc<dyn Artifact>,
    _engine: UniversalEngine,
}

impl VMArtifact {
    pub(crate) fn artifact(&self) -> &dyn Artifact {
        &*self.artifact
    }
}

#[derive(loupe::MemoryUsage)]
pub(crate) struct Wasmer2VM {
    #[loupe(skip)]
    config: VMConfig,
    engine: UniversalEngine,
}

impl Wasmer2VM {
    pub(crate) fn new(config: VMConfig) -> Self {
        use wasmer_compiler::{CpuFeature, Target, Triple};
        // We only support singlepass compiler at the moment.
        assert_eq!(WASMER2_CONFIG.compiler, WasmerCompiler::Singlepass);
        let compiler = Singlepass::new();
        // We only support universal engine at the moment.
        assert_eq!(WASMER2_CONFIG.engine, WasmerEngine::Universal);
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
        Self {
            config,
            engine: Universal::new(compiler)
                .target(Target::new(Triple::host(), target_features))
                .features(WASMER_FEATURES)
                .engine(),
        }
    }

    pub(crate) fn compile_uncached(&self, code: &[u8]) -> Result<VMArtifact, CompilationError> {
        self.engine
            .validate(code)
            .map_err(|e| CompilationError::WasmerCompileError { msg: e.to_string() })?;
        Ok(VMArtifact {
            artifact: self
                .engine
                .compile(code, &self)
                .map_err(|e| CompilationError::WasmerCompileError { msg: e.to_string() })?,
            _engine: self.engine.clone(),
        })
    }

    pub(crate) unsafe fn deserialize(
        &self,
        serialized: &[u8],
    ) -> Result<VMArtifact, DeserializeError> {
        Ok(VMArtifact {
            artifact: self.engine.deserialize(serialized)?,
            _engine: self.engine.clone(),
        })
    }

    fn run_method(
        &self,
        artifact: &VMArtifact,
        mut import: Wasmer2Imports<'_, '_>,
        method_name: &str,
    ) -> Result<(), VMError> {
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

        let entrypoint = get_entrypoint_index(&*artifact.artifact, method_name)?;
        unsafe {
            let instance = {
                let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
                // SAFETY/FIXME: this transmute shouldn't be necessary, but we are limited by
                // wasmer's API right now.
                //
                // An important caveat is that the `'static` lifetime here refers to the lifetime
                // of `VMLogic` reference to which is retained by the `InstanceHandle` we create.
                // However this `InstanceHandle` only lives during the execution of this body, so
                // we can be sure that `VMLogic` remains live and valid at any time.
                let import =
                    std::mem::transmute::<_, &mut Wasmer2Imports<'static, 'static>>(&mut import);
                // SAFETY: we ensure that the tables are valid during the lifetime of this instance
                // by retaining an instance to `UniversalEngine` which holds the allocations.
                let handle = artifact
                    .artifact
                    .instantiate(
                        &self,
                        import,
                        Box::new(()),
                        // SAFETY: We have verified that the `FastGasCounter` layout matches the
                        // expected layout. `gas` remains dereferenceable throughout this function
                        // by the virtue of it being contained within `import` which lives for the
                        // entirety of this function.
                        InstanceConfig::new_with_counter(gas),
                    )
                    .map_err(|err| translate_instantiation_error(err, import.vmlogic))?;
                // SAFETY: being called immediately after instantiation.
                artifact
                    .artifact
                    .finish_instantiation(self, &handle)
                    .map_err(|err| translate_instantiation_error(err, import.vmlogic))?;
                handle
            };
            let external = instance.lookup_by_declaration(&ExportIndex::Function(entrypoint));
            if let VMExtern::Function(f) = external {
                let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
                // Signature for the entry point should be `() -> ()`. This is only a sanity check
                // – this should've been already checked by `get_entrypoint_index`.
                if f.signature.params().is_empty() && f.signature.results().is_empty() {
                    let trampoline = f.call_trampoline.expect("externs always have a trampoline");
                    // SAFETY: we double-checked the signature, and all of the remaining arguments
                    // come from an exported function definition which must be valid since it comes
                    // from wasmer itself.
                    wasmer_vm::wasmer_call_trampoline(
                        self,
                        f.vmctx,
                        trampoline,
                        f.address,
                        [].as_mut_ptr() as *mut _,
                    )
                    .map_err(|e| {
                        translate_runtime_error(RuntimeError::from_trap(e), import.vmlogic)
                    })?;
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

        Ok(())
    }

    pub(crate) fn run_module<'a>(
        &self,
        artifact: &VMArtifact,
        memory: &mut Wasmer2Memory,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        current_protocol_version: ProtocolVersion,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        let vmmemory = memory.vm();
        let mut logic = VMLogic::new_with_protocol_version(
            ext,
            context,
            &self.config,
            fees_config,
            promise_results,
            memory,
            current_protocol_version,
        );
        let import = imports::wasmer2::build(vmmemory, &mut logic, current_protocol_version);
        if let Err(e) = get_entrypoint_index(&*artifact.artifact, method_name) {
            return (None, Some(e));
        }
        let err = self.run_method(artifact, import, method_name).err();
        (Some(logic.outcome()), err)
    }
}

unsafe impl wasmer_vm::TrapHandler for Wasmer2VM {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn custom_trap_handler(&self, _: &dyn Fn(&wasmer_vm::TrapHandlerFn) -> bool) -> bool {
        false
    }
}

impl wasmer_engine::Tunables for &Wasmer2VM {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        MemoryStyle::Static {
            bound: memory.maximum.unwrap_or(Pages(self.config.limit_config.max_memory_pages)),
            offset_guard_size: 0x1000,
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
    fn run(
        &self,
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        wasm_config: &VMConfig,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        current_protocol_version: ProtocolVersion,
        cache: Option<&dyn CompiledContractCache>,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        let _span = tracing::debug_span!(
            target: "vm",
            "run_wasmer2",
            "code.len" = code.code().len(),
            %method_name
        )
        .entered();

        if method_name.is_empty() {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName,
                ))),
            );
        }
        let artifact =
            cache::wasmer2_cache::compile_module_cached_wasmer2(code, wasm_config, cache);
        let artifact = match into_vm_result(artifact) {
            Ok(it) => it,
            Err(err) => return (None, Some(err)),
        };

        let mut memory = Wasmer2Memory::new(
            wasm_config.limit_config.initial_memory_pages,
            wasm_config.limit_config.max_memory_pages,
        )
        .expect("Cannot create memory for a contract call");

        // FIXME: this mostly duplicates the `run_module` method.
        // Note that we don't clone the actual backing memory, just increase the RC.
        let vmmemory = memory.vm();
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
        let import = imports::wasmer2::build(vmmemory, &mut logic, current_protocol_version);
        if let Err(e) = get_entrypoint_index(&*artifact.artifact, method_name) {
            return (None, Some(e));
        }
        let err = self.run_method(&artifact, import, method_name).err();
        (Some(logic.outcome()), err)
    }

    fn precompile(
        &self,
        code: &[u8],
        code_hash: &near_primitives::hash::CryptoHash,
        wasm_config: &VMConfig,
        cache: &dyn CompiledContractCache,
    ) -> Option<VMError> {
        let result = crate::cache::wasmer2_cache::compile_and_serialize_wasmer2(
            code,
            code_hash,
            wasm_config,
            cache,
        );
        into_vm_result(result).err()
    }

    fn check_compile(&self, code: &Vec<u8>) -> bool {
        self.compile_uncached(code).is_ok()
    }
}
