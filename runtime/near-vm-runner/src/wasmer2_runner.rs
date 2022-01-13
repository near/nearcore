use crate::cache::into_vm_result;
use crate::imports::wasmer2::Wasmer2Imports;
use crate::prepare::WASM_FEATURES;
use crate::{cache, imports};
use memoffset::offset_of;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_stable_hasher::StableHasher;
use near_vm_errors::{CompilationError, FunctionCallError, MethodResolveError, VMError, WasmTrap};
use near_vm_logic::gas_counter::FastGasCounter;
use near_vm_logic::types::{PromiseResult, ProtocolVersion};
use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMOutcome};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine::{DeserializeError, Engine};
use wasmer_engine_universal::{Universal, UniversalEngine};
use wasmer_types::{
    ExportIndex, Features, FunctionIndex, InstanceConfig, MemoryType, Pages, WASM_PAGE_SIZE,
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
                &MemoryStyle::Static {
                    bound: max_pages,
                    offset_guard_size: wasmer_types::WASM_PAGE_SIZE as u64,
                },
            )
            .expect("creating memory must not fail"),
        )))
    }

    // Returns the pointer to memory at the specified offset and the size of the buffer starting at
    // the returned pointer.
    fn data_offset(&self, offset: u64) -> Option<(*mut u8, usize)> {
        let size = self.0.size().bytes().0;
        let offset = usize::try_from(offset).ok()?;
        // `checked_sub` here verifies that offsetting the buffer by offset still lands us
        // in-bounds of the allocated object.
        let remaining = size.checked_sub(offset)?;
        Some(unsafe {
            // SAFETY: we verified that offsetting the base pointer by `offset` still lands us
            // in-bounds of the original object.
            (self.0.vmmemory().as_ref().base.add(offset), remaining)
        })
    }

    fn get_memory_buffer(&self, offset: u64, len: usize) -> *mut u8 {
        let memory = self.data_offset(offset).map(|(data, remaining)| (data, len <= remaining));
        if let Some((ptr, true)) = memory {
            ptr
        } else {
            panic!("memory access out of bounds")
        }
    }

    pub(crate) fn vm(&self) -> VMMemory {
        VMMemory { from: self.0.clone(), instance_ref: None }
    }
}

impl MemoryLike for Wasmer2Memory {
    fn fits_memory(&self, offset: u64, len: u64) -> bool {
        self.data_offset(offset)
            .and_then(|(_, remaining)| {
                let len = usize::try_from(len).ok()?;
                Some(len <= remaining)
            })
            .unwrap_or(false)
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
        unsafe {
            let memory = self.get_memory_buffer(offset, buffer.len());
            // SAFETY: we verified indices into are valid and the pointer will always be valid as
            // well. Our runtime is currently only executing Wasm code on a single thread, so data
            // races aren't a concern here.
            std::ptr::copy_nonoverlapping(memory, buffer.as_mut_ptr(), buffer.len());
        }
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        unsafe { *self.get_memory_buffer(offset, 1) }
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        unsafe {
            let memory = self.get_memory_buffer(offset, buffer.len());
            // SAFETY: we verified indices into are valid and the pointer will always be valid as
            // well. Our runtime is currently only executing Wasm code on a single thread, so data
            // races aren't a concern here.
            std::ptr::copy_nonoverlapping(buffer.as_ptr(), memory, buffer.len());
        }
    }
}

fn get_entrypoint_index(
    artifact: &dyn wasmer_engine::Artifact,
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

fn translate_instantiation_error(
    err: wasmer_engine::InstantiationError,
    logic: &mut VMLogic,
) -> VMError {
    use wasmer_engine::InstantiationError::*;
    match err {
        Start(err) => translate_runtime_error(err, logic),
        Link(e) => VMError::FunctionCallError(FunctionCallError::LinkError { msg: e.to_string() }),
        CpuFeature(e) => {
            panic!("host does not support the CPU features required to run contracts: {}", e)
        }
    }
}

fn translate_runtime_error(error: wasmer_engine::RuntimeError, logic: &mut VMLogic) -> VMError {
    // Errors produced by host function calls also become `RuntimeError`s that wrap a dynamic
    // instance of `VMLogicError` internally. See the implementation of `Wasmer2Imports`.
    let error = match error.downcast::<near_vm_errors::VMLogicError>() {
        Ok(vm_logic) => return vm_logic.into(),
        Err(original) => original,
    };
    let msg = error.message();
    let trap_code = error.to_trap().unwrap_or_else(|| {
        panic!("runtime error is not a trap: {}", msg);
    });
    VMError::FunctionCallError(match trap_code {
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
    seed: (1 << 10) | (5 << 6) | 0,
    engine: WasmerEngine::Universal,
    compiler: WasmerCompiler::Singlepass,
};

pub(crate) fn wasmer2_vm_hash() -> u64 {
    WASMER2_CONFIG.config_hash()
}

#[derive(Clone)]
pub(crate) struct VMArtifact {
    artifact: Arc<dyn wasmer_engine::Artifact>,
    // FIXME(nagisa): Creating an artifact currently allocates data in the arena owned by the
    // `UniversalEngine`. We cache artifacts in memory that outlive the VM they are part of, so for
    // the time being lets keep a reference to the engine with each artifact.
    //
    // There are two outstanding tasks that we may want to resolve which will allow removing this
    // specific field: first is removing the in-memory cache (and/or making sure that the VM
    // lifetime outlives the VM cache). The second is removing the allocations in question – those
    // allocate data such as trampolines and should only occur when an instantiation happens.
    _engine: UniversalEngine,
}

impl VMArtifact {
    pub(crate) fn artifact(&self) -> &dyn wasmer_engine::Artifact {
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
    pub(crate) fn new_for_target(config: VMConfig, target: wasmer_compiler::Target) -> Self {
        // We only support singlepass compiler at the moment.
        assert_eq!(WASMER2_CONFIG.compiler, WasmerCompiler::Singlepass);
        let compiler = Singlepass::new();
        // We only support universal engine at the moment.
        assert_eq!(WASMER2_CONFIG.engine, WasmerEngine::Universal);
        Self {
            config,
            engine: Universal::new(compiler).target(target).features(WASMER_FEATURES).engine(),
        }
    }

    pub(crate) fn new(config: VMConfig) -> Self {
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

    fn run_method<'vmlogic, 'vmlogic_refs>(
        &self,
        artifact: &VMArtifact,
        mut import: Wasmer2Imports<'vmlogic, 'vmlogic_refs>,
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
                let import = std::mem::transmute::<
                    &mut Wasmer2Imports<'vmlogic, 'vmlogic_refs>,
                    &mut Wasmer2Imports<'static, 'static>,
                >(&mut import);
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
                        translate_runtime_error(
                            wasmer_engine::RuntimeError::from_trap(e),
                            import.vmlogic,
                        )
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
            cache::wasmer2_cache::compile_module_cached_wasmer2(code, &self.config, cache);
        let artifact = match into_vm_result(artifact) {
            Ok(it) => it,
            Err(err) => return (None, Some(err)),
        };

        let mut memory = Wasmer2Memory::new(
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
        cache: &dyn CompiledContractCache,
    ) -> Option<VMError> {
        let result = crate::cache::wasmer2_cache::compile_and_serialize_wasmer2(
            code,
            code_hash,
            &self.config,
            cache,
        );
        into_vm_result(result).err()
    }

    fn check_compile(&self, code: &Vec<u8>) -> bool {
        self.compile_uncached(code).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use wasmer_types::WASM_PAGE_SIZE;

    #[test]
    fn get_memory_buffer() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        // these should not panic with memory out of bounds
        memory.get_memory_buffer(0, WASM_PAGE_SIZE);
        memory.get_memory_buffer(WASM_PAGE_SIZE as u64 - 1, 1);
        memory.get_memory_buffer(WASM_PAGE_SIZE as u64, 0);
    }

    #[test]
    #[should_panic]
    fn get_memory_buffer_oob1() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        memory.get_memory_buffer(1 + WASM_PAGE_SIZE as u64, 0);
    }

    #[test]
    #[should_panic]
    fn get_memory_buffer_oob2() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        memory.get_memory_buffer(WASM_PAGE_SIZE as u64, 1);
    }

    #[test]
    fn memory_data_offset() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        assert_matches!(memory.data_offset(0), Some((_, size)) => assert_eq!(size, WASM_PAGE_SIZE));
        assert_matches!(memory.data_offset(WASM_PAGE_SIZE as u64), Some((_, size)) => {
            assert_eq!(size, 0)
        });
        assert_matches!(memory.data_offset(WASM_PAGE_SIZE as u64 + 1), None);
        assert_matches!(memory.data_offset(0xFFFF_FFFF_FFFF_FFFF), None);
    }

    #[test]
    fn memory_read() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        let mut buffer = vec![42; WASM_PAGE_SIZE];
        near_vm_logic::MemoryLike::read_memory(&memory, 0, &mut buffer);
        // memory should be zeroed at creation.
        assert!(buffer.iter().all(|&v| v == 0));
    }

    #[test]
    #[should_panic]
    fn memory_read_oob() {
        let memory = super::Wasmer2Memory::new(1, 1).unwrap();
        let mut buffer = vec![42; WASM_PAGE_SIZE + 1];
        near_vm_logic::MemoryLike::read_memory(&memory, 0, &mut buffer);
    }

    #[test]
    fn memory_write() {
        let mut memory = super::Wasmer2Memory::new(1, 1).unwrap();
        let mut buffer = vec![42; WASM_PAGE_SIZE];
        near_vm_logic::MemoryLike::write_memory(
            &mut memory,
            WASM_PAGE_SIZE as u64 / 2,
            &buffer[..WASM_PAGE_SIZE / 2],
        );
        near_vm_logic::MemoryLike::read_memory(&memory, 0, &mut buffer);
        assert!(buffer[..WASM_PAGE_SIZE / 2].iter().all(|&v| v == 0));
        assert!(buffer[WASM_PAGE_SIZE / 2..].iter().all(|&v| v == 42));
        // Now the buffer is half 0s and half 42s

        near_vm_logic::MemoryLike::write_memory(
            &mut memory,
            0,
            &buffer[WASM_PAGE_SIZE / 4..3 * (WASM_PAGE_SIZE / 4)],
        );
        near_vm_logic::MemoryLike::read_memory(&memory, 0, &mut buffer);
        assert!(buffer[..WASM_PAGE_SIZE / 4].iter().all(|&v| v == 0));
        assert!(buffer[WASM_PAGE_SIZE / 4..].iter().all(|&v| v == 42));
    }

    #[test]
    #[should_panic]
    fn memory_write_oob() {
        let mut memory = super::Wasmer2Memory::new(1, 1).unwrap();
        let mut buffer = vec![42; WASM_PAGE_SIZE + 1];
        near_vm_logic::MemoryLike::write_memory(&mut memory, 0, &mut buffer);
    }
}
