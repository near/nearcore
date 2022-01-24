use crate::cache::into_vm_result;
use crate::errors::IntoVMError;
use crate::prepare::WASM_FEATURES;
use crate::{cache, imports};
use memoffset::offset_of;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::{
    CompilationError, FunctionCallError, HostError, MethodResolveError, PrepareError, VMError,
    WasmTrap,
};
use near_vm_logic::types::{PromiseResult, ProtocolVersion};
use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMLogicError, VMOutcome};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use wasmer::{
    Bytes, ImportObject, Instance, InstantiationError, Memory, MemoryType, Module, Pages,
    RuntimeError, Store,
};

use near_stable_hasher::StableHasher;
use near_vm_logic::gas_counter::FastGasCounter;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_types::InstanceConfig;
use wasmer_vm::TrapCode;

const WASMER_FEATURES: wasmer::Features = wasmer::Features {
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

pub struct Wasmer2Memory(Memory);

impl Wasmer2Memory {
    pub fn new(
        store: &Store,
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, VMError> {
        Ok(Wasmer2Memory(
            Memory::new(
                store,
                MemoryType::new(Pages(initial_memory_pages), Some(Pages(max_memory_pages)), false),
            )
            .expect("TODO creating memory cannot fail"),
        ))
    }

    pub fn clone(&self) -> Memory {
        self.0.clone()
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
        for (i, cell) in self.0.view()[offset..(offset + buffer.len())].iter().enumerate() {
            buffer[i] = cell.get();
        }
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        self.0.view()[offset as usize].get()
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        let offset = offset as usize;
        self.0.view()[offset..(offset + buffer.len())]
            .iter()
            .zip(buffer.iter())
            .for_each(|(cell, v)| cell.set(*v));
    }
}

impl IntoVMError for wasmer::InstantiationError {
    fn into_vm_error(self) -> VMError {
        match self {
            wasmer::InstantiationError::Link(e) => {
                VMError::FunctionCallError(FunctionCallError::LinkError { msg: e.to_string() })
            }
            wasmer::InstantiationError::Start(e) => e.into_vm_error(),
            wasmer::InstantiationError::HostEnvInitialization(_) => {
                VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Instantiate),
                ))
            }
        }
    }
}

impl IntoVMError for wasmer::RuntimeError {
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

impl IntoVMError for wasmer::ExportError {
    fn into_vm_error(self) -> VMError {
        match self {
            wasmer::ExportError::IncompatibleType => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature),
            ),
            wasmer::ExportError::Missing(_) => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
            ),
        }
    }
}

fn check_method(module: &Module, method_name: &str) -> Result<(), VMError> {
    let info = module.info();
    use wasmer_types::ExportIndex::Function;
    if let Some(Function(index)) = info.exports.get(method_name) {
        let func = info.functions.get(index.clone()).unwrap();
        let sig = info.signatures.get(func.clone()).unwrap();
        if sig.params().is_empty() && sig.results().is_empty() {
            Ok(())
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

fn run_method(
    module: &Module,
    import: &ImportObject,
    method_name: &str,
    logic: &mut VMLogic,
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

    let instance = {
        let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
        Instance::new_with_config(
            module,
            unsafe {
                InstanceConfig::new_with_counter(
                    logic.gas_counter_pointer() as *mut wasmer_types::FastGasCounter
                )
            },
            &import,
        )
        .map_err(|err| translate_instantiation_error(err, logic))?
    };
    let f = instance.exports.get_function(method_name).map_err(|err| err.into_vm_error())?;
    let f = f.native::<(), ()>().map_err(|err| err.into_vm_error())?;

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
        f.call().map_err(|err| translate_runtime_error(err, logic))?
    }

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/drop_instance").entered();
        drop(instance)
    }

    Ok(())
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
    seed: (1 << 10) | (4 << 6) | 1,
    engine: WasmerEngine::Universal,
    compiler: WasmerCompiler::Singlepass,
};

pub(crate) fn wasmer2_vm_hash() -> u64 {
    WASMER2_CONFIG.config_hash()
}

pub(crate) fn default_wasmer2_store() -> Store {
    // We only support singlepass compiler at the moment.
    assert_eq!(WASMER2_CONFIG.compiler, WasmerCompiler::Singlepass);
    let compiler = Singlepass::new();
    // We only support universal engine at the moment.
    assert_eq!(WASMER2_CONFIG.engine, WasmerEngine::Universal);
    let target_features = if cfg!(feature = "no_cpu_compatibility_checks") {
        let mut fs = wasmer::CpuFeature::set();
        // These features should be sufficient to run the single pass compiler.
        fs.insert(wasmer::CpuFeature::SSE2);
        fs.insert(wasmer::CpuFeature::SSE3);
        fs.insert(wasmer::CpuFeature::SSSE3);
        fs.insert(wasmer::CpuFeature::SSE41);
        fs.insert(wasmer::CpuFeature::SSE42);
        fs.insert(wasmer::CpuFeature::POPCNT);
        fs.insert(wasmer::CpuFeature::AVX);
        fs
    } else {
        wasmer::CpuFeature::for_host()
    };
    let engine = wasmer::Universal::new(compiler)
        .features(WASMER_FEATURES)
        .target(wasmer::Target::new(wasmer::Triple::host(), target_features))
        .engine();
    Store::new(&engine)
}

pub(crate) fn run_wasmer2_module<'a>(
    module: &Module,
    store: &Store,
    memory: &mut Wasmer2Memory,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    current_protocol_version: ProtocolVersion,
) -> (Option<VMOutcome>, Option<VMError>) {
    // Do we really need that code?
    if method_name.is_empty() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName,
            ))),
        );
    }

    // Note that we don't clone the actual backing memory, just increase the RC.
    let memory_copy = memory.clone();

    let mut logic = VMLogic::new_with_protocol_version(
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        memory,
        current_protocol_version,
    );

    let import = imports::wasmer2::build(store, memory_copy, &mut logic, current_protocol_version);

    if let Err(e) = check_method(module, method_name) {
        return (None, Some(e));
    }

    let err = run_method(module, &import, method_name, &mut logic).err();
    (Some(logic.outcome()), err)
}

pub(crate) struct Wasmer2VM;

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
        // NaN behavior is deterministic as of now: https://github.com/wasmerio/wasmer/issues/1269
        // So doesn't require x86. However, when it is on x86, AVX is required:
        // https://github.com/wasmerio/wasmer/issues/1567
        #[cfg(not(feature = "no_cpu_compatibility_checks"))]
        if (cfg!(target_arch = "x86") || !cfg!(target_arch = "x86_64"))
            && !is_x86_feature_detected!("avx")
        {
            panic!("AVX support is required in order to run Wasmer VM Singlepass backend.");
        }

        if method_name.is_empty() {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName,
                ))),
            );
        }

        let store = default_wasmer2_store();
        let module =
            cache::wasmer2_cache::compile_module_cached_wasmer2(code, wasm_config, cache, &store);
        let module = match into_vm_result(module) {
            Ok(it) => it,
            Err(err) => return (None, Some(err)),
        };

        let mut memory = Wasmer2Memory::new(
            &store,
            wasm_config.limit_config.initial_memory_pages,
            wasm_config.limit_config.max_memory_pages,
        )
        .expect("Cannot create memory for a contract call");
        // Note that we don't clone the actual backing memory, just increase the RC.
        let memory_copy = memory.clone();

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

        let import_object =
            imports::wasmer2::build(&store, memory_copy, &mut logic, current_protocol_version);

        if let Err(e) = check_method(&module, method_name) {
            return (None, Some(e));
        }

        let err = run_method(&module, &import_object, method_name, &mut logic).err();
        (Some(logic.outcome()), err)
    }

    fn precompile(
        &self,
        code: &[u8],
        code_hash: &near_primitives::hash::CryptoHash,
        wasm_config: &VMConfig,
        cache: &dyn CompiledContractCache,
    ) -> Option<VMError> {
        let store = crate::wasmer2_runner::default_wasmer2_store();
        let result = crate::cache::wasmer2_cache::compile_and_serialize_wasmer2(
            code,
            code_hash,
            wasm_config,
            cache,
            &store,
        );
        into_vm_result(result).err()
    }

    fn check_compile(&self, code: &Vec<u8>) -> bool {
        let store = default_wasmer2_store();
        Module::new(&store, code).is_ok()
    }
}
