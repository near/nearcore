use crate::cache::into_vm_result;
use crate::errors::IntoVMError;
use crate::{cache, imports};
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::{
    CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMError, WasmTrap,
};
use near_vm_logic::types::{PromiseResult, ProtocolVersion};
use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMLogicError, VMOutcome};
use std::hash::{Hash, Hasher};
use wasmer::{Bytes, ImportObject, Instance, Memory, MemoryType, Module, Pages, Store};

use near_stable_hasher::StableHasher;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_vm::TrapCode;

pub struct Wasmer2Memory(Memory);

impl Wasmer2Memory {
    pub fn new(
        store: &Store,
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, VMError> {
        Ok(Wasmer2Memory(
            Memory::new(
                &store,
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
            return (&e).into();
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

pub fn run_wasmer2(
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
    let _span = tracing::debug_span!(target: "vm", "run_wasmer2").entered();
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
        cache::wasmer2_cache::compile_module_cached_wasmer2(&code, wasm_config, cache, &store);
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
        imports::build_wasmer2(&store, memory_copy, &mut logic, current_protocol_version);

    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    let err = run_method(&module, &import_object, method_name).err();
    (Some(logic.outcome()), err)
}

fn run_method(module: &Module, import: &ImportObject, method_name: &str) -> Result<(), VMError> {
    let _span = tracing::debug_span!(target: "vm", "run_method").entered();

    let instance = {
        let _span = tracing::debug_span!(target: "vm", "run_method/instantiate").entered();
        Instance::new(&module, &import).map_err(|err| err.into_vm_error())?
    };
    let f = instance.exports.get_function(method_name).map_err(|err| err.into_vm_error())?;
    let f = f.native::<(), ()>().map_err(|err| err.into_vm_error())?;

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/call").entered();
        f.call().map_err(|err| err.into_vm_error())?
    }

    {
        let _span = tracing::debug_span!(target: "vm", "run_method/drop_instance").entered();
        drop(instance)
    }

    Ok(())
}

pub(crate) fn compile_wasmer2_module(code: &[u8]) -> bool {
    let store = default_wasmer2_store();
    Module::new(&store, code).is_ok()
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
    seed: i32,
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
//  kind << 10, kind is 1 for Wasmer
//  major version << 6
//  minor version
const WASMER2_CONFIG: Wasmer2Config = Wasmer2Config {
    seed: (1 << 10) | (2 << 6) | 0,
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
    let engine = wasmer::Universal::new(compiler).engine();
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

    let import = imports::build_wasmer2(store, memory_copy, &mut logic, current_protocol_version);

    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    let err = run_method(module, &import, method_name).err();
    (Some(logic.outcome()), err)
}
