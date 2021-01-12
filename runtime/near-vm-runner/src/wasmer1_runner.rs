use crate::errors::IntoVMError;
use crate::imports;
use near_primitives::types::CompiledContractCache;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::{
    CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMError,
};
use near_vm_logic::types::{ProfileData, PromiseResult, ProtocolVersion};
use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMOutcome};
use wasmer::{Bytes, Instance, Memory, MemoryType, Module, Pages, Singlepass, Store, JIT};

pub struct Wasmer1Memory(Memory);

impl Wasmer1Memory {
    pub fn new(
        store: &Store,
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, VMError> {
        Ok(Wasmer1Memory(
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

impl MemoryLike for Wasmer1Memory {
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

impl IntoVMError for wasmer::CompileError {
    fn into_vm_error(self) -> VMError {
        VMError::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::WasmerCompileError { msg: self.to_string() },
        ))
    }
}

impl IntoVMError for wasmer::InstantiationError {
    fn into_vm_error(self) -> VMError {
        match self {
            wasmer::InstantiationError::Link(e) => {
                VMError::FunctionCallError(FunctionCallError::LinkError { msg: e.to_string() })
            }
            _ => VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Instantiate),
            )),
        }
    }
}

impl IntoVMError for wasmer::RuntimeError {
    fn into_vm_error(self) -> VMError {
        VMError::FunctionCallError(FunctionCallError::WasmerRuntimeError(self.message()))
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

pub fn run_wasmer1<'a>(
    code_hash: Vec<u8>,
    code: &[u8],
    method_name: &[u8],
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    promise_results: &'a [PromiseResult],
    profile: Option<ProfileData>,
    current_protocol_version: ProtocolVersion,
    cache: Option<&'a dyn CompiledContractCache>,
) -> (Option<VMOutcome>, Option<VMError>) {
    if method_name.is_empty() {
        return (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName,
            ))),
        );
    }

    let engine = JIT::new(Singlepass::default()).engine();
    let store = Store::new(&engine);
    let module = match Module::new(&store, code) {
        Ok(x) => x,
        Err(err) => return (None, Some(err.into_vm_error())),
    };

    let mut memory = Wasmer1Memory::new(
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
        profile,
        current_protocol_version,
    );

    if logic.add_contract_compile_fee(code.len() as u64).is_err() {
        return (
            Some(logic.outcome()),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(
                near_vm_errors::HostError::GasExceeded,
            ))),
        );
    }
    let import_object = imports::build_wasmer1(&store, memory_copy, &mut logic);

    let method_name = match std::str::from_utf8(method_name) {
        Ok(x) => x,
        Err(_) => {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodUTF8Error,
                ))),
            )
        }
    };

    if let Err(e) = check_method(&module, method_name) {
        return (None, Some(e));
    }

    match Instance::new(&module, &import_object) {
        Ok(instance) => match instance.exports.get_function(&method_name) {
            Ok(f) => match f.native::<(), ()>() {
                Ok(f) => match f.call() {
                    Ok(_) => (Some(logic.outcome()), None),
                    Err(e) => (Some(logic.outcome()), Some(e.into_vm_error())),
                },
                Err(e) => (Some(logic.outcome()), Some(e.into_vm_error())),
            },
            Err(e) => (Some(logic.outcome()), Some(e.into_vm_error())),
        },
        Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
    }
}

pub fn compile_module(code: &[u8]) -> bool {
    let engine = JIT::new(Singlepass::default()).engine();
    let store = Store::new(&engine);
    Module::new(&store, code).is_ok()
}
