use crate::errors::IntoVMError;
use crate::prepare::WASM_FEATURES;
use crate::runner::VMResult;
use crate::{imports, prepare};
use near_primitives::checked_feature;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{
    CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMError, VMLogicError,
    WasmTrap,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, MemoryLike, VMContext, VMLogic};
use std::cell::RefCell;
use std::ffi::c_void;
use std::str;
use wasmtime::ExternType::Func;
use wasmtime::{Engine, Linker, Memory, MemoryType, Module, Store, TrapCode};

thread_local! {
    pub(crate) static CALLER: RefCell<Option<wasmtime::Caller<'static, ()>>> = RefCell::new(None);
}
pub struct WasmtimeMemory(Memory);

impl WasmtimeMemory {
    pub fn new(
        store: &mut Store<()>,
        initial_memory_bytes: u32,
        max_memory_bytes: u32,
    ) -> Result<Self, VMError> {
        Ok(WasmtimeMemory(
            Memory::new(store, MemoryType::new(initial_memory_bytes, Some(max_memory_bytes)))
                .map_err(|_| PrepareError::Memory)?,
        ))
    }
}

impl MemoryLike for WasmtimeMemory {
    fn fits_memory(&self, offset: u64, len: u64) -> bool {
        CALLER.with(|caller| match offset.checked_add(len) {
            None => false,
            Some(end) => self.0.data_size(caller.borrow_mut().as_mut().unwrap()) as u64 >= end,
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
        CALLER.with(|caller| {
            let offset = offset as usize;
            let mut caller = caller.borrow_mut();
            let caller = caller.as_mut().unwrap();
            for i in 0..buffer.len() {
                buffer[i] = self.0.data(&mut *caller)[i + offset];
            }
        })
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        CALLER.with(|caller| self.0.data(caller.borrow_mut().as_mut().unwrap())[offset as usize])
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        CALLER.with(|caller| {
            let offset = offset as usize;
            let mut caller = caller.borrow_mut();
            let caller = caller.as_mut().unwrap();
            for i in 0..buffer.len() {
                self.0.data_mut(&mut *caller)[i + offset] = buffer[i];
            }
        })
    }
}

fn trap_to_error(trap: &wasmtime::Trap) -> VMError {
    if trap.i32_exit_status() == Some(239) {
        match imports::wasmtime::last_error() {
            Some(VMLogicError::HostError(h)) => {
                VMError::FunctionCallError(FunctionCallError::HostError(h))
            }
            Some(VMLogicError::ExternalError(s)) => VMError::ExternalError(s),
            Some(VMLogicError::InconsistentStateError(e)) => VMError::InconsistentStateError(e),
            None => panic!("Error is not properly set"),
        }
    } else {
        match trap.trap_code() {
            Some(TrapCode::StackOverflow) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::StackOverflow))
            }
            Some(TrapCode::MemoryOutOfBounds) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds))
            }
            Some(TrapCode::TableOutOfBounds) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds))
            }
            Some(TrapCode::IndirectCallToNull) => VMError::FunctionCallError(
                FunctionCallError::WasmTrap(WasmTrap::IndirectCallToNull),
            ),
            Some(TrapCode::BadSignature) => VMError::FunctionCallError(
                FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature),
            ),
            Some(TrapCode::IntegerOverflow) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic))
            }
            Some(TrapCode::IntegerDivisionByZero) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic))
            }
            Some(TrapCode::BadConversionToInteger) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic))
            }
            Some(TrapCode::UnreachableCodeReached) => {
                VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::Unreachable))
            }
            Some(TrapCode::Interrupt) => VMError::FunctionCallError(
                FunctionCallError::Nondeterministic("interrupt".to_string()),
            ),
            _ => VMError::FunctionCallError(FunctionCallError::WasmUnknownError {
                debug_message: "unknown trap".to_string(),
            }),
        }
    }
}

impl IntoVMError for anyhow::Error {
    fn into_vm_error(self) -> VMError {
        let cause = self.root_cause();
        match cause.downcast_ref::<wasmtime::Trap>() {
            Some(trap) => trap_to_error(trap),
            None => VMError::FunctionCallError(FunctionCallError::LinkError {
                msg: format!("{:#?}", cause),
            }),
        }
    }
}

impl IntoVMError for wasmtime::Trap {
    fn into_vm_error(self) -> VMError {
        trap_to_error(&self)
    }
}

#[cfg(not(feature = "lightbeam"))]
pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
    Engine::new(config).unwrap()
}

#[cfg(feature = "lightbeam")]
pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
    Engine::new(config.strategy(wasmtime::Strategy::Lightbeam).unwrap()).unwrap()
}

pub(super) fn default_config() -> wasmtime::Config {
    let mut config = wasmtime::Config::default();
    config.wasm_threads(WASM_FEATURES.threads);
    config.wasm_reference_types(WASM_FEATURES.reference_types);
    config.wasm_simd(WASM_FEATURES.simd);
    config.wasm_bulk_memory(WASM_FEATURES.bulk_memory);
    config.wasm_multi_value(WASM_FEATURES.multi_value);
    config.wasm_multi_memory(WASM_FEATURES.multi_memory);
    config.wasm_module_linking(WASM_FEATURES.module_linking);
    config
}

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}

pub(crate) struct WasmtimeVM {
    config: VMConfig,
}

impl WasmtimeVM {
    pub(crate) fn new(config: VMConfig) -> Self {
        Self { config }
    }
}

impl crate::runner::VM for WasmtimeVM {
    fn run(
        &self,
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        current_protocol_version: ProtocolVersion,
        _cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult {
        let _span = tracing::debug_span!(
            target: "vm",
            "run_wasmtime",
            "code.len" = code.code().len(),
            %method_name
        )
        .entered();
        let mut config = default_config();
        let engine = get_engine(&mut config);
        let mut store = Store::new(&engine, ());
        let mut memory = WasmtimeMemory::new(
            &mut store,
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        )
        .unwrap();
        let memory_copy = memory.0;
        let mut logic = VMLogic::new_with_protocol_version(
            ext,
            context,
            &self.config,
            fees_config,
            promise_results,
            &mut memory,
            current_protocol_version,
        );

        let result = logic.checks_before_code_loading(
            method_name,
            current_protocol_version,
            code.code().len(),
        );
        if result.is_err() {
            return VMResult::abort(logic, result.unwrap_err());
        }

        let prepared_code = match prepare::prepare_contract(code.code(), &self.config) {
            Ok(code) => code,
            Err(err) => return VMResult::abort(logic, VMError::from(err)),
        };
        let module = match Module::new(&engine, prepared_code) {
            Ok(module) => module,
            Err(err) => return VMResult::abort(logic, err.into_vm_error()),
        };
        let mut linker = Linker::new(&engine);

        if !checked_feature!(
            "protocol_feature_fix_contract_loading_cost",
            FixContractLoadingCost,
            current_protocol_version
        ) {
            if logic.add_contract_loading_fee(code.code().len() as u64).is_err() {
                let err = VMError::FunctionCallError(FunctionCallError::HostError(
                    near_vm_errors::HostError::GasExceeded,
                ));
                return VMResult::abort(logic, err);
            }
        }

        // Unfortunately, due to the Wasmtime implementation we have to do tricks with the
        // lifetimes of the logic instance and pass raw pointers here.
        let raw_logic = &mut logic as *mut _ as *mut c_void;
        imports::wasmtime::link(&mut linker, memory_copy, raw_logic, current_protocol_version);
        match module.get_export(method_name) {
            Some(export) => match export {
                Func(func_type) => {
                    if func_type.params().len() != 0 || func_type.results().len() != 0 {
                        let err =
                            VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                                MethodResolveError::MethodInvalidSignature,
                            ));
                        if checked_feature!(
                            "protocol_feature_fix_contract_loading_cost",
                            FixContractLoadingCost,
                            current_protocol_version
                        ) {
                            return VMResult::abort(logic, err);
                        } else {
                            return VMResult::nop_outcome(err);
                        }
                    }
                }
                _ => {
                    let err = VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodNotFound,
                    ));
                    if checked_feature!(
                        "protocol_feature_fix_contract_loading_cost",
                        FixContractLoadingCost,
                        current_protocol_version
                    ) {
                        return VMResult::abort(logic, err);
                    } else {
                        return VMResult::nop_outcome(err);
                    }
                }
            },
            None => {
                let err = VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound,
                ));
                if checked_feature!(
                    "protocol_feature_fix_contract_loading_cost",
                    FixContractLoadingCost,
                    current_protocol_version
                ) {
                    return VMResult::abort(logic, err);
                } else {
                    return VMResult::nop_outcome(err);
                }
            }
        }
        match linker.instantiate(&mut store, &module) {
            Ok(instance) => match instance.get_func(&mut store, method_name) {
                Some(func) => match func.typed::<(), (), _>(&mut store) {
                    Ok(run) => match run.call(&mut store, ()) {
                        Ok(_) => VMResult::ok(logic),
                        Err(err) => (VMResult::abort(logic, err.into_vm_error())),
                    },
                    Err(err) => VMResult::abort(logic, err.into_vm_error()),
                },
                None => {
                    let err = VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodNotFound,
                    ));
                    if checked_feature!(
                        "protocol_feature_fix_contract_loading_cost",
                        FixContractLoadingCost,
                        current_protocol_version
                    ) {
                        return VMResult::abort(logic, err);
                    } else {
                        return VMResult::nop_outcome(err);
                    }
                }
            },
            Err(err) => VMResult::abort(logic, err.into_vm_error()),
        }
    }

    fn precompile(
        &self,
        _code: &[u8],
        _code_hash: &CryptoHash,
        _cache: &dyn CompiledContractCache,
    ) -> Option<VMError> {
        Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::UnsupportedCompiler {
                msg: "Precompilation not supported in Wasmtime yet".to_string(),
            },
        )))
    }

    fn check_compile(&self, code: &Vec<u8>) -> bool {
        let mut config = default_config();
        let engine = get_engine(&mut config);
        Module::new(&engine, code).is_ok()
    }
}
