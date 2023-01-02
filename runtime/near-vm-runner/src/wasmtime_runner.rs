use crate::errors::{ContractPrecompilatonResult, IntoVMError};
use crate::prepare::WASM_FEATURES;
use crate::{imports, prepare};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{
    CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMLogicError,
    VMRunnerError, WasmTrap,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, MemSlice, MemoryLike, VMContext, VMLogic, VMOutcome};
use std::borrow::Cow;
use std::cell::RefCell;
use std::ffi::c_void;
use wasmtime::ExternType::Func;
use wasmtime::{Engine, Linker, Memory, MemoryType, Module, Store, TrapCode};

type Caller = wasmtime::Caller<'static, ()>;
thread_local! {
    pub(crate) static CALLER: RefCell<Option<Caller>> = RefCell::new(None);
}
pub struct WasmtimeMemory(Memory);

impl WasmtimeMemory {
    pub fn new(
        store: &mut Store<()>,
        initial_memory_bytes: u32,
        max_memory_bytes: u32,
    ) -> Result<Self, FunctionCallError> {
        Ok(WasmtimeMemory(
            Memory::new(store, MemoryType::new(initial_memory_bytes, Some(max_memory_bytes)))
                .map_err(|_| PrepareError::Memory)?,
        ))
    }
}

fn with_caller<T>(func: impl FnOnce(&mut Caller) -> T) -> T {
    CALLER.with(|caller| func(caller.borrow_mut().as_mut().unwrap()))
}

impl MemoryLike for WasmtimeMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        let end = slice.end::<usize>()?;
        if end <= with_caller(|caller| self.0.data_size(caller)) {
            Ok(())
        } else {
            Err(())
        }
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        let range = slice.range::<usize>()?;
        with_caller(|caller| {
            self.0.data(caller).get(range).map(|slice| Cow::Owned(slice.to_vec())).ok_or(())
        })
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        with_caller(|caller| {
            let memory = self.0.data(caller).get(start..end).ok_or(())?;
            buffer.copy_from_slice(memory);
            Ok(())
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(buffer.len()).ok_or(())?;
        with_caller(|caller| {
            let memory = self.0.data_mut(caller).get_mut(start..end).ok_or(())?;
            memory.copy_from_slice(buffer);
            Ok(())
        })
    }
}

fn trap_to_error(trap: &wasmtime::Trap) -> Result<FunctionCallError, VMRunnerError> {
    if trap.i32_exit_status() == Some(239) {
        match imports::wasmtime::last_error() {
            Some(VMLogicError::HostError(h)) => Ok(FunctionCallError::HostError(h)),
            Some(VMLogicError::ExternalError(s)) => Err(VMRunnerError::ExternalError(s)),
            Some(VMLogicError::InconsistentStateError(e)) => {
                Err(VMRunnerError::InconsistentStateError(e))
            }
            None => panic!("Error is not properly set"),
        }
    } else {
        Ok(match trap.trap_code() {
            Some(TrapCode::StackOverflow) => FunctionCallError::WasmTrap(WasmTrap::StackOverflow),
            Some(TrapCode::MemoryOutOfBounds) => {
                FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
            }
            Some(TrapCode::TableOutOfBounds) => {
                FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds)
            }
            Some(TrapCode::IndirectCallToNull) => {
                FunctionCallError::WasmTrap(WasmTrap::IndirectCallToNull)
            }
            Some(TrapCode::BadSignature) => {
                FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature)
            }
            Some(TrapCode::IntegerOverflow) => {
                FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
            }
            Some(TrapCode::IntegerDivisionByZero) => {
                FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
            }
            Some(TrapCode::BadConversionToInteger) => {
                FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic)
            }
            Some(TrapCode::UnreachableCodeReached) => {
                FunctionCallError::WasmTrap(WasmTrap::Unreachable)
            }
            Some(TrapCode::Interrupt) => {
                return Err(VMRunnerError::Nondeterministic("interrupt".to_string()));
            }
            _ => {
                return Err(VMRunnerError::WasmUnknownError {
                    debug_message: "unknown trap".to_string(),
                });
            }
        })
    }
}

impl IntoVMError for anyhow::Error {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        let cause = self.root_cause();
        match cause.downcast_ref::<wasmtime::Trap>() {
            Some(trap) => trap_to_error(trap),
            None => Ok(FunctionCallError::LinkError { msg: format!("{:#?}", cause) }),
        }
    }
}

impl IntoVMError for wasmtime::Trap {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
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
    config.max_wasm_stack(1024 * 1024 * 1024).unwrap(); // wasm stack metering is implemented by pwasm-utils, we don't want wasmtime to trap before that
    config.wasm_threads(WASM_FEATURES.threads);
    config.wasm_reference_types(WASM_FEATURES.reference_types);
    config.wasm_simd(WASM_FEATURES.simd);
    config.wasm_bulk_memory(WASM_FEATURES.bulk_memory);
    config.wasm_multi_value(WASM_FEATURES.multi_value);
    config.wasm_multi_memory(WASM_FEATURES.multi_memory);
    assert_eq!(
        WASM_FEATURES.module_linking, false,
        "wasmtime currently does not support the module-linking feature"
    );
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
    ) -> Result<VMOutcome, VMRunnerError> {
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

        let result = logic.before_loading_executable(
            method_name,
            current_protocol_version,
            code.code().len(),
        );
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        let prepared_code = match prepare::prepare_contract(code.code(), &self.config) {
            Ok(code) => code,
            Err(err) => return Ok(VMOutcome::abort(logic, FunctionCallError::from(err))),
        };
        let module = match Module::new(&engine, prepared_code) {
            Ok(module) => module,
            Err(err) => return Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
        };
        let mut linker = Linker::new(&engine);

        let result = logic.after_loading_executable(current_protocol_version, code.code().len());
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        // Unfortunately, due to the Wasmtime implementation we have to do tricks with the
        // lifetimes of the logic instance and pass raw pointers here.
        let raw_logic = &mut logic as *mut _ as *mut c_void;
        imports::wasmtime::link(&mut linker, memory_copy, raw_logic, current_protocol_version);
        match module.get_export(method_name) {
            Some(export) => match export {
                Func(func_type) => {
                    if func_type.params().len() != 0 || func_type.results().len() != 0 {
                        let err = FunctionCallError::MethodResolveError(
                            MethodResolveError::MethodInvalidSignature,
                        );
                        return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                            logic,
                            err,
                            current_protocol_version,
                        ));
                    }
                }
                _ => {
                    return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                        logic,
                        FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
                        current_protocol_version,
                    ));
                }
            },
            None => {
                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                    logic,
                    FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
                    current_protocol_version,
                ));
            }
        }
        match linker.instantiate(&mut store, &module) {
            Ok(instance) => match instance.get_func(&mut store, method_name) {
                Some(func) => match func.typed::<(), (), _>(&mut store) {
                    Ok(run) => match run.call(&mut store, ()) {
                        Ok(_) => Ok(VMOutcome::ok(logic)),
                        Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
                    },
                    Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
                },
                None => {
                    return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                        logic,
                        FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
                        current_protocol_version,
                    ));
                }
            },
            Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
        }
    }

    fn precompile(
        &self,
        _code: &ContractCode,
        _cache: &dyn CompiledContractCache,
    ) -> Result<Result<ContractPrecompilatonResult, CompilationError>, near_vm_errors::CacheError>
    {
        Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable))
    }
}
