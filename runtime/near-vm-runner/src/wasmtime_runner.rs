use crate::errors::IntoVMError;
use crate::prepare::WASM_FEATURES;
use crate::{imports, prepare};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{
    CompilationError, FunctionCallError, MethodResolveError, VMError, VMLogicError, WasmTrap,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMLogic, VMOutcome};
use std::str;
use wasmtime::ExternType::Func;
use wasmtime::{Engine, Linker, Memory, Module, Store, TrapCode};

pub(crate) struct VMCtx<'a> {
    pub(crate) logic: VMLogic<'a>,

    // This must be an Option due to a circular dependency at creation time otherwise: the Store<VMCtx<'a>> needs a
    // VMCtx to be built, but the Memory can only be built once the Store has been built.
    pub(crate) mem: Option<Memory>,
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

pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
    Engine::new(config).unwrap()
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
    ) -> (Option<VMOutcome>, Option<VMError>) {
        let _span = tracing::debug_span!(
            target: "vm",
            "run_wasmtime",
            "code.len" = code.code().len(),
            %method_name
        )
        .entered();
        let mut config = default_config();
        let engine = get_engine(&mut config);
        let prepared_code = match prepare::prepare_contract(code.code(), &self.config) {
            Ok(code) => code,
            Err(err) => return (None, Some(VMError::from(err))),
        };
        let module = match Module::new(&engine, prepared_code) {
            Ok(module) => module,
            Err(err) => return (None, Some(err.into_vm_error())),
        };

        let mut logic = VMLogic::new_with_protocol_version(
            ext,
            context,
            &self.config,
            fees_config,
            promise_results,
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

        let ctx = VMCtx { logic, mem: None };
        let mut store = Store::new(&engine, ctx);
        let memory = Memory::new(
            &mut store,
            wasmtime::MemoryType::new(
                self.config.limit_config.initial_memory_pages,
                Some(self.config.limit_config.max_memory_pages),
            ),
        )
        .unwrap();
        store.data_mut().mem = Some(memory);
        let mut linker = Linker::new(&engine);

        imports::wasmtime::link(&mut linker, memory, current_protocol_version);
        if method_name.is_empty() {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName,
                ))),
            );
        }
        match module.get_export(method_name) {
            Some(export) => match export {
                Func(func_type) => {
                    if func_type.params().len() != 0 || func_type.results().len() != 0 {
                        return (
                            None,
                            Some(VMError::FunctionCallError(
                                FunctionCallError::MethodResolveError(
                                    MethodResolveError::MethodInvalidSignature,
                                ),
                            )),
                        );
                    }
                }
                _ => {
                    return (
                        None,
                        Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                            MethodResolveError::MethodNotFound,
                        ))),
                    )
                }
            },
            None => {
                return (
                    None,
                    Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodNotFound,
                    ))),
                )
            }
        }
        match linker.instantiate(&mut store, &module) {
            Ok(instance) => match instance.get_func(&mut store, method_name) {
                Some(func) => match func.typed::<(), (), _>(&store) {
                    Ok(run) => match run.call(&mut store, ()) {
                        Ok(_) => (Some(store.into_data().logic.outcome()), None),
                        Err(err) => {
                            (Some(store.into_data().logic.outcome()), Some(err.into_vm_error()))
                        }
                    },
                    Err(err) => {
                        (Some(store.into_data().logic.outcome()), Some(err.into_vm_error()))
                    }
                },
                None => (
                    None,
                    Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodNotFound,
                    ))),
                ),
            },
            Err(err) => (Some(store.into_data().logic.outcome()), Some(err.into_vm_error())),
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
