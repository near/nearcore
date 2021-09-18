use wasmtime::Module;

// mod only to apply feature to it. Is it possible to avoid it?
#[cfg(feature = "wasmtime_vm")]
pub mod wasmtime_runner {
    use crate::errors::IntoVMError;
    use crate::wasmer_runner::GasMode;
    use crate::{imports, prepare};
    use near_primitives::contract::ContractCode;
    use near_primitives::runtime::fees::RuntimeFeesConfig;
    use near_primitives::{
        config::VMConfig, types::CompiledContractCache, version::ProtocolVersion,
    };
    use near_vm_errors::{FunctionCallError, MethodResolveError, VMError, VMLogicError, WasmTrap};
    use near_vm_logic::{
        types::PromiseResult, External, GasCounterMode, InstanceLike, MemoryLike, VMContext,
        VMLogic, VMOutcome,
    };
    use std::ffi::c_void;
    use std::str;
    use wasmtime::ExternType::Func;
    use wasmtime::{Config, Engine, Limits, Linker, Memory, MemoryType, Module, Store, TrapCode};

    pub struct WasmtimeMemory(Memory);

    impl WasmtimeMemory {
        pub fn new(
            store: &Store,
            initial_memory_bytes: u32,
            max_memory_bytes: u32,
        ) -> Result<Self, VMError> {
            Ok(WasmtimeMemory(Memory::new(
                store,
                MemoryType::new(Limits::new(initial_memory_bytes, Some(max_memory_bytes))),
            )))
        }

        pub fn clone(&self) -> Memory {
            self.0.clone()
        }
    }

    impl MemoryLike for WasmtimeMemory {
        fn fits_memory(&self, offset: u64, len: u64) -> bool {
            match offset.checked_add(len) {
                None => false,
                Some(end) => self.0.data_size() as u64 >= end,
            }
        }

        fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
            let offset = offset as usize;
            // data_unchecked() is unsafe.
            unsafe {
                for i in 0..buffer.len() {
                    buffer[i] = self.0.data_unchecked()[i + offset];
                }
            }
        }

        fn read_memory_u8(&self, offset: u64) -> u8 {
            // data_unchecked() is unsafe.
            unsafe { self.0.data_unchecked()[offset as usize] }
        }

        fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
            // data_unchecked_mut() is unsafe.
            unsafe {
                let offset = offset as usize;
                for i in 0..buffer.len() {
                    self.0.data_unchecked_mut()[i + offset] = buffer[i];
                }
            }
        }
    }

    fn trap_to_error(trap: &wasmtime::Trap) -> VMError {
        if trap.i32_exit_status() == Some(239) {
            match imports::last_wasmtime_error() {
                Some(VMLogicError::HostError(h)) => {
                    VMError::FunctionCallError(FunctionCallError::HostError(h.clone()))
                }
                Some(VMLogicError::ExternalError(s)) => VMError::ExternalError(s.clone()),
                Some(VMLogicError::InconsistentStateError(e)) => {
                    VMError::InconsistentStateError(e.clone())
                }
                None => panic!("Error is not properly set"),
            }
        } else {
            match trap.trap_code() {
                Some(TrapCode::StackOverflow) => {
                    VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::StackOverflow))
                }
                Some(TrapCode::MemoryOutOfBounds) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds),
                ),
                Some(TrapCode::TableOutOfBounds) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::MemoryOutOfBounds),
                ),
                Some(TrapCode::IndirectCallToNull) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::IndirectCallToNull),
                ),
                Some(TrapCode::BadSignature) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::IncorrectCallIndirectSignature),
                ),
                Some(TrapCode::IntegerOverflow) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
                ),
                Some(TrapCode::IntegerDivisionByZero) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
                ),
                Some(TrapCode::BadConversionToInteger) => VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::IllegalArithmetic),
                ),
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

    pub struct WasmtimeInstance(pub wasmtime::Instance);

    impl InstanceLike for WasmtimeInstance {
        fn get_remaining_ops(&self) -> u64 {
            let remaining_ops: wasmtime::Global =
                self.0.get_global("remaining_ops").expect("Global based gas meter is not injected");
            let ops = remaining_ops.get().unwrap_i64();
            ops as u64
        }

        fn set_remaining_ops(&self, ops: u64) {
            let remaining_ops: wasmtime::Global =
                self.0.get_global("remaining_ops").expect("Global based gas meter is not injected");
            remaining_ops.set(wasmtime::Val::I64(ops as i64)).unwrap();
        }
    }

    pub fn run_wasmtime(
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        wasm_config: &VMConfig,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        current_protocol_version: ProtocolVersion,
        _cache: Option<&dyn CompiledContractCache>,
        gas_counter_mode: GasCounterMode,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        let mut config = Config::default();
        let engine = get_engine(&mut config);
        let store = Store::new(&engine);
        let mut memory = WasmtimeMemory::new(
            &store,
            wasm_config.limit_config.initial_memory_pages,
            wasm_config.limit_config.max_memory_pages,
        )
        .unwrap();
        let prepared_code =
            match prepare::prepare_contract(code.code(), wasm_config, gas_counter_mode) {
                Ok(code) => code,
                Err(err) => return (None, Some(VMError::from(err))),
            };
        let module = match Module::new(&engine, prepared_code) {
            Ok(module) => module,
            Err(err) => return (None, Some(err.into_vm_error())),
        };
        // Note that we don't clone the actual backing memory, just increase the RC.
        let memory_copy = memory.clone();
        let mut linker = Linker::new(&store);
        let mut logic = VMLogic::new_with_protocol_version(
            ext,
            context,
            wasm_config,
            fees_config,
            promise_results,
            &mut memory,
            current_protocol_version,
            gas_counter_mode,
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

        let gas_mode = if wasm_config.regular_op_cost > 0 {
            GasMode::Paid(logic.gas_to_use() / wasm_config.regular_op_cost as u64)
        } else {
            GasMode::Free
        };

        // Unfortunately, due to the Wasmtime implementation we have to do tricks with the
        // lifetimes of the logic instance and pass raw pointers here.
        let raw_logic = &mut logic as *mut _ as *mut c_void;
        imports::link_wasmtime(&mut linker, memory_copy, raw_logic, current_protocol_version);
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
        match linker.instantiate(&module) {
            Ok(instance) => {
                let instance = WasmtimeInstance(instance);
                logic.set_instance(Some(&instance as *const dyn InstanceLike));
                let result = run_method_inner(&instance.0, method_name, gas_mode, &mut logic);
                logic.set_instance(None);

                match result {
                    Ok(()) => (Some(logic.outcome()), None),
                    Err(err) => (Some(logic.outcome()), Some(err)),
                }
            }
            Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
        }
    }

    fn run_method_inner(
        instance: &wasmtime::Instance,
        method_name: &str,
        gas_mode: GasMode,
        logic: &mut VMLogic,
    ) -> Result<(), VMError> {
        if logic.gas_counter_mode() == GasCounterMode::Wasm {
            if let GasMode::Paid(_) = gas_mode {
                logic
                    .sync_from_wasm_counter()
                    .map_err(|e: VMLogicError| -> VMError { (&e).into() })?;
            }
        }

        let result = match instance.get_func(method_name) {
            Some(func) => match func.typed::<(), ()>() {
                Ok(run) => match run.call(()) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.into_vm_error()),
                },
                Err(err) => Err(err.into_vm_error()),
            },
            None => Err(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodNotFound,
            ))),
        };

        if logic.gas_counter_mode() == GasCounterMode::Wasm {
            if let GasMode::Paid(_) = gas_mode {
                logic
                    .sync_from_wasm_counter()
                    .map_err(|e: VMLogicError| -> VMError { (&e).into() })?;
            }
        }

        result
    }

    #[cfg(not(feature = "lightbeam"))]
    pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
        Engine::new(config).unwrap()
    }

    #[cfg(feature = "lightbeam")]
    pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
        Engine::new(config.strategy(wasmtime::Strategy::Lightbeam).unwrap()).unwrap()
    }
}

pub fn compile_module(code: &[u8]) -> bool {
    let mut config = wasmtime::Config::default();
    let engine = wasmtime_runner::get_engine(&mut config);
    Module::new(&engine, code).is_ok()
}

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}
