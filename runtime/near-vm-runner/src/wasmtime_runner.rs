use wasmtime::{Engine, Module};

// mod only to apply feature to it. Is it possible to avoid it?
#[cfg(feature = "wasmtime_vm")]
pub mod wasmtime_runner {
    use crate::errors::IntoVMError;
    use crate::{imports, prepare};
    use near_runtime_fees::RuntimeFeesConfig;
    use near_vm_errors::FunctionCallError::{LinkError, WasmUnknownError};
    use near_vm_errors::{FunctionCallError, MethodResolveError, VMError, VMLogicError};
    use near_vm_logic::types::PromiseResult;
    use near_vm_logic::{External, MemoryLike, VMConfig, VMContext, VMLogic, VMOutcome};
    use std::ffi::c_void;
    use std::str;
    use wasmtime::ExternType::Func;
    use wasmtime::{Engine, Limits, Linker, Memory, MemoryType, Module, Store};

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
            VMError::FunctionCallError(WasmUnknownError)
        }
    }

    impl IntoVMError for anyhow::Error {
        fn into_vm_error(self) -> VMError {
            let cause = self.root_cause();
            match cause.downcast_ref::<wasmtime::Trap>() {
                Some(trap) => trap_to_error(trap),
                None => VMError::FunctionCallError(LinkError { msg: format!("{:#?}", cause) }),
            }
        }
    }

    impl IntoVMError for wasmtime::Trap {
        fn into_vm_error(self) -> VMError {
            if self.i32_exit_status() == Some(239) {
                trap_to_error(&self)
            } else {
                VMError::FunctionCallError(WasmUnknownError)
            }
        }
    }

    pub fn run_wasmtime<'a>(
        _code_hash: Vec<u8>,
        code: &[u8],
        method_name: &[u8],
        ext: &mut dyn External,
        context: VMContext,
        wasm_config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
    ) -> (Option<VMOutcome>, Option<VMError>) {
        let engine = Engine::default();
        let store = Store::new(&engine);
        let mut memory = WasmtimeMemory::new(
            &store,
            wasm_config.limit_config.initial_memory_pages,
            wasm_config.limit_config.max_memory_pages,
        )
        .unwrap();
        let prepared_code = match prepare::prepare_contract(code, wasm_config) {
            Ok(code) => code,
            Err(err) => return (None, Some(VMError::from(err))),
        };
        let module = Module::new(&engine, prepared_code).unwrap();
        // Note that we don't clone the actual backing memory, just increase the RC.
        let memory_copy = memory.clone();
        let mut linker = Linker::new(&store);
        let mut logic =
            VMLogic::new(ext, context, wasm_config, fees_config, promise_results, &mut memory);
        if logic.add_contract_compile_fee(code.len() as u64).is_err() {
            return (
                Some(logic.outcome()),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    near_vm_errors::HostError::GasExceeded,
                ))),
            );
        }
        // Unfortunately, due to the Wasmtime implementation we have to do tricks with the
        // lifetimes of the logic instance and pass raw pointers here.
        let raw_logic = &mut logic as *mut _ as *mut c_void;
        imports::link_wasmtime(&mut linker, memory_copy, raw_logic);
        let func_name = match str::from_utf8(method_name) {
            Ok(name) => name,
            Err(_) => {
                return (
                    None,
                    Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodUTF8Error,
                    ))),
                )
            }
        };
        if method_name.is_empty() {
            return (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName,
                ))),
            );
        }
        match module.get_export(func_name) {
            Some(export) => match export {
                Func(func_type) => {
                    if !func_type.params().is_empty() || !func_type.results().is_empty() {
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
            Ok(instance) => match instance.get_func(func_name) {
                Some(func) => match func.get0::<()>() {
                    Ok(run) => match run() {
                        Ok(_) => (Some(logic.outcome()), None),
                        Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
                    },
                    Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
                },
                None => (
                    None,
                    Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                        MethodResolveError::MethodUTF8Error,
                    ))),
                ),
            },
            Err(err) => (Some(logic.outcome()), Some(err.into_vm_error())),
        }
    }
}

pub fn compile_module(code: &[u8]) {
    let engine = Engine::default();
    Module::new(&engine, code).unwrap();
}
