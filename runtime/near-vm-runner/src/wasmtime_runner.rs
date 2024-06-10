use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{
    CacheError, CompilationError, FunctionCallError, MethodResolveError, PrepareError,
    VMLogicError, VMRunnerError, WasmTrap,
};
use crate::logic::types::PromiseResult;
use crate::logic::Config;
use crate::logic::{External, MemSlice, MemoryLike, VMContext, VMLogic, VMOutcome};
use crate::{imports, prepare, ContractCode, ContractRuntimeCache};
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::hash::CryptoHash;
use std::borrow::Cow;
use std::cell::RefCell;
use wasmtime::ExternType::Func;
use wasmtime::{Engine, Linker, Memory, MemoryType, Module, Store};

type Caller = wasmtime::Caller<'static, ()>;
thread_local! {
    pub(crate) static CALLER: RefCell<Option<Caller>> = const { RefCell::new(None) };
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

trait IntoVMError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError>;
}

impl IntoVMError for anyhow::Error {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError> {
        let cause = self.root_cause();
        if let Some(container) = cause.downcast_ref::<imports::wasmtime::ErrorContainer>() {
            use {VMLogicError as LE, VMRunnerError as RE};
            return match container.take() {
                Some(LE::HostError(h)) => Ok(FunctionCallError::HostError(h)),
                Some(LE::ExternalError(s)) => Err(RE::ExternalError(s)),
                Some(LE::InconsistentStateError(e)) => Err(RE::InconsistentStateError(e)),
                None => panic!("error has already been taken out of the container?!"),
            };
        }
        if let Some(trap) = cause.downcast_ref::<wasmtime::Trap>() {
            use wasmtime::Trap as T;
            let nondeterministic_message = 'nondet: {
                return Ok(FunctionCallError::WasmTrap(match *trap {
                    T::StackOverflow => WasmTrap::StackOverflow,
                    T::MemoryOutOfBounds => WasmTrap::MemoryOutOfBounds,
                    T::TableOutOfBounds => WasmTrap::MemoryOutOfBounds,
                    T::IndirectCallToNull => WasmTrap::IndirectCallToNull,
                    T::BadSignature => WasmTrap::IncorrectCallIndirectSignature,
                    T::IntegerOverflow => WasmTrap::IllegalArithmetic,
                    T::IntegerDivisionByZero => WasmTrap::IllegalArithmetic,
                    T::BadConversionToInteger => WasmTrap::IllegalArithmetic,
                    T::UnreachableCodeReached => WasmTrap::Unreachable,
                    T::Interrupt => break 'nondet "interrupt",
                    T::HeapMisaligned => break 'nondet "heap misaligned",
                    t => {
                        return Err(VMRunnerError::WasmUnknownError {
                            debug_message: format!("unhandled trap type: {:?}", t),
                        })
                    }
                }));
            };
            return Err(VMRunnerError::Nondeterministic(nondeterministic_message.into()));
        }
        Ok(FunctionCallError::LinkError { msg: format!("{:#?}", cause) })
    }
}

#[allow(clippy::needless_pass_by_ref_mut)]
pub fn get_engine(config: &mut wasmtime::Config) -> Engine {
    Engine::new(config).unwrap()
}

pub(crate) fn wasmtime_vm_hash() -> u64 {
    // TODO: take into account compiler and engine used to compile the contract.
    64
}

pub(crate) struct WasmtimeVM {
    config: Config,
}

impl WasmtimeVM {
    pub(crate) fn new(config: Config) -> Self {
        Self { config }
    }

    pub(crate) fn default_wasmtime_config(&self) -> wasmtime::Config {
        let features =
            crate::features::WasmFeatures::from(self.config.limit_config.contract_prepare_version);
        let mut config = wasmtime::Config::from(features);
        config.max_wasm_stack(1024 * 1024 * 1024); // wasm stack metering is implemented by instrumentation, we don't want wasmtime to trap before that
        config
    }
}

impl crate::runner::VM for WasmtimeVM {
    fn run(
        &self,
        _code_hash: CryptoHash,
        code: Option<&ContractCode>,
        method_name: &str,
        ext: &mut dyn External,
        context: &VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        _cache: Option<&dyn ContractRuntimeCache>,
    ) -> Result<VMOutcome, VMRunnerError> {
        let Some(code) = code else {
            return Err(VMRunnerError::CacheError(CacheError::ReadError(std::io::Error::from(
                std::io::ErrorKind::NotFound,
            ))));
        };
        let mut config = self.default_wasmtime_config();
        let engine = get_engine(&mut config);
        let mut store = Store::new(&engine, ());
        let mut memory = WasmtimeMemory::new(
            &mut store,
            self.config.limit_config.initial_memory_pages,
            self.config.limit_config.max_memory_pages,
        )
        .unwrap();
        let memory_copy = memory.0;
        let mut logic =
            VMLogic::new(ext, context, &self.config, fees_config, promise_results, &mut memory);

        let result = logic.before_loading_executable(method_name, code.code().len() as u64);
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        let prepared_code =
            match prepare::prepare_contract(code.code(), &self.config, VMKind::Wasmtime) {
                Ok(code) => code,
                Err(err) => return Ok(VMOutcome::abort(logic, FunctionCallError::from(err))),
            };
        let start = std::time::Instant::now();
        let module = match Module::new(&engine, prepared_code) {
            Ok(module) => module,
            Err(err) => return Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
        };
        crate::metrics::compilation_duration(VMKind::Wasmtime, start.elapsed());
        let mut linker = Linker::new(&engine);

        let result = logic.after_loading_executable(code.code().len() as u64);
        if let Err(e) = result {
            return Ok(VMOutcome::abort(logic, e));
        }

        imports::wasmtime::link(&mut linker, memory_copy, &store, &mut logic);
        match module.get_export(method_name) {
            Some(export) => match export {
                Func(func_type) => {
                    if func_type.params().len() != 0 || func_type.results().len() != 0 {
                        let err = FunctionCallError::MethodResolveError(
                            MethodResolveError::MethodInvalidSignature,
                        );
                        return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(logic, err));
                    }
                }
                _ => {
                    return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                        logic,
                        FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
                    ));
                }
            },
            None => {
                return Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(
                    logic,
                    FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
                ));
            }
        }
        match linker.instantiate(&mut store, &module) {
            Ok(instance) => match instance.get_func(&mut store, method_name) {
                Some(func) => match func.typed::<(), ()>(&mut store) {
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
                    ));
                }
            },
            Err(err) => Ok(VMOutcome::abort(logic, err.into_vm_error()?)),
        }
    }

    fn precompile(
        &self,
        _code: &ContractCode,
        _cache: &dyn ContractRuntimeCache,
    ) -> Result<
        Result<ContractPrecompilatonResult, CompilationError>,
        crate::logic::errors::CacheError,
    > {
        Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable))
    }
}
