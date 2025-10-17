use crate::logic::errors::{FunctionCallError, MethodResolveError, VMRunnerError};
use crate::logic::logic::Promise;
use crate::logic::recorded_storage_counter::RecordedStorageCounter;
use crate::logic::vmstate::Registers;
use crate::logic::{Config, ExecutionResultState, External, VMContext, VMOutcome};
use crate::runner::VMResult;
use crate::wasmtime_runner::{
    DEFAULT_MAX_ELEMENTS_PER_TABLE, DEFAULT_MAX_TABLES_PER_MODULE, ErrorContainer, InstancePermit,
    IntoVMError as _, RunOutcome, guest_memory_size,
};
use crate::{MEMORY_EXPORT, REMAINING_GAS_EXPORT, START_EXPORT, imports};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::LimitConfig;
use near_primitives_core::gas::Gas;
use near_primitives_core::types::Balance;
use std::sync::Arc;
use wasmtime::{
    CallHook, Caller, Engine, Extern, Instance, InstancePre, Linker, Memory, Module, ModuleExport,
    ResourcesRequired, Store, StoreLimits, StoreLimitsBuilder, Val,
};

mod logic;

enum Export<T> {
    Unresolved(ModuleExport),
    Resolved(T),
}

pub struct Ctx {
    memory: Export<Memory>,
    limits: StoreLimits,
    /// Provides access to the components outside the Wasm runtime for operations on the trie and
    /// receipts creation.
    ext: &'static mut dyn External,
    /// Part of Context API and Economics API that was extracted from the receipt.
    context: &'static VMContext,

    /// All gas and economic parameters required during contract execution.
    config: Arc<Config>,
    /// Fees charged for various operations that contract may execute.
    fees_config: Arc<RuntimeFeesConfig>,

    /// Current amount of locked tokens, does not automatically change when staking transaction is
    /// issued.
    current_account_locked_balance: Balance,
    /// Registers can be used by the guest to store blobs of data without moving them across
    /// host-guest boundary.
    registers: Registers,
    /// The DAG of promises, indexed by promise id.
    promises: Vec<Promise>,

    /// Stores the amount of stack space remaining
    remaining_stack: u64,

    /// Tracks size of the recorded trie storage proof.
    recorded_storage_counter: RecordedStorageCounter,

    result_state: ExecutionResultState,
}

impl Ctx {
    fn new(
        ext: &'static mut dyn External,
        context: &'static VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        result_state: ExecutionResultState,
        memory: ModuleExport,
    ) -> Self {
        let LimitConfig {
            max_memory_pages,
            max_tables_per_contract,
            max_elements_per_contract_table,
            ..
        } = result_state.config.limit_config;
        let max_tables_per_contract =
            max_tables_per_contract.unwrap_or(DEFAULT_MAX_TABLES_PER_MODULE);
        let max_elements_per_contract_table =
            max_elements_per_contract_table.unwrap_or(DEFAULT_MAX_ELEMENTS_PER_TABLE);
        let max_memory_size = guest_memory_size(max_memory_pages).unwrap_or(usize::MAX);

        let limits = StoreLimitsBuilder::new()
            .instances(1)
            .memories(1)
            .memory_size(max_memory_size)
            .tables(max_tables_per_contract.try_into().unwrap_or(usize::MAX))
            .table_elements(max_elements_per_contract_table)
            .build();

        let current_account_locked_balance = context.account_locked_balance;
        let config = Arc::clone(&result_state.config);
        let recorded_storage_counter = RecordedStorageCounter::new(
            ext.get_recorded_storage_size(),
            result_state.config.limit_config.per_receipt_storage_proof_size_limit,
        );
        let remaining_stack = u64::from(result_state.config.limit_config.max_stack_height);
        Self {
            memory: Export::Unresolved(memory),
            limits,
            ext,
            context,
            config,
            fees_config,
            current_account_locked_balance,
            recorded_storage_counter,
            registers: Default::default(),
            promises: vec![],
            remaining_stack,
            result_state,
        }
    }
}

#[derive(Clone)]
pub struct Prepared {
    pub pre: InstancePre<Ctx>,
    pub memory: ModuleExport,
    pub remaining_gas: Option<ModuleExport>,
    pub start: Option<ModuleExport>,
    pub num_tables: u32,
}

fn link(linker: &mut Linker<Ctx>, config: &Config) {
    macro_rules! add_import {
        (
          $mod:ident / $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >
        ) => {
            #[allow(unused_parens)]
            fn $name(mut caller: Caller<'_, Ctx>, $( $arg_name: $arg_type ),* ) -> anyhow::Result<($( $returns ),*)> {
                const TRACE: bool = imports::should_trace_host_function(stringify!($name));
                let _span = TRACE.then(|| {
                    tracing::trace_span!(target: "vm::host_function", stringify!($name)).entered()
                });
                match logic::$func(&mut caller, $( $arg_name as $arg_type, )*) {
                    Ok(result) => Ok(result as ($( $returns ),* ) ),
                    Err(err) => {
                        Err(ErrorContainer::new(err).into())
                    }
                }
            }

            linker.func_wrap(stringify!($mod), stringify!($name), $name).expect("cannot link external");
        };
    }
    imports::for_each_available_import!(config, add_import);
}

impl Prepared {
    pub unsafe fn load(
        engine: &Engine,
        config: &Config,
        code: &[u8],
    ) -> VMResult<Result<Self, FunctionCallError>> {
        let module = unsafe { Module::deserialize(engine, &code) }
            .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
        let Some(memory) = module.get_export_index(MEMORY_EXPORT) else {
            return Ok(Err(FunctionCallError::LinkError { msg: "memory export missing".into() }));
        };
        let remaining_gas = module.get_export_index(REMAINING_GAS_EXPORT);
        let start = module.get_export_index(START_EXPORT);
        let mut linker = Linker::new(engine);
        link(&mut linker, config);
        match linker.instantiate_pre(&module) {
            Err(err) => {
                let err = err.into_vm_error()?;
                Ok(Err(err))
            }
            Ok(pre) => {
                let ResourcesRequired { num_tables, .. } = module.resources_required();
                Ok(Ok(Self { pre, memory, remaining_gas, start, num_tables }))
            }
        }
    }
}

pub struct Executable {
    pub pre: InstancePre<Ctx>,
    pub memory: ModuleExport,
    pub remaining_gas: Option<ModuleExport>,
    pub start: Option<ModuleExport>,
    pub method: Box<str>,
}

impl Executable {
    pub fn run(
        self,
        result_state: ExecutionResultState,
        ext: &'static mut dyn External,
        context: &'static VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        _permit: InstancePermit<'_>,
    ) -> VMResult {
        let Self { pre, memory, remaining_gas, start, method } = self;
        let ctx = Ctx::new(ext, context, fees_config, result_state, memory);
        let mut store = Store::<Ctx>::new(pre.module().engine(), ctx);
        store.limiter(|ctx| &mut ctx.limits);
        let instance = match pre.instantiate(&mut store) {
            Ok(instance) => instance,
            Err(err) => {
                let err = err.into_vm_error()?;
                let Ctx { result_state, .. } = store.into_data();
                return Ok(VMOutcome::abort(result_state, err));
            }
        };
        if let Some(global) = remaining_gas {
            let Some(Extern::Global(global)) = instance.get_module_export(&mut store, &global)
            else {
                panic!("gas global export was present on the module, but not on the instance");
            };
            store.call_hook(move |mut store, hook| {
                match hook {
                    CallHook::CallingHost | CallHook::ReturningFromWasm => {
                        let Val::I64(remaining_gas) = global.get(&mut store) else {
                            panic!("gas global export is not i64");
                        };
                        let ctx = store.data_mut();
                        let burned = ctx
                            .result_state
                            .gas_counter
                            .remaining_gas()
                            .saturating_sub(Gas::from_gas(remaining_gas as _));
                        if burned.as_gas() > 0 {
                            ctx.result_state.gas_counter.burn_gas(burned)?;
                        }
                    }
                    CallHook::ReturningFromHost | CallHook::CallingWasm => {
                        let remaining_gas = store.data().result_state.gas_counter.remaining_gas();
                        global
                            .set(&mut store, Val::I64(remaining_gas.as_gas() as _))
                            .expect("failed to set gas global export")
                    }
                }
                Ok(())
            });
        }
        if let Some(start) = start {
            let Some(Extern::Func(start)) = instance.get_module_export(&mut store, &start) else {
                panic!("start function export was present on the module, but not on the instance");
            };
            if let Err(err) = start.call(&mut store, &[], &mut []) {
                let err = err.into_vm_error()?;
                let Ctx { result_state, .. } = store.into_data();
                return Ok(VMOutcome::abort(result_state, err));
            }
        }

        let res = call(&mut store, instance, &method);
        let Ctx { result_state, .. } = store.into_data();
        match res? {
            RunOutcome::Ok => Ok(VMOutcome::ok(result_state)),
            RunOutcome::AbortNop(error) => {
                Ok(VMOutcome::abort_but_nop_outcome_in_old_protocol(result_state, error))
            }
            RunOutcome::Abort(error) => Ok(VMOutcome::abort(result_state, error)),
        }
    }
}

fn call(
    mut store: &mut Store<Ctx>,
    instance: Instance,
    method: &str,
) -> Result<RunOutcome, VMRunnerError> {
    let Some(func) = instance.get_func(&mut store, method) else {
        return Ok(RunOutcome::AbortNop(FunctionCallError::MethodResolveError(
            MethodResolveError::MethodNotFound,
        )));
    };
    match func.typed(&mut store) {
        Ok(run) => match run.call(store, ()) {
            Ok(()) => Ok(RunOutcome::Ok),
            Err(err) => err.into_vm_error().map(RunOutcome::Abort),
        },
        Err(err) => err.into_vm_error().map(RunOutcome::Abort),
    }
}
