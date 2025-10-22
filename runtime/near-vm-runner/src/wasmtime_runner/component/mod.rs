use crate::logic::errors::{FunctionCallError, MethodResolveError, VMRunnerError};
use crate::logic::logic::Promise;
use crate::logic::recorded_storage_counter::RecordedStorageCounter;
use crate::logic::vmstate::Registers;
use crate::logic::{Config, ExecutionResultState, External, VMContext, VMOutcome};
use crate::runner::VMResult;
use crate::wasmtime_runner::{
    DEFAULT_MAX_ELEMENTS_PER_TABLE, DEFAULT_MAX_TABLES_PER_MODULE, InstancePermit,
    IntoVMError as _, RunOutcome, guest_memory_size,
};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::LimitConfig;
use near_primitives_core::gas::Gas;
use near_primitives_core::types::Balance;
use std::sync::Arc;
use wasmtime::component::{
    Component, ComponentExportIndex, HasSelf, Instance, InstancePre, Linker, ResourceTable,
};
use wasmtime::{CallHook, Engine, ResourcesRequired, Store, StoreLimits, StoreLimitsBuilder};

mod bindings;
mod host;

#[repr(C)]
pub struct Ctx {
    // NOTE: this must be the first field in the struct, since instrumentation assumes that to be
    // the case
    remaining_gas: u64,

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

    /// Tracks size of the recorded trie storage proof.
    recorded_storage_counter: RecordedStorageCounter,

    result_state: ExecutionResultState,

    table: ResourceTable,
}

impl Ctx {
    fn new(
        ext: &'static mut dyn External,
        context: &'static VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        result_state: ExecutionResultState,
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
            .instances(3)
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
        let remaining_gas = result_state.gas_counter.remaining_gas().as_gas();
        Self {
            remaining_gas,
            limits,
            ext,
            context,
            config,
            fees_config,
            current_account_locked_balance,
            recorded_storage_counter,
            registers: Registers::default(),
            promises: Vec::default(),
            result_state,
            table: ResourceTable::default(),
        }
    }
}

#[derive(Clone)]
pub struct Prepared {
    pub pre: InstancePre<Ctx>,
    pub num_tables: u32,
}

impl Prepared {
    pub unsafe fn load(
        engine: &Engine,
        _config: &Config,
        code: &[u8],
    ) -> VMResult<Result<Self, FunctionCallError>> {
        let component = unsafe { Component::deserialize(engine, &code) }
            .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
        let mut linker = Linker::new(engine);
        bindings::Imports::add_to_linker::<_, HasSelf<Ctx>>(&mut linker, |cx| cx)
            .map_err(|err| VMRunnerError::LoadingError(err.to_string()))?;
        match linker.instantiate_pre(&component) {
            Err(err) => {
                let err = err.into_vm_error()?;
                Ok(Err(err))
            }
            Ok(pre) => {
                let Some(ResourcesRequired { num_tables, .. }) = component.resources_required()
                else {
                    let msg = "component imports and instantiates another component or core module"
                        .into();
                    return Ok(Err(FunctionCallError::LinkError { msg }));
                };
                Ok(Ok(Self { pre, num_tables }))
            }
        }
    }
}

pub struct Executable {
    pub pre: InstancePre<Ctx>,
    pub method: ComponentExportIndex,
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
        let Self { pre, method } = self;
        let ctx = Ctx::new(ext, context, fees_config, result_state);
        let mut store = Store::<Ctx>::new(pre.component().engine(), ctx);
        store.limiter(|ctx| &mut ctx.limits);
        store.call_hook(move |mut store, hook| {
            match hook {
                CallHook::CallingHost | CallHook::ReturningFromWasm => {
                    let ctx = store.data_mut();
                    let burned = ctx
                        .result_state
                        .gas_counter
                        .remaining_gas()
                        .saturating_sub(Gas::from_gas(ctx.remaining_gas));
                    if burned.as_gas() > 0 {
                        ctx.result_state.gas_counter.burn_gas(burned)?;
                    }
                }
                CallHook::ReturningFromHost | CallHook::CallingWasm => {
                    let ctx = store.data_mut();
                    ctx.remaining_gas = ctx.result_state.gas_counter.remaining_gas().as_gas();
                }
            }
            Ok(())
        });
        let instance = match pre.instantiate(&mut store) {
            Ok(instance) => instance,
            Err(err) => {
                let err = err.into_vm_error()?;
                let Ctx { result_state, .. } = store.into_data();
                return Ok(VMOutcome::abort(result_state, err));
            }
        };

        let res = call(&mut store, instance, method);
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
    method: ComponentExportIndex,
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
