use crate::ext::External;

use std::ffi::c_void;
use std::fmt;

use crate::cache;
use crate::runtime::{self, Runtime};
use crate::types::{Config, ContractCode, Error, ReturnData, RuntimeContext};
use primitives::logging;
use primitives::types::{Balance, Gas, Mana, StorageUsage, StorageUsageChange};

use wasmer_runtime::{self, memory::Memory, units::Pages, wasm::MemoryDescriptor};

pub struct ExecutionOutcome {
    pub gas_used: Gas,
    pub mana_used: Mana,
    pub mana_left: Mana,
    pub storage_usage: StorageUsage,
    pub return_data: Result<ReturnData, Error>,
    pub balance: Balance,
    pub random_seed: Vec<u8>,
    pub logs: Vec<String>,
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ExecutionOutcome")
            .field("gas_used", &format_args!("{}", &self.gas_used))
            .field("mana_used", &format_args!("{}", &self.mana_used))
            .field("mana_left", &format_args!("{}", &self.mana_left))
            .field("return_data", &self.return_data)
            .field("balance", &format_args!("{}", &self.balance))
            .field("random_seed", &format_args!("{}", logging::pretty_utf8(&self.random_seed)))
            .field("logs", &format_args!("{}", logging::pretty_vec(&self.logs)))
            .finish()
    }
}

pub fn execute(
    code: &ContractCode,
    method_name: &[u8],
    input_data: &[u8],
    result_data: &[Option<Vec<u8>>],
    ext: &mut External,
    config: &Config,
    context: &RuntimeContext,
) -> Result<ExecutionOutcome, Error> {
    if method_name.is_empty() {
        return Err(Error::EmptyMethodName);
    }

    let module = cache::compile_cached_module(code, config)?;

    let memory = Memory::new(MemoryDescriptor {
        minimum: Pages(config.initial_memory_pages),
        maximum: Some(Pages(config.max_memory_pages)),
        shared: false,
    })
    .map_err(Into::<wasmer_runtime::error::Error>::into)?;

    let mut runtime =
        Runtime::new(ext, input_data, result_data, context, config.gas_limit, memory.clone());

    let import_object = runtime::imports::build(memory);

    let mut instance = module.instantiate(&import_object)?;

    // WORKAROUND: Wasmer has a thread-local panic trap, and to run correctly,
    // we have to set the panic trap to this module.
    //
    // Currently it only does this in Module::new(), which is wrong because
    // latest module created is not necessarily the one currently executing.
    //
    // instance.module() is a way to trigger the code we need.
    instance.module();

    instance.context_mut().data = &mut runtime as *mut _ as *mut c_void;

    let method_name = std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?;

    match instance.call(&method_name, &[]) {
        Ok(_) => Ok(ExecutionOutcome {
            gas_used: runtime.gas_counter,
            mana_used: runtime.mana_counter,
            mana_left: context.mana - runtime.mana_counter,
            storage_usage: (context.storage_usage as StorageUsageChange + runtime.storage_counter)
                as StorageUsage,
            return_data: Ok(runtime.return_data),
            balance: runtime.balance,
            random_seed: runtime.random_seed,
            logs: runtime.logs,
        }),
        Err(e) => Ok(ExecutionOutcome {
            gas_used: runtime.gas_counter,
            mana_used: 0,
            mana_left: context.mana,
            storage_usage: context.storage_usage,
            return_data: Err(Into::<wasmer_runtime::error::Error>::into(e).into()),
            balance: context.initial_balance,
            random_seed: runtime.random_seed,
            logs: runtime.logs,
        }),
    }
}
