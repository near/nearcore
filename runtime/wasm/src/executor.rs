use crate::ext::External;

use std::ffi::c_void;
use std::fmt;

use crate::cache;
use crate::runtime::{self, Runtime};
use crate::types::{Config, ContractCode, Error, ReturnData, RuntimeContext};
use primitives::logging;
use primitives::types::{Balance, StorageUsage, StorageUsageChange};

use wasmer_runtime::{self, memory::Memory, units::Pages, wasm::MemoryDescriptor};

pub struct ExecutionOutcome {
    pub frozen_balance: Balance,
    pub liquid_balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: Result<ReturnData, Error>,
    pub random_seed: Vec<u8>,
    pub logs: Vec<String>,
}

impl fmt::Debug for ExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ExecutionOutcome")
            .field("return_data", &self.return_data)
            .field("frozen_balance", &format_args!("{}", &self.frozen_balance))
            .field("liquid_balance", &format_args!("{}", &self.liquid_balance))
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

    debug!(target:"runtime", "Executing method {:?}", String::from_utf8(method_name.to_vec()).unwrap_or_else(|_| hex::encode(method_name)));

    let memory = Memory::new(MemoryDescriptor {
        minimum: Pages(config.initial_memory_pages),
        maximum: Some(Pages(config.max_memory_pages)),
        shared: false,
    })
    .map_err(Into::<wasmer_runtime::error::Error>::into)?;

    let mut runtime =
        Runtime::new(ext, input_data, result_data, context, config.clone(), memory.clone());

    let raw_ptr = &mut runtime as *mut _ as *mut c_void;
    let import_object = runtime::imports::build(memory, raw_ptr);

    let method_name = std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?;

    match module
        .instantiate(&import_object)
        .and_then(|instance| instance.call(&method_name, &[]).map_err(|e| e.into()))
    {
        Ok(_) => {
            let e = ExecutionOutcome {
                storage_usage: (context.storage_usage as StorageUsageChange
                    + runtime.storage_counter) as StorageUsage,
                return_data: Ok(runtime.return_data),
                frozen_balance: runtime.frozen_balance,
                liquid_balance: runtime.liquid_balance,
                random_seed: runtime.random_seed,
                logs: runtime.logs,
            };
            debug!(target:"runtime", "{:?}", e);
            Ok(e)
        }
        Err(e) => {
            let e = ExecutionOutcome {
                storage_usage: context.storage_usage,
                return_data: Err(Into::<wasmer_runtime::error::Error>::into(e).into()),
                frozen_balance: runtime.frozen_balance,
                liquid_balance: runtime.liquid_balance,
                random_seed: runtime.random_seed,
                logs: runtime.logs,
            };
            debug!(target:"runtime", "{:?}", e);
            Ok(e)
        }
    }
}
