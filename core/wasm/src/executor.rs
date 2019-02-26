use crate::ext::External;

use std::ffi::c_void;
use std::fmt;

use crate::runtime::{self, Runtime};
use crate::types::{RuntimeContext, Config, ReturnData, Error};
use primitives::types::{Balance, Mana, Gas};
use primitives::logging;
use crate::cache;

use wasmer_runtime::{
    self,
    memory::Memory,
    wasm::MemoryDescriptor,
    units::Pages,
};

const PUBLIC_FUNCTION_PREFIX: &str = "near_func_";

pub struct ExecutionOutcome {
    pub gas_used: Gas,
    pub mana_used: Mana,
    pub mana_left: Mana,
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

pub fn execute<'a>(
    code: &'a [u8],
    method_name: &'a [u8],
    input_data: &'a [u8],
    result_data: &'a [Option<Vec<u8>>],
    ext: &'a mut External,
    config: &'a Config,
    context: &'a RuntimeContext,
) -> Result<ExecutionOutcome, Error> {
    if method_name.is_empty() {
        return Err(Error::EmptyMethodName);
    }

    let wasm_cache = cache::compile_cached_module(code, config)?;

    // into_module method is unsafe because the runtime cannot confirm
    // that this cache was not tampered with or corrupted.
    // In our case the cache is cloned from memory, so it's safe to use.
    let module = unsafe { wasm_cache.into_module() }
        .map_err(|e| Error::Cache(format!("Cache error: {:?}", e)))?;

    let memory = Memory::new(MemoryDescriptor {
        minimum: Pages(config.initial_memory_pages),
        maximum: Some(Pages(config.max_memory_pages)),
        shared: false
    }).map_err(Into::<wasmer_runtime::error::Error>::into)?;

    let mut runtime = Runtime::new(
        ext,
        input_data,
        result_data,
        context,
        config.gas_limit,
        memory.clone(),
    );

    let import_object = runtime::imports::build(memory);

    let mut instance = module.instantiate(&import_object)?;

    instance.context_mut().data = &mut runtime as *mut _ as *mut c_void;

    // All public functions should start with `PUBLIC_FUNCTION_PREFIX` in WASM.
    let method_name = format!("{}{}",
        PUBLIC_FUNCTION_PREFIX,
        std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?);

    match instance.call(&method_name, &[]) {
        Ok(_) => Ok(ExecutionOutcome {
            gas_used: runtime.gas_counter,
            mana_used: runtime.mana_counter,
            mana_left: context.mana - runtime.mana_counter,
            return_data: Ok(runtime.return_data),
            balance: runtime.balance,
            random_seed: runtime.random_seed,
            logs: runtime.logs,
        }),
        Err(e) => Ok(ExecutionOutcome {
            gas_used: runtime.gas_counter,
            mana_used: 0,
            mana_left: context.mana,
            return_data: Err(Into::<wasmer_runtime::error::Error>::into(e).into()),
            balance: context.initial_balance,
            random_seed: runtime.random_seed,
            logs: runtime.logs,
        })
    }
}
