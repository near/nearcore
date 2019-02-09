use crate::ext::External;

use crate::prepare;
use std::ffi::c_void;

use crate::runtime::{self, Runtime};
use crate::types::{RuntimeContext, Config, ReturnData, Error};
use primitives::types::{Balance, Mana, Gas};

use wasmer_runtime::{
    self,
    Memory,
    wasm::MemoryDescriptor,
    units::Pages,
};

const PUBLIC_FUNCTION_PREFIX: &str = "near_func_";

#[derive(Debug)]
pub struct ExecutionOutcome {
    pub gas_used: Gas,
    pub mana_used: Mana,
    pub mana_left: Mana,
    pub return_data: Result<ReturnData, Error>,
    pub balance: Balance,
    pub random_seed: Vec<u8>,
    pub logs: Vec<String>,
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

    let instrumented_code = prepare::prepare_contract(code, &config).map_err(Error::Prepare)?;

    let memory = Memory::new(MemoryDescriptor {
        minimum: Pages(17),
        maximum: Some(Pages(32)),
        shared: false,
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

    let mut instance = wasmer_runtime::instantiate(&instrumented_code, &import_object)?;

    instance.context_mut().data = &mut runtime as *mut _ as *mut c_void;

    // All public functions should start with `PUBLIC_FUNCTION_PREFIX` in WASM.
    let method_name = format!("{}{}",
        PUBLIC_FUNCTION_PREFIX,
        std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?);

    // Resolving function by method_name
    // let mut func = instance.func(&method_name)
    //    .map_err(Into::<wasmer_runtime::error::Error>::into)?;

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
