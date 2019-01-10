use ext::External;
use wasmi;

use prepare;
use resolver::EnvModuleResolver;

use runtime::Runtime;
use types::{RuntimeContext, Config, ReturnData, Error};
use primitives::types::{Balance, Mana, Gas};

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

pub fn execute(
    code: &[u8],
    method_name: &[u8],
    input_data: &[u8],
    result_data: &[Option<Vec<u8>>],
    ext: &mut External,
    config: &Config,
    context: &RuntimeContext,
) -> Result<ExecutionOutcome, Error> {
    let prepare::PreparedContract {
        instrumented_code,
        memory
    } = prepare::prepare_contract(code, &config).map_err(Error::Prepare)?;

    // Parse module from code
    let module = wasmi::Module::from_buffer(&instrumented_code).map_err(Error::Interpreter)?;
    // Setup functions
    let instantiation_resolver = EnvModuleResolver::with_memory(memory.clone());
    // Make a module instance
    let module_instance = wasmi::ModuleInstance::new(
        &module,
        &wasmi::ImportsBuilder::new().with_resolver("env", &instantiation_resolver),
    ).map_err(Error::Interpreter)?;

    let mut runtime = Runtime::new(
        ext,
        input_data,
        result_data,
        memory,
        context,
        config.gas_limit,
    );

    // All public functions should start with `PUBLIC_FUNCTION_PREFIX` in WASM.
    let method_name = format!("{}{}", PUBLIC_FUNCTION_PREFIX, std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?);

    match module_instance.run_start(&mut runtime) {
        Err(e) => Ok(ExecutionOutcome {
            gas_used: runtime.gas_counter,
            mana_used: 0,
            mana_left: context.mana,
            return_data: Err(e.into()),
            balance: context.initial_balance,
            random_seed: runtime.random_seed,
            logs: runtime.logs,
        }),
        Ok(module_instance) => match module_instance.invoke_export(&method_name, &[], &mut runtime) {
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
                return_data: Err(e.into()),
                balance: context.initial_balance,
                random_seed: runtime.random_seed,
                logs: runtime.logs,
            })
        }
    }
}
