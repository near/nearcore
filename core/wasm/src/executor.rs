use ext::External;
use wasmi;

use prepare;
use resolver::EnvModuleResolver;

use runtime::Runtime;
use types::{Config, ReturnData, Error};

#[derive(Debug, Clone)]
pub struct ExecutionOutcome {
    pub gas_used: u64,
    pub mana_used: u32,
    pub return_data: ReturnData,
}

pub fn execute(
    code: &[u8],
    method_name: &[u8],
    input_data: &[u8],
    result_data: &[Option<Vec<u8>>],
    ext: &mut External,
    config: &Config,
    mana_limit: u32,
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
        mana_limit,
        config.gas_limit);

    let module_instance = module_instance
        .run_start(&mut runtime)
        .map_err(Error::Trap)?;

    let method_name = ::std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?;

    match method_name.chars().next() {
        Some('_') => return Err(Error::PrivateMethod),
        None => return Err(Error::EmptyMethodName),
        _ => (),
    };

    module_instance.invoke_export(method_name, &[], &mut runtime)?;

    // TODO: Add MANA usage counter
    Ok(ExecutionOutcome {
        gas_used: runtime.gas_counter,
        mana_used: runtime.mana_counter,
        return_data: runtime.return_data,
    })
}
