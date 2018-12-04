use ext::External;
use wasmi;

use prepare;
use resolver::EnvModuleResolver;

use runtime::{Runtime, RuntimeContext};
use types::*;

pub fn execute(
    code: &[u8],
    method_name: &[u8],
    _input_data: &[u8],
    output_data: &mut Vec<u8>,
    ext: &mut External,
    config: &Config,
) -> Result<(), Error> {
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

    let mut runtime = Runtime::new(ext, RuntimeContext {}, memory, config.gas_limit);

    let module_instance = module_instance
        .run_start(&mut runtime)
        .map_err(Error::Trap)?;

    let method_name = ::std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?;

    match method_name.chars().next() {
        Some('_') => return Err(Error::PrivateMethod),
        None => return Err(Error::EmptyMethodName),
        _ => (),
    };

    let result = module_instance.invoke_export(method_name, &[], &mut runtime)?;

    runtime.parse_result(result, output_data)?;

    Ok(())
}
