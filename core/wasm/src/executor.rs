use ext::Externalities;
use wasmi;

use resolver::EnvModuleResolver;
use prepare;

use runtime::{Runtime, RuntimeContext};
use types::*;

pub fn execute(
	code: &[u8],
    method_name: &[u8],
	_input_data: &[u8],
	_output_data: &mut Vec<u8>,
    ext: &mut Externalities,
	config: &Config,
) -> Result<(), Error> {
	let prepare::PreparedContract {
		instrumented_code,
		memory,
	} = prepare::prepare_contract(code, &config).map_err(Error::Prepare)?;

    // Parse module from code
    let module = wasmi::Module::from_buffer(&instrumented_code).map_err(Error::Interpreter)?;
    // Setup functions
    let instantiation_resolver = EnvModuleResolver::with_memory(memory.clone());
    // Make a module instance
    let module_instance = wasmi::ModuleInstance::new(
        &module,
        &wasmi::ImportsBuilder::new().with_resolver("env", &instantiation_resolver)
    ).map_err(Error::Interpreter)?;

    let mut runtime = Runtime::new(
        ext,
        RuntimeContext {
            /*
            address: self.params.address,
            sender: self.params.sender,
            origin: self.params.origin,
            code_address: self.params.code_address,
            value: self.params.value.value(),
            */
        },
        memory,
        config.gas_limit,
    );
    

    let module_instance = module_instance.run_start(&mut runtime).map_err(Error::Trap)?;

    let method_name = ::std::str::from_utf8(method_name).map_err(|_| Error::BadUtf8)?;
    // let invoke_result = module_instance.invoke_export("call", &[], &mut runtime)?;
    let _result = module_instance.invoke_export(
            method_name,
            &[],
            &mut runtime,
        )?;
 
    /*
    let mut execution_outcome = ExecutionOutcome::NotSpecial;
    if let Err(WasmiError::Trap(ref trap)) = invoke_xresult {
        if let wasmi::TrapKind::Host(ref boxed) = *trap.kind() {
            let ref runtime_err = boxed.downcast_ref::<runtime::Error>()
                .expect("Host errors other than runtime::Error never produced; qed");

            match **runtime_err {
                runtime::Error::Suicide => { execution_outcome = ExecutionOutcome::Suicide; },
                runtime::Error::Return => { execution_outcome = ExecutionOutcome::Return; },
                _ => {}
            }
        }
    }

    if let (ExecutionOutcome::NotSpecial, Err(e)) = (execution_outcome, invoke_result) {
        // trace!(target: "wasm", "Error executing contract: {:?}", e);
        return Err(vm::Error::from(Error::from(e)));
    }

    (
        runtime.gas_left().expect("Cannot fail since it was not updated since last charge"),
        runtime.into_result(),
    )
    */

    Ok(())
}

