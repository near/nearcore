use call::Callback;

use ext::Externalities;
use wasmi::{self, MemoryRef, Error as WasmiError, Trap};

use resolver::EnvModuleResolver;

use runtime::{self, Runtime, RuntimeContext};

/// Wrapped error
#[derive(Debug)]
pub enum Error {
	Interpreter(WasmiError),
	Trap(Trap),
}

impl From<WasmiError> for Error {
	fn from(e: WasmiError) -> Self {
		Error::Interpreter(e)
	}
}

impl From<Trap> for Error {
	fn from(e: Trap) -> Self {
		Error::Trap(e)
	}
}

enum ExecutionOutcome {
	WTF,
	GGG,
	YOYOYOY,
}


pub struct Exectutor {

}

impl Exectutor {
    pub fn new(

    ) -> Exectutor {
        Exectutor {

        }
    }

    pub fn execute(
		ext: &mut Externalities,
    ) -> Result<(), Error> {
        // Fetch code
        let wasm_binary = fs::read("wasm_with_mem.wasm")
            .expect("Unable to read file");

        // Load wasm binary and prepare it for instantiation.
        let module = wasmi::Module::from_buffer(&wasm_binary)
            .expect("failed to load wasm");

		let (module, data) = parser::payload(&self.params, ext.schedule().wasm())?;
        // Parse module from code
		let loaded_module = wasmi::Module::from_parity_wasm_module(module).map_err(Error::Interpreter)?;
        // Setup memory and functions
		let instantiation_resolver = EnvModuleResolver::with_limit(16);
        // Make a module instance
		let module_instance = wasmi::ModuleInstance::new(
			&loaded_module,
			&wasmi::ImportsBuilder::new().with_resolver("env", &instantiation_resolver)
		).map_err(Error::Interpreter)?;
        // Getting initial memory
		let initial_memory = instantiation_resolver.memory_size().map_err(Error::Interpreter)?;

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
            instantiation_resolver.memory_ref(),
        );

        let module_instance = module_instance.run_start(&mut runtime).map_err(Error::Trap)?;

        let invoke_result = module_instance.invoke_export("call", &[], &mut runtime);
        /*
        let mut execution_outcome = ExecutionOutcome::NotSpecial;
        if let Err(WasmiError::Trap(ref trap)) = invoke_result {
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
}