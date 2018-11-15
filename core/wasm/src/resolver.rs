use std::cell::RefCell;

use wasmi::{self, ValueType, MemoryRef, Signature, FuncInstance, Error as WasmiError};


pub(crate) struct EnvModuleResolver {
	max_memory: u32,
	memory: RefCell<Option<MemoryRef>>,
}

impl EnvModuleResolver {
	/// New import resolver with specifed maximum amount of inital memory (in wasm pages = 64kb)
	pub fn with_limit(max_memory: u32) -> EnvModuleResolver {
		EnvModuleResolver {
			max_memory: max_memory,
			memory: RefCell::new(None),
		}
	}

	/// Returns memory that was instantiated during the contract module
	/// start. If contract does not use memory at all, the dummy memory of length (0, 0)
	/// will be created instead. So this method always returns memory instance
	/// unless errored.
	pub fn memory_ref(&self) -> MemoryRef {
		{
			let mut mem_ref = self.memory.borrow_mut();
			if mem_ref.is_none() {
				*mem_ref = Some(
					wasmi::MemoryInstance::alloc(
						wasmi::memory_units::Pages(0),
						Some(wasmi::memory_units::Pages(0)),
					).expect("Memory allocation (0, 0) should not fail; qed")
				);
			}
		}

		self.memory.borrow().clone().expect("it is either existed or was created as (0, 0) above; qed")
	}

	/// Returns memory size module initially requested
	pub fn memory_size(&self) -> Result<u32, WasmiError> {
		Ok(self.memory_ref().current_size().0 as u32)
	}
}

impl wasmi::ModuleImportResolver for EnvModuleResolver {
    fn resolve_func(
        &self,
        field_name: &str,
        _signature: &Signature,
    ) -> Result<wasmi::FuncRef, WasmiError> {
        let func_ref = match field_name {
            /*
            "db_get" => FuncInstance::alloc_host(
                Signature::new(
                    &[ValueType::I32, ValueType::I32, ValueType::I32][..],
                    Some(ValueType::I32)),
                123,
            ),
            "db_put" => FuncInstance::alloc_host(
                Signature::new(
                    &[ValueType::I32, ValueType::I32, ValueType::I32][..],
                    None),
                321,
            ),
            */
            _ => {
                return Err(WasmiError::Instantiation(
                    format!("Export {} not found", field_name),
                ))
            }
        };
        
        Ok(func_ref)
    }

    fn resolve_memory(
		&self,
		field_name: &str,
		descriptor: &wasmi::MemoryDescriptor,
	) -> Result<MemoryRef, WasmiError> {
		if field_name == "memory" {
			let effective_max = descriptor.maximum().unwrap_or(self.max_memory + 1);
			if descriptor.initial() > self.max_memory || effective_max > self.max_memory
			{
				Err(WasmiError::Instantiation("Module requested too much memory".to_owned()))
			} else {
				let mem = wasmi::MemoryInstance::alloc(
					wasmi::memory_units::Pages(descriptor.initial() as usize),
					descriptor.maximum().map(|x| wasmi::memory_units::Pages(x as usize)),
				)?;
				*self.memory.borrow_mut() = Some(mem.clone());
				Ok(mem)
			}
		} else {
			Err(WasmiError::Instantiation("Memory imported under unknown name".to_owned()))
		}
	}
}