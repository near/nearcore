use ext::ids;
use memory::Memory;
use wasmi::{self, Error as WasmiError, FuncInstance, MemoryRef, Signature, ValueType};

pub(crate) struct EnvModuleResolver {
    memory: Memory,
}

impl EnvModuleResolver {
    /// New import resolver with specifed maximum amount of inital memory (in wasm pages = 64kb)
    pub fn with_memory(memory: Memory) -> EnvModuleResolver {
        EnvModuleResolver { memory }
    }
}

impl wasmi::ModuleImportResolver for EnvModuleResolver {
    fn resolve_func(
        &self,
        field_name: &str,
        _signature: &Signature,
    ) -> Result<wasmi::FuncRef, WasmiError> {
        let func_ref = match field_name {
            "storage_read_len" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::STORAGE_READ_LEN_FUNC,
            ),
            "storage_read_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::STORAGE_READ_INTO_FUNC,
            ),
            "storage_write" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::STORAGE_WRITE_FUNC,
            ),
            "gas" => {
                FuncInstance::alloc_host(Signature::new(&[ValueType::I32][..], None), ids::GAS_FUNC)
            }
            _ => {
                return Err(WasmiError::Instantiation(format!(
                    "Export {} not found",
                    field_name
                )))
            }
        };

        Ok(func_ref)
    }

    fn resolve_memory(
        &self,
        field_name: &str,
        _descriptor: &wasmi::MemoryDescriptor,
    ) -> Result<MemoryRef, WasmiError> {
        match field_name {
            "memory" => Ok(self.memory.memref.clone()),
            _ => Err(WasmiError::Instantiation(
                "Memory imported under unknown name".to_owned(),
            )),
        }
    }
}
