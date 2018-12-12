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
            "promise_create" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32, ValueType::I32, ValueType::I64,][..], Some(ValueType::I32)),
                ids::PROMISE_CREATE_FUNC,
            ),
            "promise_then" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                ids::PROMISE_THEN_FUNC,
            ),
            "promise_and" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                ids::PROMISE_AND_FUNC,
            ),
            "input_read_len" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I32)),
                ids::INPUT_READ_LEN_FUNC,
            ),
            "input_read_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::INPUT_READ_INTO_FUNC,
            ),
            "result_count" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I32)),
                ids::RESULT_COUNT_FUNC,
            ),
            "result_is_ok" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::RESULT_IS_OK_FUNC,
            ),
            "result_read_len" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::RESULT_READ_LEN_FUNC,
            ),
            "result_read_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::RESULT_READ_INTO_FUNC,
            ),
            "return_value" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::RETURN_VALUE_FUNC,
            ),
            "return_promise" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::RETURN_PROMISE_FUNC,
            ),
            "balance" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I64)),
                ids::BALANCE_FUNC,
            ),
            "mana_left" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I32)),
                ids::MANA_LEFT_FUNC,
            ),
            "gas_left" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I64)),
                ids::GAS_LEFT_FUNC,
            ),
            "received_amount" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I64)),
                ids::RECEIVED_AMOUNT_FUNC,
            ),
            "assert" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::ASSERT_FUNC,
            ),
            "abort" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                ids::ABORT_FUNC,
            ),
            "sender_id" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::SENDER_ID_FUNC,
            ),
            "account_id" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::ACCOUNT_ID_FUNC,
            ),
            "account_alias_to_id" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::ACCOUNT_ALIAS_TO_ID_FUNC,
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
