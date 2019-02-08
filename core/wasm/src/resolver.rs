use crate::runtime::Runtime;

pub(crate) fn build_imports() -> ImportObject {
    imports! {
        // Define the "env" namespace that was implicitly used
        // by our sample application.
        "env" => {
            // name         // func    // signature
            "storage_read_len" => !func(storage_read_len<[u32, u32, i32] -> []>,
            "gas" => gas<[u32] -> []>,
        },
    };
            "storage_read_len" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::STORAGE_READ_LEN_FUNC,
            ),
            "storage_read_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::STORAGE_READ_INTO_FUNC,
            ),
            "storage_iter" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::STORAGE_ITER_FUNC,
            ),
            "storage_iter_next" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::STORAGE_ITER_NEXT_FUNC,
            ),
            "storage_iter_peek_len" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::STORAGE_ITER_PEEK_LEN_FUNC,
            ),
            "storage_iter_peek_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::STORAGE_ITER_PEEK_INTO_FUNC,
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
            "read_len" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                ids::READ_LEN_FUNC,
            ),
            "read_into" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                ids::READ_INTO_FUNC,
            ),
            "hash" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::HASH_FUNC,
            ),
            "hash32" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ids::HASH_32_FUNC,
            ),
            "random_buf" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                ids::RANDOM_BUF_FUNC,
            ),
            "random32" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I32)),
                ids::RANDOM_32_FUNC,
            ),
            "block_index" => FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I64)),
                ids::BLOCK_INDEX_FUNC,
            ),
            "gas" => {
                FuncInstance::alloc_host(Signature::new(&[ValueType::I32][..], None), ids::GAS_FUNC)
            },
            "debug" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::DEBUG_FUNC,
            ),
            "log" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], None),
                ids::LOG_FUNC,
            ),
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
