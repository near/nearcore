use std::collections::HashMap;

extern crate wasm;

use wasm::ext::{External, Result as ExtResult, Error as ExtError};

extern crate byteorder;

extern crate primitives;
use primitives::types::{AccountId, PromiseId, ReceiptId, Mana, Balance};

#[derive(Default)]
struct MyExt {
    storage: HashMap<Vec<u8>, Vec<u8>>,
    num_receipts: u32,
}

fn generate_promise_id(index: u32) -> ReceiptId {
    [index as u8; 32].to_vec()
}

impl External for MyExt {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<()> {
        println!("PUT '{:?}' -> '{:?}'", key, value);
        self.storage.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn storage_get(&self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let value = self.storage.get(key);
        match value {
            Some(buf) => {
                println!("GET '{:?}' -> '{:?}'", key, buf);
                Ok(Some(buf.to_vec()))
            }
            None => {
                println!("GET '{:?}' -> EMPTY", key);
                Ok(None)
            }
        }
    }

    fn promise_create(
        &mut self,
        account_id: AccountId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: Mana,
        _amount: Balance,
    ) -> ExtResult<PromiseId> {
        match self.num_receipts {
            0 => assert_eq!(&account_id, &"test1".to_string()),
            1 => assert_eq!(&account_id, &"test2".to_string()),
            _ => (),
        };
        self.num_receipts += 1;
        Ok(PromiseId::Receipt(generate_promise_id(self.num_receipts - 1)))
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: Mana,
    ) -> ExtResult<PromiseId> {
        match promise_id {
            PromiseId::Receipt(_) => {
                assert!(false);
                Err(ExtError::WrongPromise)
            },
            PromiseId::Joiner(v) => {
                assert_eq!(v[0], generate_promise_id(0));
                assert_eq!(v[1], generate_promise_id(1));
                Ok(PromiseId::Callback(b"call_it_please".to_vec()))
            },
            _ => Err(ExtError::WrongPromise),
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LittleEndian};
    use std::fs;
    use wasm::executor::{self, ExecutionOutcome};
    use wasm::types::{Error, Config, RuntimeContext, ReturnData};
    
    use super::*;

    fn run(
        method_name: &[u8],
        input_data: &[u8],
        result_data: &[Option<Vec<u8>>],
        context: &RuntimeContext,
    ) -> Result<ExecutionOutcome, Error> {
        let wasm_binary = fs::read("res/wasm_with_mem.wasm").expect("Unable to read file");

        let mut ext = MyExt::default();
        let config = Config::default();

        executor::execute(
            &wasm_binary,
            &method_name,
            &input_data,
            &result_data,
            &mut ext,
            &config,
            &context,
        )
    }

    fn encode_i32(val: i32) -> [u8; 4] {
        let mut tmp = [0u8; 4];
        LittleEndian::write_i32(&mut tmp, val);
        tmp
    }

    fn encode_u64(val: u64) -> [u8; 8] {
        let mut tmp = [0u8; 8];
        LittleEndian::write_u64(&mut tmp, val);
        tmp
    }

    fn runtime_context(
        balance: Balance,
        amount: Balance,
        mana: Mana,
    ) -> RuntimeContext {
        RuntimeContext::new(
            balance,
            amount,
            &"alice".to_string(),
            &"bob".to_string(),
            mana
        )
    }

    #[test]
    fn test_storage()  {
        let input_data = [0u8; 0];
        
        let return_data = run(
            b"run_test",
            &input_data,
            &[],
            &runtime_context(0, 0, 0),
        ).map(|outcome| outcome.return_data)
        .expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_i32(20)),
            _ => assert!(false, "Expected returned value"),
        };
    }


    #[test]
    fn test_input()  {
        let input_data = [10u8, 0, 0, 0, 30u8, 0, 0, 0];

        let return_data = run(
            b"sum_with_input",
            &input_data,
            &[],
            &runtime_context(0, 0, 0),
        ).map(|outcome| outcome.return_data)
        .expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_i32(40)),
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_result_ok()  {
        let input_data = [0u8; 0];
        let result_data = vec![
            Some(encode_i32(2).to_vec()),
            Some(encode_i32(4).to_vec()),
            Some(encode_i32(6).to_vec()),
        ];
        
        let return_data = run(
            b"sum_with_multiple_results",
            &input_data,
            &result_data,
            &runtime_context(0, 0, 0),
        ).map(|outcome| outcome.return_data)
        .expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_i32(12)),
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_promises() {
        let input_data = [0u8; 0];

        let outcome = run(
            b"create_promises_and_join",
            &input_data,
            &[],
            &runtime_context(0, 0, 4),
        ).expect("ok");

        assert_eq!(outcome.mana_used, 4); 

        match outcome.return_data {
            ReturnData::Promise(promise_id) => assert_eq!(&promise_id, &PromiseId::Callback(b"call_it_please".to_vec())),
            _ => assert!(false, "Expected returned promise"),
        };
    }

    #[test]
    fn test_promises_no_mana() {
        let input_data = [0u8; 0];

        let outcome = run(
            b"create_promises_and_join",
            &input_data,
            &[],
            &runtime_context(0, 0, 3),
        );

        match outcome {
            Err(_) => assert!(true, "That's legit"),
            _ => assert!(false, "Expected to fail with mana limit"),
        };
    }

    #[test]
    fn test_assert_sum_ok()  {
        let input_data = [10u8, 0, 0, 0, 30u8, 0, 0, 0, 40u8, 0, 0, 0];

        run(
            b"assert_sum",
            &input_data,
            &[],
            &runtime_context(0, 0, 0),
        ).expect("ok");
    }

    #[test]
    fn test_assert_sum_fail() {
        let input_data = [10u8, 0, 0, 0, 30u8, 0, 0, 0, 45u8, 0, 0, 0];

        let outcome = run(
            b"assert_sum",
            &input_data,
            &[],
            &runtime_context(0, 0, 0),
        );

        match outcome {
            Err(_) => assert!(true, "That's legit"),
            _ => assert!(false, "Expected to fail with assert failure"),
        };
    }

    #[test]
    fn test_get_mana()  {
        let input_data = [0u8; 0];

        let outcome = run(
            b"get_mana_left",
            &input_data,
            &[],
            &runtime_context(0, 0, 10),
        ).expect("ok");

        assert_eq!(outcome.mana_left, 10);

        match outcome.return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_i32(10)),
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_get_gas()  {
        let input_data = [0u8; 0];

        let return_data = run(
            b"get_gas_left",
            &input_data,
            &[],
            &runtime_context(0, 0, 0),
        ).map(|outcome| outcome.return_data)
        .expect("ok");

        let approximate_expected_gas = Config::default().gas_limit;

        match return_data {
            ReturnData::Value(output_data) => {
                assert_eq!(output_data.len(), 8);
                let actual_gas = LittleEndian::read_u64(&output_data);
                assert!(actual_gas <= approximate_expected_gas);
                assert!(approximate_expected_gas - actual_gas < 10);
            },
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_get_balance_and_amount()  {
        let input_data = [0u8; 0];

        let outcome = run(
            b"get_prev_balance",
            &input_data,
            &[],
            &runtime_context(90, 10, 0),
        ).expect("ok");

        assert_eq!(outcome.balance, 100);

        match outcome.return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_u64(90)),
            _ => assert!(false, "Expected returned value"),
        };
    }
}
