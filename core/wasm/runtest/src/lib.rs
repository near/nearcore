use std::collections::HashMap;

extern crate wasm;

use wasm::ext::{External, Result as ExtResult, Error as ExtError};

extern crate byteorder;

extern crate primitives;
use primitives::types::{AccountAlias, PromiseId, ReceiptId};

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
        _account_alias: AccountAlias,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: u32,
        _amount: u64,
    ) -> ExtResult<PromiseId> {
        self.num_receipts += 1;
        Ok(PromiseId::Receipt(generate_promise_id(self.num_receipts - 1)))
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: u32,
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
    use wasm::executor;
    use wasm::types::{Error, Config, ReturnData};
    
    use super::*;

    fn run(
        method_name: &[u8],
        input_data: &[u8],
        result_data: &[Option<Vec<u8>>],
        mana_limit: u32,
    ) -> Result<ReturnData, Error> {
        
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
            mana_limit,
        ).map(|outcome| outcome.return_data)
    }

    fn encode_int(val: i32) -> [u8; 4] {
        let mut tmp = [0u8; 4];
        LittleEndian::write_i32(&mut tmp, val);
        tmp
    }

    #[test]
    fn test_storage()  {
        let input_data = [0u8; 0];
        
        let return_data = run(
            b"run_test",
            &input_data,
            &[],
            0,
        ).expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_int(20)),
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
            0,
        ).expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_int(40)),
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_result_ok()  {
        let input_data = [0u8; 0];
        let result_data = vec![
            Some(encode_int(2).to_vec()),
            Some(encode_int(4).to_vec()),
            Some(encode_int(6).to_vec()),
        ];
        
        let return_data = run(
            b"sum_with_multiple_results",
            &input_data,
            &result_data,
            0,
        ).expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_int(12)),
            _ => assert!(false, "Expected returned value"),
        };
    }

    #[test]
    fn test_promises() {
        let input_data = [0u8; 0];

        let return_data = run(
            b"create_promises_and_join",
            &input_data,
            &[],
            4,
        ).expect("ok");
        match return_data {
            ReturnData::Promise(promise_id) => assert_eq!(&promise_id, &PromiseId::Callback(b"call_it_please".to_vec())),
            _ => assert!(false, "Expected returned promise"),
        };
    }

    #[test]
    fn test_promises_no_mana() {
        let input_data = [0u8; 0];

        let return_data = run(
            b"create_promises_and_join",
            &input_data,
            &[],
            3,
        );

        match return_data {
            Err(_) => assert!(true, "That's legit"),
            _ => assert!(false, "Expected to fail with mana limit"),
        };
    }
}
