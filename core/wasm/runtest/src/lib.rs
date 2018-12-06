use std::collections::HashMap;

extern crate wasm;

use wasm::ext::{External, Result as ExtResult, Error as ExtError};

extern crate byteorder;

extern crate primitives;
use primitives::types::{AccountAlias, PromiseId};

#[derive(Default)]
struct MyExt {
    storage: HashMap<Vec<u8>, Vec<u8>>,
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
        Err(ExtError::NotImplemented)
    }

    fn promise_then(
        &mut self,
        _promise_id: PromiseId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: u32,
    ) -> ExtResult<PromiseId> {
        Err(ExtError::NotImplemented)
    }

    fn promise_and(
        &mut self,
        _promise_id1: PromiseId,
        _promise_id2: PromiseId,
    ) -> ExtResult<PromiseId> {
        Err(ExtError::NotImplemented)
    }

}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LittleEndian};
    use std::fs;
    use wasm::executor;
    use wasm::types::{Error, Config, ReturnData};
    
    use super::*;

    fn run(method_name: &[u8], input_data: &[u8], result_data: &[Option<Vec<u8>>]) -> Result<ReturnData, Error> {
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
        ).expect("ok");
        
        match return_data {
            ReturnData::Value(output_data) => assert_eq!(&output_data, &encode_int(12)),
            _ => assert!(false, "Expected returned value"),
        };
    }
}
