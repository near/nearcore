use std::collections::HashMap;
use std::fs;

extern crate wasm;

use wasm::executor;
use wasm::ext::{External, Result};
use wasm::types::Config;

#[derive(Default)]
struct MyExt {
    storage: HashMap<Vec<u8>, Vec<u8>>,
}

impl External for MyExt {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        println!("PUT '{:?}' -> '{:?}'", key, value);
        self.storage.insert(Vec::from(key), Vec::from(value));
        Ok(())
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        let value = self.storage.get(key);
        match value {
            Some(buf) => {
                println!("GET '{:?}' -> '{:?}'", key, buf);
                Ok(Some(&buf))
            }
            None => {
                println!("GET '{:?}' -> EMPTY", key);
                Ok(None)
            }
        }
    }
}

fn main() {
    let wasm_binary = fs::read("res/wasm_with_mem.wasm").expect("Unable to read file");

    let input_data = [0u8; 0];
    let mut output_data = Vec::new();
    let mut ext = MyExt::default();
    let config = Config::default();

    let result = executor::execute(
        &wasm_binary,
        b"run_test",
        &input_data,
        &mut output_data,
        &mut ext,
        &config,
    );

    println!("{:?}", result);
}
