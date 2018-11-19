use std::fs;
use std::str::from_utf8;

extern crate wasm;

use wasm::executor;
use wasm::ext::{WasmCosts, Externalities, Result};
use wasm::types::Config;

struct MyExt {
    wasm_costs: WasmCosts,
}

impl MyExt {
    pub fn new() -> MyExt {
        MyExt {
            wasm_costs: WasmCosts::default(),
        }
    }
}

impl Externalities for MyExt {
    fn wasm_costs(&self) -> &WasmCosts {
        &self.wasm_costs
    }

    fn storage_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        println!("PUT '{}' -> '{}'", from_utf8(key).unwrap(), from_utf8(value).unwrap());
        Ok(())
    }
}

fn main() {
    let wasm_binary = fs::read("res/wasm_with_mem.wasm")
        .expect("Unable to read file");

    let input_data = [0u8; 0];
    let mut output_data = Vec::new();
    let mut ext = MyExt::new();
    let config = Config::default();

    let result = executor::execute(
        &wasm_binary,
        &input_data,
        &mut output_data,
        &mut ext,
        &config,
    );

    println!("{:?}", result);
}
