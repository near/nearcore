//! Benchmark the codegen quality with a compute benchmark.
//!
//! ```shell
//! cargo run --example coremark --release --features "singlepass"
//! ```

use wasmer::{imports, Function, FunctionType, Instance, Module, Store, Type, Value};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm_bytes = include_bytes!("coremark.wasm");
    let compiler = Singlepass::default();
    let store = Store::new(&Universal::new(compiler).engine());
    println!("Compiling module...");
    let module = Module::new(&store, wasm_bytes)?;
    let clock_ms_sig = FunctionType::new(vec![], vec![Type::I64]);
    let start = std::time::Instant::now();
    let import_object = imports! {
        "env" => {
            "clock_ms" => Function::new(&store, clock_ms_sig, move |_| {
                Ok(vec![Value::I64(start.elapsed().as_millis() as u64 as i64)])
            })
        }
    };
    println!("Instantiating module...");
    let instance = Instance::new(&module, &import_object)?;
    let run = instance.lookup_function("run").expect("function lookup");
    println!("Calling CoreMark 1.0. Should take 12~20 seconds...");
    let results = run.call(&[])?;
    println!("Score: {:?}", results);
    Ok(())
}

#[test]
#[cfg(feature = "singlepass")]
fn test_compiler_singlepass() -> Result<(), Box<dyn std::error::Error>> {
    main()
}
