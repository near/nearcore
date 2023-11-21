#![no_main]

use libfuzzer_sys::arbitrary::Arbitrary;
use near_vm::*;
use near_vm_engine::{Engine, Executable};
use near_vm_engine_universal::Universal;
use near_vm_vm::Artifact;
use near_vm_compiler::CompileError;

#[derive(Clone, Debug, Arbitrary)]
struct Args {
    bytecode: Vec<u8>,
}

fn compile_uncached<'a>(
    store: &'a Store,
    engine: &'a dyn Engine,
    code: &'a [u8],
    time: bool,
) -> Result<Box<dyn near_vm_engine::Executable>, CompileError> {
    engine.validate(code)?;
    let res = engine.compile(code, store.tunables());
    res
}

libfuzzer_sys::fuzz_target!(|args: Args| {
    let compiler = Singlepass::default();
    let engine = Universal::new(compiler).engine();
    let store = Store::new(&engine);
    let code = args.bytecode;
    compile_uncached(&store, &engine, &code, false);
});
