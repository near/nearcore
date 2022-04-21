#![doc = include_str!("../README.md")]

use once_cell::sync::OnceCell;
use std::path::Path;

/// Trivial contact with a do-nothing main function.
pub fn trivial_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT
        .get_or_init(|| wat::parse_str(r#"(module (func (export "main")))"#).unwrap())
        .as_slice()
}

/// Contract with exact size in bytes.
pub fn sized_contract(size: usize) -> Vec<u8> {
    let payload = "x".repeat(size);
    let base_size =
        wat::parse_str(format!("(module (data \"{payload}\") (func (export \"main\")))"))
            .unwrap()
            .len();
    let adjusted_size = size as i64 - (base_size as i64 - size as i64);
    let payload = "x".repeat(adjusted_size as usize);
    let code = format!("(module (data \"{payload}\") (func (export \"main\")))");
    let contract = wat::parse_str(code).unwrap();
    assert_eq!(contract.len(), size);
    contract
}

/// Standard test contract which can call various host functions.
///
/// Note: the contract relies on the latest stable protocol version, and might
/// not work for tests using an older version. In particular, if a test depends
/// on a specific protocol version, it should use [`base_rs_contract`].
pub fn rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs.wasm")).as_slice()
}

/// Standard test contract which is compatible any protocol version, including
/// the oldest one.
///
/// This is useful for tests that use a specific protocol version rather then
/// just the latest one. In particular, protocol upgrade tests should use this
/// function rather than [`rs_contract`].
pub fn base_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("base_test_contract_rs.wasm")).as_slice()
}

/// Standard test contract which additionally includes all host functions from
/// the nightly protocol.
pub fn nightly_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("nightly_test_contract_rs.wasm")).as_slice()
}

pub fn ts_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_ts.wasm")).as_slice()
}

pub fn fuzzing_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("contract_for_fuzzing_rs.wasm")).as_slice()
}

/// Read given wasm file or panic if unable to.
fn read_contract(file_name: &str) -> Vec<u8> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = base.join("res").join(file_name);
    match std::fs::read(&path) {
        Ok(data) => data,
        Err(err) => panic!("{}: {}", path.display(), err),
    }
}

#[test]
fn smoke_test() {
    assert!(!rs_contract().is_empty());
    assert!(!nightly_rs_contract().is_empty());
    assert!(!ts_contract().is_empty());
    assert!(!trivial_contract().is_empty());
    assert!(!fuzzing_contract().is_empty());
    assert!(!base_rs_contract().is_empty());
}

/// Construct a contract with many entitites.
///
/// Currently supports constructing contracts that contain a specified number of functions with the
/// specified number of locals each.
///
/// Exports a function called `main` that does nothing.
pub fn large_contract(functions: u32, locals: u32) -> Vec<u8> {
    use wasm_encoder::{
        CodeSection, Export, ExportSection, Function, FunctionSection, Instruction, Module,
        TypeSection, ValType,
    };
    // Won't generate a valid WASM without functions.
    assert!(functions >= 1, "must specify at least 1 function to be generated");
    let mut module = Module::new();
    let mut type_section = TypeSection::new();
    type_section.function([], []);
    module.section(&type_section);

    let mut functions_section = FunctionSection::new();
    for _ in 0..functions {
        functions_section.function(0);
    }
    module.section(&functions_section);

    let mut exports_section = ExportSection::new();
    exports_section.export("main", Export::Function(0));
    module.section(&exports_section);

    let mut code_section = CodeSection::new();
    for _ in 0..functions {
        let mut f = Function::new([(locals, ValType::I64)]);
        f.instruction(&Instruction::End);
        code_section.function(&f);
    }
    module.section(&code_section);

    module.finish()
}
