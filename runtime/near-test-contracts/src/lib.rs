#![doc = include_str!("../README.md")]

use once_cell::sync::OnceCell;
use std::fmt::Write;
use std::path::Path;

/// Trivial contact with a do-nothing main function.
pub fn trivial_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT
        .get_or_init(|| wat::parse_str(r#"(module (func (export "main")))"#).unwrap())
        .as_slice()
}

/// Standard test contract which can call various host functions.
///
/// Note: the contract relies on the latest protocol version, and
/// might not work for tests using an older version.
pub fn rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs.wasm")).as_slice()
}

pub fn rs_contract_base_protocol() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs_base_protocol.wasm")).as_slice()
}

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
    assert!(!rs_contract_base_protocol().is_empty());
}

pub fn many_functions_contract(function_count: u32) -> Vec<u8> {
    let mut functions = String::new();
    for i in 0..function_count {
        writeln!(
            &mut functions,
            "(func
                i32.const {}
                drop
                return)",
            i
        )
        .unwrap();
    }

    let code = format!(
        r#"(module
            (export "main" (func 0))
            {})"#,
        functions
    );
    wat::parse_str(code).unwrap()
}
