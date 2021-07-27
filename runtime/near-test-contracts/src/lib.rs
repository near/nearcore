use std::io;
use std::path::Path;

use once_cell::sync::OnceCell;

pub fn rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs.wasm").unwrap()).as_slice()
}

pub fn nightly_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("nightly_test_contract_rs.wasm").unwrap()).as_slice()
}

pub fn ts_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_ts.wasm").unwrap()).as_slice()
}

pub fn tiny_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("tiny_contract_rs.wasm").unwrap()).as_slice()
}

pub fn aurora_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("aurora_engine.wasm").unwrap()).as_slice()
}

pub fn get_aurora_contract_data() -> (&'static [u8], &'static str) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (CONTRACT.get_or_init(|| read_contract("aurora_engine.wasm").unwrap()).as_slice(), "state_migration")
}

pub fn get_multisig_contract_data() -> (&'static [u8], &'static str) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (CONTRACT.get_or_init(|| read_contract("multisig.wasm").unwrap()).as_slice(), "get_request_nonce")
}

pub fn get_voting_contract_data() -> (&'static [u8], &'static str) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (CONTRACT.get_or_init(|| read_contract("voting_contract.wasm").unwrap()).as_slice(), "get_result")
}

fn read_contract(file_name: &str) -> io::Result<Vec<u8>> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = base.join("res").join(file_name);
    std::fs::read(path)
}

#[test]
fn smoke_test() {
    assert!(!rs_contract().is_empty());
    assert!(!nightly_rs_contract().is_empty());
    assert!(!ts_contract().is_empty());
    assert!(!tiny_contract().is_empty());
}
