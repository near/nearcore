#![doc = include_str!("../README.md")]

use near_vm_runner::ContractCode;
use once_cell::sync::OnceCell;
use std::path::Path;
use std::sync::Arc;

/// Temporary (placeholder) Wallet Contract. Read from file once, then cache in memory.
pub fn wallet_contract() -> Arc<ContractCode> {
    static CONTRACT: OnceCell<Arc<ContractCode>> = OnceCell::new();
    CONTRACT.get_or_init(|| Arc::new(read_contract("wallet_contract.wasm"))).clone()
}

/// Read given wasm file or panic if unable to.
fn read_contract(file_name: &str) -> ContractCode {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = base.join("res").join(file_name);
    let code = match std::fs::read(&path) {
        Ok(data) => data,
        Err(err) => panic!("{}: {}", path.display(), err),
    };
    ContractCode::new(code, None)
}

#[test]
fn smoke_test() {
    assert!(!wallet_contract().code().is_empty());
}
