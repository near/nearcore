#![doc = include_str!("../README.md")]

use near_vm_runner::ContractCode;
use once_cell::sync::OnceCell;
use std::path::Path;

/// Temporary (placeholder) Wallet Contract.
pub fn wallet_contract() -> &'static ContractCode {
    static CONTRACT: OnceCell<ContractCode> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("wallet_contract.wasm"))
}

/// Temporary (placeholder) Wallet Contract that has access to all host functions from
/// the nightly protocol.
pub fn nightly_wallet_contract() -> &'static ContractCode {
    static CONTRACT: OnceCell<ContractCode> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("nightly_wallet_contract.wasm"))
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
    assert!(!nightly_wallet_contract().code().is_empty());
}
