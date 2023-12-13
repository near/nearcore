#![doc = include_str!("../README.md")]

use near_vm_runner::ContractCode;
use std::sync::{Arc, OnceLock};

/// Temporary (placeholder) Wallet Contract.
pub fn wallet_contract() -> Arc<ContractCode> {
    static CONTRACT: OnceLock<Arc<ContractCode>> = OnceLock::new();
    CONTRACT.get_or_init(|| Arc::new(read_contract())).clone()
}

/// Include the WASM file content directly in the binary at compile time.
fn read_contract() -> ContractCode {
    let code = include_bytes!("../res/wallet_contract.wasm");
    ContractCode::new(code.to_vec(), None)
}

#[test]
fn smoke_test() {
    assert!(!wallet_contract().code().is_empty());
}
