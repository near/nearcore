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

#[cfg(test)]
mod tests {
    use crate::wallet_contract;
    use near_primitives_core::hash::CryptoHash;
    use std::str::FromStr;

    #[test]
    fn check_wallet_contract() {
        assert!(!wallet_contract().code().is_empty());
        let expected_hash_str = "04UkQ8nasN1u5aBuRna2wEHHAbQmWS2Kdq88TRz1phxAc";
        let expected_hash =
            CryptoHash::from_str(expected_hash_str).expect("Failed to parse hash from string");
        assert_eq!(*wallet_contract().hash(), expected_hash);
    }
}
