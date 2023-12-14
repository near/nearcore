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

/// near[wallet contract hash]
pub fn wallet_contract_magic_bytes() -> Arc<ContractCode> {
    static CONTRACT: OnceLock<Arc<ContractCode>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            let wallet_contract_hash = *wallet_contract().hash();
            let magic_bytes = format!("near{}", wallet_contract_hash.to_string());
            Arc::new(ContractCode::new(magic_bytes.into(), None))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use crate::{wallet_contract, wallet_contract_magic_bytes};
    use near_primitives_core::hash::CryptoHash;
    use std::str::FromStr;

    const WALLET_CONTRACT_HASH: &'static str = "4UkQ8nasN1u5aBuRna2wEHHAbQmWS2Kdq88TRz1phxAc";
    const MAGIC_BYTES_HASH: &'static str = "46ABZEDwsEGnyJqNzJ1EftKzqH3ZtZj5Loj78Wr1vopm";

    #[test]
    fn check_wallet_contract() {
        assert!(!wallet_contract().code().is_empty());
        let expected_hash =
            CryptoHash::from_str(WALLET_CONTRACT_HASH).expect("Failed to parse hash from string");
        assert_eq!(*wallet_contract().hash(), expected_hash);
    }

    #[test]
    fn check_wallet_contract_magic_bytes() {
        assert!(!wallet_contract_magic_bytes().code().is_empty());
        let expected_hash =
            CryptoHash::from_str(MAGIC_BYTES_HASH).expect("Failed to parse hash from string");
        assert_eq!(*wallet_contract_magic_bytes().hash(), expected_hash);

        let expected_code = format!("near{}", WALLET_CONTRACT_HASH);
        assert_eq!(wallet_contract_magic_bytes().code(), expected_code.as_bytes());
    }
}
