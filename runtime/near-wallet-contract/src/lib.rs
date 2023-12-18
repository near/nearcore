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
    #[cfg(feature = "nightly")]
    let code = include_bytes!("../res/wallet_contract.wasm");

    #[cfg(not(feature = "nightly"))]
    let code = &[];

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

#[cfg(feature = "nightly")]
#[cfg(test)]
mod tests {
    use crate::{wallet_contract, wallet_contract_magic_bytes};
    use near_primitives_core::hash::CryptoHash;
    use std::str::FromStr;

    const WALLET_CONTRACT_HASH: &'static str = "5wJJ2YaCq75kVSfx8zoZpevg1uLAn4h7nqUd2njKUEXe";
    const MAGIC_BYTES_HASH: &'static str = "31PSU4diHE4cpWju91fb2zTqn5JSDRZ6xNGM2ub8Lgdg";

    #[test]
    #[ignore]
    // TODO(eth-implicit) Do not ignore when Wallet Contract build becomes reproducible,
    // see https://github.com/near/nearcore/pull/10269#discussion_r1430139987.
    fn check_wallet_contract() {
        assert!(!wallet_contract().code().is_empty());
        let expected_hash =
            CryptoHash::from_str(WALLET_CONTRACT_HASH).expect("Failed to parse hash from string");
        assert_eq!(*wallet_contract().hash(), expected_hash);
    }

    #[test]
    #[ignore]
    // TODO(eth-implicit) Do not ignore when Wallet Contract build becomes reproducible,
    // see https://github.com/near/nearcore/pull/10269#discussion_r1430139987.
    fn check_wallet_contract_magic_bytes() {
        assert!(!wallet_contract_magic_bytes().code().is_empty());
        let expected_hash =
            CryptoHash::from_str(MAGIC_BYTES_HASH).expect("Failed to parse hash from string");
        assert_eq!(*wallet_contract_magic_bytes().hash(), expected_hash);

        let expected_code = format!("near{}", WALLET_CONTRACT_HASH);
        assert_eq!(wallet_contract_magic_bytes().code(), expected_code.as_bytes());
    }
}
