#![doc = include_str!("../README.md")]
use near_primitives_core::chains;
use near_vm_runner::ContractCode;
use std::sync::{Arc, OnceLock};

static MAINNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_mainnet.wasm"));

static TESTNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_testnet.wasm"));

static LOCALNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_localnet.wasm"));

/// Get wallet contract code for different Near chains.
pub fn wallet_contract(chain_id: &str) -> Arc<ContractCode> {
    match chain_id {
        chains::MAINNET => MAINNET.read_contract(),
        chains::TESTNET => TESTNET.read_contract(),
        _ => LOCALNET.read_contract(),
    }
}

/// near[wallet contract hash]
pub fn wallet_contract_magic_bytes(chain_id: &str) -> Arc<ContractCode> {
    match chain_id {
        chains::MAINNET => MAINNET.magic_bytes(),
        chains::TESTNET => TESTNET.magic_bytes(),
        _ => LOCALNET.magic_bytes(),
    }
}

struct WalletContract {
    contract: OnceLock<Arc<ContractCode>>,
    magic_bytes: OnceLock<Arc<ContractCode>>,
    code: &'static [u8],
}

impl WalletContract {
    const fn new(code: &'static [u8]) -> Self {
        Self { contract: OnceLock::new(), magic_bytes: OnceLock::new(), code }
    }

    fn read_contract(&self) -> Arc<ContractCode> {
        self.contract.get_or_init(|| Arc::new(ContractCode::new(self.code.to_vec(), None))).clone()
    }

    fn magic_bytes(&self) -> Arc<ContractCode> {
        self.magic_bytes
            .get_or_init(|| {
                let wallet_contract = self.read_contract();
                let magic_bytes = format!("near{}", wallet_contract.hash());
                Arc::new(ContractCode::new(magic_bytes.into_bytes(), None))
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{wallet_contract, wallet_contract_magic_bytes};
    use near_primitives_core::{
        chains::{MAINNET, TESTNET},
        hash::CryptoHash,
    };
    use std::str::FromStr;

    #[test]
    fn check_mainnet_wallet_contract() {
        const WALLET_CONTRACT_HASH: &'static str = "5j8XPMMKMn5cojVs4qQ65dViGtgMHgrfNtJgrC18X8Qw";
        const MAGIC_BYTES_HASH: &'static str = "77CJrGB4MNcG2fJXr87m3HCZngUMxZQYwhqGqcHSd7BB";
        check_wallet_contract(MAINNET, WALLET_CONTRACT_HASH);
        check_wallet_contract_magic_bytes(MAINNET, WALLET_CONTRACT_HASH, MAGIC_BYTES_HASH);
    }

    #[test]
    fn check_testnet_wallet_contract() {
        const WALLET_CONTRACT_HASH: &'static str = "BL1PtbXR6CeP39LXZTVfTNap2dxruEdaWZVxptW6NufU";
        const MAGIC_BYTES_HASH: &'static str = "DBV2KeAR8iaEy6aGpmvAm5HAh1WiZRQ6Tsira4UM83S9";
        check_wallet_contract(TESTNET, WALLET_CONTRACT_HASH);
        check_wallet_contract_magic_bytes(TESTNET, WALLET_CONTRACT_HASH, MAGIC_BYTES_HASH);
    }

    #[test]
    fn check_localnet_wallet_contract() {
        const WALLET_CONTRACT_HASH: &'static str = "FAq9tQRbwJPTV3PQLn2F7AUD3FW2Fw1V8ZeZuazfeu1v";
        const MAGIC_BYTES_HASH: &'static str = "5Ch7WN9GVGHY6rneCsHDHwiC6RPSXjRkXo3sA3c6TT1B";
        const LOCALNET: &str = "localnet";
        check_wallet_contract(LOCALNET, WALLET_CONTRACT_HASH);
        check_wallet_contract_magic_bytes(LOCALNET, WALLET_CONTRACT_HASH, MAGIC_BYTES_HASH);
    }

    fn check_wallet_contract(chain_id: &str, expected_hash: &str) {
        assert!(!wallet_contract(chain_id).code().is_empty());
        let expected_hash =
            CryptoHash::from_str(expected_hash).expect("Failed to parse hash from string");
        assert_eq!(
            *wallet_contract(chain_id).hash(),
            expected_hash,
            "wallet contract hash mismatch"
        );
    }

    fn check_wallet_contract_magic_bytes(
        chain_id: &str,
        expected_code_hash: &str,
        expected_magic_hash: &str,
    ) {
        assert!(!wallet_contract_magic_bytes(chain_id).code().is_empty());
        let expected_hash =
            CryptoHash::from_str(expected_magic_hash).expect("Failed to parse hash from string");
        assert_eq!(
            *wallet_contract_magic_bytes(chain_id).hash(),
            expected_hash,
            "magic bytes hash mismatch"
        );

        let expected_code = format!("near{}", expected_code_hash);
        assert_eq!(wallet_contract_magic_bytes(chain_id).code(), expected_code.as_bytes());
    }
}
