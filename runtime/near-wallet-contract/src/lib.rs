#![doc = include_str!("../README.md")]
use near_primitives_core::{
    chains, hash::CryptoHash, types::ProtocolVersion, version::ProtocolFeature,
};
use near_vm_runner::ContractCode;
use std::{
    str::FromStr,
    sync::{Arc, LazyLock, OnceLock},
};

static MAINNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_mainnet.wasm"));

static TESTNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_testnet.wasm"));

/// Initial version of WalletContract. It was released to testnet, but not mainnet.
/// We still use this one on testnet protocol version 70 for consistency.
static OLD_TESTNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_testnet_old.wasm"));

/// The protocol version on testnet where it is safe to start using the new wallet contract.
const NEW_WALLET_CONTRACT_VERSION: ProtocolVersion =
    ProtocolFeature::FixMinStakeRatio.protocol_version();

static LOCALNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_localnet.wasm"));

/// Old version of the wallet contract on testet. We still support it for
/// backwards compatibility. Example account:
/// https://testnet.nearblocks.io/address/0xcc5a584f545b2ca3ebacc1346556d1f5b82b8fc6
static ALT_TESTNET_CODE_HASH: LazyLock<CryptoHash> =
    LazyLock::new(|| CryptoHash::from_str("4reLvkAWfqk5fsqio1KLudk46cqRz9erQdaHkWZKMJDZ").unwrap());

/// Get wallet contract code for different Near chains.
pub fn wallet_contract(chain_id: &str, protocol_version: ProtocolVersion) -> Arc<ContractCode> {
    match chain_id {
        chains::MAINNET => MAINNET.read_contract(),
        chains::TESTNET => {
            if protocol_version < NEW_WALLET_CONTRACT_VERSION {
                OLD_TESTNET.read_contract()
            } else {
                TESTNET.read_contract()
            }
        }
        _ => LOCALNET.read_contract(),
    }
}

/// near[wallet contract hash]
pub fn wallet_contract_magic_bytes(
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> Arc<ContractCode> {
    match chain_id {
        chains::MAINNET => MAINNET.magic_bytes(),
        chains::TESTNET => {
            if protocol_version < NEW_WALLET_CONTRACT_VERSION {
                OLD_TESTNET.magic_bytes()
            } else {
                TESTNET.magic_bytes()
            }
        }
        _ => LOCALNET.magic_bytes(),
    }
}

/// Checks if the given code hash corresponds to the wallet contract (signalling
/// the runtime should treat the wallet contract as the code for the account).
pub fn code_hash_matches_wallet_contract(
    chain_id: &str,
    code_hash: &CryptoHash,
    protocol_version: ProtocolVersion,
) -> bool {
    let magic_bytes = wallet_contract_magic_bytes(&chain_id, protocol_version);

    if code_hash == magic_bytes.hash() {
        return true;
    }

    // Extra check needed for an old version of the wallet contract
    // that was on testnet. Accounts with that hash are still intentionally
    // made to run the current version of the wallet contract because
    // the previous version had a bug in its implementation.
    if chain_id == chains::TESTNET {
        let alt_testnet_code_hash: &CryptoHash = &ALT_TESTNET_CODE_HASH;
        return code_hash == alt_testnet_code_hash;
    }

    false
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
    use crate::{
        code_hash_matches_wallet_contract, wallet_contract, wallet_contract_magic_bytes,
        ALT_TESTNET_CODE_HASH,
    };
    use near_primitives_core::{
        chains::{MAINNET, TESTNET},
        hash::CryptoHash,
        version::{ProtocolFeature, PROTOCOL_VERSION},
    };
    use std::str::FromStr;

    #[test]
    fn test_code_hash_matches_wallet_contract() {
        let chain_ids = [MAINNET, TESTNET, "localnet"];
        let other_code_hash =
            CryptoHash::from_str("9rmLr4dmrg5M6Ts6tbJyPpbCrNtbL9FCdNv24FcuWP5a").unwrap();
        for id in chain_ids {
            assert!(
                code_hash_matches_wallet_contract(
                    id,
                    wallet_contract_magic_bytes(id, PROTOCOL_VERSION).hash(),
                    PROTOCOL_VERSION
                ),
                "Wallet contract magic bytes matches wallet contract"
            );
            assert_eq!(
                code_hash_matches_wallet_contract(id, &ALT_TESTNET_CODE_HASH, PROTOCOL_VERSION),
                id == TESTNET,
                "Special case only matches on testnet"
            );
            assert!(
                !code_hash_matches_wallet_contract(id, &other_code_hash, PROTOCOL_VERSION),
                "Other code hashes do not match wallet contract"
            );
        }
    }

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
    fn check_old_testnet_wallet_contract() {
        // Make sure the old contract is returned on v70 on testnet.
        const WALLET_CONTRACT_HASH: &'static str = "3Za8tfLX6nKa2k4u2Aq5CRrM7EmTVSL9EERxymfnSFKd";
        let protocol_version = ProtocolFeature::EthImplicitAccounts.protocol_version();
        let contract = wallet_contract(TESTNET, protocol_version);

        assert!(!contract.code().is_empty());
        let expected_hash = CryptoHash::from_str(WALLET_CONTRACT_HASH).unwrap();
        assert_eq!(*contract.hash(), expected_hash, "wallet contract hash mismatch");

        let magic_bytes = wallet_contract_magic_bytes(TESTNET, protocol_version);
        assert!(!magic_bytes.code().is_empty());
        let expected_hash: &CryptoHash = &ALT_TESTNET_CODE_HASH;
        assert_eq!(magic_bytes.hash(), expected_hash, "magic bytes hash mismatch");
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
        assert!(!wallet_contract(chain_id, PROTOCOL_VERSION).code().is_empty());
        let expected_hash =
            CryptoHash::from_str(expected_hash).expect("Failed to parse hash from string");
        assert_eq!(
            *wallet_contract(chain_id, PROTOCOL_VERSION).hash(),
            expected_hash,
            "wallet contract hash mismatch"
        );
    }

    fn check_wallet_contract_magic_bytes(
        chain_id: &str,
        expected_code_hash: &str,
        expected_magic_hash: &str,
    ) {
        assert!(!wallet_contract_magic_bytes(chain_id, PROTOCOL_VERSION).code().is_empty());
        let expected_hash =
            CryptoHash::from_str(expected_magic_hash).expect("Failed to parse hash from string");
        assert_eq!(
            *wallet_contract_magic_bytes(chain_id, PROTOCOL_VERSION).hash(),
            expected_hash,
            "magic bytes hash mismatch"
        );

        let expected_code = format!("near{}", expected_code_hash);
        assert_eq!(
            wallet_contract_magic_bytes(chain_id, PROTOCOL_VERSION).code(),
            expected_code.as_bytes()
        );
    }
}
