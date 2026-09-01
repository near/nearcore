#![doc = include_str!("../README.md")]
use near_primitives_core::{
    chains, hash::CryptoHash, types::ProtocolVersion, version::ProtocolFeature,
};
use near_vm_runner::ContractCode;
use std::sync::{Arc, OnceLock};

static MAINNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_mainnet.wasm"));

static TESTNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_testnet.wasm"));

/// Initial version of WalletContract. It was released to testnet, but not mainnet.
/// We still use this one on testnet protocol version 70 for consistency.
/// Example account:
/// https://testnet.nearblocks.io/address/0xcc5a584f545b2ca3ebacc1346556d1f5b82b8fc6
static OLD_TESTNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_testnet_pv70.wasm"));

static LOCALNET: WalletContract =
    WalletContract::new(include_bytes!("../res/wallet_contract_localnet.wasm"));

const MAINNET_GLOBAL_CONTRACTS: [WalletGlobalContract; 2] = [
    WalletGlobalContract {
        // 2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4
        hash: CryptoHash([
            0x1d, 0xaa, 0x83, 0x5c, 0x46, 0x37, 0xf7, 0xae, 0x3d, 0x92, 0x40, 0x95, 0xba, 0x3f,
            0x0b, 0xf2, 0x82, 0x9b, 0xcf, 0xa1, 0x7b, 0x10, 0x68, 0xcd, 0x58, 0xbd, 0x85, 0x3d,
            0xca, 0xd7, 0xce, 0xb5,
        ]),
        latest_protocol_version: Some(
            ProtocolFeature::UpdatedEthWalletContract.protocol_version() - 1,
        ),
    },
    WalletGlobalContract {
        hash: CryptoHash([0; 32]), // TODO: fill in real hash
        latest_protocol_version: None,
    },
];

const TESTNET_GLOBAL_CONTRACTS: [WalletGlobalContract; 2] = [
    WalletGlobalContract {
        // 3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn
        hash: CryptoHash([
            0x23, 0x8f, 0xea, 0xc1, 0xf8, 0x6c, 0xc9, 0xf9, 0xf4, 0x00, 0x3e, 0x3f, 0x6d, 0x5a,
            0xeb, 0xc0, 0x4e, 0xae, 0xa9, 0xc3, 0x94, 0x03, 0x2b, 0xd2, 0x94, 0x70, 0xe9, 0x60,
            0x9b, 0x67, 0xf6, 0xc5,
        ]),
        latest_protocol_version: Some(
            ProtocolFeature::UpdatedEthWalletContract.protocol_version() - 1,
        ),
    },
    WalletGlobalContract {
        hash: CryptoHash([0; 32]), // TODO: fill in real hash
        latest_protocol_version: None,
    },
];

/// Identifies a legacy ETH wallet contract variant by chain.
#[derive(Clone, Debug)]
pub enum LegacyEthWallet {
    Mainnet,
    /// Current testnet wallet contract (from protocol version 71+).
    Testnet,
    /// Initial wallet contract released to testnet at protocol version 70,
    /// before it was updated. Never deployed to mainnet.
    OldTestnet,
    Localnet,
}

impl LegacyEthWallet {
    /// Resolve a code hash to a legacy ETH wallet variant, if it matches any
    /// known wallet contract magic bytes.
    pub fn resolve(code_hash: CryptoHash) -> Option<Self> {
        if MAINNET.check_magic_bytes(&code_hash) {
            return Some(LegacyEthWallet::Mainnet);
        }
        if TESTNET.check_magic_bytes(&code_hash) {
            return Some(LegacyEthWallet::Testnet);
        }
        if OLD_TESTNET.check_magic_bytes(&code_hash) {
            return Some(LegacyEthWallet::OldTestnet);
        }
        if LOCALNET.check_magic_bytes(&code_hash) {
            return Some(LegacyEthWallet::Localnet);
        }
        None
    }

    fn wallet_contract(&self) -> &'static WalletContract {
        match self {
            LegacyEthWallet::Mainnet => &MAINNET,
            LegacyEthWallet::Testnet => &TESTNET,
            LegacyEthWallet::OldTestnet => &OLD_TESTNET,
            LegacyEthWallet::Localnet => &LOCALNET,
        }
    }

    /// Return the contract code for this legacy ETH wallet variant.
    pub fn contract(&self) -> Arc<ContractCode> {
        self.wallet_contract().read_contract()
    }
}

/// Get wallet contract code for different Near chains.
pub fn wallet_contract(code_hash: CryptoHash) -> Option<Arc<ContractCode>> {
    LegacyEthWallet::resolve(code_hash).map(|w| w.contract())
}

/// near[wallet contract hash]
pub fn wallet_contract_magic_bytes(chain_id: &str) -> Arc<ContractCode> {
    match chain_id {
        chains::MAINNET => MAINNET.magic_bytes(),
        chains::TESTNET => TESTNET.magic_bytes(),
        _ => LOCALNET.magic_bytes(),
    }
}

/// Returns the global contract hash for the ETH wallet contract on a given chain.
/// This is the hash of the deployed global contract that ETH implicit accounts
/// should use when the EthImplicitGlobalContract protocol feature is enabled.
///
/// For other chains (localnet, test chains): Uses the hash of the embedded
/// wallet contract WASM, allowing tests to deploy the same contract as a
/// global contract.
pub fn eth_wallet_global_contract_hash(
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> CryptoHash {
    match chain_id {
        chains::MAINNET | chains::MOCKNET => WalletGlobalContract::resolve_for_protocol_version(
            &MAINNET_GLOBAL_CONTRACTS,
            protocol_version,
        ),
        chains::TESTNET => WalletGlobalContract::resolve_for_protocol_version(
            &TESTNET_GLOBAL_CONTRACTS,
            protocol_version,
        ),
        _ => *LOCALNET.read_contract().hash(),
    }
}

/// Checks if the given `code_hash` matches any previous (now superseded) wallet contracts
/// for the current network and protocol version.
pub fn is_earlier_eth_wallet_global_contract_hash(
    code_hash: &CryptoHash,
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> bool {
    match chain_id {
        chains::MAINNET | chains::MOCKNET => WalletGlobalContract::hash_matches_earlier_version(
            &MAINNET_GLOBAL_CONTRACTS,
            code_hash,
            protocol_version,
        ),
        chains::TESTNET => WalletGlobalContract::hash_matches_earlier_version(
            &TESTNET_GLOBAL_CONTRACTS,
            code_hash,
            protocol_version,
        ),
        _ => false,
    }
}

/// Checks if the given code hash corresponds to the wallet contract (signalling
/// the runtime should treat the wallet contract as the code for the account).
pub fn code_hash_matches_wallet_contract(chain_id: &str, code_hash: &CryptoHash) -> bool {
    let magic_bytes = wallet_contract_magic_bytes(&chain_id);

    if code_hash == magic_bytes.hash() {
        return true;
    }

    // Extra check needed for an old version of the wallet contract
    // that was on testnet. Accounts with that hash are still intentionally
    // made to run the current version of the wallet contract because
    // the previous version had a bug in its implementation.
    if chain_id == chains::TESTNET {
        let alt_testnet_code = OLD_TESTNET.magic_bytes();
        return code_hash == alt_testnet_code.hash();
    }

    false
}

struct WalletGlobalContract {
    hash: CryptoHash,
    latest_protocol_version: Option<ProtocolVersion>,
}

impl WalletGlobalContract {
    fn resolve_for_protocol_version(
        contracts: &[Self],
        protocol_version: ProtocolVersion,
    ) -> CryptoHash {
        for contract in contracts {
            match contract.latest_protocol_version {
                None => {
                    return contract.hash;
                }
                Some(latest_protocol_version) if protocol_version <= latest_protocol_version => {
                    return contract.hash;
                }
                _ => (),
            }
        }
        unreachable!("List of possible contracts must have one current version");
    }

    fn hash_matches_earlier_version(
        contracts: &[Self],
        code_hash: &CryptoHash,
        protocol_version: ProtocolVersion,
    ) -> bool {
        for contract in contracts {
            if let Some(latest_protocol_version) = contract.latest_protocol_version
                && latest_protocol_version < protocol_version
                && &contract.hash == code_hash
            {
                return true;
            }
        }
        false
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

    fn check_magic_bytes(&self, code_hash: &CryptoHash) -> bool {
        code_hash == self.magic_bytes().hash()
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
        OLD_TESTNET, code_hash_matches_wallet_contract, eth_wallet_global_contract_hash,
        is_earlier_eth_wallet_global_contract_hash, wallet_contract_magic_bytes,
    };
    use near_primitives_core::{
        chains::{MAINNET, MOCKNET, TESTNET},
        hash::CryptoHash,
        version::ProtocolFeature,
    };
    use std::str::FromStr;

    #[test]
    fn test_code_hash_matches_wallet_contract() {
        let chain_ids = [MAINNET, TESTNET, "localnet"];
        let testnet_code_v70 = OLD_TESTNET.magic_bytes();
        let other_code_hash =
            CryptoHash::from_str("9rmLr4dmrg5M6Ts6tbJyPpbCrNtbL9FCdNv24FcuWP5a").unwrap();
        for id in chain_ids {
            assert!(
                code_hash_matches_wallet_contract(id, wallet_contract_magic_bytes(id).hash()),
                "Wallet contract magic bytes matches wallet contract"
            );
            assert_eq!(
                code_hash_matches_wallet_contract(id, testnet_code_v70.hash()),
                id == TESTNET,
                "Special case only matches on testnet"
            );
            assert!(
                !code_hash_matches_wallet_contract(id, &other_code_hash),
                "Other code hashes do not match wallet contract"
            );
        }
    }

    #[test]
    fn test_eth_wallet_global_contract_hash_values() {
        let updated_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version();
        let non_updated_pv = updated_pv - 1;

        let old_mainnet_expected: CryptoHash =
            "2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4".parse().unwrap();
        let old_testnet_expected: CryptoHash =
            "3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn".parse().unwrap();

        assert_eq!(eth_wallet_global_contract_hash(MAINNET, non_updated_pv), old_mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(MOCKNET, non_updated_pv), old_mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(TESTNET, non_updated_pv), old_testnet_expected);

        let new_mainnet_expected: CryptoHash = "11111111111111111111111111111111".parse().unwrap();
        let new_testnet_expected: CryptoHash = "11111111111111111111111111111111".parse().unwrap();

        // Latest versions are returned on the newer protocol version
        assert_eq!(eth_wallet_global_contract_hash(MAINNET, updated_pv), new_mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(MOCKNET, updated_pv), new_mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(TESTNET, updated_pv), new_testnet_expected);

        // The old versions are still detected
        assert!(is_earlier_eth_wallet_global_contract_hash(
            &old_mainnet_expected,
            MAINNET,
            updated_pv
        ));
        assert!(is_earlier_eth_wallet_global_contract_hash(
            &old_mainnet_expected,
            MOCKNET,
            updated_pv
        ));
        assert!(is_earlier_eth_wallet_global_contract_hash(
            &old_testnet_expected,
            TESTNET,
            updated_pv
        ));

        // The old versions on other chains do not count
        assert!(!is_earlier_eth_wallet_global_contract_hash(
            &old_testnet_expected,
            MAINNET,
            updated_pv
        ));
        assert!(!is_earlier_eth_wallet_global_contract_hash(
            &old_mainnet_expected,
            TESTNET,
            updated_pv
        ));
    }
}
