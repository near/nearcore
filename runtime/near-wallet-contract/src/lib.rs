#![doc = include_str!("../README.md")]
use near_primitives_core::{chains, hash::CryptoHash};
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

/// Get wallet contract code for different Near chains.
pub fn wallet_contract(code_hash: CryptoHash) -> Option<Arc<ContractCode>> {
    fn check(code_hash: &CryptoHash, contract: &WalletContract) -> Option<Arc<ContractCode>> {
        let magic_bytes = contract.magic_bytes();
        if code_hash == magic_bytes.hash() { Some(contract.read_contract()) } else { None }
    }
    if let Some(c) = check(&code_hash, &MAINNET) {
        return Some(c);
    }
    if let Some(c) = check(&code_hash, &TESTNET) {
        return Some(c);
    }
    if let Some(c) = check(&code_hash, &OLD_TESTNET) {
        return Some(c);
    }
    if let Some(c) = check(&code_hash, &LOCALNET) {
        return Some(c);
    }
    return None;
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
pub fn eth_wallet_global_contract_hash(chain_id: &str) -> CryptoHash {
    match chain_id {
        // 2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4
        chains::MAINNET | chains::MOCKNET => CryptoHash([
            0x1d, 0xaa, 0x83, 0x5c, 0x46, 0x37, 0xf7, 0xae, 0x3d, 0x92, 0x40, 0x95, 0xba, 0x3f,
            0x0b, 0xf2, 0x82, 0x9b, 0xcf, 0xa1, 0x7b, 0x10, 0x68, 0xcd, 0x58, 0xbd, 0x85, 0x3d,
            0xca, 0xd7, 0xce, 0xb5,
        ]),
        // 3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn
        chains::TESTNET => CryptoHash([
            0x23, 0x8f, 0xea, 0xc1, 0xf8, 0x6c, 0xc9, 0xf9, 0xf4, 0x00, 0x3e, 0x3f, 0x6d, 0x5a,
            0xeb, 0xc0, 0x4e, 0xae, 0xa9, 0xc3, 0x94, 0x03, 0x2b, 0xd2, 0x94, 0x70, 0xe9, 0x60,
            0x9b, 0x67, 0xf6, 0xc5,
        ]),
        _ => *LOCALNET.read_contract().hash(),
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
        OLD_TESTNET, code_hash_matches_wallet_contract, eth_wallet_global_contract_hash,
        wallet_contract_magic_bytes,
    };
    use near_primitives_core::{
        chains::{MAINNET, MOCKNET, TESTNET},
        hash::CryptoHash,
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
        let mainnet_expected: CryptoHash =
            "2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4".parse().unwrap();
        let testnet_expected: CryptoHash =
            "3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn".parse().unwrap();
        assert_eq!(eth_wallet_global_contract_hash(MAINNET), mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(MOCKNET), mainnet_expected);
        assert_eq!(eth_wallet_global_contract_hash(TESTNET), testnet_expected);
    }
}
