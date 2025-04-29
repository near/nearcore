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
    use crate::{OLD_TESTNET, code_hash_matches_wallet_contract, wallet_contract_magic_bytes};
    use near_primitives_core::{
        chains::{MAINNET, TESTNET},
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
}
