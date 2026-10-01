//! Original ETH wallet WASM fixtures. See `res/wallet_contract/README.md` for provenance.

/// Original mainnet wallet contract.
pub fn legacy_mainnet() -> &'static [u8] {
    include_bytes!("../res/wallet_contract/wallet_contract_mainnet.wasm")
}

/// Original testnet wallet contract.
pub fn legacy_testnet() -> &'static [u8] {
    include_bytes!("../res/wallet_contract/wallet_contract_testnet.wasm")
}

/// Original testnet pv70 wallet contract.
pub fn legacy_testnet_pv70() -> &'static [u8] {
    include_bytes!("../res/wallet_contract/wallet_contract_testnet_pv70.wasm")
}

/// Original localnet wallet contract.
pub fn legacy_localnet() -> &'static [u8] {
    include_bytes!("../res/wallet_contract/wallet_contract_localnet.wasm")
}

/// Mainnet global wallet deployed for `UpdatedEthWalletContract`.
pub fn global_mainnet() -> &'static [u8] {
    include_bytes!("../res/wallet_contract/global_contract_mainnet.wasm")
}
