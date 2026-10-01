//! ETH implicit wallet contract hashes used by the runtime.
//!
//! Legacy accounts store the hash of `near<base58(WASM hash)>` as their local
//! code hash. Execution resolves these accounts to a global contract, so only
//! the hashes are needed here. Tests pin the original code hashes; WASM needed
//! for execution tests lives in near-test-contracts.

use near_primitives_core::{
    chains, hash::CryptoHash, types::ProtocolVersion, version::ProtocolFeature,
};

// Legacy mainnet wallet magic bytes.
// 77CJrGB4MNcG2fJXr87m3HCZngUMxZQYwhqGqcHSd7BB
const MAINNET_MAGIC_HASH: CryptoHash = CryptoHash([
    0x5a, 0xbc, 0x64, 0x62, 0x2d, 0xdd, 0x36, 0x61, 0x89, 0x9e, 0x5d, 0x76, 0x5e, 0x7a, 0x52, 0x95,
    0x11, 0x5e, 0x90, 0x57, 0xc5, 0x4e, 0x9b, 0xbb, 0x9f, 0x0f, 0x0e, 0x02, 0xcd, 0x1d, 0x21, 0x16,
]);

// Legacy testnet wallet magic bytes.
// DBV2KeAR8iaEy6aGpmvAm5HAh1WiZRQ6Tsira4UM83S9
const TESTNET_MAGIC_HASH: CryptoHash = CryptoHash([
    0xb4, 0xfb, 0xbc, 0x96, 0x4b, 0x35, 0x0b, 0xd0, 0x21, 0xb4, 0xa7, 0x85, 0xb9, 0xdd, 0xd6, 0x3e,
    0x41, 0xd6, 0xf1, 0x32, 0x70, 0x2a, 0x6b, 0x38, 0xa2, 0x3b, 0x56, 0x48, 0xab, 0x2f, 0xb4, 0x72,
]);

// Legacy testnet pv70 wallet magic bytes.
// 4reLvkAWfqk5fsqio1KLudk46cqRz9erQdaHkWZKMJDZ
const TESTNET_PV70_MAGIC_HASH: CryptoHash = CryptoHash([
    0x39, 0x4a, 0xbe, 0xb3, 0x5e, 0x70, 0x76, 0x09, 0xde, 0x8f, 0x73, 0xb6, 0x3d, 0x43, 0xbd, 0x1a,
    0x37, 0x6f, 0xfe, 0x67, 0x93, 0x5c, 0xaa, 0x68, 0x93, 0x7d, 0xd2, 0x9b, 0xc0, 0x4e, 0x67, 0x3c,
]);

// Legacy localnet wallet magic bytes.
// 5Ch7WN9GVGHY6rneCsHDHwiC6RPSXjRkXo3sA3c6TT1B
const LOCALNET_MAGIC_HASH: CryptoHash = CryptoHash([
    0x3e, 0x6d, 0x7d, 0xd8, 0xc2, 0x57, 0x8a, 0xc8, 0xa5, 0xeb, 0xe7, 0x8f, 0xd3, 0x2d, 0x60, 0x0a,
    0x55, 0x5c, 0xef, 0x5e, 0xf7, 0x44, 0x5d, 0x3a, 0x96, 0x46, 0x58, 0x66, 0x9d, 0x70, 0x7c, 0xf2,
]);

// WASM hash used as the global contract on local and other non-production chains.
// FAq9tQRbwJPTV3PQLn2F7AUD3FW2Fw1V8ZeZuazfeu1v
const LOCALNET_GLOBAL_CONTRACT_HASH: CryptoHash = CryptoHash([
    0xd2, 0x88, 0x4a, 0x90, 0x0d, 0x4f, 0xa3, 0x70, 0xe4, 0x8f, 0x1e, 0x3d, 0x48, 0x65, 0xd4, 0xdc,
    0x0d, 0xbd, 0x09, 0x3c, 0x89, 0xf0, 0xe7, 0xba, 0xef, 0x2e, 0xdd, 0xc1, 0x42, 0xc4, 0x9e, 0x8d,
]);

// Wallet hashes before and after UpdatedEthWalletContract.
const MAINNET_GLOBAL_CONTRACTS: [CryptoHash; 2] = [
    // 2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4
    CryptoHash([
        0x1d, 0xaa, 0x83, 0x5c, 0x46, 0x37, 0xf7, 0xae, 0x3d, 0x92, 0x40, 0x95, 0xba, 0x3f, 0x0b,
        0xf2, 0x82, 0x9b, 0xcf, 0xa1, 0x7b, 0x10, 0x68, 0xcd, 0x58, 0xbd, 0x85, 0x3d, 0xca, 0xd7,
        0xce, 0xb5,
    ]),
    // 5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw
    // Deployed in https://nearblocks.io/txns/DKiEEbvssm6ozPTFesTGewRVcrteCmyTHzi4JvTPxaR7
    CryptoHash([
        0x40, 0x63, 0x8b, 0x73, 0xc9, 0x6c, 0xeb, 0x00, 0xe7, 0x03, 0x79, 0xdc, 0x71, 0x65, 0x89,
        0xfd, 0xcc, 0xaf, 0x0d, 0x35, 0xec, 0x7b, 0x28, 0xf3, 0x8a, 0x79, 0x99, 0xe3, 0x89, 0x50,
        0x54, 0x72,
    ]),
];

// Wallet hashes before and after UpdatedEthWalletContract.
const TESTNET_GLOBAL_CONTRACTS: [CryptoHash; 2] = [
    // 3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn
    CryptoHash([
        0x23, 0x8f, 0xea, 0xc1, 0xf8, 0x6c, 0xc9, 0xf9, 0xf4, 0x00, 0x3e, 0x3f, 0x6d, 0x5a, 0xeb,
        0xc0, 0x4e, 0xae, 0xa9, 0xc3, 0x94, 0x03, 0x2b, 0xd2, 0x94, 0x70, 0xe9, 0x60, 0x9b, 0x67,
        0xf6, 0xc5,
    ]),
    // H7BByXFswWtJzpoatHsnTKiUAomKubBTFi8tq9vzULX7
    // Deployed in https://testnet.nearblocks.io/txns/GYdoLMuhaoJThrbcTepWKn5YqemJTb7B3bem2Pw3Es4f
    CryptoHash([
        0xef, 0x4f, 0xff, 0x25, 0x0b, 0xc3, 0x65, 0x06, 0x3a, 0x73, 0x72, 0xc3, 0xe9, 0x3b, 0x30,
        0x81, 0x99, 0x0b, 0xd5, 0x04, 0xc6, 0x3a, 0xf2, 0xd1, 0xff, 0x5a, 0x1d, 0x16, 0x7e, 0x0a,
        0x6c, 0x66,
    ]),
];

/// Recognize legacy wallet magic hashes on every chain, including testnet PV70.
/// Existing accounts keep these markers even though execution uses a global contract.
pub(crate) fn is_legacy_eth_wallet(code_hash: CryptoHash) -> bool {
    [MAINNET_MAGIC_HASH, TESTNET_MAGIC_HASH, TESTNET_PV70_MAGIC_HASH, LOCALNET_MAGIC_HASH]
        .contains(&code_hash)
}

fn global_contracts(chain_id: &str) -> Option<&'static [CryptoHash; 2]> {
    match chain_id {
        chains::MAINNET | chains::MOCKNET => Some(&MAINNET_GLOBAL_CONTRACTS),
        chains::TESTNET => Some(&TESTNET_GLOBAL_CONTRACTS),
        _ => None,
    }
}

/// Select the deployed wallet for this network and protocol version.
/// Other chains use the localnet WASM hash so tests can deploy it as a global contract.
pub(crate) fn eth_wallet_global_contract_hash(
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> CryptoHash {
    let Some([previous, current]) = global_contracts(chain_id) else {
        return LOCALNET_GLOBAL_CONTRACT_HASH;
    };
    if ProtocolFeature::UpdatedEthWalletContract.enabled(protocol_version) {
        *current
    } else {
        *previous
    }
}

/// Recognize superseded global wallets so existing accounts use the updated contract.
pub(crate) fn is_earlier_eth_wallet_global_contract_hash(
    code_hash: &CryptoHash,
    chain_id: &str,
    protocol_version: ProtocolVersion,
) -> bool {
    ProtocolFeature::UpdatedEthWalletContract.enabled(protocol_version)
        && global_contracts(chain_id).is_some_and(|[previous, _]| code_hash == previous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives_core::chains::{MAINNET, MOCKNET, TESTNET};
    use near_primitives_core::hash::hash;
    use near_test_contracts::wallet_contract::{global_mainnet, legacy_localnet};

    #[test]
    fn test_legacy_magic_hashes() {
        // Original WASM hashes, pinned before removing the archived binaries.
        // See near-test-contracts/res/wallet_contract/README.md for their provenance.
        let wallets = [
            ("5j8XPMMKMn5cojVs4qQ65dViGtgMHgrfNtJgrC18X8Qw", MAINNET_MAGIC_HASH),
            ("BL1PtbXR6CeP39LXZTVfTNap2dxruEdaWZVxptW6NufU", TESTNET_MAGIC_HASH),
            ("3Za8tfLX6nKa2k4u2Aq5CRrM7EmTVSL9EERxymfnSFKd", TESTNET_PV70_MAGIC_HASH),
            ("FAq9tQRbwJPTV3PQLn2F7AUD3FW2Fw1V8ZeZuazfeu1v", LOCALNET_MAGIC_HASH),
        ];
        for (original_hash, magic_hash) in wallets {
            let code_hash: CryptoHash = original_hash.parse().unwrap();
            assert_eq!(hash(format!("near{code_hash}").as_bytes()), magic_hash);
            assert!(is_legacy_eth_wallet(magic_hash));
            assert!(!is_legacy_eth_wallet(code_hash));
        }
        assert!(!is_legacy_eth_wallet(CryptoHash::default()));
    }

    #[test]
    fn test_global_hash_protocol_boundaries() {
        let updated_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version();
        let mainnet_hashes = [
            "2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4",
            "5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw",
        ];
        let testnet_hashes = [
            "3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn",
            "H7BByXFswWtJzpoatHsnTKiUAomKubBTFi8tq9vzULX7",
        ];
        assert_eq!(hash(global_mainnet()), mainnet_hashes[1].parse().unwrap());
        for (chain_id, expected) in
            [(MAINNET, mainnet_hashes), (MOCKNET, mainnet_hashes), (TESTNET, testnet_hashes)]
        {
            let [old, current] = expected.map(|h| h.parse::<CryptoHash>().unwrap());
            for pv in [0, updated_pv - 1, updated_pv, updated_pv + 1, ProtocolVersion::MAX] {
                assert_eq!(
                    eth_wallet_global_contract_hash(chain_id, pv),
                    if pv < updated_pv { old } else { current }
                );
                for h in mainnet_hashes
                    .into_iter()
                    .chain(testnet_hashes)
                    .chain(["11111111111111111111111111111111"])
                {
                    let code_hash = h.parse().unwrap();
                    assert_eq!(
                        is_earlier_eth_wallet_global_contract_hash(&code_hash, chain_id, pv),
                        pv >= updated_pv && code_hash == old
                    );
                }
            }
        }
    }

    #[test]
    fn test_localnet_global_hash() {
        let expected = hash(legacy_localnet());
        assert_eq!(LOCALNET_GLOBAL_CONTRACT_HASH, expected);
        let updated_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version();
        for chain_id in ["localnet", "test-chain", "", "MAINNET"] {
            for pv in [0, updated_pv - 1, updated_pv, ProtocolVersion::MAX] {
                assert_eq!(eth_wallet_global_contract_hash(chain_id, pv), expected);
                for code_hash in MAINNET_GLOBAL_CONTRACTS
                    .iter()
                    .chain(TESTNET_GLOBAL_CONTRACTS.iter())
                    .chain([&expected])
                {
                    assert!(!is_earlier_eth_wallet_global_contract_hash(code_hash, chain_id, pv));
                }
            }
        }
    }
}
