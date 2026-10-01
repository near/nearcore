//! ETH implicit wallet contract hashes used by the runtime.
//!
//! Legacy accounts store the hash of `near<base58(WASM hash)>` as their local
//! code hash. Execution resolves these accounts to a global contract, so only
//! the hashes are needed here. The original WASM is retained in near-test-contracts
//! and checked against these constants by the tests below.

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

const MAINNET_GLOBAL_CONTRACTS: [WalletGlobalContract; 2] = [
    WalletGlobalContract {
        // 2zodJZK2e4nnv5AqwCRnenNSmkikXhEd7PPY6BmfTmW4
        global_contract_hash: CryptoHash([
            0x1d, 0xaa, 0x83, 0x5c, 0x46, 0x37, 0xf7, 0xae, 0x3d, 0x92, 0x40, 0x95, 0xba, 0x3f,
            0x0b, 0xf2, 0x82, 0x9b, 0xcf, 0xa1, 0x7b, 0x10, 0x68, 0xcd, 0x58, 0xbd, 0x85, 0x3d,
            0xca, 0xd7, 0xce, 0xb5,
        ]),
        latest_protocol_version: Some(
            ProtocolFeature::UpdatedEthWalletContract.protocol_version() - 1,
        ),
    },
    WalletGlobalContract {
        // 5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw
        // Deployed in https://nearblocks.io/txns/DKiEEbvssm6ozPTFesTGewRVcrteCmyTHzi4JvTPxaR7
        global_contract_hash: CryptoHash([
            0x40, 0x63, 0x8b, 0x73, 0xc9, 0x6c, 0xeb, 0x00, 0xe7, 0x03, 0x79, 0xdc, 0x71, 0x65,
            0x89, 0xfd, 0xcc, 0xaf, 0x0d, 0x35, 0xec, 0x7b, 0x28, 0xf3, 0x8a, 0x79, 0x99, 0xe3,
            0x89, 0x50, 0x54, 0x72,
        ]),
        latest_protocol_version: None,
    },
];

const TESTNET_GLOBAL_CONTRACTS: [WalletGlobalContract; 2] = [
    WalletGlobalContract {
        // 3PpYvRxBfC5BkZxTw8ZFG3D52w1ZRhvDDWirKoxphMDn
        global_contract_hash: CryptoHash([
            0x23, 0x8f, 0xea, 0xc1, 0xf8, 0x6c, 0xc9, 0xf9, 0xf4, 0x00, 0x3e, 0x3f, 0x6d, 0x5a,
            0xeb, 0xc0, 0x4e, 0xae, 0xa9, 0xc3, 0x94, 0x03, 0x2b, 0xd2, 0x94, 0x70, 0xe9, 0x60,
            0x9b, 0x67, 0xf6, 0xc5,
        ]),
        latest_protocol_version: Some(
            ProtocolFeature::UpdatedEthWalletContract.protocol_version() - 1,
        ),
    },
    WalletGlobalContract {
        // H7BByXFswWtJzpoatHsnTKiUAomKubBTFi8tq9vzULX7
        // Deployed in https://testnet.nearblocks.io/txns/GYdoLMuhaoJThrbcTepWKn5YqemJTb7B3bem2Pw3Es4f
        global_contract_hash: CryptoHash([
            0xef, 0x4f, 0xff, 0x25, 0x0b, 0xc3, 0x65, 0x06, 0x3a, 0x73, 0x72, 0xc3, 0xe9, 0x3b,
            0x30, 0x81, 0x99, 0x0b, 0xd5, 0x04, 0xc6, 0x3a, 0xf2, 0xd1, 0xff, 0x5a, 0x1d, 0x16,
            0x7e, 0x0a, 0x6c, 0x66,
        ]),
        latest_protocol_version: None,
    },
];

/// Recognize legacy wallet magic hashes on every chain, including testnet PV70.
pub(crate) fn is_legacy_eth_wallet(code_hash: CryptoHash) -> bool {
    [MAINNET_MAGIC_HASH, TESTNET_MAGIC_HASH, TESTNET_PV70_MAGIC_HASH, LOCALNET_MAGIC_HASH]
        .contains(&code_hash)
}

/// Returns the global contract hash for the ETH wallet contract on a given chain.
/// This is the hash of the deployed global contract that ETH implicit accounts
/// should use when the EthImplicitGlobalContract protocol feature is enabled.
///
/// For other chains (localnet, test chains): Uses the hash of the localnet
/// wallet contract WASM, allowing tests to deploy the same contract as a
/// global contract.
pub(crate) fn eth_wallet_global_contract_hash(
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
        _ => LOCALNET_GLOBAL_CONTRACT_HASH,
    }
}

/// Checks if the given `code_hash` matches any previous (now superseded) wallet contracts
/// for the current network and protocol version.
pub(crate) fn is_earlier_eth_wallet_global_contract_hash(
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

struct WalletGlobalContract {
    global_contract_hash: CryptoHash,
    /// The latest protocol version where this instance of the wallet contract
    /// is used by the protocol for eth-implicit accounts. If `None` then
    /// this instance applies to all future protocol versions.
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
                    return contract.global_contract_hash;
                }
                Some(latest_protocol_version) if protocol_version <= latest_protocol_version => {
                    return contract.global_contract_hash;
                }
                _ => (),
            }
        }
        unreachable!("list of possible contracts must have one current version");
    }

    fn hash_matches_earlier_version(
        contracts: &[Self],
        code_hash: &CryptoHash,
        protocol_version: ProtocolVersion,
    ) -> bool {
        for contract in contracts {
            if let Some(latest_protocol_version) = contract.latest_protocol_version
                && latest_protocol_version < protocol_version
                && &contract.global_contract_hash == code_hash
            {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives_core::chains::{MAINNET, MOCKNET, TESTNET};
    use near_primitives_core::hash::hash;
    use near_test_contracts::wallet_contract::{
        global_mainnet, legacy_localnet, legacy_mainnet, legacy_testnet, legacy_testnet_pv70,
    };

    #[test]
    fn test_legacy_magic_hashes_match_original_wasm() {
        let wallets = [
            (legacy_mainnet(), MAINNET_MAGIC_HASH),
            (legacy_testnet(), TESTNET_MAGIC_HASH),
            (legacy_testnet_pv70(), TESTNET_PV70_MAGIC_HASH),
            (legacy_localnet(), LOCALNET_MAGIC_HASH),
        ];
        for (wasm, magic_hash) in wallets {
            let code_hash = hash(wasm);
            assert_eq!(hash(format!("near{code_hash}").as_bytes()), magic_hash);
            assert!(is_legacy_eth_wallet(magic_hash));
            assert!(!is_legacy_eth_wallet(code_hash));
        }
        assert!(!is_legacy_eth_wallet(CryptoHash::default()));
    }

    #[test]
    fn test_localnet_global_hash_matches_original_wasm() {
        assert_eq!(LOCALNET_GLOBAL_CONTRACT_HASH, hash(legacy_localnet()));
        let updated_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version();
        for chain_id in ["localnet", "test-chain", "", "MAINNET"] {
            for pv in [0, updated_pv - 1, updated_pv, ProtocolVersion::MAX] {
                assert_eq!(
                    eth_wallet_global_contract_hash(chain_id, pv),
                    LOCALNET_GLOBAL_CONTRACT_HASH
                );
                for contract in
                    MAINNET_GLOBAL_CONTRACTS.iter().chain(TESTNET_GLOBAL_CONTRACTS.iter())
                {
                    assert!(!is_earlier_eth_wallet_global_contract_hash(
                        &contract.global_contract_hash,
                        chain_id,
                        pv
                    ));
                }
            }
        }
    }

    #[test]
    fn test_global_hash_protocol_boundaries() {
        let updated_pv = ProtocolFeature::UpdatedEthWalletContract.protocol_version();
        assert_eq!(hash(global_mainnet()), MAINNET_GLOBAL_CONTRACTS[1].global_contract_hash);
        for (chain_id, contracts) in [
            (MAINNET, &MAINNET_GLOBAL_CONTRACTS),
            (MOCKNET, &MAINNET_GLOBAL_CONTRACTS),
            (TESTNET, &TESTNET_GLOBAL_CONTRACTS),
        ] {
            let old = contracts[0].global_contract_hash;
            let current = contracts[1].global_contract_hash;
            for pv in [0, updated_pv - 1, updated_pv, updated_pv + 1, ProtocolVersion::MAX] {
                assert_eq!(
                    eth_wallet_global_contract_hash(chain_id, pv),
                    if pv < updated_pv { old } else { current }
                );
                assert_eq!(
                    is_earlier_eth_wallet_global_contract_hash(&old, chain_id, pv),
                    pv >= updated_pv
                );
                assert!(!is_earlier_eth_wallet_global_contract_hash(&current, chain_id, pv));
                assert!(!is_earlier_eth_wallet_global_contract_hash(
                    &CryptoHash::default(),
                    chain_id,
                    pv
                ));
            }
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

        let new_mainnet_expected: CryptoHash =
            "5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw".parse().unwrap();
        let new_testnet_expected: CryptoHash =
            "H7BByXFswWtJzpoatHsnTKiUAomKubBTFi8tq9vzULX7".parse().unwrap();

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

    #[test]
    fn test_contract_lists_sorted_by_protocol_version() {
        assert_list_sorted_by_protocol_version(&MAINNET_GLOBAL_CONTRACTS);
        assert_list_sorted_by_protocol_version(&TESTNET_GLOBAL_CONTRACTS);
    }

    fn assert_list_sorted_by_protocol_version(list: &[WalletGlobalContract]) {
        let length = list.len();
        if length == 0 {
            panic!("mainnet and testnet must have non-empty list of wallet contracts.");
        }
        let mut version = list[0]
            .latest_protocol_version
            .expect("the first entry expires at some protocol version.");
        for (index, contract) in list.iter().enumerate().skip(1) {
            match contract.latest_protocol_version {
                Some(newer_version) => {
                    assert_ne!(index, length - 1, "The final entry in the list must never expire");
                    assert!(
                        version < newer_version,
                        "Later instances of the wallet contract must expire on later protocol versions."
                    );
                    version = newer_version;
                }
                None => {
                    assert_eq!(index, length - 1, "Only the final entry never expires.");
                }
            }
        }
    }
}
