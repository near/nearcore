use crate::runtime::config::RuntimeConfig;
use crate::runtime::parameter_table::ParameterTable;
use crate::types::ProtocolVersion;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

macro_rules! include_config {
    ($file:expr) => {
        include_str!(concat!("../../res/runtime_configs/", $file))
    };
}

/// The base config file with all initial parameter values defined.
/// Later version are calculated by applying diffs to this base.
static BASE_CONFIG: &str = include_config!("parameters.txt");

/// Stores pairs of protocol versions for which runtime config was updated and
/// the file containing the diffs in bytes.
static CONFIG_DIFFS: &[(ProtocolVersion, &str)] = &[
    (42, include_config!("42.txt")),
    (48, include_config!("48.txt")),
    (49, include_config!("49.txt")),
    (50, include_config!("50.txt")),
    // max_gas_burnt increased to 300 TGas
    (52, include_config!("52.txt")),
    // Increased deployment costs, increased wasmer2 stack_limit, added limiting of contract locals,
    // set read_cached_trie_node cost, decrease storage key limit
    (53, include_config!("53.txt")),
];

/// Testnet parameters for versions <= 29, which (incorrectly) differed from mainnet parameters
pub static INITIAL_TESTNET_CONFIG: &str = include_config!("parameters_testnet.txt");

/// Stores runtime config for each protocol version where it was updated.
#[derive(Debug)]
pub struct RuntimeConfigStore {
    /// Maps protocol version to the config.
    store: BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>,
}

impl RuntimeConfigStore {
    /// Constructs a new store.
    ///
    /// If genesis_runtime_config is Some, configs for protocol versions 0 and 42 are overridden by
    /// this config and config with lowered storage cost, respectively.
    /// This is done to preserve compatibility with previous implementation, where we updated
    /// runtime config by sequential modifications to the genesis runtime config.
    /// TODO #4775: introduce new protocol version to have the same runtime config for all chains
    pub fn new(genesis_runtime_config: Option<&RuntimeConfig>) -> Self {
        let mut params = ParameterTable::from_txt(BASE_CONFIG);

        let mut store = BTreeMap::new();
        store.insert(0, Arc::new(RuntimeConfig::from_parameters(&params)));

        for (protocol_version, diff_bytes) in CONFIG_DIFFS {
            let diff = ParameterTable::from_txt(diff_bytes);
            params.apply_diff(diff);
            store.insert(*protocol_version, Arc::new(RuntimeConfig::from_parameters(&params)));
        }

        if let Some(runtime_config) = genesis_runtime_config {
            let mut config = runtime_config.clone();
            store.insert(0, Arc::new(config.clone()));

            config.storage_amount_per_byte = 10u128.pow(19);
            store.insert(42, Arc::new(config));
        }

        Self { store }
    }

    /// Constructs test store.
    pub fn with_one_config(runtime_config: RuntimeConfig) -> Self {
        Self { store: BTreeMap::from_iter([(0, Arc::new(runtime_config))].iter().cloned()) }
    }

    /// Constructs test store.
    pub fn test() -> Self {
        Self::with_one_config(RuntimeConfig::test())
    }

    /// Constructs store with a single config with zero costs.
    pub fn free() -> Self {
        Self::with_one_config(RuntimeConfig::free())
    }

    /// Returns a `RuntimeConfig` for the corresponding protocol version.
    pub fn get_config(&self, protocol_version: ProtocolVersion) -> &Arc<RuntimeConfig> {
        self.store
            .range((Bound::Unbounded, Bound::Included(protocol_version)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Not found RuntimeConfig for protocol version {}", protocol_version)
            })
            .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::to_base;
    use crate::version::ProtocolFeature::LowerStorageCost;
    use crate::version::ProtocolFeature::{
        LowerDataReceiptAndEcrecoverBaseCost, LowerStorageKeyLimit,
    };
    use near_primitives_core::hash::hash;

    const GENESIS_PROTOCOL_VERSION: ProtocolVersion = 29;
    const RECEIPTS_DEPTH: u64 = 63;

    static OLD_CONFIGS: &[(ProtocolVersion, &str)] = &[
        (0, include_config!("legacy_configs/29.json")),
        (42, include_config!("legacy_configs/42.json")),
        (48, include_config!("legacy_configs/48.json")),
        (49, include_config!("legacy_configs/49.json")),
        (50, include_config!("legacy_configs/50.json")),
        (52, include_config!("legacy_configs/52.json")),
        (53, include_config!("legacy_configs/53.json")),
    ];

    fn check_config(protocol_version: ProtocolVersion, config_bytes: &str) {
        assert_eq!(
            RuntimeConfigStore::new(None).get_config(protocol_version).as_ref(),
            &serde_json::from_str::<RuntimeConfig>(config_bytes).unwrap()
        );
    }

    #[test]
    fn test_get_config() {
        check_config(0, OLD_CONFIGS[0].1);
        check_config(GENESIS_PROTOCOL_VERSION - 1, OLD_CONFIGS[0].1);
        check_config(GENESIS_PROTOCOL_VERSION, OLD_CONFIGS[0].1);
        // First non-trivial version for which runtime config was updated.
        check_config(LowerStorageCost.protocol_version(), OLD_CONFIGS[1].1);
        check_config(ProtocolVersion::MAX, OLD_CONFIGS.last().unwrap().1);
    }

    #[test]
    fn test_runtime_config_data() {
        let expected_hashes = vec![
            "HienWJdw69DbweYfSLuvoG6H9hmLyxrSjdjew59PvgZr",
            "3wJtyFjogsZqx2j6wBwS6nFHc9TG97SZZ6XuE4h6dDSV",
            "5QQViPqxDuy4Jx4oTLDtjJHRuxJi9Uycjr5CZ7pEb2bi",
            "3qfeHpTsHD2QjbgD6RXLG1uEdYDT96cx8gAYq22f2ewR",
            "43mw9K9Uz9iwKio9JgAEuzmX6PnbDin1pR14BNJPnR3G",
            "h5Z8fdRxxhTpgvf6AE3JtL8k4i8c4KVp3znd8ntLZhR",
            "UJBggpeNQgzFsj4jaWEfJ5hfMPATfZy8NQXWaD41cHT",
        ];
        let actual_hashes = std::iter::once(&(0, BASE_CONFIG))
            .chain(CONFIG_DIFFS)
            .map(|(_protocol_version, config_str)| to_base(&hash(config_str.as_bytes())))
            .collect::<Vec<_>>();
        assert_eq!(
            expected_hashes, actual_hashes,
            "\n
Config hashes changed. \n
If you add a new config diff, add a missing hash to the end of `expected_hashes` array.
"
        )
    }

    #[test]
    fn test_old_runtime_config_data() {
        let expected_hashes = vec![
            "C7W1yQiAmmtqtw6nbkwNrJNymhX8SkAJ9PnNUUwWA9z",
            "D5PuE2rD9yXThbdrACj6B9Ga9EiJVPak6Ejxaxfqb4Ci",
            "3rVVngZj9mTG8e7YhgTbSoo7rNCpMSGhSYQBa98oFLd5",
            "B8fL27CPSdug9AQXgS77b3n9aWFUFf7WUjJcuRMvjE6C",
            "5SneeY6PB8NktERjNq8Gge68cdEgjELiAbhNsmDv1cqY",
            "EFq13cxe78LdB7bbvYF5s1PYxsDGU1teXYXW2qaJkXwW",
            "ENnHEU1otcAWjxv4HhNkKb7FsZFZr5FGNhFjRL2W2a68",
        ];
        let actual_hashes = OLD_CONFIGS
            .iter()
            .map(|(_protocol_version, config_str)| to_base(&hash(config_str.as_bytes())))
            .collect::<Vec<_>>();
        assert_eq!(
            expected_hashes, actual_hashes,
            "\n
Old config hashes changed. \n
"
        )
    }

    #[test]
    fn test_max_prepaid_gas() {
        let store = RuntimeConfigStore::new(None);
        for (protocol_version, config) in store.store.iter() {
            assert!(
                config.wasm_config.limit_config.max_total_prepaid_gas
                    / config.transaction_costs.min_receipt_with_function_call_gas()
                    <= 63,
                "The maximum desired depth of receipts for protocol version {} should be at most {}",
                protocol_version,
                RECEIPTS_DEPTH
            );
        }
    }

    #[test]
    fn test_lower_storage_cost() {
        let store = RuntimeConfigStore::new(None);
        let base_cfg = store.get_config(GENESIS_PROTOCOL_VERSION);
        let new_cfg = store.get_config(LowerStorageCost.protocol_version());
        assert!(base_cfg.storage_amount_per_byte > new_cfg.storage_amount_per_byte);
    }

    #[test]
    fn test_override_account_length() {
        // Check that default value is 32.
        let base_store = RuntimeConfigStore::new(None);
        let base_cfg = base_store.get_config(GENESIS_PROTOCOL_VERSION);
        assert_eq!(base_cfg.account_creation_config.min_allowed_top_level_account_length, 32);

        let mut cfg = base_cfg.as_ref().clone();
        cfg.account_creation_config.min_allowed_top_level_account_length = 0;

        // Check that length was changed.
        let new_store = RuntimeConfigStore::new(Some(&cfg));
        let new_cfg = new_store.get_config(GENESIS_PROTOCOL_VERSION);
        assert_eq!(new_cfg.account_creation_config.min_allowed_top_level_account_length, 0);
    }

    #[test]
    fn test_lower_data_receipt_cost() {
        let store = RuntimeConfigStore::new(None);
        let base_cfg = store.get_config(LowerStorageCost.protocol_version());
        let new_cfg = store.get_config(LowerDataReceiptAndEcrecoverBaseCost.protocol_version());
        assert!(
            base_cfg.transaction_costs.data_receipt_creation_config.base_cost.send_sir
                > new_cfg.transaction_costs.data_receipt_creation_config.base_cost.send_sir
        );
        assert!(
            base_cfg.transaction_costs.data_receipt_creation_config.cost_per_byte.send_sir
                > new_cfg.transaction_costs.data_receipt_creation_config.cost_per_byte.send_sir
        );
    }

    // Check that for protocol version with lowered data receipt cost, runtime config passed to
    // config store is overridden.
    #[test]
    fn test_override_runtime_config() {
        let store = RuntimeConfigStore::new(Some(&RuntimeConfig::free()));
        let config = store.get_config(0);
        assert_eq!(config.as_ref(), &RuntimeConfig::free());

        let config = store.get_config(LowerStorageCost.protocol_version());
        assert_eq!(config.transaction_costs.action_creation_config.transfer_cost.send_sir, 0);
        assert_eq!(config.account_creation_config.min_allowed_top_level_account_length, 0);
        assert_ne!(
            config.as_ref(),
            &serde_json::from_str::<RuntimeConfig>(OLD_CONFIGS[1].1).unwrap()
        );

        let config = store.get_config(LowerDataReceiptAndEcrecoverBaseCost.protocol_version());
        assert_eq!(config.account_creation_config.min_allowed_top_level_account_length, 32);
        assert_eq!(
            config.as_ref(),
            &serde_json::from_str::<RuntimeConfig>(OLD_CONFIGS[2].1).unwrap()
        );
    }

    #[test]
    fn test_lower_ecrecover_base_cost() {
        let store = RuntimeConfigStore::new(None);
        let base_cfg = store.get_config(LowerStorageCost.protocol_version());
        let new_cfg = store.get_config(LowerDataReceiptAndEcrecoverBaseCost.protocol_version());
        assert!(
            base_cfg.wasm_config.ext_costs.ecrecover_base
                > new_cfg.wasm_config.ext_costs.ecrecover_base
        );
    }

    #[test]
    fn test_lower_max_length_storage_key() {
        let store = RuntimeConfigStore::new(None);
        let base_cfg = store.get_config(LowerStorageKeyLimit.protocol_version() - 1);
        let new_cfg = store.get_config(LowerStorageKeyLimit.protocol_version());
        assert!(
            base_cfg.wasm_config.limit_config.max_length_storage_key
                > new_cfg.wasm_config.limit_config.max_length_storage_key
        );
    }

    #[track_caller]
    fn check_store(old_store: RuntimeConfigStore, new_store: RuntimeConfigStore) {
        for version in old_store.store.keys() {
            assert_eq!(
                **old_store.get_config(*version),
                **new_store.get_config(*version),
                "mismatch in version {version}"
            );
        }
    }

    #[test]
    fn test_old_and_new_runtime_config_format_match() {
        let old_configs = BTreeMap::from_iter(OLD_CONFIGS.iter().cloned().map(
            |(protocol_version, config_bytes)| {
                (protocol_version, Arc::new(serde_json::from_str(config_bytes).unwrap()))
            },
        ));
        let old_store = RuntimeConfigStore { store: old_configs };
        let new_store = RuntimeConfigStore::new(None);

        check_store(old_store, new_store);

        // Testnet initial config for old version was different, thus needs separate testing
        let old_genesis_runtime_config =
            serde_json::from_str(include_config!("legacy_configs/29_testnet.json")).unwrap();
        let old_testnet_store = RuntimeConfigStore::new(Some(&old_genesis_runtime_config));

        let new_genesis_runtime_config =
            RuntimeConfig::from_parameters(&ParameterTable::from_txt(INITIAL_TESTNET_CONFIG));
        let new_testnet_store = RuntimeConfigStore::new(Some(&new_genesis_runtime_config));
        check_store(old_testnet_store, new_testnet_store);
    }
}
