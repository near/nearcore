use crate::config::{
    BandwidthSchedulerConfig, CongestionControlConfig, RuntimeConfig, WitnessConfig,
};
use crate::parameter_table::{ParameterTable, ParameterTableDiff};
use crate::vm;
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::version::{PROTOCOL_VERSION, ProtocolFeature};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

macro_rules! include_config {
    ($file:expr) => {
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/res/runtime_configs/", $file))
    };
}

/// The base config file with all initial parameter values defined.
/// Later version are calculated by applying diffs to this base.
static BASE_CONFIG: &str = include_config!("parameters.yaml");

/// Stores pairs of protocol versions for which runtime config was updated and
/// the file containing the diffs in bytes.
static CONFIG_DIFFS: &[(ProtocolVersion, &str)] = &[
    (35, include_config!("35.yaml")),
    (42, include_config!("42.yaml")),
    (46, include_config!("46.yaml")),
    (48, include_config!("48.yaml")),
    (49, include_config!("49.yaml")),
    (50, include_config!("50.yaml")),
    // max_gas_burnt increased to 300 TGas
    (52, include_config!("52.yaml")),
    // Increased deployment costs, increased wasmer2 stack_limit, added limiting of contract locals,
    // set read_cached_trie_node cost, decrease storage key limit
    (53, include_config!("53.yaml")),
    (55, include_config!("55.yaml")),
    (57, include_config!("57.yaml")),
    // Introduce Zero Balance Account and increase account creation cost to 7.7Tgas
    (59, include_config!("59.yaml")),
    (61, include_config!("61.yaml")),
    (62, include_config!("62.yaml")),
    (63, include_config!("63.yaml")),
    (64, include_config!("64.yaml")),
    (66, include_config!("66.yaml")),
    (67, include_config!("67.yaml")),
    // Congestion Control.
    (68, include_config!("68.yaml")),
    // Stateless Validation.
    (69, include_config!("69.yaml")),
    // Introduce ETH-implicit accounts.
    (70, include_config!("70.yaml")),
    // Increase main_storage_proof_size_soft_limit and introduces StateStoredReceipt
    (72, include_config!("72.yaml")),
    // Fix wasm_yield_resume_byte and relax congestion control.
    (73, include_config!("73.yaml")),
    (74, include_config!("74.yaml")),
    (77, include_config!("77.yaml")),
    (78, include_config!("78.yaml")),
    (129, include_config!("129.yaml")),
];

/// Testnet parameters for versions <= 29, which (incorrectly) differed from mainnet parameters
pub static INITIAL_TESTNET_CONFIG: &str = include_config!("parameters_testnet.yaml");

/// Stores runtime config for each protocol version where it was updated.
#[derive(Clone, Debug)]
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
    /// calimero_zero_storage flag sets all storages fees to zero by setting
    /// storage_amount_per_byte to zero, to keep calimero private shards compatible with future
    /// protocol upgrades this is done for all protocol versions
    /// TODO #4775: introduce new protocol version to have the same runtime config for all chains
    pub fn new(genesis_runtime_config: Option<&RuntimeConfig>) -> Self {
        let mut params: ParameterTable =
            BASE_CONFIG.parse().expect("Failed parsing base parameter file.");

        let mut store = BTreeMap::new();
        #[cfg(not(feature = "calimero_zero_storage"))]
        {
            let initial_config = RuntimeConfig::new(&params).unwrap_or_else(|err| {
                panic!(
                    "Failed generating `RuntimeConfig` from parameters for base parameter file. \
                     Error: {err:?}"
                )
            });
            store.insert(0, Arc::new(initial_config));
        }
        #[cfg(feature = "calimero_zero_storage")]
        {
            let mut initial_config = RuntimeConfig::new(&params).unwrap_or_else(|err| {
                panic!(
                    "Failed generating `RuntimeConfig` from parameters for base parameter file. \
                     Error: {err:?}"
                )
            });
            let fees = Arc::make_mut(&mut initial_config.fees);
            fees.storage_usage_config.storage_amount_per_byte = 0;
            store.insert(0, Arc::new(initial_config));
        }

        for (protocol_version, diff_bytes) in CONFIG_DIFFS {
            let diff: ParameterTableDiff = diff_bytes.parse().unwrap_or_else(|err| {
                panic!(
                    "Failed parsing runtime parameters diff for version {protocol_version}. \
                     Error: {err:?}"
                )
            });
            params.apply_diff(diff).unwrap_or_else(|err| {
                panic!(
                    "Failed applying diff to `RuntimeConfig` for version {protocol_version}. \
                     Error: {err}"
                )
            });
            #[cfg(not(feature = "calimero_zero_storage"))]
            store.insert(
                *protocol_version,
                Arc::new(RuntimeConfig::new(&params).unwrap_or_else(|err| {
                    panic!(
                        "Failed generating `RuntimeConfig` from parameters for \
                         version {protocol_version}. Error: {err:?}"
                    )
                })),
            );
            #[cfg(feature = "calimero_zero_storage")]
            {
                let mut runtime_config = RuntimeConfig::new(&params).unwrap_or_else(|err| {
                    panic!(
                        "Failed generating `RuntimeConfig` from parameters for \
                         version {protocol_version}. Error: {err:?}"
                    )
                });
                let fees = Arc::make_mut(&mut runtime_config.fees);
                fees.storage_usage_config.storage_amount_per_byte = 0;
                store.insert(*protocol_version, Arc::new(runtime_config));
            }
        }

        if let Some(runtime_config) = genesis_runtime_config {
            let mut fees = crate::RuntimeFeesConfig::clone(&runtime_config.fees);
            fees.storage_usage_config.storage_amount_per_byte = 10u128.pow(19);
            store.insert(
                42,
                Arc::new(RuntimeConfig {
                    fees: Arc::new(fees),
                    wasm_config: Arc::clone(&runtime_config.wasm_config),
                    account_creation_config: runtime_config.account_creation_config.clone(),
                    congestion_control_config: runtime_config.congestion_control_config,
                    witness_config: runtime_config.witness_config,
                    bandwidth_scheduler_config: runtime_config.bandwidth_scheduler_config,
                    use_state_stored_receipt: runtime_config.use_state_stored_receipt,
                }),
            );
            store.insert(0, Arc::new(runtime_config.clone()));
        }

        Self { store }
    }

    /// Create store of runtime configs for the given chain id.
    ///
    /// For mainnet and other chains except testnet we don't need to override runtime config for
    /// first protocol versions.
    /// For testnet, runtime config for genesis block was (incorrectly) different, that's why we
    /// need to override it specifically to preserve compatibility.
    /// In benchmarknet, we are measuring the peak throughput that the NEAR network can handle while still being stable.
    /// This requires increasing the limits below that are set too conservatively.
    pub fn for_chain_id(chain_id: &str) -> Self {
        match chain_id {
            near_primitives_core::chains::TESTNET => {
                let genesis_runtime_config = RuntimeConfig::initial_testnet_config();
                Self::new(Some(&genesis_runtime_config))
            }
            near_primitives_core::chains::BENCHMARKNET => {
                let mut config_store = Self::new(None);
                let mut config = RuntimeConfig::clone(config_store.get_config(PROTOCOL_VERSION));
                config.congestion_control_config = CongestionControlConfig::test_disabled();
                config.bandwidth_scheduler_config = BandwidthSchedulerConfig::test_disabled();
                config.witness_config = WitnessConfig::test_disabled();
                let mut wasm_config = vm::Config::clone(&config.wasm_config);
                wasm_config.limit_config.per_receipt_storage_proof_size_limit = usize::max_value();
                config.wasm_config = Arc::new(wasm_config);
                config_store.store.insert(PROTOCOL_VERSION, Arc::new(config));
                config_store
            }
            near_primitives_core::chains::CONGESTION_CONTROL_TEST => {
                let mut config_store = Self::new(None);

                // TODO(limited_replayability): Move tests to use config from latest protocol version.
                // Get the original congestion control config. The nayduck tests are tuned to this config.
                #[allow(deprecated)]
                let source_protocol_version =
                    ProtocolFeature::_DeprecatedCongestionControl.protocol_version();
                let source_runtime_config = config_store.get_config(source_protocol_version);

                let mut config = RuntimeConfig::clone(config_store.get_config(PROTOCOL_VERSION));
                config.congestion_control_config = source_runtime_config.congestion_control_config;

                config_store.store.insert(PROTOCOL_VERSION, Arc::new(config));
                config_store
            }
            _ => Self::new(None),
        }
    }

    /// Constructs test store.
    pub fn with_one_config(runtime_config: RuntimeConfig) -> Self {
        Self { store: BTreeMap::from_iter([(0, Arc::new(runtime_config))].iter().cloned()) }
    }

    /// Constructs store with custom configs. This should only be used for testing.
    pub fn new_custom(store: BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>) -> Self {
        Self { store }
    }

    /// Constructs test store.
    pub fn test() -> Self {
        Self::with_one_config(RuntimeConfig::test())
    }

    /// Constructs test store.
    pub fn test_congestion_control_disabled() -> Self {
        let mut config = RuntimeConfig::test();
        config.congestion_control_config = CongestionControlConfig::test_disabled();

        Self::with_one_config(config)
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
    use crate::cost::ActionCosts;
    use std::collections::HashSet;

    const GENESIS_PROTOCOL_VERSION: ProtocolVersion = 29;

    #[test]
    fn all_configs_are_specified() {
        let file_versions =
            std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/res/runtime_configs/"))
                .expect("can open config directory");
        let mut files = file_versions
            .into_iter()
            .map(|de| {
                de.expect("dir entry should read successfully")
                    .path()
                    .file_name()
                    .expect("dir entry should have a filename")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<HashSet<_>>();

        for (ver, _) in super::CONFIG_DIFFS {
            assert!(files.remove(&format!("{ver}.yaml")), "{ver}.yaml file is missing?");
        }

        for file in files {
            let Some((name, "yaml")) = file.rsplit_once(".") else { continue };
            let Ok(version_num) = name.parse::<u32>() else { continue };
            panic!("CONFIG_DIFFS does not contain reference to the {version_num}.yaml file!");
        }
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
    fn test_parameter_merging() {
        let mut base_params: ParameterTable = BASE_CONFIG.parse().unwrap();
        let base_config = RuntimeConfig::new(&base_params).unwrap();

        let mock_diff_str = r#"
        max_length_storage_key: { old: 4_194_304, new: 42 }
        action_receipt_creation: {
          old: {
            send_sir: 108_059_500_000,
            send_not_sir: 108_059_500_000,
            execution: 108_059_500_000,
          },
          new: {
            send_sir: 100000000,
            send_not_sir: 108_059_500_000,
            execution: 108_059_500_000,
          },
        }
        "#;

        let mock_diff: ParameterTableDiff = mock_diff_str.parse().unwrap();

        base_params.apply_diff(mock_diff).unwrap();
        let modified_config = RuntimeConfig::new(&base_params).unwrap();

        assert_eq!(modified_config.wasm_config.limit_config.max_length_storage_key, 42);
        assert_eq!(modified_config.fees.fee(ActionCosts::new_action_receipt).send_sir, 100000000);

        assert_eq!(
            base_config.storage_amount_per_byte(),
            modified_config.storage_amount_per_byte()
        );
    }

    /// Use snapshot testing to check that the JSON representation of the configurations of each version is unchanged.
    /// If tests fail after an intended change, follow the steps below to update the config files:
    /// 1) Run the following to run tests with cargo insta so it generates all the file differences:
    ///    cargo insta test  -p near-parameters -- tests::test_json_unchanged
    /// 2) Run the following to examine the diffs at each file and see if the changes make sense:
    ///    cargo insta review
    ///    If the changes make sense, accept the changes per file (by responding to the prompts from the command).
    /// Alternatively, add --accept to the first command so that it automatically does step 2.
    #[test]
    #[cfg(not(feature = "nightly"))]
    #[cfg(not(feature = "calimero_zero_storage"))]
    fn test_json_unchanged() {
        use crate::view::RuntimeConfigView;
        use near_primitives_core::version::PROTOCOL_VERSION;

        let store = RuntimeConfigStore::new(None);
        let mut any_failure = false;

        for version in store.store.keys() {
            let snapshot_name = format!("{version}.json");
            let config_view = RuntimeConfigView::from(store.get_config(*version).as_ref().clone());
            any_failure |= std::panic::catch_unwind(|| {
                insta::assert_json_snapshot!(snapshot_name, config_view, { ".wasm_config.vm_kind" => "<REDACTED>"});
            })
            .is_err();
        }

        // Store the latest values of parameters in a human-readable snapshot.
        {
            let mut params: ParameterTable = BASE_CONFIG.parse().unwrap();
            for (_, diff_bytes) in
                CONFIG_DIFFS.iter().filter(|(version, _)| *version <= PROTOCOL_VERSION)
            {
                params.apply_diff(diff_bytes.parse().unwrap()).unwrap();
            }
            insta::with_settings!({
                snapshot_path => "../res/runtime_configs",
                prepend_module_to_snapshot => false,
                description => "THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY.",
                omit_expression => true,
            }, {
                any_failure |= std::panic::catch_unwind(|| {
                    insta::assert_snapshot!("parameters", params);
                }).is_err();
            });
        }

        // Testnet initial config for old version was different, thus needs separate testing
        let params = INITIAL_TESTNET_CONFIG.parse().unwrap();
        let new_genesis_runtime_config = RuntimeConfig::new(&params).unwrap();
        let testnet_store = RuntimeConfigStore::new(Some(&new_genesis_runtime_config));

        for version in testnet_store.store.keys() {
            let snapshot_name = format!("testnet_{version}.json");
            let config_view = RuntimeConfigView::from(store.get_config(*version).as_ref().clone());
            any_failure |= std::panic::catch_unwind(|| {
                insta::assert_json_snapshot!(snapshot_name, config_view, { ".wasm_config.vm_kind" => "<REDACTED>"});
            })
            .is_err();
        }
        if any_failure {
            panic!("some snapshot assertions failed");
        }
    }

    #[test]
    #[cfg(feature = "calimero_zero_storage")]
    fn test_calimero_storage_costs_zero() {
        let store = RuntimeConfigStore::new(None);
        for (_, config) in &store.store {
            assert_eq!(config.storage_amount_per_byte(), 0u128);
        }
    }

    #[test]
    fn test_benchmarknet_config() {
        let store = RuntimeConfigStore::for_chain_id(near_primitives_core::chains::BENCHMARKNET);
        let config = store.get_config(PROTOCOL_VERSION);
        assert_eq!(config.witness_config.main_storage_proof_size_soft_limit, usize::MAX);
    }
}
