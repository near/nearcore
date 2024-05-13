//! Settings of the parameters of the runtime.
use crate::config_store::INITIAL_TESTNET_CONFIG;
use crate::cost::RuntimeFeesConfig;
use crate::parameter_table::ParameterTable;
use near_account_id::AccountId;
use near_primitives_core::types::Balance;
use near_primitives_core::version::PROTOCOL_VERSION;

use super::parameter_table::InvalidConfigError;

// Lowered promise yield timeout length used in integration tests.
// The resharding tests for yield timeouts take too long to run otherwise.
pub const TEST_CONFIG_YIELD_TIMEOUT_LENGTH: u64 = 10;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Action gas costs, storage fees, and economic constants around them.
    ///
    /// This contains parameters that are required by the WASM runtime and the
    /// transaction runtime.
    pub fees: RuntimeFeesConfig,
    /// Config of wasm operations, also includes wasm gas costs.
    ///
    /// This contains all the configuration parameters that are only required by
    /// the WASM runtime.
    pub wasm_config: crate::vm::Config,
    /// Config that defines rules for account creation.
    pub account_creation_config: AccountCreationConfig,
    /// The maximum size of the storage proof in state witness after which we defer execution of any new receipts.
    pub storage_proof_size_soft_limit: usize,
}

impl RuntimeConfig {
    pub(crate) fn new(params: &ParameterTable) -> Result<Self, InvalidConfigError> {
        RuntimeConfig::try_from(params)
    }

    pub fn initial_testnet_config() -> RuntimeConfig {
        INITIAL_TESTNET_CONFIG
            .parse()
            .and_then(|params| RuntimeConfig::new(&params))
            .expect("Failed parsing initial testnet config")
    }

    pub fn test() -> Self {
        let config_store = super::config_store::RuntimeConfigStore::new(None);
        let mut wasm_config =
            crate::vm::Config::clone(&config_store.get_config(PROTOCOL_VERSION).wasm_config);
        // Lower the yield timeout length so that we can observe timeouts in integration tests.
        wasm_config.limit_config.yield_timeout_length_in_blocks = TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

        RuntimeConfig {
            fees: RuntimeFeesConfig::test(),
            wasm_config,
            account_creation_config: AccountCreationConfig::default(),
            storage_proof_size_soft_limit: usize::MAX,
        }
    }

    pub fn free() -> Self {
        let config_store = super::config_store::RuntimeConfigStore::new(None);
        let mut wasm_config =
            crate::vm::Config::clone(&config_store.get_config(PROTOCOL_VERSION).wasm_config);
        wasm_config.make_free();
        Self {
            fees: RuntimeFeesConfig::free(),
            wasm_config,
            account_creation_config: AccountCreationConfig::default(),
            storage_proof_size_soft_limit: usize::MAX,
        }
    }

    pub fn storage_amount_per_byte(&self) -> Balance {
        self.fees.storage_usage_config.storage_amount_per_byte
    }
}

/// The structure describes configuration for creation of new accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountCreationConfig {
    /// The minimum length of the top-level account ID that is allowed to be created by any account.
    pub min_allowed_top_level_account_length: u8,
    /// The account ID of the account registrar. This account ID allowed to create top-level
    /// accounts of any valid length.
    pub registrar_account_id: AccountId,
}

impl Default for AccountCreationConfig {
    fn default() -> Self {
        Self {
            min_allowed_top_level_account_length: 0,
            registrar_account_id: "registrar".parse().unwrap(),
        }
    }
}
