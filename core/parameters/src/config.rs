//! Settings of the parameters of the runtime.
use crate::config_store::INITIAL_TESTNET_CONFIG;
use crate::cost::RuntimeFeesConfig;
use crate::parameter_table::ParameterTable;
use near_account_id::AccountId;
use near_primitives_core::types::{Balance, Gas};
use near_primitives_core::version::PROTOCOL_VERSION;

use super::parameter_table::InvalidConfigError;

// Lowered promise yield timeout length used in integration tests.
// The resharding tests for yield timeouts take too long to run otherwise.
pub const TEST_CONFIG_YIELD_TIMEOUT_LENGTH: u64 = 10;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Clone, PartialEq)]
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
    /// The configuration for congestion control.
    pub congestion_control_config: CongestionControlConfig,
    /// Configuration specific to ChunkStateWitness.
    pub witness_config: WitnessConfig,
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
        let runtime_config = config_store.get_config(PROTOCOL_VERSION);

        let mut wasm_config = crate::vm::Config::clone(&runtime_config.wasm_config);
        // Lower the yield timeout length so that we can observe timeouts in integration tests.
        wasm_config.limit_config.yield_timeout_length_in_blocks = TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

        RuntimeConfig {
            fees: RuntimeFeesConfig::test(),
            wasm_config,
            account_creation_config: AccountCreationConfig::default(),
            congestion_control_config: runtime_config.congestion_control_config,
            witness_config: runtime_config.witness_config,
        }
    }

    pub fn free() -> Self {
        let config_store = super::config_store::RuntimeConfigStore::new(None);
        let runtime_config = config_store.get_config(PROTOCOL_VERSION);

        let mut wasm_config = crate::vm::Config::clone(&runtime_config.wasm_config);
        wasm_config.make_free();

        Self {
            fees: RuntimeFeesConfig::free(),
            wasm_config,
            account_creation_config: AccountCreationConfig::default(),
            congestion_control_config: runtime_config.congestion_control_config,
            witness_config: runtime_config.witness_config,
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

/// The configuration for congestion control.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct CongestionControlConfig {
    /// How much gas in delayed receipts of a shard is 100% incoming congestion.
    ///
    /// Based on incoming congestion levels, a shard reduces the gas it spends on
    /// accepting new transactions instead of working on incoming receipts. Plus,
    /// incoming congestion contributes to overall congestion, which reduces how
    /// much other shards are allowed to forward to this shard.
    pub max_congestion_incoming_gas: Gas,

    /// How much gas in outgoing buffered receipts of a shard is 100% congested.
    ///
    /// Outgoing congestion contributes to overall congestion, which reduces how
    /// much other shards are allowed to forward to this shard.
    pub max_congestion_outgoing_gas: Gas,

    /// How much memory space of all delayed and buffered receipts in a shard is
    /// considered 100% congested.
    ///
    /// Memory congestion contributes to overall congestion, which reduces how much
    /// other shards are allowed to forward to this shard.
    ///
    /// This threshold limits memory requirements of validators to a degree but it
    /// is not a hard guarantee.
    pub max_congestion_memory_consumption: u64,

    /// How many missed chunks in a row in a shard is considered 100% congested.
    pub max_congestion_missed_chunks: u64,

    /// The maximum amount of gas attached to receipts a shard can forward to
    /// another shard per chunk.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [`MIN_OUTGOING_GAS`] and [`MAX_OUTGOING_GAS`], or 0 if the receiver is
    /// fully congested.
    pub max_outgoing_gas: Gas,

    /// The minimum gas each shard can send to a shard that is not fully congested.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [`MIN_OUTGOING_GAS`] and [`MAX_OUTGOING_GAS`], or 0 if the receiver is
    /// fully congested.
    pub min_outgoing_gas: Gas,

    /// How much gas the chosen allowed shard can send to a 100% congested shard.
    ///
    /// This amount is the absolute minimum of new workload a congested shard has to
    /// accept every round. It ensures deadlocks are provably impossible. But in
    /// ideal conditions, the gradual reduction of new workload entering the system
    /// combined with gradually limited forwarding to congested shards should
    /// prevent shards from becoming 100% congested in the first place.
    pub allowed_shard_outgoing_gas: Gas,

    /// The maximum amount of gas in a chunk spent on converting new transactions to
    /// receipts.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [`MIN_TX_GAS`] and [`MAX_TX_GAS`], based on the incoming congestion of the
    /// local shard. Additionally, transactions can be rejected if the receiving
    /// remote shard is congested more than [`REJECT_TX_CONGESTION_THRESHOLD`] based
    /// on their general congestion level.
    pub max_tx_gas: Gas,

    /// The minimum amount of gas in a chunk spent on converting new transactions
    /// to receipts, as long as the receiving shard is not congested.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [`MIN_TX_GAS`] and [`MAX_TX_GAS`], based on the incoming congestion of the
    /// local shard. Additionally, transactions can be rejected if the receiving
    /// remote shard is congested more than [`REJECT_TX_CONGESTION_THRESHOLD`] based
    /// on their general congestion level.
    pub min_tx_gas: Gas,

    /// How much congestion a shard can tolerate before it stops all shards from
    /// accepting new transactions with the receiver set to the congested shard.
    pub reject_tx_congestion_threshold: f64,
}

// The Eq cannot be automatically derived for this class because it contains a
// f64 field. The f64 type does not implement the Eq trait because it's possible
// that NaN != NaN. In the CongestionControlConfig the field should never be NaN
// so it is okay for us to add the Eq trait to it manually.
impl Eq for CongestionControlConfig {}

impl CongestionControlConfig {
    /// Creates a config where congestion control is disabled. This config can
    /// be used for tests. It can be useful e.g. in tests with missing chunks
    /// where we still want to process all transactions.
    pub fn test_disabled() -> Self {
        let max_value = u64::MAX;
        Self {
            max_congestion_incoming_gas: max_value,
            max_congestion_outgoing_gas: max_value,
            max_congestion_memory_consumption: max_value,
            max_congestion_missed_chunks: max_value,
            max_outgoing_gas: max_value,
            min_outgoing_gas: max_value,
            allowed_shard_outgoing_gas: max_value,
            max_tx_gas: max_value,
            min_tx_gas: max_value,
            reject_tx_congestion_threshold: 2.0,
        }
    }
}

/// Configuration specific to ChunkStateWitness.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct WitnessConfig {
    /// Size limit for storage proof generated while executing receipts in a chunk.
    /// After this limit is reached we defer execution of any new receipts.
    pub main_storage_proof_size_soft_limit: usize,
    /// Maximum size of transactions contained inside ChunkStateWitness.
    /// A witness contains transactions from both the previous chunk and the current one.
    /// This parameter limits the sum of sizes of transactions from both of those chunks.
    pub combined_transactions_size_limit: usize,
    /// Soft size limit of storage proof used to validate new transactions in ChunkStateWitness.
    pub new_transactions_validation_state_size_soft_limit: usize,
    /// The standard size limit for outgoing receipts aimed at a single shard.
    /// This limit is pretty small to keep the size of source_receipt_proofs under control.
    /// It limits the total sum of outgoing receipts, not individual receipts.
    pub outgoing_receipts_usual_size_limit: u64,
    /// Large size limit for outgoing receipts to a shard, used when it's safe
    /// to send a lot of receipts without making the state witness too large.
    /// It limits the total sum of outgoing receipts, not individual receipts.
    pub outgoing_receipts_big_size_limit: u64,
}
