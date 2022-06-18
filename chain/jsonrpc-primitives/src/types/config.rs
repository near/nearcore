use chrono::{DateTime, Utc};
use num_rational::Rational;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use near_account_id::AccountId;

use crate::serde_with::{u128_dec_format, u128_dec_format_compatible};

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcProtocolConfigRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcProtocolConfigResponse {
    #[serde(flatten)]
    pub config_view: ProtocolConfigView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcProtocolConfigError {
    #[error("Block has never been observed: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcProtocolConfigError> for crate::errors::RpcError {
    fn from(error: RpcProtocolConfigError) -> Self {
        let error_data = match &error {
            RpcProtocolConfigError::UnknownBlock { error_message } => {
                Some(Value::String(format!("Block Not Found: {}", error_message)))
            }
            RpcProtocolConfigError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcProtocolConfigError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProtocolConfigView {
    /// Current Protocol Version
    pub protocol_version: super::ProtocolVersion,
    /// Official time of blockchain start.
    pub genesis_time: DateTime<Utc>,
    /// ID of the blockchain. This must be unique for every blockchain.
    /// If your testnet blockchains do not have unique chain IDs, you will have a bad time.
    pub chain_id: String,
    /// Height of genesis block.
    pub genesis_height: super::BlockHeight,
    /// Number of block producer seats at genesis.
    pub num_block_producer_seats: super::NumSeats,
    /// Defines number of shards and number of block producer seats per each shard at genesis.
    pub num_block_producer_seats_per_shard: Vec<super::NumSeats>,
    /// Expected number of hidden validators per shard.
    pub avg_hidden_validator_seats_per_shard: Vec<super::NumSeats>,
    /// Enable dynamic re-sharding.
    pub dynamic_resharding: bool,
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational,
    /// Epoch length counted in block heights.
    pub epoch_length: super::BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: super::Gas,
    /// Minimum gas price. It is also the initial gas price.
    #[serde(with = "u128_dec_format_compatible")]
    pub min_gas_price: super::Balance,
    /// Maximum gas price.
    #[serde(with = "u128_dec_format")]
    pub max_gas_price: super::Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: Rational,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig, // todo!
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: super::NumBlocks,
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational,
    /// Maximum inflation on the total supply every epoch.
    pub max_inflation_rate: Rational,
    /// Expected number of blocks per year
    pub num_blocks_per_year: super::NumBlocks,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "u128_dec_format")]
    pub fishermen_threshold: super::Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
}

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Amount of yN per byte required to have on the account.  See
    /// <https://nomicon.io/Economics/README.html#state-stake> for details.
    #[serde(with = "u128_dec_format")]
    pub storage_amount_per_byte: super::Balance,
    /// Costs of different actions that need to be performed when sending and processing transaction
    /// and receipts.
    pub transaction_costs: RuntimeFeesConfig,
    /// Config of wasm operations.
    pub wasm_config: VMConfig,
    /// Config that defines rules for account creation.
    pub account_creation_config: AccountCreationConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct RuntimeFeesConfig {
    /// Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
    /// of actions.
    /// - `send` cost is burned when a receipt is created using `promise_create` or
    ///     `promise_batch_create`
    /// - `exec` cost is burned when the receipt is being executed.
    pub action_receipt_creation_config: super::Fee,
    /// Describes the cost of creating a data receipt, `DataReceipt`.
    pub data_receipt_creation_config: DataReceiptCreationConfig,
    /// Describes the cost of creating a certain action, `Action`. Includes all variants.
    pub action_creation_config: ActionCreationConfig,
    /// Describes fees for storage.
    pub storage_usage_config: StorageUsageConfig,

    /// Fraction of the burnt gas to reward to the contract account for execution.
    pub burnt_gas_reward: Rational,

    /// Pessimistic gas price inflation ratio.
    pub pessimistic_gas_price_inflation_ratio: Rational,
}

/// Describes the cost of creating a data receipt, `DataReceipt`.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct DataReceiptCreationConfig {
    /// Base cost of creating a data receipt.
    /// Both `send` and `exec` costs are burned when a new receipt has input dependencies. The gas
    /// is charged for each input dependency. The dependencies are specified when a receipt is
    /// created using `promise_then` and `promise_batch_then`.
    /// NOTE: Any receipt with output dependencies will produce data receipts. Even if it fails.
    /// Even if the last action is not a function call (in case of success it will return empty
    /// value).
    pub base_cost: super::Fee,
    /// Additional cost per byte sent.
    /// Both `send` and `exec` costs are burned when a function call finishes execution and returns
    /// `N` bytes of data to every output dependency. For each output dependency the cost is
    /// `(send(sir) + exec()) * N`.
    pub cost_per_byte: super::Fee,
}

/// Describes the cost of creating a specific action, `Action`. Includes all variants.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ActionCreationConfig {
    /// Base cost of creating an account.
    pub create_account_cost: super::Fee,

    /// Base cost of deploying a contract.
    pub deploy_contract_cost: super::Fee,
    /// Cost per byte of deploying a contract.
    pub deploy_contract_cost_per_byte: super::Fee,

    /// Base cost of calling a function.
    pub function_call_cost: super::Fee,
    /// Cost per byte of method name and arguments of calling a function.
    pub function_call_cost_per_byte: super::Fee,

    /// Base cost of making a transfer.
    pub transfer_cost: super::Fee,

    /// Base cost of staking.
    pub stake_cost: super::Fee,

    /// Base cost of adding a key.
    pub add_key_cost: AccessKeyCreationConfig,

    /// Base cost of deleting a key.
    pub delete_key_cost: super::Fee,

    /// Base cost of deleting an account.
    pub delete_account_cost: super::Fee,
}

/// Describes the cost of creating an access key.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct AccessKeyCreationConfig {
    /// Base cost of creating a full access access-key.
    pub full_access_cost: super::Fee,
    /// Base cost of creating an access-key restricted to specific functions.
    pub function_call_cost: super::Fee,
    /// Cost per byte of method_names of creating a restricted access-key.
    pub function_call_cost_per_byte: super::Fee,
}

/// Describes cost of storage per block
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfig {
    /// Number of bytes for an account record, including rounding up for account id.
    pub num_bytes_account: u64,
    /// Additional number of bytes for a k/v record
    pub num_extra_bytes_record: u64,
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct VMConfig {
    /// Costs for runtime externals
    pub ext_costs: ExtCostsConfig,

    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,
    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// Describes limits for VM and Runtime.
    pub limit_config: VMLimitConfig,
}

/// The structure describes configuration for creation of new accounts.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AccountCreationConfig {
    /// The minimum length of the top-level account ID that is allowed to be created by any account.
    pub min_allowed_top_level_account_length: u8,
    /// The account ID of the account registrar. This account ID allowed to create top-level
    /// accounts of any valid length.
    pub registrar_account_id: AccountId,
}

fn default_read_cached_trie_node() -> super::Gas {
    SAFETY_MULTIPLIER * 760_000_000
}

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfig {
    /// Base cost for calling a host function.
    pub base: super::Gas,

    /// Base cost of loading a pre-compiled contract
    pub contract_loading_base: super::Gas,
    /// Cost per byte of loading a pre-compiled contract
    pub contract_loading_bytes: super::Gas,

    /// Base cost for guest memory read
    pub read_memory_base: super::Gas,
    /// Cost for guest memory read
    pub read_memory_byte: super::Gas,

    /// Base cost for guest memory write
    pub write_memory_base: super::Gas,
    /// Cost for guest memory write per byte
    pub write_memory_byte: super::Gas,

    /// Base cost for reading from register
    pub read_register_base: super::Gas,
    /// Cost for reading byte from register
    pub read_register_byte: super::Gas,

    /// Base cost for writing into register
    pub write_register_base: super::Gas,
    /// Cost for writing byte into register
    pub write_register_byte: super::Gas,

    /// Base cost of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_base: super::Gas,
    /// Cost per byte of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_byte: super::Gas,

    /// Base cost of decoding utf16. It's used for `log_utf16`.
    pub utf16_decoding_base: super::Gas,
    /// Cost per byte of decoding utf16. It's used for `log_utf16`.
    pub utf16_decoding_byte: super::Gas,

    /// Cost of getting sha256 base
    pub sha256_base: super::Gas,
    /// Cost of getting sha256 per byte
    pub sha256_byte: super::Gas,

    /// Cost of getting sha256 base
    pub keccak256_base: super::Gas,
    /// Cost of getting sha256 per byte
    pub keccak256_byte: super::Gas,

    /// Cost of getting sha256 base
    pub keccak512_base: super::Gas,
    /// Cost of getting sha256 per byte
    pub keccak512_byte: super::Gas,

    /// Cost of getting ripemd160 base
    pub ripemd160_base: super::Gas,
    /// Cost of getting ripemd160 per message block
    pub ripemd160_block: super::Gas,

    /// Cost of calling ecrecover
    pub ecrecover_base: super::Gas,

    /// Cost for calling logging.
    pub log_base: super::Gas,
    /// Cost for logging per byte
    pub log_byte: super::Gas,

    // ###############
    // # Storage API #
    // ###############
    /// Storage trie write key base cost
    pub storage_write_base: super::Gas,
    /// Storage trie write key per byte cost
    pub storage_write_key_byte: super::Gas,
    /// Storage trie write value per byte cost
    pub storage_write_value_byte: super::Gas,
    /// Storage trie write cost per byte of evicted value.
    pub storage_write_evicted_byte: super::Gas,

    /// Storage trie read key base cost
    pub storage_read_base: super::Gas,
    /// Storage trie read key per byte cost
    pub storage_read_key_byte: super::Gas,
    /// Storage trie read value cost per byte cost
    pub storage_read_value_byte: super::Gas,

    /// Remove key from trie base cost
    pub storage_remove_base: super::Gas,
    /// Remove key from trie per byte cost
    pub storage_remove_key_byte: super::Gas,
    /// Remove key from trie ret value byte cost
    pub storage_remove_ret_value_byte: super::Gas,

    /// Storage trie check for key existence cost base
    pub storage_has_key_base: super::Gas,
    /// Storage trie check for key existence per key byte
    pub storage_has_key_byte: super::Gas,

    /// Create trie prefix iterator cost base
    pub storage_iter_create_prefix_base: super::Gas,
    /// Create trie prefix iterator cost per byte.
    pub storage_iter_create_prefix_byte: super::Gas,

    /// Create trie range iterator cost base
    pub storage_iter_create_range_base: super::Gas,
    /// Create trie range iterator cost per byte of from key.
    pub storage_iter_create_from_byte: super::Gas,
    /// Create trie range iterator cost per byte of to key.
    pub storage_iter_create_to_byte: super::Gas,

    /// Trie iterator per key base cost
    pub storage_iter_next_base: super::Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_key_byte: super::Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_value_byte: super::Gas,

    /// Cost per reading trie node from DB
    pub touching_trie_node: super::Gas,
    /// Cost for reading trie node from memory
    #[serde(default = "default_read_cached_trie_node")]
    pub read_cached_trie_node: super::Gas,

    // ###############
    // # Promise API #
    // ###############
    /// Cost for calling `promise_and`
    pub promise_and_base: super::Gas,
    /// Cost for calling `promise_and` for each promise
    pub promise_and_per_promise: super::Gas,
    /// Cost for calling `promise_return`
    pub promise_return: super::Gas,

    // ###############
    // # Validator API #
    // ###############
    /// Cost of calling `validator_stake`.
    pub validator_stake_base: super::Gas,
    /// Cost of calling `validator_total_stake`.
    pub validator_total_stake_base: super::Gas,

    // Workaround to keep JSON serialization backwards-compatible
    // <https://github.com/near/nearcore/pull/6587#discussion_r876113324>.
    //
    // Remove once #5516 is fixed.
    #[serde(default, rename = "contract_compile_base")]
    pub _unused1: super::Gas,
    #[serde(default, rename = "contract_compile_bytes")]
    pub _unused2: super::Gas,

    // #############
    // # Alt BN128 #
    // #############
    /// Base cost for multiexp
    pub alt_bn128_g1_multiexp_base: super::Gas,
    /// Per element cost for multiexp
    pub alt_bn128_g1_multiexp_element: super::Gas,
    /// Base cost for sum
    pub alt_bn128_g1_sum_base: super::Gas,
    /// Per element cost for sum
    pub alt_bn128_g1_sum_element: super::Gas,
    /// Base cost for pairing check
    pub alt_bn128_pairing_check_base: super::Gas,
    /// Per element cost for pairing check
    pub alt_bn128_pairing_check_element: super::Gas,
}

/// This struct here exists to enforce that the value in the config is either
/// `0` or `1`. We could have used a `bool` instead, but there's a chance that
/// our current impl isn't perfect either and would need further tweaks in the
/// future.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum StackLimiterVersion {
    /// Old, buggy version, don't use it unless specifically to support old protocol version.
    V0,
    /// What we use in today's protocol.
    V1,
}

impl StackLimiterVersion {
    fn v0() -> StackLimiterVersion {
        StackLimiterVersion::V0
    }
    fn repr(self) -> u32 {
        match self {
            StackLimiterVersion::V0 => 0,
            StackLimiterVersion::V1 => 1,
        }
    }
    fn from_repr(repr: u32) -> Option<StackLimiterVersion> {
        let res = match repr {
            0 => StackLimiterVersion::V0,
            1 => StackLimiterVersion::V1,
            _ => return None,
        };
        Some(res)
    }
}

impl Serialize for StackLimiterVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.repr().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StackLimiterVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u32::deserialize(deserializer).and_then(|repr| {
            StackLimiterVersion::from_repr(repr)
                .ok_or_else(|| serde::de::Error::custom("invalid stack_limiter_version"))
        })
    }
}

/// Describes limits for VM and Runtime.
/// TODO #4139: consider switching to strongly-typed wrappers instead of raw quantities
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct VMLimitConfig {
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: super::Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See <https://wiki.parity.io/WebAssembly-StackHeight> to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,
    /// Whether a legacy version of stack limiting should be used, see
    /// [`StackLimiterVersion`].
    #[serde(default = "StackLimiterVersion::v0")]
    pub stack_limiter_version: StackLimiterVersion,

    /// The initial number of memory pages.
    /// NOTE: It's not a limiter itself, but it's a value we use for initial_memory_pages.
    pub initial_memory_pages: u32,
    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Limit of memory used by registers.
    pub registers_memory_limit: u64,
    /// Maximum number of bytes that can be stored in a single register.
    pub max_register_size: u64,
    /// Maximum number of registers that can be used simultaneously.
    pub max_number_registers: u64,

    /// Maximum number of log entries.
    pub max_number_logs: u64,
    /// Maximum total length in bytes of all log messages.
    pub max_total_log_length: u64,

    /// Max total prepaid gas for all function call actions per receipt.
    pub max_total_prepaid_gas: super::Gas,

    /// Max number of actions per receipt.
    pub max_actions_per_receipt: u64,
    /// Max total length of all method names (including terminating character) for a function call
    /// permission access key.
    pub max_number_bytes_method_names: u64,
    /// Max length of any method name (without terminating character).
    pub max_length_method_name: u64,
    /// Max length of arguments in a function call action.
    pub max_arguments_length: u64,
    /// Max length of returned data
    pub max_length_returned_data: u64,
    /// Max contract size
    pub max_contract_size: u64,
    /// Max transaction size
    pub max_transaction_size: u64,
    /// Max storage key size
    pub max_length_storage_key: u64,
    /// Max storage value size
    pub max_length_storage_value: u64,
    /// Max number of promises that a function call can create
    pub max_promises_per_function_call_action: u64,
    /// Max number of input data dependencies
    pub max_number_input_data_dependencies: u64,
    /// If present, stores max number of functions in one contract
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_functions_number_per_contract: Option<u64>,
    /// If present, stores the secondary stack limit as implemented by wasmer2.
    ///
    /// This limit should never be hit normally.
    #[serde(default = "wasmer2_stack_limit_default")]
    pub wasmer2_stack_limit: i32,
    /// If present, stores max number of locals declared globally in one contract
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_locals_per_contract: Option<u64>,
}

fn wasmer2_stack_limit_default() -> i32 {
    100 * 1024
}
