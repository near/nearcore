use crate::config::{CongestionControlConfig, WitnessConfig};
use crate::{ActionCosts, ExtCosts, Fee, ParameterCost};
use near_account_id::AccountId;
use near_primitives_core::serialize::dec_format;
use near_primitives_core::types::{Balance, Gas};
use num_rational::Rational32;

/// View that preserves JSON format of the runtime config.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct RuntimeConfigView {
    /// Amount of yN per byte required to have on the account.  See
    /// <https://nomicon.io/Economics/Economic#state-stake> for details.
    #[serde(with = "dec_format")]
    pub storage_amount_per_byte: Balance,
    /// Costs of different actions that need to be performed when sending and
    /// processing transaction and receipts.
    pub transaction_costs: RuntimeFeesConfigView,
    /// Config of wasm operations.
    pub wasm_config: VMConfigView,
    /// Config that defines rules for account creation.
    pub account_creation_config: AccountCreationConfigView,
    /// The configuration for congestion control.
    pub congestion_control_config: CongestionControlConfigView,
    /// Configuration specific to ChunkStateWitness.
    pub witness_config: WitnessConfigView,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct RuntimeFeesConfigView {
    /// Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
    /// of actions.
    /// - `send` cost is burned when a receipt is created using `promise_create` or
    ///     `promise_batch_create`
    /// - `exec` cost is burned when the receipt is being executed.
    pub action_receipt_creation_config: Fee,
    /// Describes the cost of creating a data receipt, `DataReceipt`.
    pub data_receipt_creation_config: DataReceiptCreationConfigView,
    /// Describes the cost of creating a certain action, `Action`. Includes all variants.
    pub action_creation_config: ActionCreationConfigView,
    /// Describes fees for storage.
    pub storage_usage_config: StorageUsageConfigView,

    /// Fraction of the burnt gas to reward to the contract account for execution.
    pub burnt_gas_reward: Rational32,

    /// Pessimistic gas price inflation ratio.
    pub pessimistic_gas_price_inflation_ratio: Rational32,
}

/// The structure describes configuration for creation of new accounts.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct AccountCreationConfigView {
    /// The minimum length of the top-level account ID that is allowed to be created by any account.
    pub min_allowed_top_level_account_length: u8,
    /// The account ID of the account registrar. This account ID allowed to create top-level
    /// accounts of any valid length.
    pub registrar_account_id: AccountId,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct DataReceiptCreationConfigView {
    /// Base cost of creating a data receipt.
    /// Both `send` and `exec` costs are burned when a new receipt has input dependencies. The gas
    /// is charged for each input dependency. The dependencies are specified when a receipt is
    /// created using `promise_then` and `promise_batch_then`.
    /// NOTE: Any receipt with output dependencies will produce data receipts. Even if it fails.
    /// Even if the last action is not a function call (in case of success it will return empty
    /// value).
    pub base_cost: Fee,
    /// Additional cost per byte sent.
    /// Both `send` and `exec` costs are burned when a function call finishes execution and returns
    /// `N` bytes of data to every output dependency. For each output dependency the cost is
    /// `(send(sir) + exec()) * N`.
    pub cost_per_byte: Fee,
}

/// Describes the cost of creating a specific action, `Action`. Includes all variants.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ActionCreationConfigView {
    /// Base cost of creating an account.
    pub create_account_cost: Fee,

    /// Base cost of deploying a contract.
    pub deploy_contract_cost: Fee,
    /// Cost per byte of deploying a contract.
    pub deploy_contract_cost_per_byte: Fee,

    /// Base cost of calling a function.
    pub function_call_cost: Fee,
    /// Cost per byte of method name and arguments of calling a function.
    pub function_call_cost_per_byte: Fee,

    /// Base cost of making a transfer.
    pub transfer_cost: Fee,

    /// Base cost of staking.
    pub stake_cost: Fee,

    /// Base cost of adding a key.
    pub add_key_cost: AccessKeyCreationConfigView,

    /// Base cost of deleting a key.
    pub delete_key_cost: Fee,

    /// Base cost of deleting an account.
    pub delete_account_cost: Fee,

    /// Base cost for processing a delegate action.
    ///
    /// This is on top of the costs for the actions inside the delegate action.
    pub delegate_cost: Fee,
}

/// Describes the cost of creating an access key.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct AccessKeyCreationConfigView {
    /// Base cost of creating a full access access-key.
    pub full_access_cost: Fee,
    /// Base cost of creating an access-key restricted to specific functions.
    pub function_call_cost: Fee,
    /// Cost per byte of method_names of creating a restricted access-key.
    pub function_call_cost_per_byte: Fee,
}

/// Describes cost of storage per block
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfigView {
    /// Number of bytes for an account record, including rounding up for account id.
    pub num_bytes_account: u64,
    /// Additional number of bytes for a k/v record
    pub num_extra_bytes_record: u64,
}

impl From<crate::RuntimeConfig> for RuntimeConfigView {
    fn from(config: crate::RuntimeConfig) -> Self {
        Self {
            storage_amount_per_byte: config.storage_amount_per_byte(),
            transaction_costs: RuntimeFeesConfigView {
                action_receipt_creation_config: config
                    .fees
                    .fee(ActionCosts::new_action_receipt)
                    .clone(),
                data_receipt_creation_config: DataReceiptCreationConfigView {
                    base_cost: config.fees.fee(ActionCosts::new_data_receipt_base).clone(),
                    cost_per_byte: config.fees.fee(ActionCosts::new_data_receipt_byte).clone(),
                },
                action_creation_config: ActionCreationConfigView {
                    create_account_cost: config.fees.fee(ActionCosts::create_account).clone(),
                    deploy_contract_cost: config
                        .fees
                        .fee(ActionCosts::deploy_contract_base)
                        .clone(),
                    deploy_contract_cost_per_byte: config
                        .fees
                        .fee(ActionCosts::deploy_contract_byte)
                        .clone(),
                    function_call_cost: config.fees.fee(ActionCosts::function_call_base).clone(),
                    function_call_cost_per_byte: config
                        .fees
                        .fee(ActionCosts::function_call_byte)
                        .clone(),
                    transfer_cost: config.fees.fee(ActionCosts::transfer).clone(),
                    stake_cost: config.fees.fee(ActionCosts::stake).clone(),
                    add_key_cost: AccessKeyCreationConfigView {
                        full_access_cost: config.fees.fee(ActionCosts::add_full_access_key).clone(),
                        function_call_cost: config
                            .fees
                            .fee(ActionCosts::add_function_call_key_base)
                            .clone(),
                        function_call_cost_per_byte: config
                            .fees
                            .fee(ActionCosts::add_function_call_key_byte)
                            .clone(),
                    },
                    delete_key_cost: config.fees.fee(ActionCosts::delete_key).clone(),
                    delete_account_cost: config.fees.fee(ActionCosts::delete_account).clone(),
                    delegate_cost: config.fees.fee(ActionCosts::delegate).clone(),
                },
                storage_usage_config: StorageUsageConfigView {
                    num_bytes_account: config.fees.storage_usage_config.num_bytes_account,
                    num_extra_bytes_record: config.fees.storage_usage_config.num_extra_bytes_record,
                },
                burnt_gas_reward: config.fees.burnt_gas_reward,
                pessimistic_gas_price_inflation_ratio: config
                    .fees
                    .pessimistic_gas_price_inflation_ratio,
            },
            wasm_config: VMConfigView::from(config.wasm_config),
            account_creation_config: AccountCreationConfigView {
                min_allowed_top_level_account_length: config
                    .account_creation_config
                    .min_allowed_top_level_account_length,
                registrar_account_id: config.account_creation_config.registrar_account_id,
            },
            congestion_control_config: CongestionControlConfigView::from(
                config.congestion_control_config,
            ),
            witness_config: WitnessConfigView::from(config.witness_config),
        }
    }
}

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VMConfigView {
    /// Costs for runtime externals
    pub ext_costs: ExtCostsConfigView,

    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,
    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// See [VMConfig::vm_kind](crate::vm::Config::vm_kind).
    pub vm_kind: crate::vm::VMKind,
    /// See [VMConfig::disable_9393_fix](crate::vm::Config::disable_9393_fix).
    pub disable_9393_fix: bool,
    /// See [VMConfig::storage_get_mode](crate::vm::Config::storage_get_mode).
    pub storage_get_mode: crate::vm::StorageGetMode,
    /// See [VMConfig::fix_contract_loading_cost](crate::vm::Config::fix_contract_loading_cost).
    pub fix_contract_loading_cost: bool,
    /// See [VMConfig::implicit_account_creation](crate::vm::Config::implicit_account_creation).
    pub implicit_account_creation: bool,
    /// See [VMConfig::math_extension](crate::vm::Config::math_extension).
    pub math_extension: bool,
    /// See [VMConfig::ed25519_verify](crate::vm::Config::ed25519_verify).
    pub ed25519_verify: bool,
    /// See [VMConfig::alt_bn128](crate::vm::Config::alt_bn128).
    pub alt_bn128: bool,
    /// See [VMConfig::function_call_weight](crate::vm::Config::function_call_weight).
    pub function_call_weight: bool,
    /// See [VMConfig::eth_implicit_accounts](crate::vm::Config::eth_implicit_accounts).
    pub eth_implicit_accounts: bool,
    /// See [VMConfig::yield_resume_host_functions](`crate::vm::Config::yield_resume_host_functions).
    pub yield_resume_host_functions: bool,

    /// Describes limits for VM and Runtime.
    ///
    /// TODO: Consider changing this to `VMLimitConfigView` to avoid dependency
    /// on runtime.
    pub limit_config: crate::vm::LimitConfig,
}

impl From<crate::vm::Config> for VMConfigView {
    fn from(config: crate::vm::Config) -> Self {
        Self {
            ext_costs: ExtCostsConfigView::from(config.ext_costs),
            grow_mem_cost: config.grow_mem_cost,
            regular_op_cost: config.regular_op_cost,
            disable_9393_fix: config.disable_9393_fix,
            limit_config: config.limit_config,
            storage_get_mode: config.storage_get_mode,
            fix_contract_loading_cost: config.fix_contract_loading_cost,
            implicit_account_creation: config.implicit_account_creation,
            math_extension: config.math_extension,
            ed25519_verify: config.ed25519_verify,
            alt_bn128: config.alt_bn128,
            function_call_weight: config.function_call_weight,
            vm_kind: config.vm_kind,
            eth_implicit_accounts: config.eth_implicit_accounts,
            yield_resume_host_functions: config.yield_resume_host_functions,
        }
    }
}

impl From<VMConfigView> for crate::vm::Config {
    fn from(view: VMConfigView) -> Self {
        Self {
            ext_costs: crate::ExtCostsConfig::from(view.ext_costs),
            grow_mem_cost: view.grow_mem_cost,
            regular_op_cost: view.regular_op_cost,
            disable_9393_fix: view.disable_9393_fix,
            limit_config: view.limit_config,
            storage_get_mode: view.storage_get_mode,
            fix_contract_loading_cost: view.fix_contract_loading_cost,
            implicit_account_creation: view.implicit_account_creation,
            math_extension: view.math_extension,
            ed25519_verify: view.ed25519_verify,
            alt_bn128: view.alt_bn128,
            function_call_weight: view.function_call_weight,
            vm_kind: view.vm_kind,
            eth_implicit_accounts: view.eth_implicit_accounts,
            yield_resume_host_functions: view.yield_resume_host_functions,
        }
    }
}

/// Typed view of ExtCostsConfig to preserve JSON output field names in protocol
/// config RPC output.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfigView {
    /// Base cost for calling a host function.
    pub base: Gas,

    /// Base cost of loading a pre-compiled contract
    pub contract_loading_base: Gas,
    /// Cost per byte of loading a pre-compiled contract
    pub contract_loading_bytes: Gas,

    /// Base cost for guest memory read
    pub read_memory_base: Gas,
    /// Cost for guest memory read
    pub read_memory_byte: Gas,

    /// Base cost for guest memory write
    pub write_memory_base: Gas,
    /// Cost for guest memory write per byte
    pub write_memory_byte: Gas,

    /// Base cost for reading from register
    pub read_register_base: Gas,
    /// Cost for reading byte from register
    pub read_register_byte: Gas,

    /// Base cost for writing into register
    pub write_register_base: Gas,
    /// Cost for writing byte into register
    pub write_register_byte: Gas,

    /// Base cost of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_base: Gas,
    /// Cost per byte of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_byte: Gas,

    /// Base cost of decoding utf16. It's used for `log_utf16`.
    pub utf16_decoding_base: Gas,
    /// Cost per byte of decoding utf16. It's used for `log_utf16`.
    pub utf16_decoding_byte: Gas,

    /// Cost of getting sha256 base
    pub sha256_base: Gas,
    /// Cost of getting sha256 per byte
    pub sha256_byte: Gas,

    /// Cost of getting sha256 base
    pub keccak256_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak256_byte: Gas,

    /// Cost of getting sha256 base
    pub keccak512_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak512_byte: Gas,

    /// Cost of getting ripemd160 base
    pub ripemd160_base: Gas,
    /// Cost of getting ripemd160 per message block
    pub ripemd160_block: Gas,

    /// Cost of getting ed25519 base
    pub ed25519_verify_base: Gas,
    /// Cost of getting ed25519 per byte
    pub ed25519_verify_byte: Gas,

    /// Cost of calling ecrecover
    pub ecrecover_base: Gas,

    /// Cost for calling logging.
    pub log_base: Gas,
    /// Cost for logging per byte
    pub log_byte: Gas,

    // ###############
    // # Storage API #
    // ###############
    /// Storage trie write key base cost
    pub storage_write_base: Gas,
    /// Storage trie write key per byte cost
    pub storage_write_key_byte: Gas,
    /// Storage trie write value per byte cost
    pub storage_write_value_byte: Gas,
    /// Storage trie write cost per byte of evicted value.
    pub storage_write_evicted_byte: Gas,

    /// Storage trie read key base cost
    pub storage_read_base: Gas,
    /// Storage trie read key per byte cost
    pub storage_read_key_byte: Gas,
    /// Storage trie read value cost per byte cost
    pub storage_read_value_byte: Gas,

    /// Remove key from trie base cost
    pub storage_remove_base: Gas,
    /// Remove key from trie per byte cost
    pub storage_remove_key_byte: Gas,
    /// Remove key from trie ret value byte cost
    pub storage_remove_ret_value_byte: Gas,

    /// Storage trie check for key existence cost base
    pub storage_has_key_base: Gas,
    /// Storage trie check for key existence per key byte
    pub storage_has_key_byte: Gas,

    /// Create trie prefix iterator cost base
    pub storage_iter_create_prefix_base: Gas,
    /// Create trie prefix iterator cost per byte.
    pub storage_iter_create_prefix_byte: Gas,

    /// Create trie range iterator cost base
    pub storage_iter_create_range_base: Gas,
    /// Create trie range iterator cost per byte of from key.
    pub storage_iter_create_from_byte: Gas,
    /// Create trie range iterator cost per byte of to key.
    pub storage_iter_create_to_byte: Gas,

    /// Trie iterator per key base cost
    pub storage_iter_next_base: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_key_byte: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_value_byte: Gas,

    /// Cost per reading trie node from DB
    pub touching_trie_node: Gas,
    /// Cost for reading trie node from memory
    pub read_cached_trie_node: Gas,

    // ###############
    // # Promise API #
    // ###############
    /// Cost for calling `promise_and`
    pub promise_and_base: Gas,
    /// Cost for calling `promise_and` for each promise
    pub promise_and_per_promise: Gas,
    /// Cost for calling `promise_return`
    pub promise_return: Gas,

    // ###############
    // # Validator API #
    // ###############
    /// Cost of calling `validator_stake`.
    pub validator_stake_base: Gas,
    /// Cost of calling `validator_total_stake`.
    pub validator_total_stake_base: Gas,

    // Removed parameters, only here for keeping the output backward-compatible.
    pub contract_compile_base: Gas,
    pub contract_compile_bytes: Gas,

    // #############
    // # Alt BN128 #
    // #############
    /// Base cost for multiexp
    pub alt_bn128_g1_multiexp_base: Gas,
    /// Per element cost for multiexp
    pub alt_bn128_g1_multiexp_element: Gas,
    /// Base cost for sum
    pub alt_bn128_g1_sum_base: Gas,
    /// Per element cost for sum
    pub alt_bn128_g1_sum_element: Gas,
    /// Base cost for pairing check
    pub alt_bn128_pairing_check_base: Gas,
    /// Per element cost for pairing check
    pub alt_bn128_pairing_check_element: Gas,

    /// Base cost for creating a yield promise.
    pub yield_create_base: Gas,
    /// Per byte cost of arguments and method name.
    pub yield_create_byte: Gas,
    /// Base cost for resuming a yield receipt.
    pub yield_resume_base: Gas,
    /// Per byte cost of resume payload.
    pub yield_resume_byte: Gas,
}

impl From<crate::ExtCostsConfig> for ExtCostsConfigView {
    fn from(config: crate::ExtCostsConfig) -> Self {
        Self {
            base: config.gas_cost(ExtCosts::base),
            contract_loading_base: config.gas_cost(ExtCosts::contract_loading_base),
            contract_loading_bytes: config.gas_cost(ExtCosts::contract_loading_bytes),
            read_memory_base: config.gas_cost(ExtCosts::read_memory_base),
            read_memory_byte: config.gas_cost(ExtCosts::read_memory_byte),
            write_memory_base: config.gas_cost(ExtCosts::write_memory_base),
            write_memory_byte: config.gas_cost(ExtCosts::write_memory_byte),
            read_register_base: config.gas_cost(ExtCosts::read_register_base),
            read_register_byte: config.gas_cost(ExtCosts::read_register_byte),
            write_register_base: config.gas_cost(ExtCosts::write_register_base),
            write_register_byte: config.gas_cost(ExtCosts::write_register_byte),
            utf8_decoding_base: config.gas_cost(ExtCosts::utf8_decoding_base),
            utf8_decoding_byte: config.gas_cost(ExtCosts::utf8_decoding_byte),
            utf16_decoding_base: config.gas_cost(ExtCosts::utf16_decoding_base),
            utf16_decoding_byte: config.gas_cost(ExtCosts::utf16_decoding_byte),
            sha256_base: config.gas_cost(ExtCosts::sha256_base),
            sha256_byte: config.gas_cost(ExtCosts::sha256_byte),
            keccak256_base: config.gas_cost(ExtCosts::keccak256_base),
            keccak256_byte: config.gas_cost(ExtCosts::keccak256_byte),
            keccak512_base: config.gas_cost(ExtCosts::keccak512_base),
            keccak512_byte: config.gas_cost(ExtCosts::keccak512_byte),
            ripemd160_base: config.gas_cost(ExtCosts::ripemd160_base),
            ripemd160_block: config.gas_cost(ExtCosts::ripemd160_block),
            ed25519_verify_base: config.gas_cost(ExtCosts::ed25519_verify_base),
            ed25519_verify_byte: config.gas_cost(ExtCosts::ed25519_verify_byte),
            ecrecover_base: config.gas_cost(ExtCosts::ecrecover_base),
            log_base: config.gas_cost(ExtCosts::log_base),
            log_byte: config.gas_cost(ExtCosts::log_byte),
            storage_write_base: config.gas_cost(ExtCosts::storage_write_base),
            storage_write_key_byte: config.gas_cost(ExtCosts::storage_write_key_byte),
            storage_write_value_byte: config.gas_cost(ExtCosts::storage_write_value_byte),
            storage_write_evicted_byte: config.gas_cost(ExtCosts::storage_write_evicted_byte),
            storage_read_base: config.gas_cost(ExtCosts::storage_read_base),
            storage_read_key_byte: config.gas_cost(ExtCosts::storage_read_key_byte),
            storage_read_value_byte: config.gas_cost(ExtCosts::storage_read_value_byte),
            storage_remove_base: config.gas_cost(ExtCosts::storage_remove_base),
            storage_remove_key_byte: config.gas_cost(ExtCosts::storage_remove_key_byte),
            storage_remove_ret_value_byte: config.gas_cost(ExtCosts::storage_remove_ret_value_byte),
            storage_has_key_base: config.gas_cost(ExtCosts::storage_has_key_base),
            storage_has_key_byte: config.gas_cost(ExtCosts::storage_has_key_byte),
            storage_iter_create_prefix_base: config
                .gas_cost(ExtCosts::storage_iter_create_prefix_base),
            storage_iter_create_prefix_byte: config
                .gas_cost(ExtCosts::storage_iter_create_prefix_byte),
            storage_iter_create_range_base: config
                .gas_cost(ExtCosts::storage_iter_create_range_base),
            storage_iter_create_from_byte: config.gas_cost(ExtCosts::storage_iter_create_from_byte),
            storage_iter_create_to_byte: config.gas_cost(ExtCosts::storage_iter_create_to_byte),
            storage_iter_next_base: config.gas_cost(ExtCosts::storage_iter_next_base),
            storage_iter_next_key_byte: config.gas_cost(ExtCosts::storage_iter_next_key_byte),
            storage_iter_next_value_byte: config.gas_cost(ExtCosts::storage_iter_next_value_byte),
            touching_trie_node: config.gas_cost(ExtCosts::touching_trie_node),
            read_cached_trie_node: config.gas_cost(ExtCosts::read_cached_trie_node),
            promise_and_base: config.gas_cost(ExtCosts::promise_and_base),
            promise_and_per_promise: config.gas_cost(ExtCosts::promise_and_per_promise),
            promise_return: config.gas_cost(ExtCosts::promise_return),
            validator_stake_base: config.gas_cost(ExtCosts::validator_stake_base),
            validator_total_stake_base: config.gas_cost(ExtCosts::validator_total_stake_base),
            alt_bn128_g1_multiexp_base: config.gas_cost(ExtCosts::alt_bn128_g1_multiexp_base),
            alt_bn128_g1_multiexp_element: config.gas_cost(ExtCosts::alt_bn128_g1_multiexp_element),
            alt_bn128_g1_sum_base: config.gas_cost(ExtCosts::alt_bn128_g1_sum_base),
            alt_bn128_g1_sum_element: config.gas_cost(ExtCosts::alt_bn128_g1_sum_element),
            alt_bn128_pairing_check_base: config.gas_cost(ExtCosts::alt_bn128_pairing_check_base),
            alt_bn128_pairing_check_element: config
                .gas_cost(ExtCosts::alt_bn128_pairing_check_element),
            yield_create_base: config.gas_cost(ExtCosts::yield_create_base),
            yield_create_byte: config.gas_cost(ExtCosts::yield_create_byte),
            yield_resume_base: config.gas_cost(ExtCosts::yield_resume_base),
            yield_resume_byte: config.gas_cost(ExtCosts::yield_resume_byte),
            // removed parameters
            contract_compile_base: 0,
            contract_compile_bytes: 0,
        }
    }
}

impl From<ExtCostsConfigView> for crate::ExtCostsConfig {
    fn from(view: ExtCostsConfigView) -> Self {
        let costs = enum_map::enum_map! {
                ExtCosts::base => view.base,
                ExtCosts::contract_loading_base => view.contract_loading_base,
                ExtCosts::contract_loading_bytes => view.contract_loading_bytes,
                ExtCosts::read_memory_base => view.read_memory_base,
                ExtCosts::read_memory_byte => view.read_memory_byte,
                ExtCosts::write_memory_base => view.write_memory_base,
                ExtCosts::write_memory_byte => view.write_memory_byte,
                ExtCosts::read_register_base => view.read_register_base,
                ExtCosts::read_register_byte => view.read_register_byte,
                ExtCosts::write_register_base => view.write_register_base,
                ExtCosts::write_register_byte => view.write_register_byte,
                ExtCosts::utf8_decoding_base => view.utf8_decoding_base,
                ExtCosts::utf8_decoding_byte => view.utf8_decoding_byte,
                ExtCosts::utf16_decoding_base => view.utf16_decoding_base,
                ExtCosts::utf16_decoding_byte => view.utf16_decoding_byte,
                ExtCosts::sha256_base => view.sha256_base,
                ExtCosts::sha256_byte => view.sha256_byte,
                ExtCosts::keccak256_base => view.keccak256_base,
                ExtCosts::keccak256_byte => view.keccak256_byte,
                ExtCosts::keccak512_base => view.keccak512_base,
                ExtCosts::keccak512_byte => view.keccak512_byte,
                ExtCosts::ripemd160_base => view.ripemd160_base,
                ExtCosts::ripemd160_block => view.ripemd160_block,
                ExtCosts::ed25519_verify_base => view.ed25519_verify_base,
                ExtCosts::ed25519_verify_byte => view.ed25519_verify_byte,
                ExtCosts::ecrecover_base => view.ecrecover_base,
                ExtCosts::log_base => view.log_base,
                ExtCosts::log_byte => view.log_byte,
                ExtCosts::storage_write_base => view.storage_write_base,
                ExtCosts::storage_write_key_byte => view.storage_write_key_byte,
                ExtCosts::storage_write_value_byte => view.storage_write_value_byte,
                ExtCosts::storage_write_evicted_byte => view.storage_write_evicted_byte,
                ExtCosts::storage_read_base => view.storage_read_base,
                ExtCosts::storage_read_key_byte => view.storage_read_key_byte,
                ExtCosts::storage_read_value_byte => view.storage_read_value_byte,
                ExtCosts::storage_remove_base => view.storage_remove_base,
                ExtCosts::storage_remove_key_byte => view.storage_remove_key_byte,
                ExtCosts::storage_remove_ret_value_byte => view.storage_remove_ret_value_byte,
                ExtCosts::storage_has_key_base => view.storage_has_key_base,
                ExtCosts::storage_has_key_byte => view.storage_has_key_byte,
                ExtCosts::storage_iter_create_prefix_base => view.storage_iter_create_prefix_base,
                ExtCosts::storage_iter_create_prefix_byte => view.storage_iter_create_prefix_byte,
                ExtCosts::storage_iter_create_range_base => view.storage_iter_create_range_base,
                ExtCosts::storage_iter_create_from_byte => view.storage_iter_create_from_byte,
                ExtCosts::storage_iter_create_to_byte => view.storage_iter_create_to_byte,
                ExtCosts::storage_iter_next_base => view.storage_iter_next_base,
                ExtCosts::storage_iter_next_key_byte => view.storage_iter_next_key_byte,
                ExtCosts::storage_iter_next_value_byte => view.storage_iter_next_value_byte,
                ExtCosts::touching_trie_node => view.touching_trie_node,
                ExtCosts::read_cached_trie_node => view.read_cached_trie_node,
                ExtCosts::promise_and_base => view.promise_and_base,
                ExtCosts::promise_and_per_promise => view.promise_and_per_promise,
                ExtCosts::promise_return => view.promise_return,
                ExtCosts::validator_stake_base => view.validator_stake_base,
                ExtCosts::validator_total_stake_base => view.validator_total_stake_base,
                ExtCosts::alt_bn128_g1_multiexp_base => view.alt_bn128_g1_multiexp_base,
                ExtCosts::alt_bn128_g1_multiexp_element => view.alt_bn128_g1_multiexp_element,
                ExtCosts::alt_bn128_g1_sum_base => view.alt_bn128_g1_sum_base,
                ExtCosts::alt_bn128_g1_sum_element => view.alt_bn128_g1_sum_element,
                ExtCosts::alt_bn128_pairing_check_base => view.alt_bn128_pairing_check_base,
                ExtCosts::alt_bn128_pairing_check_element => view.alt_bn128_pairing_check_element,
                ExtCosts::yield_create_base => view.yield_create_base,
                ExtCosts::yield_create_byte => view.yield_create_byte,
                ExtCosts::yield_resume_base => view.yield_resume_base,
                ExtCosts::yield_resume_byte => view.yield_resume_byte,
        }
        .map(|_, value| ParameterCost { gas: value, compute: value });
        Self { costs }
    }
}

/// Configuration specific to ChunkStateWitness.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct WitnessConfigView {
    /// Size limit for storage proof generated while executing receipts in a chunk.
    /// After this limit is reached we defer execution of any new receipts.
    pub main_storage_proof_size_soft_limit: usize,
    // Maximum size of transactions contained inside ChunkStateWitness.
    /// A witness contains transactions from both the previous chunk and the current one.
    /// This parameter limits the sum of sizes of transactions from both of those chunks.
    pub combined_transactions_size_limit: usize,
    /// Soft size limit of storage proof used to validate new transactions in ChunkStateWitness.
    pub new_transactions_validation_state_size_soft_limit: usize,
}

impl From<WitnessConfig> for WitnessConfigView {
    fn from(config: WitnessConfig) -> Self {
        Self {
            main_storage_proof_size_soft_limit: config.main_storage_proof_size_soft_limit,
            combined_transactions_size_limit: config.combined_transactions_size_limit,
            new_transactions_validation_state_size_soft_limit: config
                .new_transactions_validation_state_size_soft_limit,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct CongestionControlConfigView {
    /// How much gas in delayed receipts of a shard is 100% incoming congestion.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub max_congestion_incoming_gas: Gas,

    /// How much gas in outgoing buffered receipts of a shard is 100% congested.
    ///
    /// Outgoing congestion contributes to overall congestion, which reduces how
    /// much other shards are allowed to forward to this shard.
    pub max_congestion_outgoing_gas: Gas,

    /// How much memory space of all delayed and buffered receipts in a shard is
    /// considered 100% congested.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub max_congestion_memory_consumption: u64,

    /// How many missed chunks in a row in a shard is considered 100% congested.
    pub max_congestion_missed_chunks: u64,

    /// The maximum amount of gas attached to receipts a shard can forward to
    /// another shard per chunk.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub max_outgoing_gas: Gas,

    /// The minimum gas each shard can send to a shard that is not fully congested.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub min_outgoing_gas: Gas,

    /// How much gas the chosen allowed shard can send to a 100% congested shard.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub allowed_shard_outgoing_gas: Gas,

    /// The maximum amount of gas in a chunk spent on converting new transactions to
    /// receipts.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub max_tx_gas: Gas,

    /// The minimum amount of gas in a chunk spent on converting new transactions
    /// to receipts, as long as the receiving shard is not congested.
    ///
    /// See [`CongestionControlConfig`] for more details.
    pub min_tx_gas: Gas,

    /// How much congestion a shard can tolerate before it stops all shards from
    /// accepting new transactions with the receiver set to the congested shard.
    pub reject_tx_congestion_threshold: f64,
}

impl From<CongestionControlConfig> for CongestionControlConfigView {
    fn from(other: CongestionControlConfig) -> Self {
        Self {
            max_congestion_incoming_gas: other.max_congestion_incoming_gas,
            max_congestion_outgoing_gas: other.max_congestion_outgoing_gas,
            max_congestion_memory_consumption: other.max_congestion_memory_consumption,
            max_congestion_missed_chunks: other.max_congestion_missed_chunks,
            max_outgoing_gas: other.max_outgoing_gas,
            min_outgoing_gas: other.min_outgoing_gas,
            allowed_shard_outgoing_gas: other.allowed_shard_outgoing_gas,
            max_tx_gas: other.max_tx_gas,
            min_tx_gas: other.min_tx_gas,
            reject_tx_congestion_threshold: other.reject_tx_congestion_threshold,
        }
    }
}

impl From<CongestionControlConfigView> for CongestionControlConfig {
    fn from(other: CongestionControlConfigView) -> Self {
        Self {
            max_congestion_incoming_gas: other.max_congestion_incoming_gas,
            max_congestion_outgoing_gas: other.max_congestion_outgoing_gas,
            max_congestion_memory_consumption: other.max_congestion_memory_consumption,
            max_congestion_missed_chunks: other.max_congestion_missed_chunks,
            max_outgoing_gas: other.max_outgoing_gas,
            min_outgoing_gas: other.min_outgoing_gas,
            allowed_shard_outgoing_gas: other.allowed_shard_outgoing_gas,
            max_tx_gas: other.max_tx_gas,
            min_tx_gas: other.min_tx_gas,
            reject_tx_congestion_threshold: other.reject_tx_congestion_threshold,
        }
    }
}

#[cfg(test)]
#[cfg(not(feature = "nightly"))]
#[cfg(not(feature = "statelessnet_protocol"))]
mod tests {
    /// The JSON representation used in RPC responses must not remove or rename
    /// fields, only adding fields is allowed or we risk breaking clients.
    #[test]
    fn test_runtime_config_view() {
        use crate::view::RuntimeConfigView;
        use crate::RuntimeConfig;
        use crate::RuntimeConfigStore;
        use near_primitives_core::version::PROTOCOL_VERSION;

        let config_store = RuntimeConfigStore::new(None);
        let config = config_store.get_config(PROTOCOL_VERSION);
        let view = RuntimeConfigView::from(RuntimeConfig::clone(config));
        insta::assert_json_snapshot!(&view, { ".wasm_config.vm_kind" => "<REDACTED>"});
    }
}
