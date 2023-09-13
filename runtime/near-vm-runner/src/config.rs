use crate::logic::StorageGetMode;
use near_primitives_core::config::{AccountIdValidityRulesVersion, ExtCostsConfig, ParameterCost};
use near_primitives_core::types::Gas;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Dynamic configuration parameters required for the WASM runtime to
/// execute a smart contract.
///
/// This (`VMConfig`) and `RuntimeFeesConfig` combined are sufficient to define
/// protocol specific behavior of the contract runtime. The former contains
/// configuration for the WASM runtime specifically, while the latter contains
/// configuration for the transaction runtime and WASM runtime.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Config {
    /// Costs for runtime externals
    pub ext_costs: ExtCostsConfig,

    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,
    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// Disable the fix for the #9393 issue in near-vm-runner.
    pub disable_9393_fix: bool,

    /// Set to `StorageGetMode::FlatStorage` in order to enable the `FlatStorageReads` protocol
    /// feature.
    pub storage_get_mode: StorageGetMode,

    /// Enable the `FixContractLoadingCost` protocol feature.
    pub fix_contract_loading_cost: bool,

    /// Enable the `ImplicitAccountCreation` protocol feature.
    pub implicit_account_creation: bool,

    /// Enable the host functions added by the `MathExtension` protocol feature.
    pub math_extension: bool,

    /// Enable the host functions added by the `Ed25519Verify` protocol feature.
    pub ed25519_verify: bool,

    /// Enable the host functions added by the `AltBn128` protocol feature.
    pub alt_bn128: bool,

    /// Enable the `FunctionCallWeight` protocol feature.
    pub function_call_weight: bool,

    /// Describes limits for VM and Runtime.
    pub limit_config: LimitConfig,
}

/// Our original code for limiting WASM stack was buggy. We fixed that, but we
/// still have to use old (`V0`) limiter for old protocol versions.
///
/// This struct here exists to enforce that the value in the config is either
/// `0` or `1`. We could have used a `bool` instead, but there's a chance that
/// our current impl isn't perfect either and would need further tweaks in the
/// future.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u8)]
pub enum ContractPrepareVersion {
    /// Oldest, buggiest version.
    ///
    /// Don't use it unless specifically to support old protocol version.
    V0,
    /// Old, slow and buggy version.
    ///
    /// Better than V0, but don’t use this nevertheless.
    V1,
    /// finite-wasm 0.3.0 based contract preparation code.
    V2,
}

impl ContractPrepareVersion {
    pub fn v0() -> ContractPrepareVersion {
        ContractPrepareVersion::V0
    }
}

/// Describes limits for VM and Runtime.
/// TODO #4139: consider switching to strongly-typed wrappers instead of raw quantities
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct LimitConfig {
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See <https://wiki.parity.io/WebAssembly-StackHeight> to find out how the stack frame cost
    /// is calculated.
    pub max_stack_height: u32,
    /// Whether a legacy version of stack limiting should be used, see
    /// [`ContractPrepareVersion`].
    #[serde(default = "ContractPrepareVersion::v0")]
    pub contract_prepare_version: ContractPrepareVersion,

    /// The initial number of memory pages.
    /// NOTE: It's not a limiter itself, but it's a value we use for initial_memory_pages.
    pub initial_memory_pages: u32,
    /// What is the maximal memory pages amount is allowed to have for a contract.
    pub max_memory_pages: u32,

    /// Limit of memory used by registers.
    pub registers_memory_limit: u64,
    /// Maximum number of bytes that can be stored in a single register.
    pub max_register_size: u64,
    /// Maximum number of registers that can be used simultaneously.
    ///
    /// Note that due to an implementation quirk [read: a bug] in VMLogic, if we
    /// have this number of registers, no subsequent writes to the registers
    /// will succeed even if they replace an existing register.
    pub max_number_registers: u64,

    /// Maximum number of log entries.
    pub max_number_logs: u64,
    /// Maximum total length in bytes of all log messages.
    pub max_total_log_length: u64,

    /// Max total prepaid gas for all function call actions per receipt.
    pub max_total_prepaid_gas: Gas,

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
    /// Whether to enforce account_id well-formedness where it wasn't enforced
    /// historically.
    #[serde(default = "AccountIdValidityRulesVersion::v0")]
    pub account_id_validity_rules_version: AccountIdValidityRulesVersion,
}

fn wasmer2_stack_limit_default() -> i32 {
    100 * 1024
}

impl Config {
    pub fn test() -> Self {
        Self {
            ext_costs: ExtCostsConfig::test(),
            grow_mem_cost: 1,
            // Refer to `near_primitives_core::config::SAFETY_MULTIPLIER`.
            regular_op_cost: 3 * 1285457,
            disable_9393_fix: false,
            limit_config: LimitConfig::test(),
            fix_contract_loading_cost: cfg!(feature = "protocol_feature_fix_contract_loading_cost"),
            storage_get_mode: StorageGetMode::FlatStorage,
            implicit_account_creation: true,
            math_extension: true,
            ed25519_verify: true,
            alt_bn128: true,
            function_call_weight: true,
        }
    }

    /// Computes non-cryptographically-proof hash. The computation is fast but not cryptographically
    /// secure.
    pub fn non_crypto_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    pub fn free() -> Self {
        Self {
            ext_costs: ExtCostsConfig {
                costs: near_primitives_core::enum_map::enum_map! {
                    _ => ParameterCost { gas: 0, compute: 0 }
                },
            },
            grow_mem_cost: 0,
            regular_op_cost: 0,
            disable_9393_fix: false,
            // We shouldn't have any costs in the limit config.
            limit_config: LimitConfig { max_gas_burnt: u64::MAX, ..LimitConfig::test() },
            fix_contract_loading_cost: cfg!(feature = "protocol_feature_fix_contract_loading_cost"),
            storage_get_mode: StorageGetMode::FlatStorage,
            implicit_account_creation: true,
            math_extension: true,
            ed25519_verify: true,
            alt_bn128: true,
            function_call_weight: true,
        }
    }
}

impl LimitConfig {
    pub fn test() -> Self {
        const KB: u32 = 1024;
        let max_contract_size = 4 * 2u64.pow(20);
        Self {
            max_gas_burnt: 2 * 10u64.pow(14), // with 10**15 block gas limit this will allow 5 calls.
            max_stack_height: 256 * KB,
            contract_prepare_version: ContractPrepareVersion::V2,
            initial_memory_pages: 2u32.pow(10), // 64Mib of memory.
            max_memory_pages: 2u32.pow(11),     // 128Mib of memory.

            // By default registers are limited by 1GiB of memory.
            registers_memory_limit: 2u64.pow(30),
            // By default each register is limited by 100MiB of memory.
            max_register_size: 2u64.pow(20) * 100,
            // By default there is at most 100 registers.
            max_number_registers: 100,

            max_number_logs: 100,
            // Total logs size is 16Kib
            max_total_log_length: 16 * 1024,

            // Updating the maximum prepaid gas to limit the maximum depth of a transaction to 64
            // blocks.
            // This based on `63 * min_receipt_with_function_call_gas()`. Where 63 is max depth - 1.
            max_total_prepaid_gas: 300 * 10u64.pow(12),

            // Safety limit. Unlikely to hit it for most common transactions and receipts.
            max_actions_per_receipt: 100,
            // Should be low enough to deserialize an access key without paying.
            max_number_bytes_method_names: 2000,
            max_length_method_name: 256,            // basic safety limit
            max_arguments_length: 4 * 2u64.pow(20), // 4 Mib
            max_length_returned_data: 4 * 2u64.pow(20), // 4 Mib
            max_contract_size,                      // 4 Mib,
            max_transaction_size: 4 * 2u64.pow(20), // 4 Mib

            max_length_storage_key: 4 * 2u64.pow(20), // 4 Mib
            max_length_storage_value: 4 * 2u64.pow(20), // 4 Mib
            // Safety limit and unlikely abusable.
            max_promises_per_function_call_action: 1024,
            // Unlikely to hit it for normal development.
            max_number_input_data_dependencies: 128,
            max_functions_number_per_contract: Some(10000),
            wasmer2_stack_limit: 200 * 1024,
            // To utilize a local in an useful way, at least two `local.*` instructions are
            // necessary (they only take constant operands indicating the local to access), which
            // is 4 bytes worth of code for each local.
            max_locals_per_contract: Some(max_contract_size / 4),
            account_id_validity_rules_version: AccountIdValidityRulesVersion::V1,
        }
    }
}
