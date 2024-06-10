use crate::cost::{ExtCostsConfig, ParameterCost};
use borsh::BorshSerialize;
use near_primitives_core::config::AccountIdValidityRulesVersion;
use near_primitives_core::types::Gas;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// NOTE that VMKind is part of serialization protocol, so we cannot remove entries from this list
// if particular VM reached publicly visible networks.
//
// Additionally, this is public only for the purposes of internal tools like the estimator. This
// API should otherwise be considered a private configuration of the `near-vm-runner`
// crate.
#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    BorshSerialize,
    PartialEq,
    Eq,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum VMKind {
    /// Wasmer 0.17.x VM.
    Wasmer0,
    /// Wasmtime VM.
    Wasmtime,
    /// Wasmer 2.x VM.
    Wasmer2,
    /// NearVM.
    NearVm,
}

impl VMKind {
    pub fn replace_with_wasmtime_if_unsupported(self) -> Self {
        if cfg!(not(target_arch = "x86_64")) {
            Self::Wasmtime
        } else {
            self
        }
    }
}

/// This enum represents if a storage_get call will be performed through flat storage or trie
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum StorageGetMode {
    FlatStorage,
    Trie,
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
    /// Max receipt size
    pub max_receipt_size: u64,
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
    /// Number of blocks after which a yielded promise times out.
    pub yield_timeout_length_in_blocks: u64,
    /// Maximum number of bytes for payload passed over a yield resume.
    pub max_yield_payload_size: u64,
    /// Hard limit on the size of storage proof generated while executing a single receipt.
    pub per_receipt_storage_proof_size_limit: usize,
}

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

    /// The kind of the VM implementation to use
    pub vm_kind: VMKind,

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

    /// Enable the `EthImplicitAccounts` protocol feature.
    pub eth_implicit_accounts: bool,

    /// Enable the `promise_yield_create` and `promise_yield_resume` host functions.
    pub yield_resume_host_functions: bool,

    /// Describes limits for VM and Runtime.
    pub limit_config: LimitConfig,
}

impl Config {
    /// Computes non-cryptographically-proof hash. The computation is fast but not cryptographically
    /// secure.
    pub fn non_crypto_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    pub fn make_free(&mut self) {
        self.ext_costs = ExtCostsConfig {
            costs: near_primitives_core::enum_map::enum_map! {
                _ => ParameterCost { gas: 0, compute: 0 }
            },
        };
        self.grow_mem_cost = 0;
        self.regular_op_cost = 0;
        self.limit_config.max_gas_burnt = u64::MAX;
    }

    pub fn enable_all_features(&mut self) {
        self.yield_resume_host_functions = true;
        self.eth_implicit_accounts = true;
        self.function_call_weight = true;
        self.alt_bn128 = true;
        self.ed25519_verify = true;
        self.math_extension = true;
        self.implicit_account_creation = true;
    }
}

fn wasmer2_stack_limit_default() -> i32 {
    100 * 1024
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
