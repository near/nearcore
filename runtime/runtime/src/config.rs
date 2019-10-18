//! Settings of the parameters of the runtime.
use near_primitives::account::AccessKeyPermission;
use near_primitives::errors::{BalanceOverflowError, GasOverflowError};
use near_primitives::serialize::u128_dec_format;
use near_primitives::transaction::{
    Action, AddKeyAction, DeployContractAction, FunctionCallAction,
};
use near_primitives::types::{Balance, BlockIndex, Gas};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::Config;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RuntimeConfig {
    /// The cost to store one byte of storage per block.
    #[serde(with = "u128_dec_format")]
    pub storage_cost_byte_per_block: Balance,
    /// The minimum number of blocks of storage rent an account has to maintain to prevent forced deletion.
    pub poke_threshold: BlockIndex,
    /// Costs of different actions that need to be performed when sending and processing transaction
    /// and receipts.
    pub transaction_costs: RuntimeFeesConfig,
    /// Config of wasm operations.
    pub wasm_config: Config,
    /// The baseline cost to store account_id of short length per block.
    /// The original formula in NEP#0006 is `1,000 / (3 ^ (account_id.length - 2))` for cost per year.
    /// This value represents `1,000` above adjusted to use per block.
    #[serde(with = "u128_dec_format")]
    pub account_length_baseline_cost_per_block: Balance,
}

impl RuntimeConfig {
    pub fn free() -> Self {
        Self {
            storage_cost_byte_per_block: 0,
            poke_threshold: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: Config::free(),
            account_length_baseline_cost_per_block: 0,
        }
    }
}

pub fn safe_gas_to_balance(gas_price: Balance, gas: Gas) -> Result<Balance, GasOverflowError> {
    gas_price.checked_mul(Balance::from(gas)).ok_or_else(|| GasOverflowError {})
}

pub fn safe_add_gas(a: Gas, b: Gas) -> Result<Gas, GasOverflowError> {
    a.checked_add(b).ok_or_else(|| GasOverflowError {})
}

pub fn safe_add_balance(a: Balance, b: Balance) -> Result<Balance, BalanceOverflowError> {
    a.checked_add(b).ok_or_else(|| BalanceOverflowError {})
}

/// Total sum of gas that needs to be burnt to send these actions.
pub fn total_send_fees(
    config: &RuntimeFeesConfig,
    sender_is_receiver: bool,
    actions: &[Action],
) -> Result<Gas, GasOverflowError> {
    let cfg = &config.action_creation_config;
    let mut result = 0;
    for action in actions {
        use Action::*;
        let delta = match action {
            CreateAccount(_) => cfg.create_account_cost.send_fee(sender_is_receiver),
            DeployContract(DeployContractAction { code }) => {
                let num_bytes = code.len() as u64;
                cfg.deploy_contract_cost.send_fee(sender_is_receiver)
                    + cfg.deploy_contract_cost_per_byte.send_fee(sender_is_receiver) * num_bytes
            }
            FunctionCall(FunctionCallAction { method_name, args, .. }) => {
                let num_bytes = method_name.as_bytes().len() as u64 + args.len() as u64;
                cfg.function_call_cost.send_fee(sender_is_receiver)
                    + cfg.function_call_cost_per_byte.send_fee(sender_is_receiver) * num_bytes
            }
            Transfer(_) => cfg.transfer_cost.send_fee(sender_is_receiver),
            Stake(_) => cfg.stake_cost.send_fee(sender_is_receiver),
            AddKey(AddKeyAction { access_key, .. }) => match &access_key.permission {
                AccessKeyPermission::FunctionCall(call_perm) => {
                    let num_bytes = call_perm
                        .method_names
                        .iter()
                        // Account for null-terminating characters.
                        .map(|name| name.as_bytes().len() as u64 + 1)
                        .sum::<u64>();
                    cfg.add_key_cost.function_call_cost.send_fee(sender_is_receiver)
                        + num_bytes
                            * cfg
                                .add_key_cost
                                .function_call_cost_per_byte
                                .send_fee(sender_is_receiver)
                }
                AccessKeyPermission::FullAccess => {
                    cfg.add_key_cost.full_access_cost.send_fee(sender_is_receiver)
                }
            },
            DeleteKey(_) => cfg.delete_key_cost.send_fee(sender_is_receiver),
            DeleteAccount(_) => cfg.delete_account_cost.send_fee(sender_is_receiver),
        };
        result = safe_add_gas(result, delta)?;
    }
    Ok(result)
}

pub fn exec_fee(config: &RuntimeFeesConfig, action: &Action) -> Gas {
    let cfg = &config.action_creation_config;
    use Action::*;
    match action {
        CreateAccount(_) => cfg.create_account_cost.exec_fee(),
        DeployContract(DeployContractAction { code }) => {
            let num_bytes = code.len() as u64;
            cfg.deploy_contract_cost.exec_fee()
                + cfg.deploy_contract_cost_per_byte.exec_fee() * num_bytes
        }
        FunctionCall(FunctionCallAction { method_name, args, .. }) => {
            let num_bytes = method_name.as_bytes().len() as u64 + args.len() as u64;
            cfg.function_call_cost.exec_fee()
                + cfg.function_call_cost_per_byte.exec_fee() * num_bytes
        }
        Transfer(_) => cfg.transfer_cost.exec_fee(),
        Stake(_) => cfg.stake_cost.exec_fee(),
        AddKey(AddKeyAction { access_key, .. }) => match &access_key.permission {
            AccessKeyPermission::FunctionCall(call_perm) => {
                let num_bytes = call_perm
                    .method_names
                    .iter()
                    // Account for null-terminating characters.
                    .map(|name| name.as_bytes().len() as u64 + 1)
                    .sum::<u64>();
                cfg.add_key_cost.function_call_cost.exec_fee()
                    + num_bytes * cfg.add_key_cost.function_call_cost_per_byte.exec_fee()
            }
            AccessKeyPermission::FullAccess => cfg.add_key_cost.full_access_cost.exec_fee(),
        },
        DeleteKey(_) => cfg.delete_key_cost.exec_fee(),
        DeleteAccount(_) => cfg.delete_account_cost.exec_fee(),
    }
}

/// Total sum of gas that would need to be burnt before we start executing the given actions.
pub fn total_exec_fees(
    config: &RuntimeFeesConfig,
    actions: &[Action],
) -> Result<Gas, GasOverflowError> {
    let mut result = 0;
    for action in actions {
        let delta = exec_fee(&config, action);
        result = safe_add_gas(result, delta)?;
    }
    Ok(result)
}
/// Get the total sum of deposits for given actions.
pub fn total_deposit(actions: &[Action]) -> Result<Balance, BalanceOverflowError> {
    let mut total_balance: Balance = 0;
    for action in actions {
        total_balance = safe_add_balance(total_balance, action.get_deposit_balance())?;
    }
    Ok(total_balance)
}

/// Get the total sum of prepaid gas for given actions.
pub fn total_prepaid_gas(actions: &[Action]) -> Result<Gas, GasOverflowError> {
    let mut total_gas: Gas = 0;
    for action in actions {
        total_gas = safe_add_gas(total_gas, action.get_prepaid_gas())?;
    }
    Ok(total_gas)
}
