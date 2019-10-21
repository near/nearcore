//! Helper functions to compute the costs of certain actions assuming they succeed and the only
//! actions in the transaction batch.
use near::config::INITIAL_GAS_PRICE;
use near_primitives::types::{Balance, Gas};
use near_runtime_fees::RuntimeFeesConfig;

const GAS_PRICE: u128 = INITIAL_GAS_PRICE;

pub fn gas_burnt_to_reward(gas_burnt: Gas) -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let gas_reward = gas_burnt * cfg.burnt_gas_reward.numerator / cfg.burnt_gas_reward.denominator;
    gas_reward as Balance * GAS_PRICE
}

pub fn create_account_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.create_account_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.create_account_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}

pub fn create_account_transfer_full_key_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.create_account_cost.exec_fee()
        + cfg.action_creation_config.transfer_cost.exec_fee()
        + cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.create_account_cost.send_fee(false)
        + cfg.action_creation_config.transfer_cost.send_fee(false)
        + cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}

pub fn create_account_transfer_full_key_cost_no_reward() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.create_account_cost.exec_fee()
        + cfg.action_creation_config.transfer_cost.exec_fee()
        + cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.create_account_cost.send_fee(false)
        + cfg.action_creation_config.transfer_cost.send_fee(false)
        + cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE
}

pub fn create_account_transfer_full_key_cost_fail_on_create_account() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.create_account_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.create_account_cost.send_fee(false)
        + cfg.action_creation_config.transfer_cost.send_fee(false)
        + cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}

pub fn deploy_contract_cost(num_bytes: u64) -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.deploy_contract_cost.exec_fee()
        + num_bytes * cfg.action_creation_config.deploy_contract_cost_per_byte.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(true)
        + cfg.action_creation_config.deploy_contract_cost.send_fee(true)
        + num_bytes * cfg.action_creation_config.deploy_contract_cost_per_byte.send_fee(true);
    (exec_gas + send_gas) as Balance * GAS_PRICE
        - gas_burnt_to_reward(send_gas)
        - gas_burnt_to_reward(exec_gas)
}

pub fn function_call_cost(num_bytes: u64, prepaid_gas: u64) -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.function_call_cost.exec_fee()
        + num_bytes * cfg.action_creation_config.function_call_cost_per_byte.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.function_call_cost.send_fee(false)
        + num_bytes * cfg.action_creation_config.function_call_cost_per_byte.send_fee(false);
    (exec_gas + send_gas + prepaid_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}

pub fn transfer_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.transfer_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.transfer_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}

pub fn stake_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.stake_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(true)
        + cfg.action_creation_config.stake_cost.send_fee(true);
    (exec_gas + send_gas) as Balance * GAS_PRICE
        - gas_burnt_to_reward(send_gas)
        - gas_burnt_to_reward(exec_gas)
}

pub fn add_key_cost(num_bytes: u64) -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.add_key_cost.function_call_cost.exec_fee()
        + num_bytes
            * cfg.action_creation_config.add_key_cost.function_call_cost_per_byte.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(true)
        + cfg.action_creation_config.add_key_cost.function_call_cost.send_fee(true)
        + num_bytes
            * cfg.action_creation_config.add_key_cost.function_call_cost_per_byte.send_fee(true);
    (exec_gas + send_gas) as Balance * GAS_PRICE
        - gas_burnt_to_reward(send_gas)
        - gas_burnt_to_reward(exec_gas)
}

pub fn add_key_full_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(true)
        + cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(true);
    (exec_gas + send_gas) as Balance * GAS_PRICE
        - gas_burnt_to_reward(send_gas)
        - gas_burnt_to_reward(exec_gas)
}

pub fn delete_key_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.delete_key_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(true)
        + cfg.action_creation_config.delete_key_cost.send_fee(true);
    (exec_gas + send_gas) as Balance * GAS_PRICE
        - gas_burnt_to_reward(send_gas)
        - gas_burnt_to_reward(exec_gas)
}

pub fn delete_account_cost() -> Balance {
    let cfg = RuntimeFeesConfig::default();
    let exec_gas = cfg.action_receipt_creation_config.exec_fee()
        + cfg.action_creation_config.delete_account_cost.exec_fee();
    let send_gas = cfg.action_receipt_creation_config.send_fee(false)
        + cfg.action_creation_config.delete_account_cost.send_fee(false);
    (exec_gas + send_gas) as Balance * GAS_PRICE - gas_burnt_to_reward(send_gas)
}
