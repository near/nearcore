//! Helper functions to compute the costs of certain actions assuming they succeed and the only
//! actions in the transaction batch.
use near_primitives::types::{Balance, Gas};
use near_runtime_fees::RuntimeFeesConfig;

pub struct FeeHelper {
    cfg: RuntimeFeesConfig,
    gas_price: Balance,
}

impl FeeHelper {
    pub fn new(cfg: RuntimeFeesConfig, gas_price: Balance) -> Self {
        Self { cfg, gas_price }
    }

    pub fn gas_burnt_to_reward(&self, gas_burnt: Gas) -> Balance {
        let gas_reward = gas_burnt * *self.cfg.burnt_gas_reward.numer() as u64
            / *self.cfg.burnt_gas_reward.denom() as u64;
        gas_reward as Balance * self.gas_price
    }

    pub fn create_account_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.create_account_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.create_account_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn create_account_transfer_full_key_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.create_account_cost.exec_fee()
            + self.cfg.action_creation_config.transfer_cost.exec_fee()
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.create_account_cost.send_fee(false)
            + self.cfg.action_creation_config.transfer_cost.send_fee(false)
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn create_account_transfer_full_key_cost_no_reward(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.create_account_cost.exec_fee()
            + self.cfg.action_creation_config.transfer_cost.exec_fee()
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.create_account_cost.send_fee(false)
            + self.cfg.action_creation_config.transfer_cost.send_fee(false)
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn create_account_transfer_full_key_cost_fail_on_create_account(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.create_account_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.create_account_cost.send_fee(false)
            + self.cfg.action_creation_config.transfer_cost.send_fee(false)
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn deploy_contract_cost(&self, num_bytes: u64) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.deploy_contract_cost.exec_fee()
            + num_bytes * self.cfg.action_creation_config.deploy_contract_cost_per_byte.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(true)
            + self.cfg.action_creation_config.deploy_contract_cost.send_fee(true)
            + num_bytes
                * self.cfg.action_creation_config.deploy_contract_cost_per_byte.send_fee(true);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn function_call_exec_gas(&self, num_bytes: u64) -> Gas {
        self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.function_call_cost.exec_fee()
            + num_bytes * self.cfg.action_creation_config.function_call_cost_per_byte.exec_fee()
    }

    pub fn function_call_cost(&self, num_bytes: u64, prepaid_gas: u64) -> Balance {
        let exec_gas = self.function_call_exec_gas(num_bytes);
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.function_call_cost.send_fee(false)
            + num_bytes
                * self.cfg.action_creation_config.function_call_cost_per_byte.send_fee(false);
        (exec_gas + send_gas + prepaid_gas) as Balance * self.gas_price
    }

    pub fn transfer_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.transfer_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.transfer_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn stake_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.stake_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(true)
            + self.cfg.action_creation_config.stake_cost.send_fee(true);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn add_key_cost(&self, num_bytes: u64) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.add_key_cost.function_call_cost.exec_fee()
            + num_bytes
                * self
                    .cfg
                    .action_creation_config
                    .add_key_cost
                    .function_call_cost_per_byte
                    .exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(true)
            + self.cfg.action_creation_config.add_key_cost.function_call_cost.send_fee(true)
            + num_bytes
                * self
                    .cfg
                    .action_creation_config
                    .add_key_cost
                    .function_call_cost_per_byte
                    .send_fee(true);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn add_key_full_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(true)
            + self.cfg.action_creation_config.add_key_cost.full_access_cost.send_fee(true);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn delete_key_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.delete_key_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(true)
            + self.cfg.action_creation_config.delete_key_cost.send_fee(true);
        (exec_gas + send_gas) as Balance * self.gas_price
    }

    pub fn delete_account_cost(&self) -> Balance {
        let exec_gas = self.cfg.action_receipt_creation_config.exec_fee()
            + self.cfg.action_creation_config.delete_account_cost.exec_fee();
        let send_gas = self.cfg.action_receipt_creation_config.send_fee(false)
            + self.cfg.action_creation_config.delete_account_cost.send_fee(false);
        (exec_gas + send_gas) as Balance * self.gas_price
    }
}
