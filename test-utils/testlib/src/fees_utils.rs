//! Helper functions to compute the costs of certain actions assuming they succeed and the only
//! actions in the transaction batch.
use near_primitives::config::ActionCosts;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::transaction::Action;
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::version::PROTOCOL_VERSION;

pub struct FeeHelper {
    pub cfg: RuntimeFeesConfig,
    pub gas_price: Balance,
}

impl FeeHelper {
    pub fn new(cfg: RuntimeFeesConfig, gas_price: Balance) -> Self {
        Self { cfg, gas_price }
    }

    pub fn gas_to_balance_inflated(&self, gas: Gas) -> Balance {
        gas as Balance
            * (self.gas_price * (*self.cfg.pessimistic_gas_price_inflation_ratio.numer() as u128)
                / (*self.cfg.pessimistic_gas_price_inflation_ratio.denom() as u128))
    }

    pub fn gas_to_balance(&self, gas: Gas) -> Balance {
        gas as Balance * self.gas_price
    }

    pub fn gas_burnt_to_reward(&self, gas_burnt: Gas) -> Balance {
        let gas_reward = gas_burnt * *self.cfg.burnt_gas_reward.numer() as u64
            / *self.cfg.burnt_gas_reward.denom() as u64;
        self.gas_to_balance(gas_reward)
    }

    pub fn create_account_cost(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::create_account).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::create_account).send_fee(false);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn create_account_transfer_full_key_fee(&self) -> Gas {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::create_account).exec_fee()
            + self.cfg.fee(ActionCosts::transfer).exec_fee()
            + self.cfg.fee(ActionCosts::add_full_access_key).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::create_account).send_fee(false)
            + self.cfg.fee(ActionCosts::transfer).send_fee(false)
            + self.cfg.fee(ActionCosts::add_full_access_key).send_fee(false);
        exec_gas + send_gas
    }

    pub fn create_account_transfer_full_key_cost(&self) -> Balance {
        self.gas_to_balance(self.create_account_transfer_full_key_fee())
    }

    pub fn create_account_transfer_full_key_cost_no_reward(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::create_account).exec_fee()
            + self.cfg.fee(ActionCosts::transfer).exec_fee()
            + self.cfg.fee(ActionCosts::add_full_access_key).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::create_account).send_fee(false)
            + self.cfg.fee(ActionCosts::transfer).send_fee(false)
            + self.cfg.fee(ActionCosts::add_full_access_key).send_fee(false);
        self.gas_to_balance(send_gas) + self.gas_to_balance_inflated(exec_gas)
    }

    pub fn create_account_transfer_full_key_cost_fail_on_create_account(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::create_account).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::create_account).send_fee(false)
            + self.cfg.fee(ActionCosts::transfer).send_fee(false)
            + self.cfg.fee(ActionCosts::add_full_access_key).send_fee(false);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn deploy_contract_cost(&self, num_bytes: u64) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::deploy_contract_base).exec_fee()
            + num_bytes * self.cfg.fee(ActionCosts::deploy_contract_byte).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(true)
            + self.cfg.fee(ActionCosts::deploy_contract_base).send_fee(true)
            + num_bytes * self.cfg.fee(ActionCosts::deploy_contract_byte).send_fee(true);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn function_call_exec_gas(&self, num_bytes: u64) -> Gas {
        self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::function_call_base).exec_fee()
            + num_bytes * self.cfg.fee(ActionCosts::function_call_byte).exec_fee()
    }

    pub fn function_call_cost(&self, num_bytes: u64, prepaid_gas: u64) -> Balance {
        let exec_gas = self.function_call_exec_gas(num_bytes);
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::function_call_base).send_fee(false)
            + num_bytes * self.cfg.fee(ActionCosts::function_call_byte).send_fee(false);
        self.gas_to_balance(exec_gas + send_gas + prepaid_gas)
    }

    pub fn transfer_fee(&self) -> Gas {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::transfer).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::transfer).send_fee(false);
        exec_gas + send_gas
    }

    pub fn transfer_cost(&self) -> Balance {
        self.gas_to_balance(self.transfer_fee())
    }

    pub fn transfer_cost_64len_hex(&self) -> Balance {
        self.create_account_transfer_full_key_cost()
    }

    pub fn stake_cost(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::stake).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(true)
            + self.cfg.fee(ActionCosts::stake).send_fee(true);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn add_key_cost(&self, num_bytes: u64) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::add_function_call_key_base).exec_fee()
            + num_bytes * self.cfg.fee(ActionCosts::add_function_call_key_byte).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(true)
            + self.cfg.fee(ActionCosts::add_function_call_key_base).send_fee(true)
            + num_bytes * self.cfg.fee(ActionCosts::add_function_call_key_byte).send_fee(true);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn add_key_full_cost(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::add_full_access_key).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(true)
            + self.cfg.fee(ActionCosts::add_full_access_key).send_fee(true);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn delete_key_cost(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::delete_key).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(true)
            + self.cfg.fee(ActionCosts::delete_key).send_fee(true);
        self.gas_to_balance(exec_gas + send_gas)
    }

    pub fn prepaid_delete_account_cost(&self) -> Balance {
        let exec_gas = self.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + self.cfg.fee(ActionCosts::delete_account).exec_fee();
        let send_gas = self.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + self.cfg.fee(ActionCosts::delete_account).send_fee(false);

        let total_fee = exec_gas + send_gas;

        self.gas_to_balance(total_fee)
    }

    /// The additional cost to execute a list of actions in a meta transaction,
    /// compared to executing them directly.
    ///
    /// The overhead consists of:
    /// - The base cost of the delegate action (send and exec).
    /// - The additional send cost for all inner actions.
    pub fn meta_tx_overhead_cost(&self, actions: &[Action], receiver: &AccountId) -> Balance {
        // for tests, we assume sender != receiver
        let sir = false;
        let base = self.cfg.fee(ActionCosts::delegate);
        let receipt = self.cfg.fee(ActionCosts::new_action_receipt);
        let total_gas = base.exec_fee()
            + base.send_fee(sir)
            + receipt.send_fee(sir)
            + node_runtime::config::total_send_fees(
                &self.cfg,
                sir,
                actions,
                receiver,
                PROTOCOL_VERSION,
            )
            .unwrap();
        self.gas_to_balance(total_gas)
    }
}
