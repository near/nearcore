//! Helper functions to compute the costs of certain actions assuming they succeed and the only
//! actions in the transaction batch.
use near_parameters::{ActionCosts, RuntimeConfig, RuntimeFeesConfig};
use near_primitives::transaction::Action;
use near_primitives::types::{AccountId, Balance, Gas};

pub struct FeeHelper {
    pub rt_cfg: RuntimeConfig,
    pub gas_price: Balance,
}

impl FeeHelper {
    pub fn new(rt_cfg: RuntimeConfig, gas_price: Balance) -> Self {
        Self { rt_cfg, gas_price }
    }

    pub fn cfg(&self) -> &RuntimeFeesConfig {
        &self.rt_cfg.fees
    }

    pub fn gas_to_balance_inflated(&self, gas: Gas) -> Balance {
        Balance::from_yoctonear(u128::from(gas.as_gas()))
            .checked_mul(
                self.gas_price
                    .checked_mul(
                        (*self.cfg().pessimistic_gas_price_inflation_ratio.numer())
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap()
                    .checked_div(
                        (*self.cfg().pessimistic_gas_price_inflation_ratio.denom())
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap()
                    .as_yoctonear(),
            )
            .unwrap()
    }

    pub fn gas_to_balance(&self, gas: Gas) -> Balance {
        self.gas_price.checked_mul(u128::from(gas.as_gas())).unwrap()
    }

    pub fn gas_burnt_to_reward(&self, gas_burnt: Gas) -> Balance {
        let gas_reward = Gas::from_gas(
            gas_burnt.as_gas() * *self.cfg().burnt_gas_reward.numer() as u64
                / *self.cfg().burnt_gas_reward.denom() as u64,
        );
        self.gas_to_balance(gas_reward)
    }

    pub fn create_account_cost(&self) -> Balance {
        let create_account_exec_fee = self.cfg().fee(ActionCosts::create_account).exec_fee();
        let create_account_send_fee = self.cfg().fee(ActionCosts::create_account).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(create_account_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(create_account_send_fee)
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn create_account_transfer_full_key_fee(&self) -> Gas {
        let create_account_exec_fee = self.cfg().fee(ActionCosts::create_account).exec_fee();
        let transfer_exec_fee = self.cfg().fee(ActionCosts::transfer).exec_fee();
        let add_full_access_key_exec_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).exec_fee();
        let create_account_send_fee = self.cfg().fee(ActionCosts::create_account).send_fee(false);
        let transfer_send_fee = self.cfg().fee(ActionCosts::transfer).send_fee(false);
        let add_full_access_key_send_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(create_account_exec_fee)
            .unwrap()
            .checked_add(transfer_exec_fee)
            .unwrap()
            .checked_add(add_full_access_key_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(create_account_send_fee)
            .unwrap()
            .checked_add(transfer_send_fee)
            .unwrap()
            .checked_add(add_full_access_key_send_fee)
            .unwrap();
        exec_gas.checked_add(send_gas).unwrap()
    }

    pub fn create_account_transfer_fee(&self) -> Gas {
        let create_account_exec_fee = self.cfg().fee(ActionCosts::create_account).exec_fee();
        let transfer_exec_fee = self.cfg().fee(ActionCosts::transfer).exec_fee();
        let create_account_send_fee = self.cfg().fee(ActionCosts::create_account).send_fee(false);
        let transfer_send_fee = self.cfg().fee(ActionCosts::transfer).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(create_account_exec_fee)
            .unwrap()
            .checked_add(transfer_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(create_account_send_fee)
            .unwrap()
            .checked_add(transfer_send_fee)
            .unwrap();
        exec_gas.checked_add(send_gas).unwrap()
    }

    pub fn create_account_transfer_full_key_cost(&self) -> Balance {
        self.gas_to_balance(self.create_account_transfer_full_key_fee())
    }

    pub fn create_account_transfer_cost(&self) -> Balance {
        self.gas_to_balance(self.create_account_transfer_fee())
    }

    pub fn create_account_transfer_full_key_cost_no_reward(&self) -> Balance {
        let create_account_exec_fee = self.cfg().fee(ActionCosts::create_account).exec_fee();
        let transfer_exec_fee = self.cfg().fee(ActionCosts::transfer).exec_fee();
        let add_full_access_key_exec_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).exec_fee();
        let create_account_send_fee = self.cfg().fee(ActionCosts::create_account).send_fee(false);
        let transfer_send_fee = self.cfg().fee(ActionCosts::transfer).send_fee(false);
        let add_full_access_key_send_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(create_account_exec_fee)
            .unwrap()
            .checked_add(transfer_exec_fee)
            .unwrap()
            .checked_add(add_full_access_key_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(create_account_send_fee)
            .unwrap()
            .checked_add(transfer_send_fee)
            .unwrap()
            .checked_add(add_full_access_key_send_fee)
            .unwrap();
        self.gas_to_balance(send_gas).checked_add(self.gas_to_balance_inflated(exec_gas)).unwrap()
    }

    pub fn create_account_transfer_full_key_cost_fail_on_create_account(&self) -> Balance {
        let create_account_exec_fee = self.cfg().fee(ActionCosts::create_account).exec_fee();
        let create_account_send_fee = self.cfg().fee(ActionCosts::create_account).send_fee(false);
        let transfer_send_fee = self.cfg().fee(ActionCosts::transfer).send_fee(false);
        let add_full_access_key_send_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(create_account_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(create_account_send_fee)
            .unwrap()
            .checked_add(transfer_send_fee)
            .unwrap()
            .checked_add(add_full_access_key_send_fee)
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn deploy_contract_cost(&self, num_bytes: u64) -> Balance {
        let deploy_contract_base_exec_fee =
            self.cfg().fee(ActionCosts::deploy_contract_base).exec_fee();
        let deploy_contract_byte_exec_fee =
            self.cfg().fee(ActionCosts::deploy_contract_byte).exec_fee();
        let deploy_contract_base_send_fee =
            self.cfg().fee(ActionCosts::deploy_contract_base).send_fee(true);
        let deploy_contract_byte_send_fee =
            self.cfg().fee(ActionCosts::deploy_contract_byte).send_fee(true);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(deploy_contract_base_exec_fee)
            .unwrap()
            .checked_add(deploy_contract_byte_exec_fee.checked_mul(num_bytes).unwrap())
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(true)
            .checked_add(deploy_contract_base_send_fee)
            .unwrap()
            .checked_add(deploy_contract_byte_send_fee.checked_mul(num_bytes).unwrap())
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn function_call_exec_gas(&self, num_bytes: u64) -> Gas {
        let function_call_base_exec_fee =
            self.cfg().fee(ActionCosts::function_call_base).exec_fee();
        let function_call_byte_exec_fee =
            self.cfg().fee(ActionCosts::function_call_byte).exec_fee();

        self.cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(function_call_base_exec_fee)
            .unwrap()
            .checked_add(function_call_byte_exec_fee.checked_mul(num_bytes).unwrap())
            .unwrap()
    }

    pub fn function_call_cost(&self, num_bytes: u64, prepaid_gas: u64) -> Balance {
        let function_call_base_send_fee =
            self.cfg().fee(ActionCosts::function_call_base).send_fee(false);
        let function_call_byte_send_fee =
            self.cfg().fee(ActionCosts::function_call_byte).send_fee(false);

        let exec_gas = self.function_call_exec_gas(num_bytes);
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(function_call_base_send_fee)
            .unwrap()
            .checked_add(function_call_byte_send_fee.checked_mul(num_bytes).unwrap())
            .unwrap();
        self.gas_to_balance(
            exec_gas
                .checked_add(send_gas)
                .unwrap()
                .checked_add(Gas::from_gas(prepaid_gas))
                .unwrap(),
        )
    }

    pub fn transfer_fee(&self) -> Gas {
        let transfer_exec_fee = self.cfg().fee(ActionCosts::transfer).exec_fee();
        let transfer_send_fee = self.cfg().fee(ActionCosts::transfer).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(transfer_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(transfer_send_fee)
            .unwrap();
        exec_gas.checked_add(send_gas).unwrap()
    }

    pub fn transfer_cost(&self) -> Balance {
        self.gas_to_balance(self.transfer_fee())
    }

    pub fn stake_cost(&self) -> Balance {
        let stake_exec_fee = self.cfg().fee(ActionCosts::stake).exec_fee();
        let stake_send_fee = self.cfg().fee(ActionCosts::stake).send_fee(true);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(stake_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(true)
            .checked_add(stake_send_fee)
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn add_key_cost(&self, num_bytes: u64) -> Balance {
        let add_function_call_key_base_exec_fee =
            self.cfg().fee(ActionCosts::add_function_call_key_base).exec_fee();
        let add_function_call_key_byte_exec_fee =
            self.cfg().fee(ActionCosts::add_function_call_key_byte).exec_fee();
        let add_function_call_key_base_send_fee =
            self.cfg().fee(ActionCosts::add_function_call_key_base).send_fee(true);
        let add_function_call_key_byte_send_fee =
            self.cfg().fee(ActionCosts::add_function_call_key_byte).send_fee(true);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(add_function_call_key_base_exec_fee)
            .unwrap()
            .checked_add(add_function_call_key_byte_exec_fee.checked_mul(num_bytes).unwrap())
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(true)
            .checked_add(add_function_call_key_base_send_fee)
            .unwrap()
            .checked_add(add_function_call_key_byte_send_fee.checked_mul(num_bytes).unwrap())
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn add_key_full_cost(&self) -> Balance {
        let add_full_access_key_exec_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).exec_fee();
        let add_full_access_key_send_fee =
            self.cfg().fee(ActionCosts::add_full_access_key).send_fee(true);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(add_full_access_key_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(true)
            .checked_add(add_full_access_key_send_fee)
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn delete_key_cost(&self) -> Balance {
        let delete_key_exec_fee = self.cfg().fee(ActionCosts::delete_key).exec_fee();
        let delete_key_send_fee = self.cfg().fee(ActionCosts::delete_key).send_fee(true);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(delete_key_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(true)
            .checked_add(delete_key_send_fee)
            .unwrap();
        self.gas_to_balance(exec_gas.checked_add(send_gas).unwrap())
    }

    pub fn prepaid_delete_account_cost(&self) -> Balance {
        let delete_account_exec_fee = self.cfg().fee(ActionCosts::delete_account).exec_fee();
        let delete_account_send_fee = self.cfg().fee(ActionCosts::delete_account).send_fee(false);

        let exec_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .exec_fee()
            .checked_add(delete_account_exec_fee)
            .unwrap();
        let send_gas = self
            .cfg()
            .fee(ActionCosts::new_action_receipt)
            .send_fee(false)
            .checked_add(delete_account_send_fee)
            .unwrap();

        let total_fee = exec_gas.checked_add(send_gas).unwrap();

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
        let base = self.cfg().fee(ActionCosts::delegate);
        let receipt = self.cfg().fee(ActionCosts::new_action_receipt);
        let total_gas = base
            .exec_fee()
            .checked_add(base.send_fee(sir))
            .unwrap()
            .checked_add(receipt.send_fee(sir))
            .unwrap()
            .checked_add(
                node_runtime::config::total_send_fees(&self.rt_cfg, sir, actions, receiver)
                    .unwrap(),
            )
            .unwrap();
        self.gas_to_balance(total_gas)
    }

    pub fn gas_refund_cost(&self, gas: Gas) -> Balance {
        self.gas_price
            .checked_mul(u128::from(self.cfg().gas_penalty_for_gas_refund(gas).as_gas()))
            .unwrap()
    }
}
