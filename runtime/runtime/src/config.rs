//! Settings of the parameters of the runtime.
use near_primitives::transaction::Action;
use near_primitives::types::{Balance, BlockIndex, Gas};
use wasm::types::Config;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RuntimeConfig {
    /// The cost to store one byte of storage per block.
    pub storage_cost_byte_per_block: Balance,
    /// The minimum number of blocks of storage rent an account has to maintain to prevent forced deletion.
    pub poke_threshold: BlockIndex,
    /// Costs for different types of transactions.
    pub transaction_costs: TransactionCosts,
    /// Config of wasm operations.
    pub wasm_config: Config,
}

/// The costs of the transactions.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct TransactionCosts {
    pub create_account: Gas,
    pub deploy_contract: Gas,
    pub function_call: Gas,
    pub transfer: Gas,
    pub stake: Gas,
    pub add_key: Gas,
    pub delete_key: Gas,
    pub delete_account: Gas,
}

fn safe_gas_to_balance(
    gas_price: Balance,
    gas: Gas,
) -> Result<Balance, Box<dyn std::error::Error>> {
    gas_price.checked_mul(gas as Balance).ok_or_else(|| "gas to balance overflow".into())
}

fn safe_add_gas(val: Gas, rhs: Gas) -> Result<Gas, Box<dyn std::error::Error>> {
    val.checked_add(rhs).ok_or_else(|| "gas add overflow".into())
}

fn safe_add_balance(val: Balance, rhs: Balance) -> Result<Balance, Box<dyn std::error::Error>> {
    val.checked_add(rhs).ok_or_else(|| "balance add overflow".into())
}

impl TransactionCosts {
    pub fn action_gas_cost(&self, action: &Action) -> Gas {
        use Action::*;
        match action {
            CreateAccount(_) => self.create_account,
            DeployContract(_) => self.deploy_contract,
            FunctionCall(_) => self.function_call,
            Transfer(_) => self.transfer,
            Stake(_) => self.stake,
            AddKey(_) => self.add_key,
            DeleteKey(_) => self.delete_key,
            DeleteAccount(_) => self.delete_account,
        }
    }

    /// Get the total sum of basic gas cost for given actions
    pub fn total_basic_gas(
        &self,
        actions: &Vec<Action>,
    ) -> Result<Gas, Box<dyn std::error::Error>> {
        let mut total_gas: Gas = 0;
        for action in actions {
            total_gas = safe_add_gas(total_gas, self.action_gas_cost(action))?;
        }
        Ok(total_gas)
    }

    /// Get the total sum of deposits for given actions
    pub fn total_deposit(
        &self,
        actions: &Vec<Action>,
    ) -> Result<Balance, Box<dyn std::error::Error>> {
        let mut total_balance: Balance = 0;
        for action in actions {
            total_balance = safe_add_balance(total_balance, action.get_deposit_balance())?;
        }
        Ok(total_balance)
    }

    /// Get the total sum of prepaid gas for given actions
    pub fn total_prepaid_gas(
        &self,
        actions: &Vec<Action>,
    ) -> Result<Gas, Box<dyn std::error::Error>> {
        let mut total_gas: Gas = 0;
        for action in actions {
            total_gas = safe_add_gas(total_gas, action.get_prepaid_gas())?;
        }
        Ok(total_gas)
    }

    /// Get the total sum of prepaid gas and basic actions gas for given actions
    pub fn total_gas(&self, actions: &Vec<Action>) -> Result<Gas, Box<dyn std::error::Error>> {
        safe_add_gas(self.total_basic_gas(actions)?, self.total_prepaid_gas(actions)?)
    }

    /// Get the cost of the given list of actions
    pub fn total_cost(
        &self,
        gas_price: Balance,
        actions: &Vec<Action>,
    ) -> Result<Balance, Box<dyn std::error::Error>> {
        safe_add_balance(
            self.total_deposit(actions)?,
            safe_gas_to_balance(gas_price, self.total_gas(actions)?)?,
        )
    }
}
