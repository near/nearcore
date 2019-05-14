//! Settings of the parameters of the economics.
use primitives::transaction::TransactionBody;
use primitives::types::Balance;

/// The structure that holds the parameters of the economics.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EconomicsConfig {
    /// The cost to store one byte of storage per block.
    pub storage_cost_byte_per_block: Balance,
    /// The cost of calling a contract.
    pub contract_call_cost: Balance,
    /// Cost of requesting a callback.
    pub callback_cost: Balance,
    pub transactions_costs: TransactionsCosts,
}

impl Default for EconomicsConfig {
    fn default() -> Self {
        Self {
            storage_cost_byte_per_block: 1,
            contract_call_cost: 10,
            callback_cost: 10,
            transactions_costs: Default::default(),
        }
    }
}

/// The costs of the transactions.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionsCosts {
    pub create_account: Balance,
    pub deploy_contract: Balance,
    pub function_call: Balance,
    pub send_money: Balance,
    pub stake: Balance,
    pub swap_key: Balance,
    pub add_key: Balance,
    pub delete_key: Balance,
}

impl Default for TransactionsCosts {
    fn default() -> Self {
        Self {
            create_account: 1,
            deploy_contract: 1,
            function_call: 1,
            send_money: 1,
            stake: 1,
            swap_key: 1,
            add_key: 1,
            delete_key: 1,
        }
    }
}

impl TransactionsCosts {
    /// Get the cost of the given transaction.
    pub fn cost(&self, transaction_body: &TransactionBody) -> Balance {
        use TransactionBody::*;
        match transaction_body {
            CreateAccount(_) => self.create_account,
            DeployContract(_) => self.deploy_contract,
            FunctionCall(_) => self.function_call,
            SendMoney(_) => self.send_money,
            Stake(_) => self.stake,
            SwapKey(_) => self.swap_key,
            AddKey(_) => self.add_key,
            DeleteKey(_) => self.delete_key,
        }
    }
}
