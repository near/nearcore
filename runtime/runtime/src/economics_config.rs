//! Settings of the parameters of the economics.
use primitives::transaction::TransactionBody;
use primitives::types::Balance;
use wasm::types::Config;

/// The structure that holds the parameters of the economics.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EconomicsConfig {
    /// The cost to store one byte of storage per block.
    pub storage_cost_byte_per_block: Balance,
    pub transactions_costs: TransactionsCosts,
    /// Config of wasm operations.
    pub wasm_config: Config,
}

impl Default for EconomicsConfig {
    fn default() -> Self {
        Self {
            storage_cost_byte_per_block: 0,
            transactions_costs: Default::default(),
            wasm_config: Default::default(),
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
            create_account: 0,
            deploy_contract: 0,
            function_call: 0,
            send_money: 0,
            stake: 0,
            swap_key: 0,
            add_key: 0,
            delete_key: 0,
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
