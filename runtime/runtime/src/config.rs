//! Settings of the parameters of the runtime.
use near_primitives::transaction::TransactionBody;
use near_primitives::types::{Balance, BlockIndex};
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
    pub transactions_costs: TransactionsCosts,
    /// Config of wasm operations.
    pub wasm_config: Config,
}

/// The costs of the transactions.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct TransactionsCosts {
    pub create_account: Balance,
    pub deploy_contract: Balance,
    pub function_call: Balance,
    pub self_function_call: Balance,
    pub send_money: Balance,
    pub stake: Balance,
    pub swap_key: Balance,
    pub add_key: Balance,
    pub delete_key: Balance,
    pub delete_account: Balance,
}

impl TransactionsCosts {
    /// Get the cost of the given transaction.
    pub fn cost(&self, transaction_body: &TransactionBody) -> Balance {
        use TransactionBody::*;
        match transaction_body {
            CreateAccount(_) => self.create_account,
            DeployContract(_) => self.deploy_contract,
            FunctionCall(_)
                if Some(transaction_body.get_originator())
                    == transaction_body.get_contract_id() =>
            {
                self.self_function_call
            }
            FunctionCall(_) => self.function_call,
            SendMoney(_) => self.send_money,
            Stake(_) => self.stake,
            SwapKey(_) => self.swap_key,
            AddKey(_) => self.add_key,
            DeleteKey(_) => self.delete_key,
            DeleteAccount(_) => self.delete_account,
        }
    }
}
