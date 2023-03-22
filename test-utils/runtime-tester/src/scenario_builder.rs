use crate::run_test::{BlockConfig, NetworkConfig, RuntimeConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{
    transaction::Action,
    types::{AccountId, BlockHeight, BlockHeightDelta, Gas, Nonce},
};

use std::str::FromStr;

pub struct ScenarioBuilder {
    height: BlockHeight,
    nonce: Nonce,
    scenario: Scenario,
}

/// # Example
/// # Produce three blocks. The first one deploys a contract to the second account, other two blocks are empty.
/// # Assert that production of all blocks took less than a second.
/// ```
///     use runtime_tester::ScenarioBuilder;
///     use std::time::Duration;
///     use near_primitives::transaction::{Action, DeployContractAction};
///
///     let mut builder = ScenarioBuilder::new().
///         number_of_accounts(10).
///         in_memory_store(true);
///
///     builder.add_block();
///     builder.add_transaction(0, 9,
///                             vec![Action::DeployContract(DeployContractAction {
///                                 code: near_test_contracts::rs_contract().to_vec(),
///                             })]);
///
///     builder.add_block();
///     builder.add_block();
///
///     let runtime_stats = builder.scenario().run().result.unwrap();
///
///     for block_stats in runtime_stats.blocks_stats {
///         assert!(block_stats.block_production_time < Duration::from_secs(1),
///                 "Block at height {} was produced in {:?}",
///                 block_stats.height, block_stats.block_production_time);
///     }
/// ```
impl ScenarioBuilder {
    /// Creates builder with an empty scenario with 4 accounts.
    /// Default `use_in_memory_store` -- true.
    pub fn new() -> Self {
        let network_config = NetworkConfig { seeds: (0..4).map(id_to_seed).collect() };
        let runtime_config = RuntimeConfig {
            max_total_prepaid_gas: 300 * 10u64.pow(12),
            gas_limit: 1_000_000_000_000_000,
            epoch_length: 500,
        };

        ScenarioBuilder {
            height: 1,
            nonce: 1,
            scenario: Scenario {
                network_config,
                runtime_config,
                blocks: vec![],
                use_in_memory_store: true,
            },
        }
    }

    /// Changes number of accounts to `num_accounts`.
    pub fn number_of_accounts(mut self, num_accounts: usize) -> Self {
        self.scenario.network_config =
            NetworkConfig { seeds: (0..num_accounts).map(id_to_seed).collect() };
        self
    }

    /// Changes max_total_prepaid_gas
    pub fn max_total_prepaid_gas(mut self, max_total_prepaid_gas: Gas) -> Self {
        self.scenario.runtime_config.max_total_prepaid_gas = max_total_prepaid_gas;
        self
    }

    /// Changes gas_limit
    pub fn gas_limit(mut self, gas_limit: Gas) -> Self {
        self.scenario.runtime_config.gas_limit = gas_limit;
        self
    }

    /// Changes epoch_length
    pub fn epoch_length(mut self, epoch_length: BlockHeightDelta) -> Self {
        self.scenario.runtime_config.epoch_length = epoch_length;
        self
    }

    /// Changes `use_in_memory_store`.
    pub fn in_memory_store(mut self, in_memory_store: bool) -> Self {
        self.scenario.use_in_memory_store = in_memory_store;
        self
    }

    /// Adds empty block to the scenario with the next height (starting from 1).
    pub fn add_block(&mut self) {
        self.scenario.blocks.push(BlockConfig::at_height(self.height));
        self.height += 1;
    }

    /// Adds transaction to the last block in the scenario.
    pub fn add_transaction(
        &mut self,
        signer_index: usize,
        receiver_index: usize,
        actions: Vec<Action>,
    ) {
        assert!(!self.scenario.blocks.is_empty());

        let signer_id = AccountId::from_str(&id_to_seed(signer_index)).unwrap();
        let receiver_id = AccountId::from_str(&id_to_seed(receiver_index)).unwrap();

        let signer =
            InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_id.as_ref());

        let block = {
            let last_id = self.scenario.blocks.len() - 1;
            &mut self.scenario.blocks[last_id]
        };

        (*block).transactions.push(TransactionConfig {
            nonce: self.nonce,
            signer_id: signer_id,
            receiver_id: receiver_id,
            signer,
            actions,
        });

        self.nonce += 1
    }

    /// Returns a reference to the built scenario.
    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }
}

fn id_to_seed(id: usize) -> String {
    format!("test{}", id)
}
