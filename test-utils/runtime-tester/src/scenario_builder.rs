use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{
    transaction::Action,
    types::{AccountId, BlockHeight, Nonce},
};

use std::str::FromStr;

pub struct ScenarioBuilder {
    height: BlockHeight,
    nonce: Nonce,
    scenario: Scenario,
}

impl ScenarioBuilder {
    /// Creates builder with an empty scenario with 4 accounts.
    /// Default `use_in_memory_store` -- true.
    pub fn new() -> Self {
        let network_config =
            NetworkConfig { seeds: (0..4).map(|x| id_to_seed(x)).collect() };

        ScenarioBuilder {
            height: 1,
            nonce: 1,
            scenario: Scenario { network_config, blocks: vec![], use_in_memory_store: true },
        }
    }

    /// Changes number of accounts to `num_accounts`.
    pub fn number_of_accounts(mut self, num_accounts: usize) -> Self {
        self.scenario.network_config = NetworkConfig { seeds: (0..num_accounts).map(|x| id_to_seed(x)).collect() };
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
            signer_id: signer_id.clone(),
            receiver_id: receiver_id.clone(),
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
