#![doc = include_str!("../README.md")]

pub mod fuzzing;
pub mod run_test;
pub mod scenario_builder;

pub use crate::run_test::{BlockConfig, NetworkConfig, RuntimeConfig, Scenario, TransactionConfig};
pub use crate::scenario_builder::ScenarioBuilder;

#[test]
// Use this test as a base for creating reproducers.
fn scenario_smoke_test() {
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::AccountId;

    let num_accounts = 5;

    let seeds: Vec<String> = (0..num_accounts).map(|i| format!("test{}", i)).collect();
    let accounts: Vec<AccountId> = seeds.iter().map(|id| id.parse().unwrap()).collect();

    let mut scenario = Scenario {
        network_config: NetworkConfig { seeds },
        runtime_config: RuntimeConfig {
            max_total_prepaid_gas: 300 * 10u64.pow(12),
            gas_limit: 1_000_000_000_000_000,
            epoch_length: 500,
        },
        blocks: Vec::new(),
        use_in_memory_store: true,
    };

    for h in 1..5 {
        let mut block = BlockConfig::at_height(h);
        let transaction = {
            let signer_id = accounts[h as usize].clone();
            let receiver_id = accounts[(h - 1) as usize].clone();
            let signer =
                InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_id.as_ref());

            TransactionConfig {
                nonce: h,
                signer_id,
                receiver_id,
                signer,
                actions: vec![Action::Transfer(TransferAction { deposit: 10 })],
            }
        };
        block.transactions.push(transaction);
        scenario.blocks.push(block)
    }

    scenario.run().result.unwrap();
}
