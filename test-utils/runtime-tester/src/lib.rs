//! run_test is a framework for creating and executing runtime scenarios.
//! You can create Scenario in rust code or have it in a JSON file.
//! Scenario::run executes scenario, keeping track of different metrics.
//! So far, the only metric is how much time block production takes.
//! fuzzing provides Arbitrary trait for Scenario, thus enabling creating random scenarios.
pub mod fuzzing;
pub mod run_test;

pub use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};

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
        network_config: NetworkConfig { seeds: seeds },
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

    scenario.run().unwrap();
}
