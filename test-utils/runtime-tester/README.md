# Runtime test
Runtime test is described by Scenario.
Currently, scenario only supports one client, but you can specify the number of accounts through NetworcConfig.
```rust
#[derive(Serialize, Deserialize)]
pub struct Scenario {
    pub network_config: NetworkConfig,
    pub blocks: Vec<BlockConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub seeds: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct BlockConfig {
    pub height: BlockHeight,
    pub transactions: Vec<TransactionConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionConfig {
    pub nonce: Nonce,
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub signer: InMemorySigner,
    pub actions: Vec<Action>,
}
```

Scenario can be loaded from a json file or constructed in rust code.
```rust
pub fn from_file(path: &Path) -> io::Result<Scenario>
```

Scenario::run tries to produce all the described blocks and if succeeded returns RuntimeStats.
```rust
pub fn run(&self) -> Result<RuntimeStats, Error>
```

RuntimeStats contain stats for every produced block. Currently, only block production time is supported.
```rust
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct RuntimeStats {
    pub blocks_stats: Vec<BlockStats>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BlockStats {
    pub height: u64,
    pub block_production_time: Duration,
}
```

Be careful to remember, that block height should be positive and ascending.

# Scenario Builder
To easily create new scenarios in rust code use ScenarioBuilder.

```rust
impl ScenarioBuilder {
    pub fn new(num_accounts: usize, use_in_memory_store: bool) -> Self

    pub fn add_block(&mut self)

    pub fn add_transaction(&mut self, signer_index: usize, receiver_index: usize, actions: Vec<Action>)

    pub fn scenario(&self) -> &Scenario
}
```
`new` -- Creates builder with an empty scenario with `num_accounts` accounts.
`add_block` -- Adds empty block to the scenario with the next height (starting from 1).
`add_transaction` -- Adds transaction to the last block in the scenario.
`scenario` -- Returns a reference to the built scenario.

## Example
Produce three blocks. The first one deploys a contract to the second account, other two blocks are empty.
Assert that production of all blocks took less than a second.
```rust
#[test]
fn test_deploy_contract() {
    use runtime_tester::{ScenarioBuilder};
    use std::time::Duration;

    let mut builder = ScenarioBuilder::new(2, true);

    builder.add_block();
    builder.add_transaction(0, 1,
                            vec![Action::DeployContract(DeployContractAction {
                                code: near_test_contracts::rs_contract().to_vec(),
                            })]);

    builder.add_block();
    builder.add_block();

    let runtime_stats = builder.scenario().run().unwrap();

    for block_stats in runtime_stats.blocks_stats {
        assert!(block_stats.block_production_time < Duration::from_secs(1),
                "Block at height {} was produced in {:?}",
                block_stats.height, block_stats.block_production_time);
    }
}
```
