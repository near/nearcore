# Runtime test
## Scenario
Runtime test is described by Scenario.
Currently, scenario only supports one client, but you can specify the number of accounts through NetworcConfig.

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

## Scenario Builder
To easily create new scenarios in rust code use ScenarioBuilder.

## Example
Produce three blocks. The first one deploys a contract to the second account, other two blocks are empty.
Assert that production of all blocks took less than a second.
```rust
#[test]
fn test_deploy_contract() {
    use runtime_tester::{ScenarioBuilder};
    use std::time::Duration;

    let mut builder = ScenarioBuilder::new().
        number_of_accounts(10).
        in_memory_store(true);

    builder.add_block();
    builder.add_transaction(0, 9,
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
