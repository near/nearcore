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
Example of ScenarioBuilder usage can be found in src/scenario_builder.rs.
