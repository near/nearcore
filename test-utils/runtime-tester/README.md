# Runtime test
## Scenario
Runtime test is described by Scenario.
Currently, scenario only supports one client, but you can specify the number of accounts through NetworcConfig.

Scenario can be loaded from a json file or constructed in rust code.
```rust
pub fn from_file(path: &Path) -> io::Result<Scenario>
```

Scenario::run tries to produce all the described blocks and if
succeeded returns `RuntimeStats` wrapped in a `ScenarioResult`.

```rust
pub fn run(&self) -> ScenarioResult<RuntimeStats, Error>
```

`RuntimeStats` contain stats for every produced block. Currently, only
block production time is supported.

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

`ScenarioResult` is a wrapper around `Result` type which adds
a `homedir` field:

```rust
pub struct ScenarioResult<T, E> {
    pub result: std::result::Result<T, E>,
    pub homedir: Option<tempfile::TempDir>,
}
```

The `homedir` is populated if scenario is configured to use on-disk
storage (i.e. if `use_in_memory_store` is `false`) and allows the
caller to locate the store.

Be careful to remember, that block height should be positive and
ascending.

## Scenario Builder
To easily create new scenarios in rust code use ScenarioBuilder.
Example of ScenarioBuilder usage can be found in src/scenario_builder.rs.
