# Runtime test

Framework for creating and executing runtime scenarios.  You can
create [`Scenario`] in rust code or load it from a JSON file.

[`fuzzing`] module provides [`libfuzzer_sys::arbitrary::Arbitrary`]
trait for [`Scenario`], thus enabling creating random scenarios.

## Scenario

Runtime test is described by a [`Scenario`] object.  Currently,
scenario supports only one client, but you can specify number of
accounts through [`NetworkConfig`].

Scenario can be loaded from a json file or constructed in rust code.

```ignore
pub fn from_file(path: &Path) -> io::Result<Scenario>;
```

[`Scenario::run`] tries to produce all the described blocks and if
succeeded returns [`run_test::RuntimeStats`] wrapped in
a [`run_test::ScenarioResult`].

```ignore
pub fn run(&self) -> ScenarioResult<RuntimeStats, Error>;
```

[`run_test::RuntimeStats`] contain stats for every produced block.
Currently, only block production time is supported.

```ignore
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

[`run_test::ScenarioResult`] is a wrapper around a `Result` type which
adds a `homedir` field:

```ignore
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

To easily create new scenarios in rust code use [`ScenarioBuilder`].
Usage example can be found in `src/scenario_builder.rs` file.
