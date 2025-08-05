# Integration tests

## TestLoopEnv

`TestLoopEnv` is a framework that enables writing multi-node tests for NEAR protocol
components. It simulates an entire blockchain environment within a single test,
allowing for synchronous testing of complex scenarios.

This framework is an attempt to
achieve the best of all our previous frameworks, to make tests powerful,
deterministic and easy to write and understand. The doc how it works is on
`core/async/src/test_loop.rs`.

Here's a step-by-step guide on how to create a test.

## 1. Build the environment

See `test_cross_shard_token_transfer` for the minimalistic test setup example.

Most important parameters are configured through the genesis.
The main part of building the environment involves constructing genesis data,
including the initial state, using `TestGenesisBuilder`:

```rust
let shard_layout = ShardLayout::multi_shard_custom(create_account_ids(["account0"]).to_vec(), 1);
let validators_spec = create_validator_spec(shard_layout.num_shards() as usize, 0);
let clients = validator_spec_clients(&validators_spec);
let genesis = TestLoopBuilder::new_genesis_builder()
    .shard_layout(shard_layout)
    .validators_spec(validators_spec)
    // ...more configuration if needed
    // for example `add_user_accounts_simple`, `epoch_length`, etc.
    .build();
let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
// Or alternatively when more epoch config configuration is needed:
// let epoch_config_store = TestEpochConfigBuilder.from_genesis(&genesis)
//    .shuffle_shard_assignment_for_chunk_producers(true)
//    ...more configuration
//    .build_store_for_single_version();
let mut test_loop_env = TestLoopBuilder::new()
    .genesis(genesis)
    .epoch_config_store(epoch_config_store)
    .clients(clients)
    .build()
    .warmup();
```

## 2. Trigger and execute events

First, query the clients for desired chain information, such as which nodes are
responsible for tracking specific shards. Refer to the `ClientQueries` implementation
for more details on available queries.

```rust
let first_epoch_tracked_shards = {
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    clients.tracked_shards_for_each_client()
};
```

Perform the actions you want to test, such as money transfers, contract
deployment and execution, specific validator selection, etc. See
`execute_money_transfers` implementation for inspiration.

```rust
execute_money_transfers(&mut test_loop, &node_datas, &accounts).unwrap();
```

Then, use the `run_until` method to progress the blockchain until a certain
condition is met:

```rust
let client_handle = node_datas[0].client_sender.actor_handle();
test_loop.run_until(
    |test_loop_data| {
        test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10020
    },
    Duration::seconds(20),
);
```

Note: The time here is not actual real-world time. `TestLoopEnv` simulates the clock
to ensure high speed and reproducibility of test results. This allows tests to
run quickly while still accurately modeling time-dependent blockchain behavior.

## 3. Assert expected outcomes

Verify that the test produced the expected results. For example, if your test
environment is designed to have nodes change the shards they track, you can
assert this behavior as follows:

```rust
let clients = node_datas
    .iter()
    .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
    .collect_vec();
let later_epoch_tracked_shards = clients.tracked_shards_for_each_client();
assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);
```

After that, properly shut down the test environment:

```rust
TestLoopEnv { test_loop, datas: node_datas }
    .shutdown_and_drain_remaining_events(Duration::seconds(20));
```

## Migration

For historical context, there are multiple existing ways for writing such
tests. The following list presents these methods in order of their development:

* `run_actix(... setup_mock_all_validators(...))` - very powerful, spawns all
actors required for multi-node chain to operate and supports network
communication among them. However, very hard to understand, uses a lot of
resources and almost not maintained.
* pytest - quite powerful as well, spawns actual nodes in Python and uses
exposed RPC handlers to test different behavior. Quite obsolete as well,
exposed to flakiness.
* different environments spawning clients: `TestEnv`, `TestReshardingEnv`, ...
Good middle ground for testing specific features, but doesn't test actual
network behavior. Modifications like forcing skipping chunks require a lot
of manual intervention.

If test became problematic, it is encouraged to migrate it to `TestLoopEnv`.
However, it would be _extremely_ hard to migrate the logic precisely. Instead,
migrate tests only if they make sense to you and their current implementation
became a huge burden. We hope that reproducing such logic in `TestLoopEnv` is
much easier.

Enjoy!
