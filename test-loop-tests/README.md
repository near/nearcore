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
let validators_spec = create_validator_spec(4, 0);
let clients = validator_spec_clients(&validators_spec);
let genesis = TestLoopBuilder::new_genesis_builder()
    .validators_spec(validators_spec)
    // ...more configuration if needed
    // for example `add_user_accounts_simple`, `epoch_length`, etc.
    .build();
let mut env = TestLoopBuilder::new()
    .genesis(genesis)
    .epoch_config_store_from_genesis()
// Or alternatively when more epoch config configuration is needed:
// let epoch_config_store = TestEpochConfigBuilder.from_genesis(&genesis)
//    .shuffle_shard_assignment_for_chunk_producers(true)
//    ...more configuration
//    .build_store_for_single_version();
//  and then use it as part of test loop builder:
//  .epoch_config_store(epoch_config_store)
    .clients(clients)
    .build()
    .warmup();
```

## 2. Trigger and execute events

`TestLoopNode` can be used to interact with the cluster performing actions such as
executing transactions or waiting for blocks to be produced.

```rust
let validator_node = TestLoopNode::for_account(&env.node_datas, &clients[0]);
validator_node.run_tx(&mut env.test_loop, ..);
validator_node.run_until_head_height(&mut env.test_loop, ..);
```

Also `run_until` method can be used to progress the blockchain until a certain
condition is met:

```rust
test_loop.run_until(
    |test_loop_data| {
        validator_node.head(test_loop_data).epoch_id == expected_epoch_id
    },
    Duration::seconds(20),
);
```

Note: The time here is not actual real-world time. `TestLoopEnv` simulates the clock
to ensure high speed and reproducibility of test results. This allows tests to
run quickly while still accurately modeling time-dependent blockchain behavior.

## 3. Assert expected outcomes

Verify that the test produced the expected results.

```rust
let account = validator_node.query_account(env.test_loop_data(), ..);
assert_eq!(account.balance, 42 * ONE_NEAR);
```

After that, properly shut down the test environment:

```rust
env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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
