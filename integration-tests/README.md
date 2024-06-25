# Integration tests
 
## TestLoopEnv

`TestLoopEnv` is a framework that enables writing multi-node tests for NEAR protocol 
components. It simulates an entire blockchain environment within a single test,
allowing for synchronous testing of complex scenarios.

We recommend to use `TestLoopEnv` for writing multi-node tests and put new 
tests into `src/test_loop/tests` folder. This framework is an attempt to 
achieve the best of all our previous frameworks, to make tests powerful, 
deterministic and easy to write and understand. The doc how it works is on 
`core/async/src/test_loop.rs`.
 
Here's a step-by-step guide on how to create a test.

## 1. Build the environment

Most important parameters are configured through the genesis.
The main part of building the environment involves constructing genesis data,
including the initial state, using `TestGenesisBuilder`:

```rust
let builder = TestLoopBuilder::new();

let initial_balance = 10000 * ONE_NEAR;
let accounts = (0..NUM_ACCOUNTS)
    .map(|i| format!("account{}", i).parse().unwrap())
    .collect::<Vec<AccountId>>();

let mut genesis_builder = TestGenesisBuilder::new();
genesis_builder
    .genesis_time_from_clock(&builder.clock())
    .protocol_version_latest()
    .genesis_height(10000)
    .epoch_length(EPOCH_LENGTH)
    .shard_layout_simple_v1(&["account2", "account4", "account6"])
    // ...more configuration if needed...

for account in &accounts {
    genesis_builder.add_user_account_simple(account.clone(), initial_balance);
}
let genesis = genesis_builder.build();

let TestLoopEnv { mut test_loop, datas: node_datas } =
    builder.genesis(genesis).clients(client_accounts).build();
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

Perform the actions you want to test, such as money transfers. See
`execute_money_transfers` implementation for more details.

```rust
execute_money_transfers(&mut test_loop, &node_datas, &accounts);
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

If it takes more than 20 seconds to reach the desired height, the test will panic.

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
exposed RPC handlers to test different behaviour. Quite obsolete as well,
exposed to flakiness.
* different environments spawning clients: `TestEnv`, `TestReshardingEnv`, ...
Good middle ground for testing specific features, but doesn't test actual 
network behaviour. Modifications like forcing skipping chunks require a lot
of manual intervention.

If test became problematic, it is encouraged to migrate it to `TestLoopEnv`. 
However, it would be _extremely_ hard to migrate the logic precisely. Instead, 
migrate tests only if they make sense to you and their current implementation
became a huge burden. We hope that reproducing such logic in `TestLoopEnv` is 
much easier. 

Enjoy!
