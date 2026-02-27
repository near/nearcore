# Integration tests

## TestLoopEnv

`TestLoopEnv` is a framework that enables writing multi-node tests for NEAR protocol
components. It simulates an entire blockchain environment within a single test,
allowing for synchronous testing of complex scenarios.

This framework is an attempt to
achieve the best of all our previous frameworks, to make tests powerful,
deterministic and easy to write and understand. The core test loop framework is in
`core/async/src/test_loop/mod.rs`.

### Directory structure

- `src/setup/` — builder, environment, node setup, drop conditions
- `src/utils/` — shared helpers (account creation, network simulation, etc.)
- `src/examples/` — **start here** — minimal self-contained examples demonstrating the API
- `src/tests/` — full integration tests

Here's a step-by-step guide on how to create a test.

## 1. Build the environment

`TestLoopBuilder` provides a high-level API for setting up the test environment.
See `examples/setup.rs` for a full set of setup examples.

The simplest setup uses all defaults (one validator, one shard, no RPC):

```rust
let mut env = TestLoopBuilder::new().build().warmup();
```

Configure topology with `.validators()`, `.num_shards()`, and `.enable_rpc()`:

```rust
let mut env = TestLoopBuilder::new()
    .validators(2, 1)  // 2 block+chunk producers, 1 chunk-only validator
    .num_shards(2)
    .enable_rpc()
    .build()
    .warmup();
```

Add user accounts and override genesis parameters as needed:

```rust
let mut env = TestLoopBuilder::new()
    .add_user_account(&user_account, Balance::from_near(10))
    .epoch_length(10)
    .gas_limit(Gas::from_teragas(300))
    .protocol_version(PROTOCOL_VERSION - 1)
    .build()
    .warmup();
```

Other available genesis overrides: `genesis_height`, `transaction_validity_period`,
`max_inflation_rate`, `minimum_stake_ratio`, `gas_prices`.

### Manual genesis setup

DO NOT USE `.genesis()`/`.clients()` unless you need direct access to the
genesis object before passing it to the builder (e.g. to derive epoch configs
from it). See `resharding_example_test` for such a case.

```rust
let validators_spec = create_validators_spec(1, 0);
let clients = validators_spec_clients(&validators_spec);
let genesis = TestLoopBuilder::new_genesis_builder()
    .validators_spec(validators_spec)
    .build();
let mut env = TestLoopBuilder::new()
    .genesis(genesis)
    .clients(clients)
    .build()
    .warmup();
```

## 2. Trigger and execute events

`TestLoopEnv` provides two main abstractions for interacting with nodes:

- **`TestLoopNode`** — read-only access to a node's state (head, client, store,
  runtime queries). Holds `&TestLoopData`, so accessors like `env.node()` take
  `&self` and multiple nodes can be held simultaneously.
- **`TestLoopNodeMut`** — mutable access to a node for operations like
  `client_actor()` or `view_client_actor()`. Holds `&mut TestLoopData`.
- **`NodeRunner`** — drives the test loop forward while observing a specific
  node (run until a condition, produce blocks, execute transactions).

Convenience accessors on `TestLoopEnv`:

| Accessor | Returns | Selects |
|---|---|---|
| `env.validator()` / `env.validator_runner()` | `TestLoopNode` / `NodeRunner` | First validator (index 0) |
| `env.rpc_node()` / `env.rpc_runner()` | `TestLoopNode` / `NodeRunner` | The RPC node |
| `env.node(i)` / `env.node_runner(i)` | `TestLoopNode` / `NodeRunner` | Node at index `i` |
| `env.node_for_account(id)` / `env.runner_for_account(id)` | `TestLoopNode` / `NodeRunner` | Node with given account id |

Mutable node access is available via `_mut()` variants (`env.validator_mut()`,
`env.node_mut(i)`, etc.) which return `TestLoopNodeMut`:

```rust
// Access ClientActor for adversarial testing.
env.validator_mut().client_actor().adv_produce_blocks_on(3, true, height_selection);
```

Example usage:

```rust
// Execute a transaction via the RPC node.
env.rpc_runner().run_tx(tx, Duration::seconds(5));

// Wait for the validator to reach a certain height.
env.validator_runner().run_until_head_height(10);
```

`NodeRunner::run_until` can be used to progress the blockchain until a
condition is met. The condition closure receives a `&TestLoopNode` for
the associated node:

```rust
env.validator_runner().run_until(
    |node| node.head().epoch_id == expected_epoch_id,
    Duration::seconds(20),
);
```

Note: The time here is not actual real-world time. `TestLoopEnv` simulates the clock
to ensure high speed and reproducibility of test results. This allows tests to
run quickly while still accurately modeling time-dependent blockchain behavior.

## 3. Assert expected outcomes

Verify that the test produced the expected results.

```rust
let account = env.validator().view_account_query(&account_id).unwrap();
assert_eq!(account.amount, Balance::from_near(42));
```

After that, properly shut down the test environment:

```rust
env.shutdown_and_drain_remaining_events(Duration::seconds(20));
```

## Advanced features

### Node lifecycle

Nodes can be killed and restarted to test recovery scenarios, or new nodes can
be added to a running cluster. See `examples/node_lifecycle.rs` for full examples.

```rust
// Kill a node and restart it later.
let killed_state = env.kill_node(&identifier);
// ... run other nodes ...
env.restart_node(&new_identifier, killed_state);

// Add a brand new non-validator node.
let new_node_state = env.node_state_builder().account_id(&new_account_id).build();
env.add_node(identifier, new_node_state);
```

### Utilities

#### Account helpers

`utils/account.rs` provides various utilities for creating account IDs.
Use `create_account_id` instead of manual parsing:

```rust
// Good:
let user = create_account_id("user");

// Avoid:
let user: AccountId = "user".parse().unwrap();
```

## Examples

The `src/examples/` directory contains minimal self-contained tests demonstrating
key API patterns. These are the best starting point for writing new tests.

| Example | Demonstrates |
|---|---|
| `basic.rs` | Token transfer, contract deploy & call, JSON-RPC queries |
| `setup.rs` | Builder API: defaults, validators, shards, user accounts, genesis overrides, manual setup |
| `node_lifecycle.rs` | Killing/restarting a node, adding a new node to a running cluster |
| `gas_limit.rs` | Gas limit behavior verification |
| `delayed_receipts.rs` | Creating and observing delayed receipts |
| `missing_chunk.rs` | Triggering missing chunks using adversarial messages |
| `validator_rotation.rs` | Validator rotation across epochs |
| `resharding.rs` | Dynamic resharding (manual genesis setup) |
| `raw_client.rs` | Low-level client setup without `TestLoopBuilder` |

## Migration

For historical context, there are multiple existing ways for writing such
tests. The following list presents these methods in order of their development:

* `setup_mock_all_validators(...)` - very powerful, spawns all
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
