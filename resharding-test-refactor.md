# Resharding V3 Test Refactor: AAA Structure with Harness

## Problem

All ~35 resharding tests were opaque one-liners calling `test_resharding_v3_base(params)`.
The ~300-line base function hid everything: env setup, contract deployment, the main loop,
and all assertions. `TestReshardingParametersBuilder` mixed env config with test behavior
(loop actions) and assertions, making it hard to understand what any individual test does.

## Solution

Replace `TestReshardingParameters`, `TestReshardingParametersBuilder`,
`test_resharding_v3_base`, and `setup_global_contracts` with a `ReshardingHarness` that
exposes clear **Arrange-Act-Assert** phases in every test function.

## Design

### ReshardingHarnessBuilder

Handles env infrastructure that must be configured before the env is built:

- Topology: `num_accounts`, `num_clients`, `num_producers`, `num_validators`, `num_rpcs`, `num_archivals`
- Genesis / epoch config: `epoch_length`, `initial_balance`, `second_resharding_boundary_account`
- Behavior config: `chunk_ranges_to_drop`, `shuffle_shard_assignment_for_chunk_producers`, `track_all_shards`, `tracked_shard_schedule`, `load_memtries_for_tracked_shards`
- Runtime config: `limit_outgoing_gas`, `short_yield_timeout`
- Resharding config: `delay_flat_state_resharding`
- Run config: `num_epochs_to_wait`, `all_chunks_expected`, `disable_temporary_account_test`

`build()` does:
1. Compute derived accounts/clients/producers/validators/rpcs/archivals
2. Build epoch configs, shard layouts, genesis
3. Build `TestLoopBuilder`, configure runtime, build env, add drop conditions, warmup
4. Configure resharding delay on actors
5. Add default `temporary_account_during_resharding` loop action (unless disabled)
6. Return `ReshardingHarness`

### ReshardingHarness

```
Arrange:
  deploy_test_contract(account)       - deploy test contract, queue tx
  deploy_global_contract(deployer, mode) - deploy global contract, queue tx
  use_global_contract(user, identifier)  - settle pending txs, then queue use tx

Act:
  add_action(loop_action)             - add a loop action for the resharding run
  run()                               - auto-settle pending txs, run test loop

Assert:
  assert_completed()                  - verify all loop actions succeeded + trie sanity
  shutdown()                          - drain remaining events
```

### Nonce management

A single `next_nonce()` method returns monotonically increasing values starting at 1.
Used by all transaction-submitting methods (`deploy_test_contract`, `deploy_global_contract`,
`use_global_contract`, and the internal temporary account creation). This replaces the
previous split scheme of hardcoded nonces plus a separate counter starting at 100.

### Settle semantics

`settle()` is an internal method that advances the test loop and verifies pending
transactions succeeded. It is called automatically by:
- `use_global_contract()` — the deploy must be on-chain before use
- `run()` — any remaining pending transactions are settled before the main loop

### What every test looks like after

Simplest test:
```rust
fn slow_test_resharding_v3() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder().build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}
```

Test with contract deployment + loop actions:
```rust
fn slow_test_resharding_v3_delayed_receipts_left_child() {
    init_test_logger();
    let account: AccountId = "account4".parse().unwrap();
    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account);
    harness.add_action(call_burn_gas_contract(...));
    harness.add_action(check_receipts_presence_at_resharding_block(...));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}
```

Test with global contract (deploy/settle/use flow):
```rust
fn test_resharding_v3_global_contract_base(...) {
    let mut harness = ReshardingHarness::builder()
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.deploy_global_contract(deployer, deploy_mode);
    harness.use_global_contract(user, identifier); // settles deploy first
    harness.add_action(...);
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}
```

### Deleted

- `test_resharding_v3_base` function
- `TestReshardingParameters` struct
- `TestReshardingParametersBuilder` struct + impl
- `setup_global_contracts` free function

### Added

- `ReshardingHarness` struct + impl
- `ReshardingHarnessBuilder` struct + impl
- `compute_initial_accounts` as a free function (extracted from the builder)

## Verification

All 37 resharding tests pass with zero failures:

```
cargo test --package test-loop-tests --features test_features -- tests::resharding_v3
test result: ok. 37 passed; 0 failed; 0 ignored
```
