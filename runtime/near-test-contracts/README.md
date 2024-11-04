A collection of smart-contract used in nearcore tests.

Rust contracts are built via `build.rs`, the Assembly Script contract
is build manually and committed to the git repository.

If you want to use a contract from rust code, add

```toml
[dev-dependencies]
near-test-contracts = { path = "../near-test-contracts" }
```

to the Cargo.toml and use `near_test_contract::rs_contract()`.
