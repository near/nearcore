A collection of smart-contract used in nearcore tests.

Rust contracts are built via `build.rs`, the Assembly Script contract
is build manually and committed to the git repository.

If you want to use a contract from rust core, add

```toml
[dev-dependencies]
near-test-contracts = { path = "../near-test-contracts" }
```

to the Cargo.toml and use `near_test_contract::rs_contract()`.

If you want to use a contract from an integration test, you can read
the wasm file directly from the `./res` directory.  To populate
`./res`, you need to make sure that this crate was compiled.
