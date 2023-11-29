A temporary (placeholder) implementation of the `Wallet Contract`.

See https://github.com/near/NEPs/issues/518.

Must not use in production!

To use the contract you need to make sure that this crate was compiled.
The contract is built via `build.rs` and WASM file is generated to the `./res` directory.
You might want to `touch build.rs` before `cargo build` to force cargo to generate the WASM file.

If you want to use the contract from rust core, add

```toml
[dev-dependencies]
near-wallet-contract = { path = "../near-wallet-contract" }
```

to the Cargo.toml and use `near_wallet_contract::wallet_contract()`.

If you want to use a contract from an integration test, you can read
the wasm file directly from the `./res` directory.
