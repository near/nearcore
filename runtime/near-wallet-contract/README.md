A temporary (placeholder) implementation of the `Wallet Contract`.

See https://github.com/near/NEPs/issues/518.

Must not use in production!

If you want to use the contract from nearcore, add this crate as a dependency
to the Cargo.toml and use `near_wallet_contract::wallet_contract()`.

The contract can be rebuilt by `build.rs` if you run `cargo build` with `--features build_wallet_contract` flag.
It would generate WASM file and save it to the `./res` directory.
