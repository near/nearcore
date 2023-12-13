A temporary (placeholder) implementation of the `Wallet Contract`.

See https://github.com/near/NEPs/issues/518.

Must not use in production!

To use the contract you need to make sure that this crate was compiled.
The contract is built via `build.rs` and WASM file is generated to the `./res` directory.

If you want to use the contract from nearcore, add this crate as a dependency
to the Cargo.toml and use `near_wallet_contract::wallet_contract()`.
