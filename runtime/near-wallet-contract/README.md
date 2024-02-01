A temporary (placeholder) implementation of the `Wallet Contract`.

See https://github.com/near/NEPs/issues/518.

Must not use in production!

Currently, the contract build is disabled!

The `build.rs` generates WASM file and saves it to the `./res` directory.

If you want to use the contract from nearcore, add this crate as a dependency
to the Cargo.toml and use `near_wallet_contract::wallet_contract()`.

In order to review changes to the WASM file, rebuild the wallet contract locally
(remove it beforehand to make sure it was rebuilt later) and check the hashes
by running `check_wallet_contract` and `check_wallet_contract_magic_bytes` tests in `src/lib.rs`.
