# A collection of smart-contract used in nearcore tests.

## Contracts used in rust tests and pytests
Rust contracts are build via `build.rs`, the Assembly Script contract is build manually and committed to the git repository.
`res/near_evm.wasm` and `res/ZombieOwnership.bin` are taken from https://github.com/near/near-evm/tree/a651e9be680b59cca9aad86c1f9e9e9b54ad9c06 and it's for reproduce a
performance issue encountered in EVM contracts.

If you want to use a contract from rust core, add

```toml
[dev-dependencies]
near-test-contracts = { path = "../near-test-contracts" }
```

to the Cargo.toml and use `near_test_contract::rs_contract()`.

If you want to use a contract from an integration test, you can read the wasm file directly from the `./res` directory.
To populate `./res`, you need to make sure that this crate was compiled.

## Contracts for testing local reproduce transaction execution
There're three contracts that reflect different state-dependent transaction execution on testnet.
These contracts are supposed to be deployed on testnet and then obtain a copy of state with rpc, and 
reproduce its transaction locally with (WIP) near-vm-standalone with custom scripts.
- simple-state: a simple, one huge state stored for contract, simple contract get/set
- multiple-state: contract store multiple states under different keys on trie with get/set method
- cross-contract: cross contract call to read state from another contract or update state of another contract

Above scenarios should cover enough use cases of developer downloading states of testnet contracts, and reproduce it locally.  
 