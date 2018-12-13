<img src="docs/logo.svg" width="200px" />

## NEAR Protocol - scalable and usable blockchain

![Build status](https://img.shields.io/gitlab/pipeline/nearprotocol/nearcore.svg)
<a href="https://discord.gg/gBtUFKR">![Discord](https://img.shields.io/discord/490367152054992913.svg)</a>

NEAR Protocol is a new smart-contract platform that delivers scalability and usability.

Through sharding it aims to linearly scale with number of validation nodes on the network.

Leveraging WebAssembly, TypeScript, more sane contract management, ephemeral accounts and many other advancement, NEAR
makes using blockchain protocol for developers and consumers way easier compared to competitors.

## Status

Project is currently under heavy development. Please see Issues and Milestones to checkout the current progress and work items.

High level milestones:

 - [ ] DevNet: a tool with fully working State Transition + WebAssembly.
 - [ ] MVB: Minimum viable blockchain with smart contracts, supporting TxFlow and on chain governance.  
 - [ ] Shard chains: support for fully scalable sharded blockchain.

## Building

### Setup rust

```bash
$ curl https://sh.rustup.rs -sSf | sh
$ rustup component add clippy-preview
```

You may need to activate the environment via `. ~/.cargo/env` to use `cargo`.

### Build & Run from source code

```bash
# Download NEAR Core code.
git clone https://github.com/nearprotocol/nearcore
cd nearcore
```

It will build the first time and then run:

```bash
cargo run
```

### Logging

For runnable apps (devnet, nearcore, etc.), you can use
the `--log-level` option to configure the log level across all internal crates.
You can also use the `RUST_LOG` environment variable, with `env_logger`
[semantics](https://docs.rs/env_logger/0.6.0/env_logger/#enabling-logging)
to override the log level for specific targets. `RUST_LOG` can also be used in
integration tests which spawn runnable apps.

Example:
```bash
$ RUST_LOG=runtime=debug cargo run -- --log-level warn
```

### Simple Install for Mac and Linux

Coming soon.

## DevNet

DevNet is a development tool that runs WebAssembly and State transition without running blockchain/consensus functionality.

First, generate key pair (saves a key pair into `keystore` folder to use with `rpc.py`):

```bash
cargo run --package keystore -- keygen
```

Then build and run DevNet:

```bash
cargo run --release --package=devnet
```

Try submitting transactions or views via JSON RPC:

```bash
# See usage of rpc helper script
./scripts/rpc.py --help

# Get usage of sub command
./scripts/rpc.py send_money --help

# Send money
./scripts/rpc.py send_money -r bob -a 1

# Deploy a smart contract
./scripts/rpc.py deploy test_contract core/wasm/runtest/res/wasm_with_mem.wasm

# Call method 'run_test' for contract 'test_contract'
./scripts/rpc.py schedule_function_call test_contract run_test
./scripts/rpc.py schedule_function_call test_contract run_test --args 10 20

# Call view function 'run_test' for contract 'test_contract'
./scripts/rpc.py call_view_function test_contract run_test
./scripts/rpc.py call_view_function test_contract run_test --args 10 20

# View state for Bob's account
./scripts/rpc.py view_account -a bob

# Create an account
./scripts/rpc.py create_account cindy 1

# View full state db of the contract
./scripts/rpc.py view_state test_contract
```

## Development

If you are planning to contribute, there are few more things to setup

### Setup git hooks

```bash
./scripts/setup_hooks.sh
```

### Setup rustfmt for your editor (optional)
Installation instructions [here](https://github.com/rust-lang-nursery/rustfmt#running-rustfmt-from-your-editor)

### Lints
We currently use [clippy](https://github.com/rust-lang-nursery/rust-clippy) to enforce certain standards.
This check is run automatically during CI builds, and in a `pre-commit`
hook. You can run do a clippy check with `./scripts/run_clippy.sh`.

## Documentation

Check out documentation at [http://docs.rs/nearcore](http://docs.rs/nearcore)
