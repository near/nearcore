<br />
<br />

<p align="center">
<img src="docs/logo.svg" width="240">
</p>

<br />
<br />


## NEAR Protocol - scalable and usable blockchain

![Build status](https://img.shields.io/gitlab/pipeline/nearprotocol/nearcore.svg)
[![codecov](https://codecov.io/gh/nearprotocol/nearcore/branch/master/graph/badge.svg)](https://codecov.io/gh/nearprotocol/nearcore)
[![dependency status](https://deps.rs/repo/github/nearprotocol/nearcore/status.svg)](https://deps.rs/repo/github/nearprotocol/nearcore)
[![Join the community on Spectrum](https://withspectrum.github.io/badge/badge.svg)](https://spectrum.chat/near)
<a href="https://discord.gg/gBtUFKR">![Discord](https://img.shields.io/discord/490367152054992913.svg)</a>

* ⚖️ NEAR Protocol is a new smart-contract platform that delivers scalability and usability.
* 🛠 Through sharding, it will linearly scale with the number of validation nodes on the network.
* 🗝 Leveraging WebAssembly, TypeScript, more sane contract management, ephemeral accounts and many other advancements, NEAR
finally makes using a blockchain protocol easy for both developers and consumers.

## Quick start

[Check out quick start documentation](https://docs.nearprotocol.com/quick-start), specifically:
  - [Working with Smart Contracts in NEAR Studio](https://docs.nearprotocol.com/working-smart-contracts)
  - [Running local DevNet](https://docs.nearprotocol.com/development-environment/advanced)
  - [Join TestNet](https://docs.nearprotocol.com/development-environment/run-your-own-testnet)
  
Develop and deploy contracts without any setup required using [NEAR Studio](https://studio.nearprotocol.com):

[![NEAR Studio](https://github.com/nearprotocol/NEARStudio/blob/master/demos/guest_book.gif)](https://studio.nearprotocol.com)


## Status

This project is currently under heavy development. Please see Issues and Milestones to checkout the current progress and work items.

High level milestones:

 - [x] DevNet: a tool with fully working State Transition + WebAssembly.
 - [x] AlphaNet: Multi-node smart-contract platform.
 - [ ] BetaNet: Added economics and enchanced security.
 - [ ] TestNet: added governance module, ready to launch as MVB
 - [ ] MainNet: Launched as Minimum Viable Blockchain.
 - [ ] Shard chains: Support for scalable sharding.

## Running 

Checkout the [Running Locally](https://github.com/nearprotocol/nearcore/tree/master/ops#running-locally) and [Running Remotely](https://github.com/nearprotocol/nearcore/tree/master/ops#running-remotely) sections.

## Developing

Checkout the [Developing NEAR Runtime](https://github.com/nearprotocol/nearcore/tree/master/ops#developing-near-runtime) and [Building and Pushing Docker Image](https://github.com/nearprotocol/nearcore/tree/master/ops#building-and-pushing-docker-image) sections.

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

To add new target (e.g. `info!(target: "my target", "hello")`), 
add the desired target to the list in `node/cli/src/service.rs` in `configure_logging` function.

### Contributions

If you are planning to contribute, there are few more things to setup

#### Setup git hooks

```bash
./scripts/setup_hooks.sh
```

#### Setup rustfmt for your editor (optional)
Installation instructions [here](https://github.com/rust-lang-nursery/rustfmt#running-rustfmt-from-your-editor)

#### Lints
We currently use [clippy](https://github.com/rust-lang-nursery/rust-clippy) to enforce certain standards.
This check is run automatically during CI builds, and in a `pre-commit`
hook. You can run do a clippy check with `./scripts/run_clippy.sh`.

