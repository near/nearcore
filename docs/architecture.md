# Architecture

This document describes the high-level architecture of nearcore.
The focus here is on the implementation of the blockchain protocol, not the protocol itself.
For reference documentation of the protocol, please refer to [nomicon](https://nomicon.io/)

## Bird's Eye View

![](images/architecture.svg)

Since nearcore is an implementation of NEAR blockchain protocol, its goal is to produce a binary that runs as a blockchain client.
More specifically, the neard binary can do the following:
- generate files necessary to start a node
- start a node with a given folder that contains required data

## Entry Points

`neard/src/main.rs` contains the main function that starts a blockchain node.
However, this file mostly only contains the logic to parse arguments and dispatch different commands.

`start_with_config` in `neard/src/lib.rs` is the actual entry point and it starts all the actors.

## Code Map

This section contains some high-level overview of important crates and data structures.

### `core/primitives`

This crate contains most of the types that are shared across different crates.

### `core/primitives-core`

This crate contains types needed for runtime. 

**Architecture Invariant**: this crate needs to be published separately since near-skd-rs depends on it.
It cannot have any dependencies on other nearcore crates.

### `core/store/trie`

This directory contains the MPT state implementation.
Note that we usually use `TrieUpdate` to interact with the state.

### `chain/chain`

This crate contains most of the chain logic (consensus, block processing, etc). 
`ChainUpdate::process_block` is where most of the block processing logic happens.

**Architecture Invariant**: interface between chain and runtime is defined by `RuntimeAdapter`.
All invocations of runtime goes through `RuntimeAdapter`

### `chain/chunks`

This crate contains most of the sharding logic which includes chunk creation, distribution, and processing.
`ShardsManager` is the main struct that orchestrates everything here.

### `chain/client`

This crate defines two important structs, `Client` and `ViewClient`.
`Client` includes everything necessary for the chain (without network and runtime) to function and runs in a single thread.
`ViewClient` is a "read-only" client that answers queries without interfering with the operations of `Client`.
`ViewClient` runs in multiple threads.

### `chain/network`

This crate contains the entire implementation of the p2p network used by NEAR blockchain nodes.

Two important structs here: `PeerManagerActor` and `Peer`. 
Peer manager orchestrates all the communications from network to other components and from other components to network.
`Peer` is responsible for low-level network communications from and to a given peer.
Peer manager runs in one thread while each `Peer` runs in its own thread.

**Architecture Invariant**: Network communicates to `Client` through `NetworkClientMessages` and to `ViewClient` through `NetworkViewClientMessages`.
Conversely, `Client` and `ViewClient` communicates to network through `NetworkRequests`.

### `chain/epoch_manager`

This crate is responsible for determining validators and other epoch related information such as epoch id for each epoch.

Note: `EpochManager` is constructed in `NightshadeRuntime` rather than in `Chain`, partially because we had this idea of making epoch manager a smart contract.

### `chain/jsonrpc`

This crate implements [JSON-RPC](https://www.jsonrpc.org/) API server to enable submission of new transactions and inspection of the blockchain data, the network state, and the node status.

### `runtime/runtime`

This crate contains the main entry point to runtime -- `Runtime::apply`. 
This function takes `ApplyState`, which contains necessary information passed from chain to runtime, and a list of `SignedTransaction` and a list of `Receipt`, and returns a `ApplyResult`, which includes state changes, execution outcomes, etc.

**Architecture Invariant**: The state update is only finalized at the end of `apply`. 
During all intermediate steps state changes can be reverted.

### `runtime/near-vm-logic`

`VMLogic` contains all the implementations of host functions and is the interface between runtime and wasm. 
`VMLogic` is constructed when runtime applies function call actions.
In `VMLogic`, interaction with NEAR blockchain happens in the following two ways:
- `VMContext`, which contains lightweight information such as current block hash, current block height, epoch id, etc.
- `External`, which is a trait that contains functions to interact with blockchain by either reading some nontrivial data, or writing to the blockchain.

### `neard`

As mentioned before, `neard` is the crate that contains that main entry points.
It is also worth noting that `NightshadeRuntime` is the struct that implements `RuntimeAdapter`.







