# Overview

This document describes the high-level architecture of nearcore. The focus here
is on the implementation of the blockchain protocol, not the protocol itself.
For reference documentation of the protocol, please refer to
[nomicon](https://nomicon.io/)

## Bird's Eye View

![](images/architecture.svg)

Since nearcore is an implementation of NEAR blockchain protocol, its goal is to
produce a binary that runs as a blockchain client. More specifically, the
`neard` binary can do the following:

- generate files necessary to start a node
- start a node with a given folder that contains required data

There are three major components of nearcore:

* Network. We implement a peer-to-peer network that powers communications
  between blockchain nodes. This includes initiating connections with other
  nodes, maintaining a view of the entire network, routing messages to the right
  nodes, etc. Network is a somewhat standalone module, though it uses
  information about validators to propagate and validate certain messages.

* Chain. Chain is responsible for building and maintaining the blockchain data
  structure. This includes block and chunk production and processing, consensus,
  and validator selection. However, chain is not responsible for actually
  applying transactions and receipts.

* Runtime. Runtime is the execution engine that actually applies transactions
  and receipts and performs state transitions. This also includes compilation of
  contracts (wasm binaries) and execution of contract calls.

## Entry Points

`neard/src/main.rs` contains the main function that starts a blockchain node.
However, this file mostly only contains the logic to parse arguments and
dispatch different commands.

`start_with_config` in `nearcore/src/lib.rs` is the actual entry point and it
starts all the actors.

## Code Map

This section contains some high-level overview of important crates and data
structures.

### `core/primitives`

This crate contains most of the types that are shared across different crates.

### `core/primitives-core`

This crate contains types needed for runtime.

### `core/store/trie`

This directory contains the MPT state implementation. Note that we usually use
`TrieUpdate` to interact with the state.

### `chain/chain`

This crate contains most of the chain logic (consensus, block processing, etc).
`ChainUpdate::process_block` is where most of the block processing logic
happens.

**Architecture Invariant**: interface between chain and runtime is defined by
`RuntimeAdapter`. All invocations of runtime goes through `RuntimeAdapter`

**State update**

The blockchain state can be changed in the following two ways:

* Applying a chunk. This is how the state is normally updated: through
  `Runtime::apply`.
* State sync. State sync can happen in two cases:
  * A node is far enough behind the most recent block and triggers state sync to
    fast forward to the state of a very recent block without having to apply
    blocks in the middle.
  * A node is about to become validator for some shard in the next epoch, but it
    does not yet have the state for that shard. In this case, it would run state
    sync through the `catchup` routine.

### `chain/chunks`

This crate contains most of the sharding logic which includes chunk creation,
distribution, and processing. `ShardsManager` is the main struct that
orchestrates everything here.

### `chain/client`

This crate defines two important structs, `Client` and `ViewClient`. `Client`
includes everything necessary for the chain (without network and runtime) to
function and runs in a single thread. `ViewClient` is a "read-only" client that
answers queries without interfering with the operations of `Client`.
`ViewClient` runs in multiple threads.

### `chain/network`

This crate contains the entire implementation of the p2p network used by NEAR
blockchain nodes.

Two important structs here: `PeerManagerActor` and `Peer`. Peer manager
orchestrates all the communications from network to other components and from
other components to network. `Peer` is responsible for low-level network
communications from and to a given peer. Peer manager runs in one thread while
each `Peer` runs in its own thread.

**Architecture Invariant**: Network communicates to `Client` through
`NetworkClientMessages` and to `ViewClient` through `NetworkViewClientMessages`.
Conversely, `Client` and `ViewClient` communicates to network through
`NetworkRequests`.

### `chain/epoch_manager`

This crate is responsible for determining validators and other epoch related
information such as epoch id for each epoch.

Note: `EpochManager` is constructed in `NightshadeRuntime` rather than in
`Chain`, partially because we had this idea of making epoch manager a smart
contract.

### `chain/jsonrpc`

This crate implements [JSON-RPC](https://www.jsonrpc.org/) API server to enable
submission of new transactions and inspection of the blockchain data, the
network state, and the node status. When a request is processed, it generates a
message to either `ClientActor` or `ViewClientActor` to interact with the
blockchain. For queries of blockchain data, such as block, chunk, account, etc,
the request usually generates a message to `ViewClientActor`. Transactions, on
the other hand, are sent to `ClientActor` for further processing.

### `runtime/runtime`

This crate contains the main entry point to runtime -- `Runtime::apply`. This
function takes `ApplyState`, which contains necessary information passed from
chain to runtime, and a list of `SignedTransaction` and a list of `Receipt`, and
returns a `ApplyResult`, which includes state changes, execution outcomes, etc.

**Architecture Invariant**: The state update is only finalized at the end of
`apply`. During all intermediate steps state changes can be reverted.

### `runtime/near-vm-logic`

`VMLogic` contains all the implementations of host functions and is the
interface between runtime and wasm. `VMLogic` is constructed when runtime
applies function call actions. In `VMLogic`, interaction with NEAR blockchain
happens in the following two ways:

* `VMContext`, which contains lightweight information such as current block
  hash, current block height, epoch id, etc.
* `External`, which is a trait that contains functions to interact with
  blockchain by either reading some nontrivial data, or writing to the
  blockchain.

### `runtime/near-vm-runner`

`run` function in `runner.rs` is the entry point to the vm runner. This function
essentially spins up the vm and executes some function in a contract. It
supports different wasm compilers including wasmer0, wasmer2, and wasmtime
through compile-time feature flags. Currently we use wasmer0 and wasmer2 in
production. The `imports` module exposes host functions defined in
`near-vm-logic` to WASM code. In other words, it defines the ABI of the
contracts on NEAR.

### `neard`

As mentioned before, `neard` is the crate that contains that main entry points.
All the actors are spawned in `start_with_config`. It is also worth noting that
`NightshadeRuntime` is the struct that implements `RuntimeAdapter`.

### `core/store/src/db.rs`

This file contains schema (DBCol) of our internal RocksDB storage - a good
starting point when reading the code base.

## Cross Cutting Concerns

### Observability

The [tracing](https://tracing.rs) crate is used for structured, hierarchical
event output and logging. We also integrate [Prometheus](https://prometheus.io)
for light-weight metric output. See the [style](./style.md) documentation for
more information on the usage.

### Testing

Rust has built-in support for writing unit tests by marking functions
with `#[test]` directive.  Take full advantage of that!  Testing not
only confirms that what was written works the way it was intended but
also help during refactoring since the caught unintended behaviour
changes.

Not all tests are created equal though and while some can need only
milliseconds to run, others may run for several seconds or even
minutes.  Tests that take a long time should be marked as such with an
`expensive_tests` feature, for example:

```rust
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_catchup_random_single_part_sync() {
    test_catchup_random_single_part_sync_common(false, false, 13)
}
```

Such tests will be ignored by default and can be executed by using
`--ignored` or `--include-ignored` flag as in `cargo test --
--ignored` or by compiling the tests with `expensive_tests` feature
enabled.

Because expensive tests are not run by default, they are also not run
in CI.  Instead, they are run nightly and need to be explicitly
included in `nightly/expensive.txt` file; for example:

```text
expensive --timeout=1800 near-client near_client tests::catching_up::test_catchup_random_single_part_sync
expensive --timeout=1800 near-client near_client tests::catching_up::test_catchup_random_single_part_sync --features nightly
```

For more details regarding nightly tests see `nightly/README.md`.

Note that what counts as a slow test isn’t exactly defined as of now.
If it takes just a couple seconds than it’s probably fine.  Anything
slower should probably be classified as expensive test.  In
particular, if libtest complains the test takes more than 60 seconds
than it definitely is.
