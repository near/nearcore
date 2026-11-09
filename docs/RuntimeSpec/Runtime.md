# Runtime

Runtime layer is used to execute smart contracts and other actions created by the users and preserve the state between the executions.
It can be described from three different angles: going step-by-step through various scenarios, describing the components
of the runtime, and describing the functions that the runtime performs.

## Scenarios

- Financial transaction -- we examine what happens when the runtime needs to process a simple financial transaction;
- Cross-contract call -- the scenario when the user calls a contract that in turn calls another contract.

## Components

The components of the runtime can be described through the crates:

- the `logic` module of `near-vm-runner` -- describes the interface that smart contract uses to interact with the blockchain.
  Encapsulates the behavior of the blockchain visible to the smart contract, e.g. fee rules, storage access rules, promise rules;
- `near-vm-runner` crate -- a wrapper around Wasmtime that does the actual execution of the smart contract code. It exposes the
  interface provided by its `logic` module to the smart contract;
- `near-runtime` crate -- encapsulates the logic of how transactions and receipts should be handled. If it encounters
  a smart contract call within a transaction or a receipt it calls `near-vm-runner`, for all other actions, like account
  creation, it processes them in-place.

The utility crates are:

- `near-parameters` -- encapsulates the configuration of fees and limits;
- `runtime-params-estimator` -- benchmarks the runtime and generates the config with the fees.

Separately, from the components we describe [the Bindings Specification](Components/BindingsSpec/BindingsSpec.md) which is an
important part of the runtime that specifies the functions that the smart contract can call from its host -- the runtime.
The specification is defined in the `logic` module of `near-vm-runner`, and it is exposed to the smart contract in `near-vm-runner`.

## Functions

- Receipt consumption and production
- Fees
- Virtual machine
- Verification
