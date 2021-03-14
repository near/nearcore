# Changelog

## Pending

- Introduce `alt_bn128_g1_multiexp`, `alt_bn128_g1_sum` and `alt_bn128_pairing_check` host functions to `near-vm-logic`.

## 3.0.0

- Dependency on `near-core-primitives`

## 2.3.0

- Added disk cache to `near-vm-runner`
- Contains a dependency on `near-primitives`, so it's impossible to publish to crates.io
- Needed to differentiate from `2.2.0` to avoid patch conflicts.

## 2.2.0

- Add ability to specify protocol version when initializing VMLogic.
- Add implicit account creation logic for a new protocol version.
- Add `near-runtime-utils` crate that is shared across runtime crates.

## 2.1.1

- Bumped `borsh` dependency from `0.7.0` to `0.7.1` [#3238](https://github.com/nearprotocol/nearcore/pull/3238)

## 2.1.0

- Implemented better compiler testing with `--compile-only` flag [#3074](https://github.com/nearprotocol/nearcore/pull/3074)
- Added proper `wasmtime` support with `--vm-kind` flag.
- Added `lightbeam` with feature and run with `--vm-kind wasmtime`

## 2.0.0

- Same as `1.2.0`, but since it contained incompatible change, we had to bump major version.

## 1.2.0

- Implement gas profiler [#3038](https://github.com/nearprotocol/nearcore/pull/3038)

## 1.1.0

- Move `sha256`, `keccak256` and `keccak512` form External trait to logic, since they are pure functions. [#3030](https://github.com/nearprotocol/nearcore/issues/3030)

## 1.0.0

- Bumped `borsh` dependency from `0.6.2` to `0.7.0` [#2878](https://github.com/nearprotocol/nearcore/pull/2878)
- Added fees for cost of loading and compiling contract [#2845](https://github.com/nearprotocol/nearcore/pull/2845)
- Added `executor_id` to `ExecutionOutcome` [#2903](https://github.com/nearprotocol/nearcore/pull/2903)
- Select default VM in the compile time, not runtime. [#2897](https://github.com/nearprotocol/nearcore/pull/2897)
- Move to Wasmer 0.17.1 [#2905](https://github.com/nearprotocol/nearcore/pull/2905)
- Fix cost overflow in tx_cost computation [#2915](https://github.com/nearprotocol/nearcore/pull/2915)
- Added FAQ on Wasm integration [#2917](https://github.com/nearprotocol/nearcore/pull/2917)
