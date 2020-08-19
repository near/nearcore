# Changelog

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
