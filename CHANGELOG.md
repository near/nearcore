# Changelog

## [unreleased]

### Protocol Changes
* Upgrade from Wasmer 0 to Wasmer 2, bringing better performance and reliability. [#4934](https://github.com/near/nearcore/pull/4934)
* Lower regular_op_cost (execution of a single WASM instruction) from 3_856_371 to 2_207_874. [#4979](https://github.com/near/nearcore/pull/4979)
* Lower data receipt cost and base cost of `ecrecover` host function.
* Upgrade from one shard to four shards (Simple Nightshade Phase 0)

## `1.21.0` [09-06-2021]

### Protocol Changes

* Fix some receipts that were stuck previously due to #4228. [#4248](https://github.com/near/nearcore/pull/4248)

### Non-protocol Changes

* Improve contract module serialization/deserialization speed by 30% [#4448](https://github.com/near/nearcore/pull/4448)
* Make `AccountId` strictly typed and correct by construction [#4621](https://github.com/near/nearcore/pull/4621)
* Address test dependency issue #4556 [#4606](https://github.com/near/nearcore/pull/4606). [#4622](https://github.com/near/nearcore/pull/4622).
* Fix neard shutdown issue [#4429](https://github.com/near/nearcore/pull/4429). #[4442](https://github.com/near/nearcore/pull/4442)

## `1.20.0` [07-26-2021]

### Protocol Changes

* Introduce new host functions `ecrecover` and `ripemd160`. [#4380](https://github.com/near/nearcore/pull/4380)
* Make `Account` a versioned struct. [#4089](https://github.com/near/nearcore/pull/4089)
* Limit the size of transactions to 4MB. [#4107](https://github.com/near/nearcore/pull/4107)
* Cap maximum gas price to 20x of minimum gas price. [#4308](https://github.com/near/nearcore/pull/4308), [#4382](https://github.com/near/nearcore/pull/4382)
* Fix `storageUsage` for accounts that were affected by [#3824](https://github.com/near/nearcore/issues/3824). [#4272](https://github.com/near/nearcore/pull/4274)
* Fix a bug in computation of gas for refunds. [#4405](https://github.com/near/nearcore/pull/4405)

### Non-protocol Changes

* Compile contracts after state sync. [#4344](https://github.com/near/nearcore/pull/4344)
* Introduce `max_gas_burnt_view` config for rpc. [#4381](https://github.com/near/nearcore/pull/4381)
* Fix wasmer 0.17 memory leak [#4411](https://github.com/near/nearcore/pull/4411)
