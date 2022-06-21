# Changelog

## [unreleased]

### Protocol Changes

* Stabilized `alt_bn128_g1_multiexp`, `alt_bn128_g1_sum`, `alt_bn128_pairing_check` host functions [#6813](https://github.com/near/nearcore/pull/6813).

### Non-protocol Changes

* Added `path` option to `StoreConfig` which makes location to the
  RocksDB configurable via `config.json` file (at `store.path` path)
  rather than being hard-coded to `data` directory in neard home
  directory [#6938](https://github.com/near/nearcore/pull/6938)
* Removed `testnet` alias for `localnet` command; it’s been deprecated
  since 1.24 [#7033](https://github.com/near/nearcore/pull/7033)
* Removed undocumented `unsafe_reset_all` and `unsafe_reset_data`
  commands; they were deprecated since 1.25
* Key files can use `private_key` field instead of `secret_key` now;
  this improves interoperability with near cli which uses the former
  name [#7030](https://github.com/near/nearcore/issues/7030)

## 1.27.0 [2022-06-22]

### Protocol Changes

* Introduced protobuf encoding as the new network protocol. Borsh support will be removed in two releases as per normal protocol upgrade policies [#6672](https://github.com/near/nearcore/pull/6672)

### Non-protocol Changes

* Added `near_peer_message_received_by_type_bytes` [#6661](https://github.com/near/nearcore/pull/6661) and `near_dropped_message_by_type_and_reason_count` [#6678](https://github.com/near/nearcore/pull/6678) metrics.
* Removed `near_<msg-type>_{total,bytes}` [#6661](https://github.com/near/nearcore/pull/6661), `near_<msg-type>_dropped`, `near_drop_message_unknown_account` and `near_dropped_messages_count` [#6678](https://github.com/near/nearcore/pull/6678) metrics.
* Added `near_action_called_count` metric [#6679]((https://github.com/near/nearcore/pull/6679)
* Removed `near_action_<action-type>_total` metrics [#6679]((https://github.com/near/nearcore/pull/6679)
* Added `near_build_info` metric which exports neard’s build information [#6680](https://github.com/near/nearcore/pull/6680)
* Make it possible to update logging at runtime: [#6665](https://github.com/near/nearcore/pull/6665)
* Use correct cost in gas profile for adding function call key [#6749](https://github.com/near/nearcore/pull/6749)

## 1.26.0 [2022-05-18]

### Protocol Changes

* Enable access key nonce range for implicit accounts to prevent tx hash collisions [#5482](https://github.com/near/nearcore/pull/5482)
* Include `promise_batch_action_function_call_weight` host function on the runtime [#6285](https://github.com/near/nearcore/pull/6285) [#6536](https://github.com/near/nearcore/pull/6536)
* Increase deployment cost [#6397](https://github.com/near/nearcore/pull/6397)
* Limit the number of locals per contract to 1_000_000
* Ensure caching all nodes in the chunk for which touching trie node cost was charged, reduce cost of future reads in a chunk [#6628](https://github.com/near/nearcore/pull/6628)
* Lower storage key limit to 2 KiB

### Non-protocol Changes

* Switch to LZ4+ZSTD compression from Snappy in RocksDB [#6365](https://github.com/near/nearcore/pull/6365)
* Moved Client Actor to separate thread - should improve performance [#6333](https://github.com/near/nearcore/pull/6333)
* Safe DB migrations using RocksDB checkpoints [#6282](https://github.com/near/nearcore/pull/6282)
* [NEP205](https://github.com/near/NEPs/issues/205): Configurable start of protocol upgrade voting [#6309](https://github.com/near/nearcore/pull/6309)
* Make max_open_files and col_state_cache_size parameters configurable [#6584](https://github.com/near/nearcore/pull/6584)
* Make RocksDB block_size configurable [#6631](https://github.com/near/nearcore/pull/6631)
* Increase default max_open_files RocksDB parameter from 512 to 10k [#6607](https://github.com/near/nearcore/pull/6607)
* Use kebab-case names for neard subcommands to make them consistent with flag names.  snake_case names are still valid for existing subcommands but kebab-case will be used for new commands.

## 1.25.0 [2022-03-16]

### Protocol Changes
* `max_gas_burnt` has been increased to 300.

### Non-protocol Changes
* More Prometheus metrics related to epoch, sync state, node version, chunk fullness and missing chunks have been added.
* Progress bar is now displayed when downloading `config.json` and `genesis.json`.
* Status line printed in logs by `neard` is now more descriptive.
- `view_state` is now a command of `neard`; `state-viewer` is no longer a separate binary.
- `RUST_LOG` environment variable is now correctly respected.
- `NetworkConfig::verify` will now fail if configuration is invalid rather than printing error and continuing.
- Fixed a minor bug which resulted in DB Not Found errors when requesting chunks.
- Updated to wasmer-near 2.2.0 which fixes a potential crash and improves cost estimator working.
- `neard init` will no longer override node or validator private keys.
- Rosetta RPC now populates `related_transactions` field.
- Rosetta RPC support is now compiled in by default. The feature still needs to be explicitly turned on and is experimental.
- Rosetta RPC /network/status end point correctly works on non-archival nodes.
- `unsafe_reset_all` and `unsafe_reset_data` commands are now deprecated. Use `rm` explicitly instead.

## 1.24.0 [2022-02-14]

### Protocol Changes

* Enable access key nonce range for implicit accounts to prevent tx hash collisions.
* Upgraded our version of pwasm-utils to 0.18 -- the old one severely undercounted stack usage in some cases.

### Non-protocol Changes

* Fix a bug in chunk requesting where validator might request chunks even if parent block hasn’t been processed yet.
* Fix memory leak in near-network.
* Change block sync to request 5 blocks at a time
* Change NUM_ORPHAN_ANCESTORS_CHECK to 3

## 1.23.0 [2021-12-13]

### Protocol Changes

* Further lower regular_op_cost from 2_207_874 to 822_756.
* Limit number of wasm functions in one contract to 10_000. [#4954](https://github.com/near/nearcore/pull/4954)
* Add block header v3, required by new validator selection algorithm
* Move to new validator selection and sampling algorithm. Now we would be able to use all available seats. First step to enable chunk only producers.

### Non-protocol Changes

* Increase RocksDB cache size to 512 MB for state column to speed up blocks processing [#5212](https://github.com/near/nearcore/pull/5212)

## 1.22.0 [2021-11-15]

### Protocol Changes
* Upgrade from Wasmer 0 to Wasmer 2, bringing better performance and reliability. [#4934](https://github.com/near/nearcore/pull/4934)
* Lower regular_op_cost (execution of a single WASM instruction) from 3_856_371 to 2_207_874. [#4979](https://github.com/near/nearcore/pull/4979)
* Lower data receipt cost and base cost of `ecrecover` host function.
* Upgrade from one shard to four shards (Simple Nightshade Phase 0)

## 1.21.0 [2021-09-06]

### Protocol Changes

* Fix some receipts that were stuck previously due to #4228. [#4248](https://github.com/near/nearcore/pull/4248)

### Non-protocol Changes

* Improve contract module serialization/deserialization speed by 30% [#4448](https://github.com/near/nearcore/pull/4448)
* Make `AccountId` strictly typed and correct by construction [#4621](https://github.com/near/nearcore/pull/4621)
* Address test dependency issue #4556 [#4606](https://github.com/near/nearcore/pull/4606). [#4622](https://github.com/near/nearcore/pull/4622).
* Fix neard shutdown issue [#4429](https://github.com/near/nearcore/pull/4429). #[4442](https://github.com/near/nearcore/pull/4442)

## 1.20.0 [2021-07-26]

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
