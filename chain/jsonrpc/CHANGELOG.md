# Changelog

## 0.2.2

* Extended error structures to be more explicit. See [#2976 decision comment for reference](https://github.com/near/nearcore/issues/2976#issuecomment-865834617)

## 0.2.1

* Refactored methods:
  * `broadcast_tx_async`
  * `broadcast_tx_commit`
  * `EXPERIMENTAL_broadcast_tx_sync`
  * `EXPERIMENTAL_check_tx`
  * `EXPERIMENTAL_tx_status`
  * `tx`

## Breaking changes

Response from `EXPERIMENTAL_broadcast_tx_sync` and `EXPERIMENTAL_check_tx` doesn't return `is_routed` 
field anymore. In case of a transaction getting routed an error will be returned. Also, `EXPERIMENTAL_check_tx` 
returns response with transaction hash instead of empty body.

* Added `EXPERIMENTAL_tx_status` endpoint exposing receipts in addition to all
  the rest data available in `tx` endpoint
  ([#3383](https://github.com/nearprotocol/nearcore/pull/3383))

## 0.2.0

* Started tracking all the JSON-RPC API changes.

### Breaking changes

* Removed `EXPERIMENTAL_genesis_records` API to shrink the memory footprint on
  the node since we don't need genesis records (potentially gigabytes of data)
  for operation.

## 0.1.0

* Start the versioning timeline here
