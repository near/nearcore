# Changelog

## 2.4.0

* Introduced a new status code for a missing block - 422 Unprocessable Content
> Block is considered as missing if rpc returned `UNKNOWN_BLOCK` error while requested block height is less than the latest block height

## 2.3.0

* Starting from this version we decided to use nearcore's version system

### Breaking changes

* Updated `rpc_handler` function ([#11806](https://github.com/near/nearcore/pull/11806) and [#11822](https://github.com/nearprotocol/nearcore/pull/11822)). The `jsonrpc` will start returning other HTTP codes than 200 OK for some errors:
  * On internal server error it will return 500
  * On timeout error it will return 408
  * On request validation error will return 400

## 0.2.3

* Added `send_tx` method which gives configurable execution guarantees options and potentially replaces existing `broadcast_tx_async`, `broadcast_tx_commit`
* Field `final_execution_status` is presented in the response of methods `tx`, `EXPERIMENTAL_tx_status`, `broadcast_tx_commit`, `send_tx`
* Allowed use json in request for methods `EXPERIMENTAL_tx_status`, `tx`, `broadcast_tx_commit`, `broadcast_tx_async`, `send_tx`
* Parameter `wait_until` (same entity as `final_execution_status` in the response) is presented as optional request parameter for methods `EXPERIMENTAL_tx_status`, `tx`, `send_tx`. The response will be returned only when the desired level of finality is reached

### Breaking changes

* Removed `EXPERIMENTAL_check_tx` method. Use `tx` method instead
* Removed `EXPERIMENTAL_broadcast_tx_sync` method. Use `send_tx` method instead
* `EXPERIMENTAL_tx_status`, `tx` methods now wait for recently sent tx (~3-6 seconds) and then show it. Previously, `UnknownTransaction` was immediately returned
* `EXPERIMENTAL_tx_status`, `tx` methods wait 10 seconds and then return `TimeoutError` for never existed transactions. Previously, `UnknownTransaction` was immediately returned

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
