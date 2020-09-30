# Changelog

## Unreleased

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
