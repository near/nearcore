# Changelog

## 1.32.x

* Add `nightly` feature to NEAR Indexer Framework to respect this feature for `nearcore` lib (requried for `betanet`)

## 1.26.0

* `state_changes` field is moved from the top-level `StreamerMessage` to `IndexerShard` struct to align better with the sharded nature of NEAR protocol. In the future, when nearcore will be able to track only a subset of shards, this API will work naturally, so we take pro-active measures to solidify the APIs
* All the NEAR Indexer Framework types were extracted to a separate crate `near-indexer-primitives`
* Increase the streamer size from 16 to 100 in order to increase the speed of streaming messages (affects reindexing jobs)

## Breaking changes

The field `state_changes` is moved from the root of `StreamerMessage`
to the `IndexerShard.state_changes` and now contains only changes related
to the specific shard.

## 0.10.1

* (mainnet only) Add additional handler to inject restored receipts to the block #47317863. See [PR 4248](https://github.com/near/nearcore/pull/4248) for reference

## 0.10.0

* Add additional logs on Indexer Framework start
* Avoid double genesis validation by removing the explicit validation on Indexer instantiation
* Replaced the method how genesis is being read to optimize memory usage

## Breaking changes

Since the change of reading genesis method to optimize memory usage. You'd be able to iterate over genesis records with `near_config.genesis.for_each_record(|record| {...})`. Nothing is changed for you your indexer does nothing about genesis records.

## 0.9.2

* Optimize the delayed receipts tracking process introduced in previous version to avoid indexer stuck.

## 0.9.1

* Introduce a hot-fix. Execution outcome for local receipt might appear not in the same block as the receipt. Local receipts are not saved in database and unable to be fetched. To include a receipt in `IndexerExecutionOutcomeWithReceipt` and prevent NEAR Indexer Framework from panic we fetch previous blocks to find corresponding local receipt to include.

## 0.9.0 (do not use this version, it contains a bug)

* Introduce `IndexerShard` structure which contains corresponding chunks and `IndexerExecutionOutcomeWithReceipt`
* `receipt` field in `IndexerExecutionOutcomeWithReceipt` is no longer optional as it used to be always set anyway,
  so now we explicitly communicate this relation ("every outcome has a corresponding receipt") through the type system
* Introduce `IndexerExecutionOutcomeWithOptionalReceipt` which is the same as `IndexerExecutionOutcomeWithReceipt`
  but with optional `receipt` field.

## Breaking changes

* `IndexerChunkView` doesn't contain field `receipt_execution_outcomes` anymore, this field has been moved to `IndexerShard`
* `StreamerMessage` structure was aligned more with NEAR Protocol specification and now looks like:
  ```
  StreamerMessage {
    block: BlockView,
    shards: Vec<IndexerShard>,
    state_changes: StateChangesView,
  }
  ```

## 0.8.1

* Add `InitConfigArgs` and `indexer_init_configs`

As current `neard::init_configs()` signature is a bit hard to read and use we introduce `InitConfigArgs` struct to make a process of passing arguments more explicit. That's why we introduce `indexer_init_configs` which is just a wrapper on `neard::init_configs()` but takes `dir` and `InitConfigArgs` as an input.

## 0.8.0

* Upgrade dependencies

## Breaking change

actix update changed the way we used to deal with starting the node and getting necessary data from neard.
The `start()` method was deleted, `Indexer` struct doesn't have `actix_runtime` anymore and runtime should be
created and started on the Indexer implementation, not on the Indexer Framework one.

## 0.7.0

* State changes return changes with cause instead of kinds

## Breaking changes

* `StreamerMessage` now contains `StateChangesView` which is an alias for `Vec<StateChangesWithCauseView>`, previously it contained `StateChangesKindsView`

## 0.6.0

* Add a way to turn off the requirement to wait for the node to be fully synced before starting streaming.

## Breaking changes

* `IndexerConfig` was extended with another field `await_for_node_synced`. Corresponding enum is `AwaitForNodeSyncedEnum` with variants:
  - `WaitForFullSync` - await for node to be fully synced (previous default behaviour)
  - `StreamWhileSyncing`- start streaming right away while node is syncing (it's useful in case of Indexing from genesis)

## 0.5.0

* Attach receipt execution outcomes to a relevant chunk and preserve their execution order (!)

## Breaking changes

Since #3529 nearcore stores `ExecutionOutcome`s in their execution order, and we can also attribute outcomes to specific chunk. That's why:

* `receipt_execution_outcomes` was moved from `StreamerMessage` to a relevant `IndexerChunkView`
* `ExecutionOutcomesWithReceipts` type alias was removed (just use `Vec<IndexerExecutionOutcomeWithReceipt>` instead)

## 0.4.0

* Prepend chunk's receipts with local receipts to attach latter to specific chunk

## Breaking changes

* For local receipt to have a relation to specific chunk we have prepended them to original receipts in particular chunk
  as in the most cases local receipts are executed before normal receipts. That's why there is no reason to have `local_receipts`
  field in `StreamerMessage` struct anymore. `local_receipts` field was removed.

## 0.3.1

* Add local receipt to `receipt_execution_outcomes` if possible

## 0.3.0

### Breaking changes

* To extended the `receipt_execution_outcomes` with information about the corresponding receipt we had to break the API
  (the old outcome structure is just one layer deeper now [under `execution_outcome` field])

## 0.2.0

* Refactor the way of fetching `ExecutionOutcome`s (use the new way to get all of them for specific block)
* Rename `StreamerMessage.outcomes` field to `receipt_execution_outcomes` and change type to `HashMap<CryptoHash, ExecutionOutcomeWithId>` and now it includes only `ExecutionOutcome`s for receipts (no transactions)
* Introduce `IndexerTransactionWithOutcome` struct to contain `SignedTransactionView` and `ExecutionOutcomeWithId` for the transaction
* Introduce `IndexerChunkView` to replace `StreamerMessage.chunks` to include `IndexerTransactionWithOutcome` vec in `transactions`
