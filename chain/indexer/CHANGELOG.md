# Changelog

## 0.9.0

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

* `StreamerMessage` now contains `StateChangesView` which is an alias for  `Vec<StateChangesWithCauseView>`, previously it contained `StateChangesKindsView`

## 0.6.0

* Add a way to turn off the requirement to wait for the node to be fully synced before starting streaming.

## Breaking changes

* `IndexerConfig` was extended with another field `await_for_node_synced`. Corresponding enum is `AwaitForNodeSyncedEnum` with variants:
  * `WaitForFullSync` - await for node to be fully synced (previous default behaviour)
  * `StreamWhileSyncing`- start streaming right away while node is syncing (it's useful in case of Indexing from genesis)

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
