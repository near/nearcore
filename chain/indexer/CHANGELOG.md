# Changelog

## 0.3.0


### Breaking changes

* To extended the `receipt_execution_outcomes` with information about the corresponding receipt we had to break the API 
(the old outcome structure is just one layer deeper now [under `execution_outcome` field])
 `ExecutionOutcomesWithReceipts` type

## 0.2.0

* Refactor the way of fetching `ExecutionOutcome`s (use the new way to get all of them for specific block)
* Rename `StreamerMessage.outcomes` field to `receipt_execution_outcomes` and change type to `HashMap<CryptoHash, ExecutionOutcomeWithId>` and now it includes only `ExecutionOutcome`s for receipts (no transactions)
* Introduce `IndexerTransactionWithOutcome` struct to contain `SignedTransactionView` and `ExecutionOutcomeWithId` for the transaction
* Introduce `IndexerChunkView` to replace `StreamerMessage.chunks` to include `IndexerTransactionWithOutcome` vec in `transactions` 
