# Changelog

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
