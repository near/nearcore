# Account Receipt Storage

These are the keys and values that the runtime stores for receipts of an account

## Received data

*key = `receiver_id: String`,`data_id: CryptoHash`*
*value = `Option<Vec<u8>>`*

Runtime saves incoming data from [DataReceipt](Receipts.md#data) until every [input_data_ids](Receipts.md#input_data_ids) in [postponed receipt](#postponed-receipts) [ActionReceipt](Receipts.md#actionreceipt) for `receiver_id` account is satisfied.

## Postponed receipts

For each awaited `DataReceipt` we store

*key = `receiver_id`,`data_id`*
*value = `receipt_id`*

Runtime saves incoming [ActionReceipt](Receipts.md#actionreceipt) until all required `DataReceipt` items for the same `receiver_id` have been received and matched, so that the postponed `receipt_id` receipt can be executed once its data dependencies are fully satisfied. See the [Receipt Case](Receipts.md#case-1-call-to-multiple-contracts-and-await-responses) for more details.

The postponed receipt itself is stored under

*key = `receiver_id`,`receipt_id`*
*value = `Receipt`*

## Pending data count

*key = `receiver_id`,`receipt_id`*
*value = `u32`*

The number of input data items that the postponed receipt `receipt_id` still waits for. When it reaches zero, the
runtime executes the receipt and removes it.

## Yielded receipts

A receipt created by `promise_yield_create` waits for data from `promise_yield_resume` or for a timeout. The runtime
stores:

- *key = `receiver_id`,`data_id`*, *value = `Receipt`*: the yielded receipt.
- *key = `receiver_id`,`data_id`*, *value = `PromiseYieldStatus`*: `Yielded`, or `ResumeInitiated` after the first
  `promise_yield_resume`.

For a receipt created by `promise_yield_create_with_id`, the runtime also stores the mapping between the yield id that
the contract chose and the `data_id`:

- *key = `receiver_id`,`yield_id`*, *value = `data_id`*
- *key = `receiver_id`,`data_id`*, *value = `yield_id`*
