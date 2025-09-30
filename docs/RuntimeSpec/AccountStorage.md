# Account Receipt Storage

There is a definition of all the keys and values we store in the Account Storage

## Received data

*key = `receiver_id: String`,`data_id: CryptoHash`*
*value = `Option<Vec<u8>>`*

Runtime saves incoming data from [DataReceipt](Receipts.md#data) until every [input_data_ids](Receipts.md#input_data_ids) in [postponed receipt](#postponed-receipts) [ActionReceipt](Receipts.md#actionreceipt) for `receiver_id` account is satisfied.

## Postponed receipts

For each awaited `DataReceipt` we store

*key = `receiver_id`,`data_id`*
*value = `receipt_id`*

Runtime saves incoming [ActionReceipt](Receipts.md#actionreceipt) until all ``
