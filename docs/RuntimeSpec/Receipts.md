# Receipt

All cross-contract (we assume that each account lives in its own shard) communication in Near happens through Receipts.

Receipts are stateful in a sense that they serve not only as messages between accounts but also can be stored in the account storage to await DataReceipts.

Each receipt has a [`predecessor_id`](#predecessor_id) (who sent it) and [`receiver_id`](#receiver_id) the current account.

Receipts are one of 2 types: action receipts or data receipts.

Data Receipts are receipts that contains some data for some `ActionReceipt` with the same `receiver_id`.
Data Receipts have 2 fields: the unique data identifier `data_id` and `data` the received result.
`data` is an `Option` field and it indicates whether the result was a success or a failure. If it's `Some`, it means
the remote execution was successful and it represents the result as a vector of bytes.

Each `ActionReceipt` also contains fields related to data:

- [`input_data_ids`](#input_data_ids) - a vector of input data with the `data_id`s required for the execution of this receipt.
- [`output_data_receivers`](#output_data_receivers) - a vector of output data receivers. It indicates where to send outgoing data.
Each `DataReceiver` consists of `data_id` and `receiver_id` for routing.

Before any action receipt is executed, all input data dependencies need to be satisfied.
Which means all corresponding data receipts have to be received.
If any of the data dependencies are missing, the action receipt is postponed until all missing data dependencies arrive.

Because chain and runtime guarantees that no receipts are missing, we can rely that every action receipt will be executed eventually ([Receipt Matching explanation](#receipt-matching)).

Each `Receipt` has the following fields:

#### predecessor_id

- **`type`**: `AccountId`

The account_id which issued a receipt.
In case of a gas or deposit refund, the account ID is `system`.

#### receiver_id

- **`type`**: `AccountId`

The destination account_id.

#### receipt_id

- **`type`**: `CryptoHash`

An unique id for the receipt.

#### receipt

- **`type`**: [ActionReceipt](#actionreceipt) | [DataReceipt](#datareceipt)

There are 2 types of Receipt: [ActionReceipt](#actionreceipt) and [DataReceipt](#datareceipt). An `ActionReceipt` is a request to apply [Actions](Actions.md), while a `DataReceipt` is a result of the application of these actions.

## ActionReceipt

`ActionReceipt` represents a request to apply actions on the `receiver_id` side. It could be derived as a result of a `Transaction` execution or another `ActionReceipt` processing. `ActionReceipt` consists the following fields:

#### signer_id

- **`type`**: `AccountId`

An account_id which signed the original [transaction](Transactions.md).
In case of a deposit refund, the account ID is `system`.

#### signer_public_key

- **`type`**: `PublicKey`

The public key of an [AccessKey](../DataStructures/AccessKey.md) which was used to sign the original transaction.
In case of a deposit refund, the public key is empty (all bytes are 0).

#### gas_price

- **`type`**: `u128`

Gas price which was set in a block where the original [transaction](Transactions.md) has been applied.

#### output_data_receivers

- **`type`**: `[DataReceiver{ data_id: CryptoHash, receiver_id: AccountId }]`

If smart contract finishes its execution with some value (not Promise), runtime creates a [`DataReceipt`]s for each of the `output_data_receivers`.

#### input_data_ids

- **`type`**: `[CryptoHash]_`

`input_data_ids` are the receipt data dependencies. `input_data_ids` correspond to `DataReceipt.data_id`.

#### actions

- **`type`**: [`FunctionCall`](Actions.md#functioncallaction) | [`TransferAction`](Actions.md#transferaction) | [`StakeAction`](Actions.md#stakeaction) | [`AddKeyAction`](Actions.md#addkeyaction) | [`DeleteKeyAction`](Actions.md#deletekeyaction) | [`CreateAccountAction`](Actions.md#createaccountaction) | [`DeleteAccountAction`](Actions.md#deleteaccountaction)

## DataReceipt

`DataReceipt` represents a final result of some contract execution.

#### data_id

- **`type`**: `CryptoHash`

A unique `DataReceipt` identifier.

#### data

- **`type`**: `Option([u8])`

Associated data in bytes. `None` indicates an error during execution.

## Creating Receipt

Receipts can be generated during the execution of a [SignedTransaction](/RuntimeSpec/Transactions#signed-transaction) (see [example](./Scenarios/FinancialTransaction.md)) or during application of some `ActionReceipt` which contains a [`FunctionCall`](#actions) action. The result of the `FunctionCall` could be either another `ActionReceipt` or a `DataReceipt` (returned data).

## Receipt Matching

Runtime doesn't require that Receipts come in a particular order. Each Receipt is processed individually. The goal of the `Receipt Matching` process is to match all [`ActionReceipt`s](#actionreceipt) to the corresponding [`DataReceipt`s](#datareceipt).

## Processing ActionReceipt

For each incoming [`ActionReceipt`](#actionreceipt) runtime checks whether we have all the [`DataReceipt`s](#datareceipt) (defined as [`ActionsReceipt.input_data_ids`](#input_data_ids)) required for execution. If all the required [`DataReceipt`s](#datareceipt) are already in the [storage](#received-datareceipt), runtime can apply this `ActionReceipt` immediately. Otherwise we save this receipt as a [Postponed ActionReceipt](#postponed-actionreceipt). Also we save [Pending DataReceipts Count](#pending-datareceipt-count) and [a link from pending `DataReceipt` to the `Postponed ActionReceipt`](#pending-datareceipt-for-postponed-actionreceipt). Now runtime will wait for all the missing `DataReceipt`s to apply the `Postponed ActionReceipt`.

#### Postponed ActionReceipt

A Receipt which runtime stores until all the designated [`DataReceipt`s](#datareceipt) arrive.

- **`key`** = `account_id`,`receipt_id`
- **`value`** = `[u8]`

_Where `account_id` is [`Receipt.receiver_id`](#receiver_id), `receipt_id` is [`Receipt.receipt_id`](#receipt_id) and value is a serialized [`Receipt`](#receipt-1) (which type must be [ActionReceipt](#actionreceipt))._

#### Pending DataReceipt Count

A counter which counts pending [`DataReceipt`s](#datareceipt) for a [Postponed Receipt](#postponed-actionreceipt) initially set to the length of missing [`input_data_ids`](#input_data_ids) of the incoming `ActionReceipt`. It's decrementing with every new received [`DataReceipt`](#datareceipt):

- **`key`** = `account_id`,`receipt_id`
- **`value`** = `u32`

_Where `account_id` is AccountId, `receipt_id` is CryptoHash and value is an integer._

#### Pending DataReceipt for Postponed ActionReceipt

We index each pending `DataReceipt` so when a new [`DataReceipt`](#datareceipt) arrives we connect it to the [Postponed Receipt](#postponed-actionreceipt) it belongs to.

- **`key`** = `account_id`,`data_id`
- **`value`** = `receipt_id`

## Processing DataReceipt

#### Received DataReceipt

First of all, runtime saves the incoming `DataReceipt` to the storage as:

- **`key`** = `account_id`,`data_id`
- **`value`** = `[u8]`

_Where `account_id` is [`Receipt.receiver_id`](#receiver_id), `data_id` is [`DataReceipt.data_id`](#data_id) and value is a [`DataReceipt.data`](#data) (which is typically a serialized result of the call to a particular contract)._

Next, runtime checks if there are any [`Postponed ActionReceipt`](#postponed-actionreceipt) waiting for this `DataReceipt` by querying [`Pending DataReceipt` to the Postponed Receipt](#pending-datareceipt-for-postponed-actionreceipt). If there is no postponed `receipt_id` yet, we do nothing else. If there is a postponed `receipt_id`, we do the following:

- decrement [`Pending Data Count`](#pending-datareceipt-count) for the postponed `receipt_id`
- remove found [`Pending DataReceipt` to the `Postponed ActionReceipt`](#pending-datareceipt-for-postponed-actionreceipt)

If [`Pending DataReceipt Count`](#pending-datareceipt-count) is now 0 that means all the [`Receipt.input_data_ids`](#input_data_ids) are in storage and runtime can safely apply the [Postponed Receipt](#postponed-actionreceipt) and remove it from the store.

## Case 1: Call to multiple contracts and await responses

Suppose runtime got the following `ActionReceipt`:

```python
# Non-relevant fields are omitted.
Receipt{
    receiver_id: "alice",
    receipt_id: "693406"
    receipt: ActionReceipt {
        input_data_ids: []
    }
}
```

If execution return Result::Value

Suppose runtime got the following `ActionReceipt` (we use a python-like pseudo code):

```python
# Non-relevant fields are omitted.
Receipt{
    receiver_id: "alice",
    receipt_id: "5e73d4"
    receipt: ActionReceipt {
        input_data_ids: ["e5fa44", "7448d8"]
    }
}
```

We can't apply this receipt right away: there are missing DataReceipt'a with IDs: ["e5fa44", "7448d8"]. Runtime does the following:

```python
postponed_receipts["alice,5e73d4"] = borsh_serialize(
    Receipt{
        receiver_id: "alice",
        receipt_id: "5e73d4"
        receipt: ActionReceipt {
            input_data_ids: ["e5fa44", "7448d8"]
        }
    }
)
pending_data_receipt_store["alice,e5fa44"] = "5e73d4"
pending_data_receipt_store["alice,7448d8"] = "5e73d4"
pending_data_receipt_count = 2
```

_Note: the subsequent Receipts could arrived in the current block or next, that's why we save [Postponed ActionReceipt](#postponed-actionreceipt) in the storage_

Then the first pending `Pending DataReceipt` arrives:

```python
# Non-relevant fields are omitted.
Receipt {
    receiver_id: "alice",
    receipt: DataReceipt {
        data_id: "e5fa44",
        data: "some data for alice",
    }
}
```

```python
data_receipts["alice,e5fa44"] = borsh_serialize(Receipt{
    receiver_id: "alice",
    receipt: DataReceipt {
        data_id: "e5fa44",
        data: "some data for alice",
    }
};
pending_data_receipt_count["alice,5e73d4"] = 1`
del pending_data_receipt_store["alice,e5fa44"]
```

And finally the last `Pending DataReceipt` arrives:

```python
# Non-relevant fields are omitted.
Receipt{
    receiver_id: "alice",
    receipt: DataReceipt {
        data_id: "7448d8",
        data: "some more data for alice",
    }
}
```

```python
data_receipts["alice,7448d8"] = borsh_serialize(Receipt{
    receiver_id: "alice",
    receipt: DataReceipt {
        data_id: "7448d8",
        data: "some more data for alice",
    }
};
postponed_receipt_id = pending_data_receipt_store["alice,5e73d4"]
postponed_receipt = postponed_receipts[postponed_receipt_id]
del postponed_receipts[postponed_receipt_id]
del pending_data_receipt_count["alice,5e73d4"]
del pending_data_receipt_store["alice,7448d8"]
apply_receipt(postponed_receipt)
```

## Receipt Validation Error

Some postprocessing validation is done after an action receipt is applied. The validation includes:

- Whether the generated receipts are valid. A generated receipt can be invalid, if, for example, a function call
generates a receipt to call another function on some other contract, but the contract name is invalid. Here there are
mainly two types of errors:

- account id is invalid. If the receiver id of the receipt is invalid, a

```rust
/// The `receiver_id` of a Receipt is not valid.
InvalidReceiverId { account_id: AccountId },
``` 

error is returned.

- some action is invalid. The errors returned here are the same as the validation errors mentioned in [actions](Actions.md).
- Whether the account still has enough balance to pay for storage. If, for example, the execution of one function call
action leads to some receipts that require transfer to be generated as a result, the account may no longer have enough
balance after the transferred amount is deducted. In this case, a

```rust
/// ActionReceipt can't be completed, because the remaining balance will not be enough to cover storage.
LackBalanceForState {
    /// An account which needs balance
    account_id: AccountId,
    /// Balance required to complete an action.
    amount: Balance,
},
```
