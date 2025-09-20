# Cross-Contract Call

This guide assumes that you have read the [Financial Transaction](FinancialTransaction.md) section.

Suppose Alice is a calling a function `reserve_trip(city: String, date: u64)` on a smart contract deployed to a `travel_agency`
account which in turn calls `reserve(date: u64)` on a smart contract deployed to a `hotel_near` account and attaches
a callback to method `hotel_reservation_complete(date: u64)` on `travel_agency`.

<img src="/images/receipt_flow_diagram.svg" alt="Receipt Flow Diagram"/>

## Pre-requisites

It possible for Alice to call the `travel_agency` in several different ways.

In the simplest scenario Alice has an account `alice_near` and she has a full access key.
She then composes the following transaction that calls the `travel_agency`:

```
Transaction {
    signer_id: "alice_near",
    public_key: "ed25519:32zVgoqtuyRuDvSMZjWQ774kK36UTwuGRZMmPsS6xpMy",
    nonce: 57,
    receiver_id: "travel_agency",
    block_hash: "CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf",
    actions: vec![
        Action::FunctionCall(FunctionCallAction {
            method_name: "reserve_trip",
            args: "{\"city\": \"Venice\", \"date\": 20191201}",
            gas: 1000000,
            tokens: 100,
        })
    ],
}
```

Here the public key corresponds to the full access key of `alice_near` account. All other fields in `Transaction` were
discussed in the [Financial Transaction](FinancialTransaction.md) section. The `FunctionCallAction` action describes how
the contract should be called. The `receiver_id` field in `Transaction` already establishes what contract should be executed,
`FunctionCallAction` merely describes how it should be executed. Interestingly, the arguments is just a blob of bytes,
it is up to the contract developer what serialization format they choose for their arguments. In this example, the contract
developer has chosen to use JSON and so the tool that Alice uses to compose this transaction is expected to use JSON too
to pass the arguments. `gas` declares how much gas `alice_near` has prepaid for dynamically calculated fees of the smart
contract executions and other actions that this transaction may spawn. The `tokens` is the amount of `alice_near` attaches
to be deposited to whatever smart contract that it is calling to. Notice, `gas` and `tokens` are in different units of
measurement.

Now, consider a slightly more complex scenario. In this scenario Alice uses a restricted access key to call the function.
That is the permission of the access key is not `AccessKeyPermission::FullAccess` but is instead: `AccessKeyPermission::FunctionCall(FunctionCallPermission)`
where

```
FunctionCallPermission {
    allowance: Some(3000),
    receiver_id: "travel_agency",
    method_names: [ "reserve_trip", "cancel_trip" ]
}
```

This scenario might arise when someone Alice's parent has given them a restricted access to `alice_near` account by
creating an access key that can be used strictly for trip management.
This access key allows up to `3000` tokens to be spent (which includes token transfers and payments for gas), it can
be only used to call `travel_agency` and it can be only used with the `reserve_trip` and `cancel_trip` methods.
The way runtime treats this case is almost exactly the same as the previous one, with the only difference on how it verifies
the signature of on the signed transaction, and that it also checks for allowance to not be exceeded.

Finally, in the last scenario, Alice does not have an account (or the existence of `alice_near` is irrelevant). However,
alice has full or restricted access key directly on `travel_agency` account. In that case `signer_id == receiver_id` in the
`Transaction` object and runtime will convert transaction to the first receipt and apply that receipt in the same block.

This section will focus on the first scenario, since the other two are the same with some minor differences.

## Transaction to receipt

The process of converting transaction to receipt is very similar to the [Financial Transaction](FinancialTransaction.md)
with several key points to note:

- Since Alice attaches 100 tokens to the function call, we subtract them from `alice_near` upon converting transaction to receipt,
  similar to the regular financial transaction;
- Since we are attaching 1000000 prepaid gas, we will not only subtract the gas costs of processing the receipt from `alice_near`, but
  will also purchase 1000000 gas using the current gas price.

## Processing the `reserve_trip` receipt

The receipt created on the shard that hosts `alice_near` will eventually arrive to the shard hosting `travel_agency` account.
It will be processed in `Runtime::apply` which will check that receipt does not have data dependencies (which is the case because
this function call is not a callback) and will call `Runtime::apply_action_receipt`.
At this point receipt processing is similar to receipt processing from the [Financial Transaction](FinancialTransaction.md)
section, with one difference that we will also call `action_function_call` which will do the following:

- Retrieve the Wasm code of the smart contract (either from the database or from the cache);
- Initialize runtime context through `VMContext` and create `RuntimeExt` which provides access to the trie when the smart contract
  call the storage API. Specifically `"{\"city\": \"Venice\", \"date\": 20191201}"` arguments will be set in `VMContext`.
- Calls `near_vm_runner::run` which does the following:
  - Inject gas, stack, and other kinds of metering;
  - Verify that Wasm code does not use floats;
  - Checks that bindings API functions that the smart contract is trying to call are actually those provided by `near_vm_logic`;
  - Compiles Wasm code into the native binary;
  - Calls `reserve_trip` on the smart contract.
    - During the execution of the smart contract it will at some point call `promise_create` and `promise_then`, which will
      call method on `RuntimeExt` that will record that two promises were created and that the second one should
      wait on the first one. Specifically, `promise_create` will call `RuntimeExt::create_receipt(vec![], "hotel_near")`
      returning `0` and then `RuntimeExt::create_receipt(vec![0], "travel_agency")`;
- `action_function_call` then collects receipts from `VMContext` along with the execution result, logs, and information
  about used gas;
- `apply_action_receipt` then goes over the collected receipts from each action and returns them at the end of `Runtime::apply` together with
  other receipts.

## Processing the `reserve` receipt

This receipt will have `output_data_receivers` with one element corresponding to the receipt that calls `hotel_reservation_complete`,
which will tell the runtime that it should create `DataReceipt` and send it towards `travel_agency` once the execution of `reserve(date: u64)` is complete.

The rest of the smart contract execution is similar to the above.

## Processing the `hotel_reservation_complete` receipt

Upon receiving the `hotel_reservation_complete` receipt the runtime will notice that its `input_data_ids` is not empty
which means that it cannot be executed until `reserve` receipt is complete. It will store the receipt in the trie together
with the counter of how many `DataReceipt` it is waiting on.

It will not call the Wasm smart contract at this point.

## Processing the `DataReceipt`

Once the runtime receives the `DataReceipt` it takes the receipt with `hotel_reservation_complete` function call
and executes it following the same execution steps as with the `reserve_trip` receipt.
