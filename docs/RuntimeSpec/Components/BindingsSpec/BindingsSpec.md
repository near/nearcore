# Bindings Specification

This is the low-level interface available to the smart contracts, it consists of the functions that the host (represented by
Wasmer inside near-vm-runner) exposes to the guest (the smart contract compiled to Wasm).

Due to Wasm restrictions the methods operate only with primitive types, like `u64`.

Also for all functions in the bindings specification the following is true:

- Method execution could result in `MemoryAccessViolation` error if one of the following happens:
  - The method causes host to read a piece of memory from the guest but it points outside the guest's memory;
  - The guest causes host to read from the register, but register id is invalid.

Execution of a bindings function call result in an error being generated. This error causes execution of the smart contract
to be terminated and the error message written into the logs of the transaction that caused the execution. Many bindings
functions can throw specialized error messages, but there is also a list of error messages that can be thrown by almost
any function:

- `IntegerOverflow` -- happens when guest passes some data to the host but when host tries to apply arithmetic operation
  on it it causes overflow or underflow;
- `GasExceeded` -- happens when operation performed by the guest causes more gas than the remaining prepaid gas;
- `GasLimitExceeded` -- happens when the execution uses more gas than allowed by the global limit imposed in the economics
  config;
- `StorageError` -- happens when method fails to do some operation on the trie.

The following binding methods cannot be invoked in a view call:

- `signer_account_id`
- `signer_account_pk`
- `predecessor_account_id`
- `attached_deposit`
- `prepaid_gas`
- `used_gas`
- `promise_create`
- `promise_then`
- `promise_and`
- `promise_batch_create`
- `promise_batch_then`
- `promise_batch_action_create_account`
- `promise_batch_action_deploy_account`
- `promise_batch_action_function_call`
- `promise_batch_action_transfer`
- `promise_batch_action_stake`
- `promise_batch_action_add_key_with_full_access`
- `promise_batch_action_add_key_with_function_call`
- `promise_batch_action_delete_key`
- `promise_batch_action_delete_account`
- `promise_results_count`
- `promise_result`
- `promise_return`

If they are invoked the smart contract execution will panic with `ProhibitedInView(<method name>)`.
