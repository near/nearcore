# Bindings Specification

This is the low-level interface available to the smart contracts, it consists of the functions that the host (represented by
near-vm-runner, which runs contracts on Wasmtime) exposes to the guest (the smart contract compiled to Wasm).

Due to Wasm restrictions the methods operate only with primitive types, like `u64`.

Also for all functions in the bindings specification the following is true:

- Method execution could result in `MemoryAccessViolation` error if the method causes host to read a piece of memory
  from the guest but it points outside the guest's memory;
- Method execution could result in `InvalidRegisterId` error if the guest causes host to read from a register that is
  not set.

Execution of a bindings function call result in an error being generated. This error causes execution of the smart contract
to be terminated and the error message written into the logs of the transaction that caused the execution. Many bindings
functions can throw specialized error messages, but there is also a list of error messages that can be thrown by almost
any function:

- `IntegerOverflow` -- happens when guest passes some data to the host but when host tries to apply arithmetic operation
  on it it causes overflow or underflow;
- `GasExceeded` -- happens when operation performed by the guest causes more gas than the remaining prepaid gas;
- `GasLimitExceeded` -- happens when the execution uses more gas than allowed by the global limit imposed in the economics
  config;

If the node fails to read or write the trie during a host call, this is not a contract error. The node fails to apply
the chunk instead.

The following binding methods cannot be invoked in a view call:

- `signer_account_id`
- `signer_account_pk`
- `predecessor_account_id`
- `refund_to_account_id`
- `prepaid_gas`
- `used_gas`
- `promise_create`
- `promise_then`
- `promise_and`
- `promise_batch_create`
- `promise_batch_then`
- `promise_set_refund_to`
- `promise_batch_action_create_account`
- `promise_batch_action_deploy_contract`
- `promise_batch_action_deploy_global_contract`
- `promise_batch_action_deploy_global_contract_by_account_id`
- `promise_batch_action_use_global_contract`
- `promise_batch_action_use_global_contract_by_account_id`
- `promise_batch_action_state_init`
- `promise_batch_action_state_init_by_account_id`
- `set_state_init_data_entry`
- `promise_batch_action_universal_state_init`
- `promise_batch_action_function_call`
- `promise_batch_action_function_call_weight`
- `promise_batch_action_transfer`
- `promise_batch_action_transfer_to_gas_key`
- `promise_batch_action_stake`
- `promise_batch_action_add_key_with_full_access`
- `promise_batch_action_add_key_with_function_call`
- `promise_batch_action_add_gas_key_with_full_access`
- `promise_batch_action_add_gas_key_with_function_call`
- `promise_batch_action_delete_key`
- `promise_batch_action_delete_account`
- `promise_yield_create`
- `promise_yield_create_with_id`
- `promise_yield_resume`
- `promise_yield_resume_with_yield_id`
- `promise_results_count`
- `promise_result`
- `promise_return`
- `storage_write`
- `storage_remove`

Some of these functions exist only from a given protocol version. `promise_yield_create_with_id`,
`promise_yield_resume_with_yield_id`, `promise_batch_action_transfer_to_gas_key`,
`promise_batch_action_add_gas_key_with_full_access` and `promise_batch_action_add_gas_key_with_function_call` exist from
protocol version 85. From protocol version 88 (nearcore 2.14.0), `promise_batch_action_universal_state_init` exists and
is also prohibited in view calls. Before protocol version 88, a call to a contract that imports it fails with a link
error.

If they are invoked, the smart contract execution will panic with `ProhibitedInView { method_name }`. The `method_name`
field names the host function that made the check. `promise_create` and `promise_then` report `promise_batch_create`
and `promise_batch_then`. `promise_batch_action_function_call_weight` reports `promise_batch_action_function_call`.
