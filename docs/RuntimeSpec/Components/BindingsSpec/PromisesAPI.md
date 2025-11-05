# Promises API

#### promise_create

```rust
promise_create(account_id_len: u64,
               account_id_ptr: u64,
               method_name_len: u64,
               method_name_ptr: u64,
               arguments_len: u64,
               arguments_ptr: u64,
               amount_ptr: u64,
               gas: u64) -> u64
```

Creates a promise that will execute a method on account with given arguments and attaches the given amount.
`amount_ptr` point to slices of bytes representing `u128`.

###### Panics

- If `account_id_len + account_id_ptr` or `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr`
  or `amount_ptr + 16` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

###### Returns

- Index of the new promise that uniquely identifies it within the current execution of the method.

---

##### promise_then

```rust
promise_then(promise_idx: u64,
             account_id_len: u64,
             account_id_ptr: u64,
             method_name_len: u64,
             method_name_ptr: u64,
             arguments_len: u64,
             arguments_ptr: u64,
             amount_ptr: u64,
             gas: u64) -> u64
```

Attaches the callback that is executed after promise pointed by `promise_idx` is complete.

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If `account_id_len + account_id_ptr` or `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr`
  or `amount_ptr + 16` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

###### Returns

- Index of the new promise that uniquely identifies it within the current execution of the method.

---

#### promise_and

```rust
promise_and(promise_idx_ptr: u64, promise_idx_count: u64) -> u64
```

Creates a new promise which completes when time all promises passed as arguments complete. Cannot be used with registers.
`promise_idx_ptr` points to an array of `u64` elements, with `promise_idx_count` denoting the number of elements.
The array contains indices of promises that need to be waited on jointly.

###### Panics

- If `promise_ids_ptr + 8 * promise_idx_count` extend outside the guest memory with `MemoryAccessViolation`;
- If any of the promises in the array do not correspond to existing promises panics with `InvalidPromiseIndex`.
- If called in a view function panics with `ProhibitedInView`.

###### Returns

- Index of the new promise that uniquely identifies it within the current execution of the method.

---

#### promise_results_count

```rust
promise_results_count() -> u64
```

If the current function is invoked by a callback we can access the execution results of the promises that
caused the callback. This function returns the number of complete and incomplete callbacks.

Note, we are only going to have incomplete callbacks once we have `promise_or` combinator.

###### Normal execution

- If there is only one callback `promise_results_count()` returns `1`;
- If there are multiple callbacks (e.g. created through `promise_and`) `promise_results_count()` returns their number.
- If the function was called not through the callback `promise_results_count()` returns `0`.

###### Panics

- If called in a view function panics with `ProhibitedInView`.

---

#### promise_result

```rust
promise_result(result_idx: u64, register_id: u64) -> u64
```

If the current function is invoked by a callback we can access the execution results of the promises that
caused the callback. This function returns the result in blob format and places it into the register.

###### Normal execution

- If promise result is complete and successful copies its blob into the register;
- If promise result is complete and failed or incomplete keeps register unused;

###### Returns

- If promise result is not complete returns `0`;
- If promise result is complete and successful returns `1`;
- If promise result is complete and failed returns `2`.

###### Panics

- If `result_idx` does not correspond to an existing result panics with `InvalidResultIndex`.
- If copying the blob exhausts the memory limit it panics with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

###### Current bugs

- We currently have two separate functions to check for result completion and copy it.

---

#### promise_return

```rust
promise_return(promise_idx: u64)
```

When promise `promise_idx` finishes executing its result is considered to be the result of the current function.

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.

###### Current bugs

- The current name `return_promise` is inconsistent with the naming convention of Promise API.

#### promise_batch_create

```rust
promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64
```

Creates a new promise towards given `account_id` without any actions attached to it.

###### Panics

- If `account_id_len + account_id_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

###### Returns

- Index of the new promise that uniquely identifies it within the current execution of the method.

---

#### promise_batch_then

```rust
promise_batch_then(promise_idx: u64, account_id_len: u64, account_id_ptr: u64) -> u64
```

Attaches a new empty promise that is executed after promise pointed by `promise_idx` is complete.

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If `account_id_len + account_id_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

###### Returns

- Index of the new promise that uniquely identifies it within the current execution of the method.

---

#### promise_batch_action_create_account

```rust
promise_batch_action_create_account(promise_idx: u64)
```

Appends `CreateAccount` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R48

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_deploy_contract

```rust
promise_batch_action_deploy_contract(promise_idx: u64, code_len: u64, code_ptr: u64)
```

Appends `DeployContract` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R49

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If `code_len + code_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_function_call

```rust
promise_batch_action_function_call(promise_idx: u64,
                                   method_name_len: u64,
                                   method_name_ptr: u64,
                                   arguments_len: u64,
                                   arguments_ptr: u64,
                                   amount_ptr: u64,
                                   gas: u64)
```

Appends `FunctionCall` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R50

_NOTE: Calling `promise_batch_create` and then `promise_batch_action_function_call` will produce the same promise as calling `promise_create` directly._

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If `account_id_len + account_id_ptr` or `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr`
  or `amount_ptr + 16` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_transfer

```rust
promise_batch_action_transfer(promise_idx: u64, amount_ptr: u64)
```

Appends `Transfer` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R51

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If `amount_ptr + 16` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_stake

```rust
promise_batch_action_stake(promise_idx: u64,
                           amount_ptr: u64,
                           bls_public_key_len: u64,
                           bls_public_key_ptr: u64)
```

Appends `Stake` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R52

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If the given BLS public key is not a valid BLS public key (e.g. wrong length) `InvalidPublicKey`.
- If `amount_ptr + 16` or `bls_public_key_len + bls_public_key_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_add_key_with_full_access

```rust
promise_batch_action_add_key_with_full_access(promise_idx: u64,
                                              public_key_len: u64,
                                              public_key_ptr: u64,
                                              nonce: u64)
```

Appends `AddKey` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R54
The access key will have `FullAccess` permission, details: [/Proposals/0005-access-keys.md#guide-level-explanation](click here)

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If the given public key is not a valid public key (e.g. wrong length) `InvalidPublicKey`.
- If `public_key_len + public_key_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_add_key_with_function_call

```rust
promise_batch_action_add_key_with_function_call(promise_idx: u64,
                                                public_key_len: u64,
                                                public_key_ptr: u64,
                                                nonce: u64,
                                                allowance_ptr: u64,
                                                receiver_id_len: u64,
                                                receiver_id_ptr: u64,
                                                method_names_len: u64,
                                                method_names_ptr: u64)
```

Appends `AddKey` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-156752ec7d78e7b85b8c7de4a19cbd4R54
The access key will have `FunctionCall` permission, details: [/Proposals/0005-access-keys.md#guide-level-explanation](click here)

- If the `allowance` value (not the pointer) is `0`, the allowance is set to `None` (which means unlimited allowance). And positive value represents a `Some(...)` allowance.
- Given `method_names` is a `utf-8` string with `,` used as a separator. The vm will split the given string into a vector of strings.

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If the given public key is not a valid public key (e.g. wrong length) `InvalidPublicKey`.
- if `method_names` is not a valid `utf-8` string, fails with `BadUTF8`.
- If `public_key_len + public_key_ptr`, `allowance_ptr + 16`, `receiver_id_len + receiver_id_ptr` or
  `method_names_len + method_names_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_delete_key

```rust
promise_batch_action_delete_key(promise_idx: u64,
                                public_key_len: u64,
                                public_key_ptr: u64)
```

Appends `DeleteKey` action to the batch of actions for the given promise pointed by `promise_idx`.
Details for the action: https://github.com/nearprotocol/NEPs/pull/8/files#diff-15b6752ec7d78e7b85b8c7de4a19cbd4R55

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If the given public key is not a valid public key (e.g. wrong length) `InvalidPublicKey`.
- If `public_key_len + public_key_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.

---

#### promise_batch_action_delete_account

```rust
promise_batch_action_delete_account(promise_idx: u64,
                                    beneficiary_id_len: u64,
                                    beneficiary_id_ptr: u64)
```

Appends `DeleteAccount` action to the batch of actions for the given promise pointed by `promise_idx`.
Action is used to delete an account. It can be performed on a newly created account, on your own account or an account with
insufficient funds to pay rent. Takes `beneficiary_id` to indicate where to send the remaining funds.

###### Panics

- If `promise_idx` does not correspond to an existing promise panics with `InvalidPromiseIndex`.
- If the promise pointed by the `promise_idx` is an ephemeral promise created by `promise_and`.
- If `beneficiary_id_len + beneficiary_id_ptr` points outside the memory of the guest or host, with `MemoryAccessViolation`.
- If called in a view function panics with `ProhibitedInView`.
