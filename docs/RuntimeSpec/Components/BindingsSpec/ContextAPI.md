# Context API

Context API mostly provides read-only functions that access current information about the blockchain, the accounts
(that originally initiated the chain of cross-contract calls, the immediate contract that called the current one, the account of the current contract),
other important information like storage usage.

Many of the below functions are currently implemented through `data_read` which allows to read generic context data.
However, there is no reason to have `data_read` instead of the specific functions:

- `data_read` does not solve forward compatibility. If later we want to add another context function, e.g. `executed_operations`
  we can just declare it as a new function, instead of encoding it as `DATA_TYPE_EXECUTED_OPERATIONS = 42` which is passed
  as the first argument to `data_read`;
- `data_read` does not help with renaming. If later we decide to rename `signer_account_id` to `originator_id` then one could
  argue that contracts that rely on `data_read` would not break, while contracts relying on `signer_account_id()` would. However
  the name change often means the change of the semantics, which means the contracts using this function are no longer safe to
  execute anyway.

However there is one reason to not have `data_read` -- it makes `API` more human-like which is a general direction Wasm APIs, like WASI are moving towards to.

---

```rust
current_account_id(register_id: u64)
```

Saves the account id of the current contract that we execute into the register.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;

---

#### signer_account_id

```rust
signer_account_id(register_id: u64)
```

All contract calls are a result of some transaction that was signed by some account using
some access key and submitted into a memory pool (either through the wallet using RPC or by a node itself). This function returns the id of that account.

###### Normal operation

- Saves the bytes of the signer account id into the register.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

###### Current bugs

- Currently we conflate `originator_id` and `sender_id` in our code base.

---

#### signer_account_pk

```rust
signer_account_pk(register_id: u64)
```

Saves the public key fo the access key that was used by the signer into the register.
In rare situations smart contract might want to know the exact access key that was used to send the original transaction,
e.g. to increase the allowance or manipulate with the public key.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

###### Current bugs

- Not implemented.

---

#### predecessor_account_id

```rust
predecessor_account_id(register_id: u64)
```

All contract calls are a result of a receipt, this receipt might be created by a transaction
that does function invocation on the contract or another contract as a result of cross-contract call.

###### Normal operation

- Saves the bytes of the predecessor account id into the register.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

###### Current bugs

- Not implemented.

---

#### input

```rust
input(register_id: u64)
```

Reads input to the contract call into the register. Input is expected to be in JSON-format.

###### Normal operation

- If input is provided saves the bytes (potentially zero) of input into register.
- If input is not provided does not modify the register.

###### Returns

- If input was not provided returns `0`;
- If input was provided returns `1`; If input is zero bytes returns `1`, too.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;

###### Current bugs

- Implemented as part of `data_read`. However there is no reason to have one unified function, like `data_read` that can
  be used to read all

---

```rust
block_index() -> u64
```

Returns the current block height from genesis.

---

```rust
block_timestamp() -> u64
```

Returns the current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).

---

```rust
epoch_height() -> u64
```

Returns the current epoch height from genesis.

---

```rust
storage_usage() -> u64
```

Returns the number of bytes used by the contract if it was saved to the trie as of the
invocation. This includes:

- The data written with `storage_*` functions during current and previous execution;
- The bytes needed to store the account protobuf and the access keys of the given account.
