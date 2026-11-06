# Context API

Context API mostly provides read-only functions that access current information about the blockchain, the accounts
(that originally initiated the chain of cross-contract calls, the immediate contract that called the current one, the account of the current contract),
other important information like storage usage.

Some of the functions below behave differently depending on how the contract execution was initiated:

- an **internal** call executes a receipt, created by a transaction or by a cross-contract call;
- a **view** call is a read-only call made through RPC;
- an **external** call is authorized and paid for by the contract itself, so the caller does not need an account
  or a balance. External calls are part of the `ExternalContractCalls` protocol feature, which is not yet stable.
  An external call runs the contract method with the reserved name `__nightly_external_call` (the name will change
  when the feature is stabilized). A view call can also run that method, but an internal call can't: a `FunctionCall`
  action to it fails with `ReservedMethodName`.

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
For a receipt created from a delegate action (meta transaction), this is the relayer that signed the outer transaction.

###### Normal operation

- Saves the bytes of the signer account id into the register.
- In an external call there is no signer, so saves the bytes of the current account id instead: the contract signs
  for the call itself.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

---

#### signer_account_pk

```rust
signer_account_pk(register_id: u64)
```

Saves the public key fo the access key that was used by the signer into the register.
In rare situations smart contract might want to know the exact access key that was used to send the original transaction,
e.g. to increase the allowance or manipulate with the public key.
The key is borsh-encoded: one key type byte (0 ed25519, 1 secp256k1, 2 ML-DSA-65), then the key bytes.
For a receipt created from a delegate action, this is the relayer's key.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`;
- If called in an external call panics with `ProhibitedInExternalCall`.

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
- If called in a view function panics with `ProhibitedInView`;
- If called in an external call panics with `ProhibitedInExternalCall`.

---

#### refund_to_account_id

```rust
refund_to_account_id(register_id: u64)
```

If a receipt fails execution, a balance refund usually goes to the predecessor of the receipt. However, the predecessor
can set this to another account id when sending the receipt.

###### Normal operation

- Saves the bytes of the account id receiving balance refunds into the register.
- In an external call the contract pays for the call, so saves the bytes of the current account id instead.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

---

#### input

```rust
input(register_id: u64)
```

Reads input to the contract call into the register. Input is expected to be in JSON-format.

###### Normal operation

- If input is provided saves the bytes (potentially zero) of input into register.
- If input is not provided writes 0 bytes into the register.

###### Panics

- If the registers exceed the memory limit panics with `MemoryAccessViolation`;

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
- The bytes needed to store the account and the access keys of the given account.
