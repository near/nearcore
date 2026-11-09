# Trie API

Here we provide a specification of trie API. After this NEP is merged, the cases where our current implementation does
not follow the specification are considered to be bugs that need to be fixed.

In an external call (see [Context API](ContextAPI.md)), the storage functions below can also panic with
`RecordedStorageExceeded`: the trie nodes and values the call reads go into the chunk's storage proof, and an external
call may add at most `max_external_call_recorded_size` (20 KiB) to it. A read that would record a value past the limit
fails without reading it. A read that records the trie node that crosses the limit fails after recording it, and so does
a `storage_remove` that crosses it. The call is then rejected, and nobody pays for it (see
[Gas in External Contract Calls](../../../architecture/how/gas.md#gas-in-external-contract-calls)).

Removals are costly in an external call. Each `storage_remove` counts toward the limit with a fixed estimate of 2000
bytes, even for a key that isn't present, so a call can remove no more than ten keys, and fewer, as the trie nodes it
reads count as well. Larger cleanups belong in the receipts the call creates, once external calls can create them.

---

#### storage_write

```rust
storage_write(key_len: u64, key_ptr: u64, value_len: u64, value_ptr: u64, register_id: u64) -> u64
```

Writes key-value into storage.

###### Normal operation

- If key is not in use it inserts the key-value pair and does not modify the register;
- If key is in use it inserts the key-value and copies the old value into the `register_id`.

###### Returns

- If key was not used returns `0`;
- If key was used returns `1`.

###### Panics

- If `key_len + key_ptr` or `value_len + value_ptr` exceeds the memory container it panics with `MemoryAccessViolation`.
  (When we say that something panics with the given error, it means that the host function returns this error and the
  execution of the contract stops. For mocks of the host that would only cause a non-name panic.)
- If `key_len` is `u64::MAX` and `key_ptr` is an unused register it panics with `InvalidRegisterId`. The same applies to
  `value_len` and `value_ptr`;
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;
- If the key is longer than `max_length_storage_key` (2,048 bytes) it panics with `KeyLengthExceeded`;
- If the value is longer than `max_length_storage_value` (4,194,304 bytes) it panics with `ValueLengthExceeded`;
- If called in a view call it panics with `ProhibitedInView`;
- If the storage proof recorded for this receipt exceeds `per_receipt_storage_proof_size_limit` it panics with
  `RecordedStorageExceeded`;

---

#### storage_read

```rust
storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64
```

Reads the value stored under the given key.

###### Normal operation

- If key is used copies the content of the value into the `register_id`, even if the content is zero bytes;
- If key is not present then does not modify the register.

###### Returns

- If key was not present returns `0`;
- If key was present returns `1`.

###### Panics

- If `key_len + key_ptr` exceeds the memory container it panics with `MemoryAccessViolation`;
- If `key_len` is `u64::MAX` and `key_ptr` is an unused register it panics with `InvalidRegisterId`;
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;
- If the key is longer than `max_length_storage_key` (2,048 bytes) it panics with `KeyLengthExceeded`;
- If the storage proof recorded for this receipt exceeds `per_receipt_storage_proof_size_limit` it panics with
  `RecordedStorageExceeded`;

---

#### storage_remove

```rust
storage_remove(key_len: u64, key_ptr: u64, register_id: u64) -> u64
```

Removes the value stored under the given key.

###### Normal operation

Very similar to `storage_read`:

- If key is used, removes the key-value from the trie and copies the content of the value into the `register_id`, even if the content is zero bytes.
- If key is not present then does not modify the register.

###### Returns

- If key was not present returns `0`;
- If key was present returns `1`.

###### Panics

- If `key_len + key_ptr` exceeds the memory container it panics with `MemoryAccessViolation`;
- If `key_len` is `u64::MAX` and `key_ptr` is an unused register it panics with `InvalidRegisterId`;
- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;
- If the key is longer than `max_length_storage_key` (2,048 bytes) it panics with `KeyLengthExceeded`;
- If called in a view call it panics with `ProhibitedInView`;
- If the storage proof recorded for this receipt exceeds `per_receipt_storage_proof_size_limit` it panics with
  `RecordedStorageExceeded`;
- If called in an external call and the removal, counted as 2000 bytes even if the key is not present, crosses
  `max_external_call_recorded_size` it panics with `RecordedStorageExceeded` (see above);

---

#### storage_has_key

```rust
storage_has_key(key_len: u64, key_ptr: u64) -> u64
```

Checks if there is a key-value pair.

###### Normal operation

- If key is used returns `1`, even if the value is zero bytes;
- Otherwise returns `0`.

###### Panics

- If `key_len + key_ptr` exceeds the memory container it panics with `MemoryAccessViolation`;
- If `key_len` is `u64::MAX` and `key_ptr` is an unused register it panics with `InvalidRegisterId`;
- If the key is longer than `max_length_storage_key` (2,048 bytes) it panics with `KeyLengthExceeded`;
- If the storage proof recorded for this receipt exceeds `per_receipt_storage_proof_size_limit` it panics with
  `RecordedStorageExceeded`;

---

#### storage_iter_prefix

```rust
storage_iter_prefix(prefix_len: u64, prefix_ptr: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error. It charges no gas.

---

#### storage_iter_range

```rust
storage_iter_range(start_len: u64, start_ptr: u64, end_len: u64, end_ptr: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error. It charges no gas.

---

#### storage_iter_next

```rust
storage_iter_next(iterator_id: u64, key_register_id: u64, value_register_id: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error. It charges no gas.
