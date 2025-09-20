# Trie API

Here we provide a specification of trie API. After this NEP is merged, the cases where our current implementation does
not follow the specification are considered to be bugs that need to be fixed.

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

- If `key_len + key_ptr` or `value_len + value_ptr` exceeds the memory container or points to an unused register it panics
  with `MemoryAccessViolation`. (When we say that something panics with the given error we mean that we use Wasmer API to
  create this error and terminate the execution of VM. For mocks of the host that would only cause a non-name panic.)
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;

###### Current bugs

- `External::storage_set` trait can return an error which is then converted to a generic non-descriptive
  `StorageUpdateError`, [here](https://github.com/nearprotocol/nearcore/blob/942bd7bdbba5fb3403e5c2f1ee3c08963947d0c6/runtime/wasm/src/runtime.rs#L210)
  however the actual implementation does not return error at all, [see](https://github.com/nearprotocol/nearcore/blob/4773873b3cd680936bf206cebd56bdc3701ddca9/runtime/runtime/src/ext.rs#L95);
- Does not return into the registers.

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

- If `key_len + key_ptr` exceeds the memory container or points to an unused register it panics with `MemoryAccessViolation`;
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;

###### Current bugs

- This function currently does not exist.

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

- If `key_len + key_ptr` exceeds the memory container or points to an unused register it panics with `MemoryAccessViolation`;
- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If returning the preempted value into the registers exceed the memory container it panics with `MemoryAccessViolation`;

###### Current bugs

- Does not return into the registers.

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

---

#### storage_iter_prefix

```rust
storage_iter_prefix(prefix_len: u64, prefix_ptr: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error.
Creates an iterator object inside the host.
Returns the identifier that uniquely differentiates the given iterator from other iterators that can be simultaneously
created.

###### Normal operation

- It iterates over the keys that have the provided prefix. The order of iteration is defined by the lexicographic
  order of the bytes in the keys. If there are no keys, it creates an empty iterator, see below on empty iterators;

###### Panics

- If `prefix_len + prefix_ptr` exceeds the memory container it panics with `MemoryAccessViolation`;

---

#### storage_iter_range

```rust
storage_iter_range(start_len: u64, start_ptr: u64, end_len: u64, end_ptr: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error.
Similarly to `storage_iter_prefix`
creates an iterator object inside the host.

###### Normal operation

Unless lexicographically `start < end`, it creates an empty iterator.
Iterates over all key-values such that keys are between `start` and `end`, where `start` is inclusive and `end` is exclusive.

Note, this definition allows for `start` or `end` keys to not actually exist on the given trie.

###### Panics

- If `start_len + start_ptr` or `end_len + end_ptr` exceeds the memory container or points to an unused register it panics with `MemoryAccessViolation`;

---

#### storage_iter_next

```rust
storage_iter_next(iterator_id: u64, key_register_id: u64, value_register_id: u64) -> u64
```

DEPRECATED, calling it will result in `HostError::Deprecated` error.
Advances iterator and saves the next key and value in the register.

###### Normal operation

- If iterator is not empty (after calling next it points to a key-value), copies the key into `key_register_id` and value into `value_register_id` and returns `1`;
- If iterator is empty returns `0`.

This allows us to iterate over the keys that have zero bytes stored in values.

###### Panics

- If `key_register_id == value_register_id` panics with `MemoryAccessViolation`;
- If the registers exceed the memory limit panics with `MemoryAccessViolation`;
- If `iterator_id` does not correspond to an existing iterator panics with `InvalidIteratorId`
- If between the creation of the iterator and calling `storage_iter_next` any modification to storage was done through
  `storage_write` or `storage_remove` the iterator is invalidated and the error message is `IteratorWasInvalidated`.

###### Current bugs

- Not implemented, currently we have `storage_iter_next` and `data_read` + `DATA_TYPE_STORAGE_ITER` that together fulfill
  the purpose, but have unspecified behavior.
