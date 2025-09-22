# Registers API

<!-- cspell:ignore Ewasm BigNum -->
Registers allow the host function to return the data into a buffer located inside the host oppose to the buffer
located on the client. A special operation can be used to copy the content of the buffer into the host. Memory pointers
can then be used to point either to the memory on the guest or the memory on the host, see below. Benefits:

- We can have functions that return values that are not necessarily used, e.g. inserting key-value into a trie can
  also return the preempted old value, which might not be necessarily used. Previously, if we returned something we
  would have to pass the blob from host into the guest, even if it is not used;
- We can pass blobs of data between host functions without going through the guest, e.g. we can remove the value
  from the storage and insert it into under a different key;
- It makes API cleaner, because we don't need to pass `buffer_len` and `buffer_ptr` as arguments to other functions;
- It allows merging certain functions together, see `storage_iter_next`;
- This is consistent with other APIs that were created for high performance, e.g. allegedly Ewasm has implemented
  SNARK-like computations in Wasm by exposing a BigNum library through stack-like interface to the guest. The guest
  can manipulate then with the stack of 256-bit numbers that is located on the host.

#### Host → host blob passing

The registers can be used to pass the blobs between host functions. For any function that
takes a pair of arguments `*_len: u64, *_ptr: u64` this pair is pointing to a region of memory either on the guest or
the host:

- If `*_len != u64::MAX` it points to the memory on the guest;
- If `*_len == u64::MAX` it points to the memory under the register `*_ptr` on the host.

For example:
`storage_write(u64::MAX, 0, u64::MAX, 1, 2)` -- insert key-value into storage, where key is read from register 0,
value is read from register 1, and result is saved to register 2.

Note, if some function takes `register_id` then it means this function can copy some data into this register. If
`register_id == u64::MAX` then the copying does not happen. This allows some micro-optimizations in the future.

Note, we allow multiple registers on the host, identified with `u64` number. The guest does not have to use them in
order and can for instance save some blob in register `5000` and another value in register `1`.

#### Specification

```rust
read_register(register_id: u64, ptr: u64)
```

Writes the entire content from the register `register_id` into the memory of the guest starting with `ptr`.

###### Panics

- If the content extends outside the memory allocated to the guest. In Wasmer, it returns `MemoryAccessViolation` error message;
- If `register_id` is pointing to unused register returns `InvalidRegisterId` error message.

###### Undefined Behavior

- If the content of register extends outside the preallocated memory on the host side, or the pointer points to a
  wrong location this function will overwrite memory that it is not supposed to overwrite causing an undefined behavior.

---

```rust
register_len(register_id: u64) -> u64
```

Returns the size of the blob stored in the given register.

###### Normal operation

- If register is used, then returns the size, which can potentially be zero;
- If register is not used, returns `u64::MAX`
