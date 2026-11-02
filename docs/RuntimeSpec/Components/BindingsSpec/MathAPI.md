# Math API

#### random_seed

```rust
random_seed(register_id: u64)
```

Returns random seed that can be used for pseudo-random number generation in deterministic way.

An external call can't read the random seed. A chunk producer runs an external call before the block its chunk goes
into exists (see [Context API](ContextAPI.md)), so the seed would be known before the call is included, and whoever
submits the call could wait for a value that suits them.

###### Panics

- If the size of the registers exceed the set limit `MemoryAccessViolation`;
- If called in an external call panics with `ProhibitedInExternalCall`.

---

#### sha256

```rust
sha256(value_len: u64, value_ptr: u64, register_id: u64)
```

Hashes the random sequence of bytes using sha256 and returns it into `register_id`.

###### Panics

- If `value_len + value_ptr` points outside the memory or the registers use more memory than the limit with `MemoryAccessViolation`.

---

#### keccak256

```rust
keccak256(value_len: u64, value_ptr: u64, register_id: u64)
```

Hashes the random sequence of bytes using keccak256 and returns it into `register_id`.

###### Panics

- If `value_len + value_ptr` points outside the memory or the registers use more memory than the limit with `MemoryAccessViolation`.

---

#### keccak512

```rust
keccak512(value_len: u64, value_ptr: u64, register_id: u64)
```

Hashes the random sequence of bytes using keccak512 and returns it into `register_id`.

###### Panics

- If `value_len + value_ptr` points outside the memory or the registers use more memory than the limit with `MemoryAccessViolation`.
