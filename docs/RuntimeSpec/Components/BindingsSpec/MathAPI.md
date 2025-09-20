# Math API

#### random_seed

```rust
random_seed(register_id: u64)
```

Returns random seed that can be used for pseudo-random number generation in deterministic way.

###### Panics

- If the size of the registers exceed the set limit `MemoryAccessViolation`;

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
