# `near-stable-hasher`

`near-stable-hasher` is a library that is essentially a wrapper around, now deprecated, `std::hash::SipHasher`.
Its purpose is to provide a stable hash function, which doesn't change depending on `rust_version`, `architecture`, `platform`,
`time`, etc.

In addition, note that `SipHasher` is deprecated since `Rust` `1.13.0`.
Eventually `SipHasher` will be removed from `Rust`.
We need to ensure, nothing breaks during this transition period.

## Structs

This crate provides only one struct. See `StableHasher`.

### Example:

```rust
fn test_stable_hasher() {
  let mut sh = StableHasher::new();

  sh.write(&[1, 2, 3, 4, 5]);
  let finish = sh.finish();
  assert_eq!(finish, 12661990674860217757)
}
```
