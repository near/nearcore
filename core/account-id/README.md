# near-account-id

This crate provides a type for representing a syntactically valid, unique account identifier on the [NEAR](https://near.org) network, according to the [NEAR Account ID](https://docs.near.org/concepts/basics/account#account-id-rules) rules.

[![crates.io](https://img.shields.io/crates/v/near-account-id?label=latest)](https://crates.io/crates/near-account-id)
[![Documentation](https://docs.rs/near-account-id/badge.svg)](https://docs.rs/near-account-id)
![MIT or Apache 2.0 licensed](https://img.shields.io/crates/l/near-account-id.svg)

## Usage

```rust
use near_account_id::AccountId;

let alice: AccountId = "alice.near".parse()?;

assert!("ƒelicia.near".parse::<AccountId>().is_err()); // (ƒ is not f)
```

See the [docs](https://docs.rs/near-account-id) for more information.

## License

Licensed under either of

- Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
   ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
