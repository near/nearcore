# near-account-id

This crate provides a type for representing a valid, unique account identifier on the [NEAR](https://near.org) network.

[![crates.io](https://img.shields.io/crates/v/near-account-id?label=latest)](https://crates.io/crates/near-account-id)
[![Documentation](https://docs.rs/near-account-id/badge.svg)](https://docs.rs/near-account-id)
[![Version](https://img.shields.io/badge/rustc-1.56+-ab6000.svg)](https://blog.rust-lang.org/2021/10/21/Rust-1.56.0.html)
[![Apache 2.0 licensed](https://img.shields.io/crates/l/near-account-id.svg)](https://github.com/near/nearcore/blob/master/licenses/LICENSE-APACHE)

## Usage

```rust
use near_account_id::AccountId;

let alice: AccountId = "alice.near".parse().unwrap();

// Basic reports for why validation failed
assert!(
  matches!(
    "z".parse::<AccountId>(),
    Err(err) if err.kind().is_too_short()
  )
);

assert!(
  matches!(
    "bob__carol".parse::<AccountId>(),
    Err(err) if err.kind().is_invalid()
  )
);
```

## Account ID Rules

- Minimum length is `2`
- Maximum length is `64`
- An Account ID consists of Account ID parts separated by `.`, example:
  - `root` ✔
  - `alice.near` ✔
  - `bob.stage.testnet` ✔
- Must not start or end with separators (`_`, `-` or `.`):
  - `_alice.` ✗
  - `.bob.near-` ✗
- Each part of the Account ID consists of lowercase alphanumeric symbols separated either by `_` or `-`, example:
  - `1_4m_n0t-al1c3.near` ✔
- Separators are not permitted to immediately follow each other, example:
  - `alice..near` ✗
  - `not-_alice.near` ✗

Learn more here: <https://docs.near.org/docs/concepts/account#account-id-rules>
