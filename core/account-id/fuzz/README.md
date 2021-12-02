# Fuzzing `near-account-id`

## Setup

First, ensure `cargo-fuzz` is installed:

```console
cargo install cargo-fuzz
```

## Execution

Finally, there are two fuzzing targets available: one for [`serde`](https://github.com/serde-rs/serde) and another for [`borsh`](https://github.com/near/borsh-rs). You can run both tests with:

```console
cd core/account-id/fuzz
RUSTC_BOOTSTRAP=1 cargo fuzz run serde
RUSTC_BOOTSTRAP=1 cargo fuzz run borsh
```
