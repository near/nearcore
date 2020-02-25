//! This file describes the rules on what amount of tokens is allowed to be transferrable depending
//! on the account balance, amount of tokens used for validation and the lockup amount.
//!
//! Lockup contract is a version of this contract without vesting. Depending on the relative values
//! of the following quantities the account can be in a different state:
//! * `balance` -- amount of tokens that are not locked for validating;
//! * `locked` -- amount of tokens locked for validating;
//! * `lockup` -- amount of tokens that are locked and not available for transfer.
//!
//! Note, below "total balance" refers to `balance + locked`.
//!
//! A) `locked + balance == lockup`. This situation is transitional, it briefly exists at the
//!    following moments: 1) right after initialization of the contract, but immediately on the next
//!    block it becomes `locked + balance < lockup` because tiny amount of tokens is subtracted
//!    from the `balance` to pay for the storage rent. In this situation `locked == 0`; 2) as a
//!    transition between states (B) and (D) or (C) and (D) as the balance is decreasing due to
//!    storage rent.
//!
//!     locked       balance
//! ├─────────────┼───────────────┤
//!             lockup
//! ├─────────────────────────────┤
//!
//! Transferrable balance is `0`.
//!
//! B) `locked + balance > lockup && locked < lockup`. This is the most common situation if the
//!    account is regularly performing validation. In this situation the account earned some tokens
//!    for performing validation and grew its total balance beyond initial `lockup`.
//!
//!    If account stops validating, `balance` will very slowly shrink due to storage rent leading to
//!    situation (A) and then to (D).
//!
//!     locked       balance
//! ├─────────────┼───────────────┤
//!             lockup
//! ├─────────────────────────┤
//!
//! Transferrable balance is `locked + balance - lockup`.
//!
//! C) `locked + balance > lockup && locked >= lockup`. This is the second most common situation, it
//!    is typical for an account that performs aggressive staking where it restakes the tokens that
//!    it has earned for validation.
//!
//!     locked                      balance
//! ├────────────────────────────┼───────────────┤
//!             lockup
//! ├────────────────────────┤
//!
//! Transferrable balance is `balance`.
//!
//! D) `locked + balance < lockup`. This situation occurs either because contract's total balance
//!    shrunk due to storage rent, or because  it has previously validated but got slashed.
//!
//!     locked         balance
//! ├─────────────┼───────────────┤
//!             lockup
//! ├─────────────────────────────────┤
//!
//! Transferrable balance is `0`.
//!
//! E) Lock has expired.
//!
//! Transferrable balance is `balance`.

use near_bindgen::env;

pub fn get_transferrable_amount(lockup: u128, lockup_timestamp: u64) -> u128 {
    let balance = env::account_balance();
    let locked = env::account_locked_balance();
    if lockup_timestamp >= env::block_timestamp() {
        // E
        balance + locked
    } else if locked + balance <= lockup {
        // lockup_timestamp < env::block_timestamp()
        // A, D
        0
    } else if locked < lockup {
        // lockup_timestamp < env::block_timestamp()
        // locked + balance > lockup
        // B
        locked + balance - lockup
    } else {
        // lockup_timestamp < env::block_timestamp()
        // locked + balance > lockup
        // locked >= lockup
        // C
        balance
    }
}
/*
if lockup_timestamp >= env::block_timestamp() {
        // E
        balance + locked
    } else if locked + balance > lockup && locked < lockup {
        // B
        locked + balance - lockup
    } else if locked + balance > lockup && locked >= lockup {
        // C
        balance
    } else if locked + balance <= lockup {
        // A, D
        0
    } else {
        unreachable!()
    }
    */
