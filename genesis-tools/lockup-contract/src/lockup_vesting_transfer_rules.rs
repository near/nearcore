//! This file describes the rules on what amount of tokens is allowed to be transferrable depending
//! on the account balance, amount of tokens used for validation, the lockup amount, **the amount
//! of vested tokens**, and **the amount of not-vested tokens**.
//!
//! Depending on the relative values of the following quantities the account can be in a different state:
//! * `balance` -- amount of tokens that are not locked for validating;
//! * `locked` -- amount of tokens locked for validating;
//! * `vested` -- amount of tokens that have vested;
//! * `unvested` -- amount of tokens that have not vested yet.
//!
//! Note, below "total balance" refers to `balance + locked`.
//! Note, the lockup+vesting contract requires `lockup == vested + unvested`.
//!
//! For the situation when the lock has not expired the cases (A)-(D) are the same as described in
//! `lockup_transfer_rules.rs`.
//!
//! Here we describe situation when lock has expired.
//!
//! E) Lock has expired and `locked + balance >= unvested && unvested >= locked`. This is a normal
//!    operation of the contract after the lock has expired. We can either have
//!    `locked + balance >= unvested + vested` or `locked + balance < unvested + vested`, it does
//!    not matter. What matters is that after the transfer `locked + balance >= unvested`.
//!
//!     locked       balance
//! ├─────────────┼───────────────┤
//!            unvested
//! ├──────────────────────────┤
//!
//! Transferrable balance is `locked + balance - unvested`.
//!
//! F) Lock has expired and `locked >= unvested`. This is normal operation when the account owner
//!    is still using it mostly for staking (is typical for accounts that have almost completed
//!    vesting schedule).
//!
//!     locked       balance
//! ├─────────────┼───────────────┤
//!   unvested
//! ├──────────┤
//!
//! Transferrable balance is `balance`.
//!
//! G) Lock has expired and `locked + balance <= unvested`. This can happen on account that's not
//!    validating and lost some funds due to storage rent or if the account was slashed.
//!
//!
//!     locked       balance
//! ├─────────────┼───────────────┤
//!              unvested
//! ├─────────────────────────────────────┤
//!
//! Transferrable balance is `0`.

use near_bindgen::env;

pub fn get_transferrable_amount(lockup: u128, lockup_timestamp: u64, unvested: u128) -> u128 {
    let balance = env::account_balance();
    let locked = env::account_locked_balance();
    if lockup_timestamp < env::block_timestamp() {
        if locked + balance > lockup && locked < lockup {
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
    } else {
        if locked + balance >= unvested && unvested >= locked {
            // E
            locked + balance - unvested
        } else if unvested <= locked {
            // F
            balance
        } else if locked + balance <= unvested {
            // G
            0
        } else {
            unreachable!()
        }
    }
}

pub fn get_unvested_amount(
    vesting_start_timestamp: u64,
    vesting_cliff_timestamp: u64,
    vesting_end_timestamp: u64,
    lockup: u128,
) -> u128 {
    let block_timestamp = env::block_timestamp();
    if block_timestamp <= vesting_cliff_timestamp {
        lockup
    } else if block_timestamp >= vesting_end_timestamp {
        0
    } else {
        let time_left = (vesting_end_timestamp - block_timestamp) as u128; // fits 64
        let total_time = (vesting_end_timestamp - vesting_start_timestamp) as u128; // fits 64

        // We need to compute the following formula avoiding overflows or intermediate rounding.
        // lockup * time_left / total_time
        // We deconstruct lockup = a << 64 + b  and compute the formula in two parts:
        let a: u128 = lockup >> 64; // fits 64
        let b: u128 = lockup - (a << 64); // fits 64
                                          // The following does not overflow u128 since all components fit u64.
        let div_a = (a * time_left) / total_time; // fits 64 since time_left <= total_time
        let rem_a = (a * time_left) % total_time; // fits 64, since rem_a < total_time.
        let div_b = (b * time_left) / total_time + (rem_a << 64) / total_time;
        let rem_b = (b * time_left) % total_time + (rem_a << 64) % total_time;

        (div_a << 64) + div_b + rem_b / total_time
    }
}
