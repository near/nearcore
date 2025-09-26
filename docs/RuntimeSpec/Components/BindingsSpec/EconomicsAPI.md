# Economics API

Accounts own certain balance; and each transaction and each receipt have certain amount of balance and prepaid gas
attached to them.
During the contract execution, the contract has access to the following `u128` values:

- `account_balance` -- the balance attached to the given account. This includes the `attached_deposit` that was attached
  to the transaction;
- `attached_deposit` -- the balance that was attached to the call that will be immediately deposited before
  the contract execution starts;
- `prepaid_gas` -- the tokens attached to the call that can be used to pay for the gas;
- `used_gas` -- the gas that was already burnt during the contract execution and attached to promises (cannot exceed `prepaid_gas`);

If contract execution fails `prepaid_gas - used_gas` is refunded back to `signer_account_id` and `attached_deposit`
is refunded back to `predecessor_account_id`.

The following spec is the same for all functions:

```rust
account_balance(balance_ptr: u64)
attached_deposit(balance_ptr: u64)

```

-- writes the value into the `u128` variable pointed by `balance_ptr`.

###### Panics

- If `balance_ptr + 16` points outside the memory of the guest with `MemoryAccessViolation`;
- If called in a view function panics with `ProhibitedInView`.

###### Current bugs

- Use a different name;

---

####

```rust
prepaid_gas() -> u64
used_gas() -> u64
```

###### Panics

- If called in a view function panics with `ProhibitedInView`.
