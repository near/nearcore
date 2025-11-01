# Staking and slashing

## Stake invariant

`Account` has two fields representing its tokens: `amount` and `locked`. `amount + locked` is the total number of
tokens an account has: locking/unlocking actions involve transferring balance between the two fields, and slashing
is done by subtracting from the `locked` value.

On a stake action the balance gets locked immediately (but the locked balance can only increase), and the stake proposal is 
passed to the epoch manager. Proposals get accumulated during an epoch and get processed all at once when an epoch is finalized.
Unlocking only happens at the start of an epoch.

Account's stake is defined per epoch and is stored in `EpochInfo`'s `validators` and `fishermen` sets. `locked` is always
equal to the maximum of the last three stakes and the highest proposal in the current epoch.


### Returning stake

`locked` is the number of tokens locked for staking, it's computed the following way:

- initially it's the value in genesis or `0` for new accounts
- on a staking proposal with a value higher than `locked`, it increases to that value
- at the start of each epoch it's recomputed:
    1. consider the most recent 3 epochs
    2. for non-slashed accounts, take the maximum of their stakes in those epochs
    3. if an account made a proposal in the block that starts the epoch, also take the maximum with the proposal value
    4. change `locked` to the resulting value (and update `amount` so that `amount + locked` stays the same)

### Slashing

TODO.

