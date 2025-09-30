# Economics

`This is under heavy development`

## Units

| Name | Value |
| - | - |
| yoctoNEAR | smallest indivisible amount of native currency *NEAR*. |
| NEAR | `10**24` yoctoNEAR |
| block | smallest on-chain unit of time |
| gas | unit to measure usage of blockchain |

## General Parameters

| Name | Value |
| - | - |
| `INITIAL_SUPPLY` | `10**33` yoctoNEAR |
| `MIN_GAS_PRICE` | `10**5` yoctoNEAR |
| `REWARD_PCT_PER_YEAR` | `0.05` |
| `EPOCH_LENGTH` | `43,200` blocks |
| `EPOCHS_A_YEAR` | `730` epochs |
| `INITIAL_MAX_STORAGE` | `10 * 2**40` bytes == `10` TB |
| `TREASURY_PCT` | `0.1` |
| `TREASURY_ACCOUNT_ID` | `treasury` |
| `CONTRACT_PCT` | `0.3` |
| `INVALID_STATE_SLASH_PCT` | `0.05` |
| `ADJ_FEE` | `0.01` |
| `TOTAL_SEATS` | `100` |
| `ONLINE_THRESHOLD_MIN` | `0.9` |
| `ONLINE_THRESHOLD_MAX` | `0.99` |
| `BLOCK_PRODUCER_KICKOUT_THRESHOLD` | `0.9` |
| `CHUNK_PRODUCER_KICKOUT_THRESHOLD` | `0.6` |

## General Variables

| Name | Description | Initial value |
| - | - | - |
| `totalSupply[t]` | Total supply of NEAR at given epoch[t] | `INITIAL_SUPPLY` |
| `gasPrice[t]` | The cost of 1 unit of *gas* in NEAR tokens (see Transaction Fees section below) | `MIN_GAS_PRICE` |
| `storageAmountPerByte[t]` | keeping constant, `INITIAL_SUPPLY / INITIAL_MAX_STORAGE` | `~9.09 * 10**19` yoctoNEAR |

## Issuance

The protocol sets a ceiling for the maximum issuance of tokens, and dynamically decreases this issuance depending on the amount of total fees in the system.

| Name | Description |
| - | - |
| `reward[t]` | `totalSupply[t]` * `REWARD_PCT_PER_YEAR` * `epochTime[t]` / `NUM_SECONDS_IN_A_YEAR` |
| `epochFee[t]` | `sum([(1 - DEVELOPER_PCT_PER_YEAR) * block.txFee + block.stateFee for block in epoch[t]])` |
| `issuance[t]` | The amount of token issued at a certain epoch[t], `issuance[t] = reward[t] - epochFee[t]` |

Where `totalSupply[t]` is the total number of tokens in the system at a given time *t* and `epochTime[t]` is the
duration of the epoch in seconds.
If `epochFee[t] > reward[t]` the issuance is negative, thus the `totalSupply[t]` decreases in given epoch.

## Transaction Fees

Each transaction before inclusion must buy gas enough to cover the cost of bandwidth and execution.

Gas unifies execution and bytes of bandwidth usage of blockchain. Each WASM instruction or pre-compiled function gets assigned an amount of gas based on measurements on common-denominator computer. Same goes for weighting the used bandwidth based on general unified costs.

Gas is priced dynamically in `NEAR` tokens. At each block `t`, we update `gasPrice[t] = gasPrice[t - 1] * (1 + (gasUsed[t - 1] / gasLimit[t - 1] - 0.5) * ADJ_FEE)`.

Where `gasUsed[t] = sum([sum([gas(tx) for tx in chunk]) for chunk in block[t]])`.
`gasLimit[t]` is defined as `gasLimit[t] = gasLimit[t - 1] + validatorGasDiff[t - 1]`, where `validatorGasDiff` is parameter with which each chunk producer can either increase or decrease gas limit based on how long it to execute the previous chunk. `validatorGasDiff[t]` can be only within `±0.1%` of `gasLimit[t]` and only if `gasUsed[t - 1] > 0.9 * gasLimit[t - 1]`.

## State Stake

Amount of `NEAR` on the account represents right for this account to take portion of the blockchain's overall global state. Transactions fail if account doesn't have enough balance to cover the storage required for given account.

```python
def check_storage_cost(account):
    # Compute requiredAmount given size of the account.
    requiredAmount = sizeOf(account) * storageAmountPerByte
    return Ok() if account.amount + account.locked >= requiredAmount else Error(requiredAmount)

# Check when transaction is received to verify that it is valid.
def verify_transaction(tx, signer_account):
    # ...
    # Updates signer's account with the amount it will have after executing this tx.
    update_post_amount(signer_account, tx)
    result = check_storage_cost(signer_account)
    # If enough balance OR account is been deleted by the owner.
    if not result.ok() or DeleteAccount(tx.signer_id) in tx.actions:
        assert LackBalanceForState(signer_id: tx.signer_id, amount: result.err())

# After account touched / changed, we check it still has enough balance to cover it's storage.
def on_account_change(block_height, account):
    # ... execute transaction / receipt changes ...
    # Validate post-condition and revert if it fails.
    result = check_storage_cost(sender_account)
    if not result.ok():
        assert LackBalanceForState(signer_id: tx.signer_id, amount: result.err())
```

Where `sizeOf(account)` includes size of `account_id`, `account` structure and size of all the data stored under the account.

Account can end up with not enough balance in case it gets slashed. Account will become unusable as all originating transactions will fail (including deletion).
The only way to recover it in this case is by sending extra funds from a different accounts.

## Validators

NEAR validators provide their resources in exchange for a reward `epochReward[t]`, where [t] represents the considered epoch

| Name | Description |
| - | - |
| `epochReward[t]` | `= coinbaseReward[t] + epochFee[t]` |
| `coinbaseReward[t]` | The maximum inflation per epoch[t], as a function of `REWARD_PCT_PER_YEAR / EPOCHS_A_YEAR` |


### Validator Selection

```rust
struct Proposal {
    account_id: AccountId,
    stake: Balance,
    public_key: PublicKey,
}
```

During the epoch, outcome of staking transactions produce `proposals`, which are collected, in the form of `Proposal`s.
There are separate proposals for block producers and chunk-only producers, see [Selecting Chunk and Block Producers](../ChainSpec/SelectingBlockProducers.md).
for more information.
At the end of every epoch `T`, next algorithm gets executed to determine validators for epoch `T + 2`:

1. For every chunk/block producer in `epoch[T]` determine `num_blocks_produced`, `num_chunks_produced` based on what they produced during the epoch.
2. Remove validators, for whom `num_blocks_produced < num_blocks_expected * BLOCK_PRODUCER_KICKOUT_THRESHOLD` or `num_chunks_produced < num_chunks_expected * CHUNK_PRODUCER_KICKOUT_THRESHOLD`.
3. Collect chunk-only and block producer `proposals`, if validator was also a validator in `epoch[T]`, considered stake of the proposal is `0 if proposal.stake == 0 else proposal.stake + reward[proposal.account_id]`.
4. Use the chunk/block producer selection algorithms outlined in [Selecting Chunk and Block Producers](../ChainSpec/SelectingBlockProducers.md).

### Validator Rewards Calculation

Note: all calculations are done in Rational numbers.

Total reward every epoch `t` is equal to:

```python
total_reward[t] = floor(totalSupply * max_inflation_rate * epoch_length)
```

where `max_inflation_rate` is the genesis parameter while `totalSupply` and `epoch_length` are
taken from the last block in the epoch.

After that a fraction of the reward goes to the treasury and the remaining amount will be used for computing validator rewards:

```python
treasury_reward[t] = floor(reward[t] * protocol_reward_rate)
validator_reward[t] = total_reward[t] - treasury_reward[t]
```

Validators that didn't meet the threshold for either blocks or chunks get kicked out and don't get any reward, otherwise uptime
of a validator is computed as an average of (block produced/expected, chunk produced/expected,
and chunk endorsements produced/expected) and adjusted wrt the `ONLINE_THRESHOLD`, `ONLINE_THRESHOLD_MIN`, `ONLINE_THRESHOLD_MAX` parameters:

```python
pct_online[t][j] = mean(num_blocks_produced[t][j] / num_blocks_expected[t][j],
                        num_chunks_produced[t][j] / num_chunks_expected[t][j],
                        num_endorsements_produced[t][j] / num_endorsements_expected[t][j])
if pct_online > ONLINE_THRESHOLD:
    uptime[t][j] = min(1, (pct_online[t][j] - ONLINE_THRESHOLD_MIN) / (ONLINE_THRESHOLD_MAX - ONLINE_THRESHOLD_MIN))
else:
    uptime[t][j] = 0
```

Where `expected_produced_blocks` and `expected_produced_chunks` is the number of blocks and chunks respectively that is expected to be produced by given validator `j` in the epoch `t`.

The specific `validator[t][j]` reward for epoch `t` is then proportional to the fraction of stake of this validator from total stake:

```python
validator_reward[t][j] = floor(uptime[t][j] * stake[t][j] * total_reward[t] / total_stake[t])
```

### Slashing

#### ChunkProofs

```python
# Check that chunk is invalid, because the proofs in header don't match the body.
def chunk_proofs_condition(chunk):
    # TODO

# At the end of the epoch, run update validators and
# determine how much to slash validators.
def end_of_epoch_update_validators(validators):
    # ...
    for validator in validators:
        if validator.is_slashed:
            validator.stake -= INVALID_STATE_SLASH_PCT * validator.stake
```

#### ChunkState

```python
# Check that chunk header post state root is invalid,
# because the execution of previous chunk doesn't lead to it.
def chunk_state_condition(prev_chunk, prev_state, chunk_header):
    # TODO

# At the end of the epoch, run update validators and
# determine how much to slash validators.
def end_of_epoch(..., validators):
    # ...
    for validator in validators:
        if validator.is_slashed:
            validator.stake -= INVALID_STATE_SLASH_PCT * validator.stake
```

## Protocol Treasury

Treasury account `TREASURY_ACCOUNT_ID` receives fraction of reward every epoch `t`:

```python
# At the end of the epoch, update treasury
def end_of_epoch(..., reward):
    # ...
    accounts[TREASURY_ACCOUNT_ID].amount = treasury_reward[t]
```

## Contract Rewards

Contract account is rewarded with 30% of gas burnt during the execution of its functions.
The reward is credited to the contract account after applying the corresponding receipt with [`FunctionCallAction`](../RuntimeSpec/Actions.md#functioncallaction), gas is converted to tokens using gas price of the current block.

You can read more about:

- [receipts execution](../RuntimeSpec/Receipts.md);
- [runtime fees](../RuntimeSpec/Fees/Fees.md) with description [how gas is charged](../RuntimeSpec/Fees/Fees.md#gas-tracking).
