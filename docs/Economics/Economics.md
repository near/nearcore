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
| `MIN_GAS_PRICE` | `10**8` yoctoNEAR (0.0001 NEAR per Tgas) |
| `MAX_GAS_PRICE` | `20 * MIN_GAS_PRICE` = `2 * 10**9` yoctoNEAR |
| `REWARD_PCT_PER_YEAR` | `0.025` |
| `EPOCH_LENGTH` | `43,200` blocks |
| `TREASURY_PCT` | `0.1` |
| `TREASURY_ACCOUNT_ID` | `treasury.near` on mainnet (genesis field `protocol_treasury_account`) |
| `CONTRACT_PCT` | `0.3` up to protocol version 86, `0` from protocol version 87 |
| `ADJ_FEE` | `0.01` |
| `TOTAL_SEATS` | `100` |
| `ONLINE_THRESHOLD_MIN` | `0.9` |
| `ONLINE_THRESHOLD_MAX` | `0.99` |
| `BLOCK_PRODUCER_KICKOUT_THRESHOLD` | `0.8` |
| `CHUNK_PRODUCER_KICKOUT_THRESHOLD` | `0.8` |
| `CHUNK_VALIDATOR_KICKOUT_THRESHOLD` | `0.7` |

## General Variables

| Name | Description | Initial value |
| - | - | - |
| `totalSupply[t]` | Total supply of NEAR at given epoch[t] | `INITIAL_SUPPLY` |
| `gasPrice[t]` | The cost of 1 unit of *gas* in NEAR tokens (see Transaction Fees section below) | `MIN_GAS_PRICE` |
| `storageAmountPerByte[t]` | Runtime parameter `storage_amount_per_byte`, constant | `10**19` yoctoNEAR (1 NEAR per 100 kB) |

## Issuance

The protocol sets a ceiling for the maximum issuance of tokens, and dynamically decreases this issuance depending on the amount of total fees in the system.

| Name | Description |
| - | - |
| `reward[t]` | `totalSupply[t]` * `REWARD_PCT_PER_YEAR` * `epochTime[t]` / `NUM_SECONDS_IN_A_YEAR` |
| `minted[t]` | The amount minted in epoch[t]. It is the treasury reward plus the rewards that validators earned, so it is at most `reward[t]`. See [Validator Rewards Calculation](#validator-rewards-calculation). |
| `epochFee[t]` | The tokens burnt in epoch[t], such as transaction fees. |
| `issuance[t]` | The net change of the total supply in epoch[t], `issuance[t] = minted[t] - epochFee[t]` |

Where `totalSupply[t]` is the total number of tokens in the system at a given time *t* and `epochTime[t]` is the
duration of the epoch in seconds.
If `epochFee[t] > minted[t]` the issuance is negative, thus the `totalSupply[t]` decreases in given epoch.

## Transaction Fees

Each transaction before inclusion must buy gas enough to cover the cost of bandwidth and execution.

Gas unifies execution and bytes of bandwidth usage of blockchain. Each WASM instruction or pre-compiled function gets assigned an amount of gas based on measurements on common-denominator computer. Same goes for weighting the used bandwidth based on general unified costs.

Gas is priced dynamically in `NEAR` tokens. At each block `t`, we update `gasPrice[t] = gasPrice[t - 1] * (1 + (gasUsed[t - 1] / gasLimit[t - 1] - 0.5) * ADJ_FEE)`.
The result is limited to the range from `MIN_GAS_PRICE` to `MAX_GAS_PRICE`.
If block `t - 1` included no new chunks, the gas price does not change.

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

## Validators

NEAR validators provide their resources in exchange for a reward `epochReward[t]`, where [t] represents the considered epoch

| Name | Description |
| - | - |
| `epochReward[t]` | `= coinbaseReward[t] * (1 - TREASURY_PCT)`. This is the most that validators can earn in epoch[t], shared by stake and scaled by uptime. It equals `validator_reward[t]` in [Validator Rewards Calculation](#validator-rewards-calculation). The part that validators do not earn is not minted. Validators do not receive transaction fees (see [Contract Rewards](#contract-rewards)). |
| `coinbaseReward[t]` | The reward budget of epoch[t]. It is the part of a yearly budget of `REWARD_PCT_PER_YEAR` of the total supply, in proportion to the duration of the epoch. It equals `total_reward[t]` in [Validator Rewards Calculation](#validator-rewards-calculation). |


### Validator Selection

```rust
struct Proposal {
    account_id: AccountId,
    stake: Balance,
    public_key: PublicKey,
}
```

During the epoch, outcome of staking transactions produce `proposals`, which are collected, in the form of `Proposal`s.
There is one type of proposal. The same proposals select block producers, chunk producers and chunk validators. See [Selecting Chunk and Block Producers](../ChainSpec/SelectingBlockProducers.md).
for more information.
At the end of every epoch `T`, next algorithm gets executed to determine validators for epoch `T + 2`:

1. For every validator in `epoch[T]` determine `num_blocks_produced`, `num_chunks_produced` and `num_endorsements_produced` based on what they produced during the epoch.
2. Remove validators that produced too little in `epoch[T]`:
   - validators for whom `num_blocks_produced < num_blocks_expected * BLOCK_PRODUCER_KICKOUT_THRESHOLD` or `num_chunks_produced < num_chunks_expected * CHUNK_PRODUCER_KICKOUT_THRESHOLD`;
   - validators that were not expected to produce any blocks or chunks, and for whom `num_endorsements_produced < num_endorsements_expected * CHUNK_VALIDATOR_KICKOUT_THRESHOLD`.

   Some validators are exempt from these removals, so that one epoch does not remove too much stake when many validators are below the thresholds.
   The epoch config parameter `validator_max_kickout_stake_perc` sets the limit. It is 30 on mainnet.
   Validators are sorted by online ratio, from the best to the worst.
   The online ratio here is the mean of the block, chunk and endorsement production ratios, counting only the roles where the validator was expected to produce something.
   Unlike the reward calculation below, this step uses the endorsement ratio as it is.
   Validators with the same online ratio are ordered by stake, higher stake first, and then by account ID in descending order.
   In this order, validators are exempt until the exempt stake is at least `100 - validator_max_kickout_stake_perc` percent of the total stake of `epoch[T]` (70% on mainnet).
   A validator that was kicked out in the previous epoch cannot be exempt.

   If every validator of `epoch[T]` is either removed in this step or was kicked out in the previous epoch, one of them is not removed in this step.
   It is the validator with the most produced blocks among the validators that were not kicked out in the previous epoch.
   On a tie, the validator that comes first in the validator list is kept.
   If every validator was kicked out in the previous epoch, no validator is kept.
   The removals described next still apply to the kept validator.

   Block producers are also removed if the last block they produced in `epoch[T]` signaled a protocol version lower than the protocol version chosen for epoch `T + 2`.
   The exemption applies only to the removals for producing too little.
   It does not apply to this protocol version rule or to validators that unstake (see step 3).
3. Collect `proposals`, if validator was also a validator in `epoch[T]`, considered stake of the proposal is `0 if proposal.stake == 0 else proposal.stake + reward[proposal.account_id]`.
4. Use the chunk/block producer selection algorithms outlined in [Selecting Chunk and Block Producers](../ChainSpec/SelectingBlockProducers.md).

### Validator Rewards Calculation

Note: all calculations are done in Rational numbers.

Total reward every epoch `t` is equal to:

```python
total_reward[t] = floor(totalSupply * max_inflation_rate * epoch_duration / NUM_SECONDS_IN_A_YEAR)
```

Here `max_inflation_rate` is the yearly inflation rate from the epoch config. It is 1/40 (2.5%) on mainnet.
`totalSupply` is taken from the last block in the epoch.
`epoch_duration` is the time in seconds from the last block of the previous epoch to the last block of this epoch.
`NUM_SECONDS_IN_A_YEAR` is `31,536,000` (365 days).
Block timestamps are in nanoseconds.
The code uses the duration in nanoseconds and divides by `NUM_SECONDS_IN_A_YEAR * 10**9`. So only the final result is rounded down.

After that a fraction of the reward goes to the treasury and the remaining amount will be used for computing validator rewards:

```python
treasury_reward[t] = floor(total_reward[t] * protocol_reward_rate)
validator_reward[t] = total_reward[t] - treasury_reward[t]
```

`protocol_reward_rate` is `TREASURY_PCT`. It is 1/10 on mainnet.
The treasury gets `treasury_reward[t]` in every epoch, regardless of what the validators earn.

Validators removed for producing too few blocks, chunks or chunk endorsements (step 2 of [Validator Selection](#validator-selection)) get no reward.
Validators kicked out for other reasons, such as an old protocol version, are rewarded by the same rules as other validators.
For all other validators, uptime is the mean of the block, chunk and chunk endorsement ratios, adjusted with the `ONLINE_THRESHOLD_MIN` and `ONLINE_THRESHOLD_MAX` parameters.
Only the roles where the validator was expected to produce something are counted.
The endorsement ratio counts as 1 if it is at least `CHUNK_VALIDATOR_KICKOUT_THRESHOLD`, and as 0 otherwise:

```python
ratios = []
if num_blocks_expected[t][j] > 0:
    ratios.append(num_blocks_produced[t][j] / num_blocks_expected[t][j])
if num_chunks_expected[t][j] > 0:
    ratios.append(num_chunks_produced[t][j] / num_chunks_expected[t][j])
if num_endorsements_expected[t][j] > 0:
    endorsement_ratio = num_endorsements_produced[t][j] / num_endorsements_expected[t][j]
    ratios.append(1 if endorsement_ratio >= CHUNK_VALIDATOR_KICKOUT_THRESHOLD else 0)
if len(ratios) == 0:
    uptime[t][j] = 0
else:
    pct_online[t][j] = mean(ratios)
    if pct_online[t][j] < ONLINE_THRESHOLD_MIN:
        uptime[t][j] = 0
    else:
        uptime[t][j] = min(1, (pct_online[t][j] - ONLINE_THRESHOLD_MIN) / (ONLINE_THRESHOLD_MAX - ONLINE_THRESHOLD_MIN))
```

Where `num_blocks_expected[t][j]`, `num_chunks_expected[t][j]` and `num_endorsements_expected[t][j]` are the numbers of blocks, chunks and chunk endorsements that validator `j` is expected to produce in the epoch `t`.

The specific `validator[t][j]` reward for epoch `t` is then proportional to the fraction of stake of this validator from total stake:

```python
validator_reward[t][j] = floor(uptime[t][j] * stake[t][j] * validator_reward[t] / total_stake[t])
```

Here `total_stake[t]` is the total stake of all validators of epoch `t`, including the validators that get no reward.
Each validator reward is added to the locked balance (stake) of the validator.

The amount minted for epoch `t` is `treasury_reward[t]` plus the sum of all `validator_reward[t][j]`.
The part of `validator_reward[t]` that validators do not earn is not minted.
The minted amount is added to the total supply in the first block of the next epoch.

### Slashing

NEAR does not slash stake.
A validator that produces too few blocks, chunks or chunk endorsements can be kicked out of the validator set (see [Validator Selection](#validator-selection)).
A validator kicked out this way gets no reward for that epoch, but its stake is not reduced.

## Protocol Treasury

Treasury account `TREASURY_ACCOUNT_ID` receives fraction of reward every epoch `t`:

```python
# At the end of the epoch, update treasury
def end_of_epoch(..., reward):
    # ...
    accounts[TREASURY_ACCOUNT_ID].amount += treasury_reward[t]
```

## Contract Rewards

Up to protocol version 86, a contract account receives 30% of the gas burnt while executing its functions (`burnt_gas_reward` is 3/10).
The reward is credited to the contract account after applying the corresponding receipt with [`FunctionCallAction`](../RuntimeSpec/Actions.md#functioncallaction).
From protocol version 85, gas is converted to tokens at the lower of the gas price of the receipt and the gas price of the current block. Before that, the gas price of the receipt is used.
If the contract account no longer exists after the receipt is applied, the reward is burnt instead.
From protocol version 87 (nearcore 2.14.0), contracts receive no gas reward, and all gas fees are burnt.

You can read more about:

- [receipts execution](../RuntimeSpec/Receipts.md);
- [runtime fees](../RuntimeSpec/Fees/Fees.md) with description [how gas is charged](../RuntimeSpec/Fees/Fees.md#gas-tracking).
