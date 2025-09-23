# EpochManager

## Finalizing an epoch

At the last block of epoch `T`, `EpochManager` computes `EpochInfo` for epoch `T+2`, which
is defined by `EpochInfo` for `T+1` and the information aggregated from blocks of epoch `T`.

`EpochInfo` is all the information that `EpochManager` stores for the epoch, which is:

- `epoch_height`: epoch height (`T+2`)
- `validators`: the list of validators selected for epoch `T+2`
- `validator_to_index`: Mapping from account id to index in `validators`
- `block_producers_settlement`: defines the mapping from height to block producer
- `chunk_producers_settlement`: defines the mapping from height and shard id to chunk producer
- `hidden_validators_settlement`: TODO
- `fishermen`, `fishermen_to_index`: TODO. disabled on mainnet through a large `fishermen_threshold` in config
- `stake_change`: TODO
- `validator_reward`: validator reward for epoch `T`
- `validator_kickout`: see [Kickout set](#kickout-set)
- `minted_amount`: minted tokens in epoch `T`
- `seat_price`: seat price of the epoch
- `protocol_version`: TODO

Aggregating blocks of the epoch computes the following sets:

- `block_stats`/`chunk_stats`: uptime statistics in the form of `produced` and `expected` blocks/chunks for each validator of `T`
- `proposals`: stake proposals made in epoch `T`. If an account made multiple proposals, the last one is used.
- `slashes`: see [Slash set](#slash-set)

### Slash set

<!-- cspell:ignore slashable -->
NOTE: slashing is currently disabled. The following is the current design, which can change.

Slash sets are maintained on block basis. If a validator gets slashed in epoch `T`, subsequent blocks of epochs `T` and
`T+1` keep it in their slash sets. At the end of epoch `T`, the slashed validator is also added to `kickout[T+2]`.
Proposals from blocks in slash sets are ignored.

It's kept in the slash set (and kickout sets) for two or three epochs depending on whether it was going to be a validator in `T+1`:

- Common case: `v` is in `validators[T]` and in `validators[T+1]`
  - proposals from `v` in `T`, `T+1` and `T+2` are ignored
  - `v` is added to `kickout[T+2]`, `kickout[T+3]` and `kickout[T+4]`  as slashed
  - `v` can stake again starting with the first block of `T+3`.
- If `v` is in `validators[T]` but not in `validators[T+1]` (e.g. if it unstaked in `T-1`)
  - proposals from `v` in `T` and `T+1` are ignored
  - `v` is added to `kickout[T+2]` and `kickout[T+3]` as slashed
  - `v` can make a proposal in `T+2` to become a validator in `T+4`
- If `v` is in `validators[T-1]` but not in `validators[T]` (e.g. did slashable behavior right before rotating out)
  - proposals from `v` in `T` and `T+1` are ignored
  - `v` is added to `kickout[T+2]` and `kickout[T+3]` as slashed
  - `v` can make a proposal in `T+2` to become a validator in `T+4`

## Computing EpochInfo

### Kickout set

`kickout[T+2]` contains validators of epoch `T+1` that stop being validators in `T+2`, and also accounts that are not
necessarily validators of `T+1`, but are kept in slashing sets due to the rule described [above](#slash-set).

`kickout[T+2]` is computed the following way:

1. `Slashed`: accounts in the slash set of the last block in `T`
2. `Unstaked`: accounts that remove their stake in epoch `T`, if their stake is non-zero for epoch `T+1`
3. `NotEnoughBlocks/NotEnoughChunks`: For each validator compute the ratio of blocks produced to expected blocks produced (same with chunks produced/expected).
    If the percentage is below `block_producer_kickout_threshold` (`chunk_producer_kickout_threshold`), the validator is kicked out.
    - Exception: If all validators of `T` are either in `kickout[T+1]` or to be kicked out, we don't kick out the
    validator with the maximum number of blocks produced. If there are multiple, we choose the one with
    lowest validator id in the epoch.
4. `NotEnoughStake`: computed after validator selection. Accounts who have stake in epoch `T+1`, but don't meet stake threshold for epoch `T+2`.
5. `DidNotGetASeat`: computed after validator selection. Accounts who have stake in epoch `T+1`, meet stake threshold for epoch `T+2`, but didn't get any seats.

### Processing proposals

The set of proposals is processed by the validator selection algorithm, but before that, the set of proposals is adjusted
the following way:

1. If an account is in the slash set as of the end of `T`, or gets kicked out for `NotEnoughBlocks/NotEnoughChunks` in epoch `T`,
  its proposal is ignored.
2. If a validator is in `validators[T+1]`, and didn't make a proposal, add an implicit proposal with its stake in `T+1`.
3. If a validator is in both `validators[T]` and `validators[T+1]`, and made a proposal in `T` (including implicit),
  then its reward for epoch `T` is automatically added to the proposal.

The adjusted set of proposals is used to compute the seat price, and determine `validators`,`block_producers_settlement`,
`chunk_producers_settlement`sets. This algorithm is described in [Economics](../../Economics/Economics.md#validator-selection).

### Validator reward

Rewards calculation is described in the [Economics](../../Economics/Economics.md#validator-rewards-calculation) section.
