# Selecting Chunk and Block Producers

## Background

Near is intended to be a sharded blockchain. At the time of writing (March 2021), challenges (an
important security feature for a sharded network) are not fully implemented. As a stepping stone
towards the full sharded solution, Near will go through a phase called "Simple Nightshade". In this
protocol, block producers will track all shards (i.e. validate all transactions, eliminating the need
for challenges), and there will be an additional type of participant called a "chunk-only producer"
which tracks only a single shard. A block includes one chunk for each shard, and it is the chunks
which include the transactions that were executed for its associated shard. The purpose of this
design is to allow decentralization (running a node which supports a chunk-only producer should be
possible for a large number of participants) while maintaining security (since block producers track
all shards). For more details on chunks see the subsequent chapter on Transactions. Note: the
purpose of the "chunk-only" nomenclature is to reduce confusion since block producers will also
produce chunks some times (they track all shards, so they will be able to produce chunks for any
shard easily); thus the key distinction is that chunk-only producers only produce chunks, i.e. never
produce blocks.

Near is a permission-less blockchain, so anyone (with sufficient stake) can become a chunk-only
producer, or a block producer. In this section we outline the algorithm by which chunk-only
producers and block producers are selected in each epoch from the proposals of participants in the
network. Additionally, we will specify the algorithm for assigning those chunk-only producers and
block producers to be the one responsible for producing the chunk/block at each height and for each
shard.

There are several desiderata for these algorithms:

- Larger stakes should be preferred (more staked tokens means more security)
- The frequency with which a given participant is selected to produce a particular chunk/block is
  proportional to that participant's stake
- All participants selected as chunk/block producers should be selected to produce at least one
  chunk/block during the epoch
- It should be possible to determine which chunk/block producer is supposed to produce the
  chunk/block at height $h$, for any $h$ within the epoch, in constant time
- The number of distinct validators should be as large as is allowed by the
  scalability in the consensus algorithm (too large and the system would be too slow, too small and
  the system would be too centralized) $^{\dagger}$

> $\dagger$ Note: By "distinct validators" we mean the number of different signing keys.
> We recognize it is possible for a single "logical entity" to split their stake into two or more
> proposals (Sybil attack), however steps to prevent this kind of attack against centralization are
> out of scope for this document.

## Assumptions

- The maximum numbers of block producers, chunk producers and chunk validators are parameters of
  the protocol itself (i.e. all nodes must agree on them). In this document, we denote them by
  `MAX_NUM_BP`, `MAX_NUM_CP` and `MAX_NUM_CV`. They are the epoch config parameters
  `num_block_producer_seats`, `num_chunk_producer_seats` and `num_chunk_validator_seats`.
- The minimum number of blocks in the epoch is known at the time of block producer selection. This
  minimum does not need to be incredibly accurate, but we will assume it is within a factor of 2 of
  the actual number of blocks in the epoch. In this document we will refer to this as the "length of
  the epoch", denoted by `epoch_length`.
- To meet the requirement that any chosen validator will be selected to produce at least one
  chunk/block in the epoch, we assume it is acceptable for the probability of this _not_ happening
  to be sufficiently low. Let `PROBABILITY_NEVER_SELECTED` be the
  maximum allowable probability that the chunk/block producer with the least stake will never
  be selected to produce a chunk/block during the epoch. We will additionally assume the chunk/block
  producer assigned to make each chunk/block is chosen independently, and in proportion to the
  participant's stake. Therefore, the probability that the block producer with least stake is never
  chosen is given by the expression $(1 - (s_\text{min} / S))^\text{epoch\_length}$, where
  $s_\text{min}$ is the least stake of any block producer and $S$ is the total stake of all block
  producers. Keeping this probability below `PROBABILITY_NEVER_SELECTED` requires a lower bound on
  $s_\text{min} / S$. The protocol does not compute this bound from a probability. Instead, each
  role uses the protocol parameter `minimum_stake_ratio` as the lower bound (see below).

In mainnet and testnet, `epoch_length` is set to `43200`. With $\text{PROBABILITY\_NEVER\_SELECTED}=0.001$,
we obtain $s_\text{min} / S = 160/1000,000$ (1/6250). Mainnet and testnet used this value for `minimum_stake_ratio`
up to protocol version 70. From protocol version 71 (nearcore 2.2.0), `minimum_stake_ratio` is 1/62500 (16 per
million) on mainnet and testnet. With this value, the protocol does not guarantee the 0.001 bound.

## Algorithm for selecting block and chunk producers

A potential validator cannot choose a role. There is only one type of proposal. The same algorithm, with the same
`minimum_stake_ratio`, selects validators for three roles: block producers (up to `num_block_producer_seats`), chunk
producers (up to `num_chunk_producer_seats`) and chunk validators (up to `num_chunk_validator_seats`). On mainnet these
are 100, 100 and 500. On testnet they are 20, 20 and 500. Each role takes the proposals in the same order, largest
stake first. As a result, a role with more seats includes every validator of a role with fewer seats. Because the
block producer and chunk producer seat counts are equal on mainnet and testnet, the same validators fill both roles.
They are also chunk validators. The other selected validators are only chunk validators. They validate chunks using
state witnesses and do not produce blocks or chunks. The stake needed to become a validator (the seat price) is the
lowest of the three role thresholds.

### select_validators

### Input

- `max_num_validators: u16` max number of validators to be selected
- `min_stake_fraction: Ratio<u128>` minimum stake ratio for selected validator
- `validator_proposals: Vec<ValidatorStake>` (proposed stakes for the next epoch from nodes sending
  staking transactions)

### Output

- `(validators: Vec<ValidatorStake>, sampler: WeightedIndex)`

### Steps

```python
# Largest stake first. Equal stakes are ordered by account ID, smallest first.
sorted_proposals =
    sorted(validator_proposals, key=lambda v: (-v.stake, v.account_id))

total_stake = 0

validators = []
for v in sorted_proposals[0:max_num_validators]:
    total_stake += v.stake
    if (v.stake / total_stake) > min_stake_fraction:
        validators.append(v)
    else:
        break

validator_sampler = WeightedIndex([v.stake for v in validators])

return (validators, validator_sampler)
```

## Algorithm for selecting block producers

### Input

- `MAX_NUM_BP: u16` Max number of block producers, see Assumptions
- `min_stake_fraction: Ratio<u128>` $s_\text{min} / S$, see Assumptions
- `validator_proposals: Vec<ValidatorStake>` (proposed stakes for the next epoch from nodes sending
  staking transactions)

```python
select_validators(MAX_NUM_BP, min_stake_fraction, validator_proposals)
```

## Algorithm for selecting chunk producers

### Input

- `MAX_NUM_CP: u16` max number of chunk producers, see Assumptions
- `min_stake_fraction: Ratio<u128>` $s_\text{min} / S$, see Assumptions
- `validator_proposals: Vec<ValidatorStake>` (proposed stakes for the next epoch from nodes sending
  staking transactions)

```python
select_validators(MAX_NUM_CP, min_stake_fraction, validator_proposals)
```

Chunk producers use the same minimum stake ratio as block producers. Before protocol version 74 (nearcore 2.5.0),
the chunk producer ratio was `min_stake_fraction/num_shards`.

## Algorithm for assigning chunk producers to shards

Note that block producers are a subset of chunk producers, so this algorithm will also assign block producers
to shards. This also means that a block producer may only be assigned to a subset of shards. For the security of
the protocol, all block producers must track all shards, even if they are not assigned to produce chunks for all shards.
We enforce that in the implementation level, not the protocol level. A validator node will panic if it doesn't track all
shards.

### Input

- `chunk_producers: Vec<ValidatorStake>`
- `num_shards: usize`
- `min_validators_per_shard: usize`
- `prev_assignment: Vec<Vec<ValidatorStake>>` chunk producers of each shard in the previous epoch
- `changes_limit: usize` the epoch config parameter `chunk_producer_assignment_changes_limit` (5 on
  mainnet and testnet), except in an epoch where the shard layout changes (see below)
- `rng_seed: [u8; 32]` the epoch seed of the epoch being assigned

### Output

- `validator_shard_assignments: Vec<Vec<ValidatorStake>>`
  - $i$-th element gives the chunk producers assigned to the shard with index $i$ in the shard layout
    (its position in the layout, which can differ from its shard ID)

### Steps

If there are fewer than `min_validators_per_shard * num_shards` chunk producers, some chunk producers must be
assigned to more than one shard:

- While any shard has fewer than `min_validators_per_shard` validators assigned to it:
  - Let `cp_i` be the next element of `chunk_producers` (cycle back to the beginning as needed)
  - Let `shard` be the shard with the fewest number of assigned validators such that `cp_i` has
    not been assigned to `shard`. If there is no such shard, or that shard already has
    `min_validators_per_shard` validators, skip `cp_i`.
  - Assign `cp_i` to `shard`
- Return the shard assignments

Otherwise, each chunk producer is assigned to exactly one shard:

- Start from `prev_assignment`, matching shards by shard index, and remove the validators that are no longer
  chunk producers.
- Assign each new chunk producer to the shard with the fewest chunk producers.
- Move chunk producers to balance the shards. Each move takes a chunk producer, chosen at random using
  `rng_seed`, from the shard with the most chunk producers to the shard with the fewest. Moves continue while any
  shard has fewer than `min_validators_per_shard` chunk producers. They also continue while the largest and
  smallest shards differ by more than one chunk producer, as long as the number of placements and moves is below
  `changes_limit`.
- Return the shard assignments

Each placement of a new chunk producer and each move counts toward `changes_limit`. These steps balance the number
of chunk producers per shard, not their stake. On mainnet and testnet, `min_validators_per_shard` is 1.

From protocol version 85 (nearcore 2.13.0), in an epoch where the shard layout changes, the first step builds the
starting assignment by shard ID instead of shard index. A shard that did not change keeps its chunk producers. The
chunk producers of a split shard are divided among its children. They are taken in order of stake, largest first,
and each one goes to the child with the least total stake so far. In such an epoch, `changes_limit` is the larger of
`chunk_producer_assignment_changes_limit` and the number of chunk producers divided by the number of shards (rounded
down). Before protocol version 85, an epoch with a new shard layout started from an empty assignment.

## Algorithm for selecting chunk validators

### Input

- `MAX_NUM_CV: u16` max number of chunk validators, see Assumptions
- `min_stake_fraction: Ratio<u128>` $s_\text{min} / S$, see Assumptions
- `validator_proposals: Vec<ValidatorStake>` (proposed stakes for the next epoch from nodes sending
  staking transactions)

```python
select_validators(MAX_NUM_CV, min_stake_fraction, validator_proposals)
```

## Algorithm for assigning chunk validators to shards

Chunk validators are assigned to shards through mandates. A mandate is an amount of a validator's stake. At each
height, each mandate is assigned to one shard, and the validator must validate the chunk of that shard. Every
validator of the epoch takes part, including the block producers and chunk producers.

- When the validators of the epoch are selected, a stake per mandate is chosen so that the validators hold about
  `target_validator_mandates_per_shard * num_shards` whole mandates in total. `target_validator_mandates_per_shard`
  is 105 on mainnet and testnet.
- A validator holds one whole mandate for each full multiple of the stake per mandate in its stake. The rest of its
  stake is a partial mandate.
- At each height, the whole mandates are shuffled and dealt to the shards in turn, in a random shard order. So every
  shard gets almost the same number of whole mandates. The partial mandates are shuffled and dealt in the same way.
  The random number generator for this is seeded with the epoch seed and the height.
- A validator validates the chunk of a shard at a height if it holds at least one whole or partial mandate for
  that shard at that height. It appears once in the chunk validators of that shard, with an assigned stake. The
  assigned stake is the sum of the stake of all whole and partial mandates that the validator holds for that shard.
- After the mandates are dealt, the validators of each shard are sorted by validator ID (their index in the epoch's
  validator list). They are then shuffled with the same random number generator, one shard at a time in order of
  shard index. The result is the shard's ordered chunk validators for that height. Block bodies list chunk
  endorsements in this order, and the height used is the chunk's `height_created`.

A chunk is endorsed when the validators that endorse it hold more than two thirds of the total assigned stake of the
shard's chunk validators. So an endorsement counts with the validator's assigned stake on that shard, not with its
full stake.

For example, say the stake per mandate is 10 NEAR. A validator with 25 NEAR holds two whole mandates of 10 NEAR and
one partial mandate of 5 NEAR. If one whole mandate and the partial mandate go to a shard at some height, the
validator appears once in the chunk validators of that shard, with 15 NEAR of assigned stake. One validator can hold
several mandates for the same shard, so a shard can have fewer distinct chunk validators than mandates.

## Algorithm for sampling validators proportional to stake

<!-- cspell:ignore Vose's byteorder -->
We sample validators with probability proportional to their stake using the following data structure.

- `weighted_sampler: WeightedIndex`
  - Allow $O(1)$ sampling
  - This structure will be based on the
    [WeightedIndex](https://docs.rs/rand/latest/rand/distr/weighted/struct.WeightedIndex.html)
    implementation (see a description of [Vose's Alias
    Method](https://en.wikipedia.org/wiki/Alias_method) for details)

This algorithm is applied using both block producers and chunk producers in the subsequent
algorithms for selecting a specific block producer and chunk producer at each height.

### Input

- `rng_seed: [u8; 32]`
  - See usages of this algorithm below to see how this seed is generated
- `validators: Vec<ValidatorStake>`
- `sampler: WeightedIndex`

### Output

- `selection: ValidatorStake`

### Steps

```python
# The seed is used as an entropy source for the random numbers.
# The first 8 bytes select a block producer uniformly.
uniform_index = int.from_bytes(rng_seed[0:8], byteorder='little') % len(validators)

# The next 16 bytes uniformly pick some weight between 0 and the total
# weight (i.e. stake) of all block producers.
let uniform_weight = int.from_bytes(rng_seed[8:24], byteorder='little') \
    % sampler.weight_sum()

# Return either the uniformly selected block producer, or its "alias"
# depending on the uniformly selected weight.
index = uniform_index \
    if uniform_weight < sampler.no_alias_odds[uniform_index] \
    else sampler.aliases[uniform_index]

return validators[index]
```

## Algorithm for selecting producer of block at height $h$

### Input

- `h: BlockHeight`
  - Height to compute the block producer for
  - Only heights within the epoch corresponding to the given block producers make sense as input
- `block_producers: Vec<ValidatorStake>` (output from above)
- `block_producer_sampler: WeightedIndex`
- `epoch_rng_seed: [u8; 32]`
  - Fixed seed for the epoch determined from Verified Random Function (VRF) output of last block in
    the previous epoch

### Output

- `block_producer: ValidatorStake`

### Steps

```python
# Concatenates the bytes of the epoch seed with the height,
# then computes the sha256 hash.
block_seed = combine(epoch_rng_seed, h)

# Use the algorithm defined above
return select_validator(rng_seed=block_seed, validators=block_producers, sampler=block_producer_sampler)
```

## Algorithm for selection of chunk producer at height $h$ for all shards

### Input

- `h: BlockHeight`
  - Height to compute the chunk producers for
- `epoch_rng_seed: [u8; 32]` (same as for selection of block producer at height $h$)
- `shard_ids: Vec<ShardId>` the shard IDs of the shard layout, in order of shard index
- `chunk_producer_sampler: Vec<WeightedIndex>` (one per shard, built from the stakes of the chunk
  producers assigned to that shard)
- `validator_shard_assignments: Vec<Vec<ValidatorStake>>` (output from assigning chunk producers to shards)

### Output

- `chunk_producers: Vec<ValidatorStake>`
  - `i`th element gives the validator that will produce the chunk for the shard with index `i`

### Steps

```python
result = []
for shard_index, shard_id in enumerate(shard_ids):
    # concatenate bytes and take hash to create unique seed
    shard_seed = combine(epoch_rng_seed, h, shard_id)
    # Use selection algorithm defined above.
    # Assignments and samplers are indexed by shard index,
    # while the seed uses the shard ID.
    cp = select_validator(
        rng_seed=shard_seed,
        validators=validator_shard_assignments[shard_index],
        sampler=chunk_producer_sampler[shard_index]
    )
    result.append(cp)

return result
```

### Excluding chunk producers that miss many chunks

Up to protocol version 86, the steps above give the producer of every chunk. From protocol version 87 (nearcore
2.14.0), chunk producers that miss many chunks on a shard are excluded from producing chunks on that shard:

- The producer of a chunk is chosen when the chunk's grandparent block is processed. The grandparent is the parent
  of the block that the chunk builds on.
- The decision uses this epoch's chunk counts up to the last final block of the grandparent. No producer is
  excluded while that last final block is less than 1000 heights after the first block of the epoch.
- After that, a chunk producer is excluded on a shard if, in this epoch, it has missed at least 100 chunks on that
  shard and has produced less than 80% of the chunks expected from it there. If every chunk producer of a shard
  would be excluded, the one with the highest production ratio stays eligible.
- The chunk producer is sampled by stake from the shard's chunk producers that are not excluded. The sampling uses
  the seed computed above. In the seed, $h$ is the grandparent's height plus 2. This is the chunk's height unless heights were
  skipped between the grandparent and the parent.
- If the grandparent is in an earlier epoch than the chunk, the steps above are used with the chunk's height and
  no producer is excluded. This happens for chunks built on the last block of the previous epoch or on the first
  block of the epoch.

Exclusion only affects chunk production on that shard. The excluded chunk producer keeps its other roles.
See also [Early chunk producer reassignment](EpochAndStaking/EpochManager.md#early-chunk-producer-reassignment).
