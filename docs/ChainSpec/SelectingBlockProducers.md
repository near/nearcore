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
- The block producer chosen at height $h$ should have been a chunk producer for some shard at
  height $h - 1$, this minimizes network communication between chunk producers and block
  producers
- The number of distinct chunk-only/block producers should be as large as is allowed by the
  scalability in the consensus algorithm (too large and the system would be too slow, too small and
  the system would be too centralized) $^{\dagger}$

> $\dagger$ Note: By "distinct block producers" we mean the number of different signing keys.
> We recognize it is possible for a single "logical entity" to split their stake into two or more
> proposals (Sybil attack), however steps to prevent this kind of attack against centralization are
> out of scope for this document.

## Assumptions

- The maximum number of distinct chunk-only producers and block producers supported by the consensus
  algorithm is a fixed constant. This will be a parameter of the protocol itself (i.e. all nodes
  must agree on the constant). In this document, we will denote the maximum number of chunk-only
  producers as `MAX_NUM_CP` and the maximum number of block producers by `MAX_NUM_BP`.
- The minimum number of blocks in the epoch is known at the time of block producer selection. This
  minimum does not need to be incredibly accurate, but we will assume it is within a factor of 2 of
  the actual number of blocks in the epoch. In this document we will refer to this as the "length of
  the epoch", denoted by `epoch_length`.
- To meet the requirement that any chosen validator will be selected to produce at least one
  chunk/block in the epoch, we assume it is acceptable for the probability of this _not_ happening
  to be sufficiently low. Let `PROBABILITY_NEVER_SELECTED` be a protocol constant which gives the
  maximum allowable probability that the chunk-only/block producer with the least stake will never
  be selected to produce a chunk/block during the epoch. We will additionally assume the chunk/block
  producer assigned to make each chunk/block is chosen independently, and in proportion to the
  participant's stake. Therefore, the probability that the block producer with least stake is never
  chosen is given by the expression $(1 - (s_\text{min} / S))^\text{epoch\_length}$, where
  $s_\text{min}$ is the least stake of any block producer and $S$ is the total relevant
  stake (what stake is "relevant" depends on whether the validator is a chunk-only producer or a
  block producer; more details below). Hence, the algorithm will enforce the condition $(1 -
  (s_\text{min} / S))^\text{epoch\_length} < \text{PROBABILITY\_NEVER\_SELECTED}$.

In mainnet and testnet, `epoch_length` is set to `43200`. Let $\text{PROBABILITY\_NEVER\_SELECTED}=0.001$,
we obtain, $s_\text{min} / S = 160/1000,000$.

## Algorithm for selecting block and chunk producers

A potential validator cannot specify whether they want to become a block producer or a chunk-only producer.
There is only one type of proposal. The same algorithm is used for selecting block producers and chunk producers,
but with different thresholds. The threshold for becoming block producers is higher, so if a node is selected as a block
producer, it will also be a chunk producer, but not the other way around. Validators who are selected as chunk producers
but not block producers are chunk-only producers.

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
sorted_proposals =
    sorted_descending(validator_proposals, key=lambda v: (v.stake, v.account_id))

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
- `num_shards: u64` number of shards
- `validator_proposals: Vec<ValidatorStake>` (proposed stakes for the next epoch from nodes sending
  staking transactions)

```python
select_validators(MAX_NUM_CP, min_stake_fraction/num_shards, validator_proposals)
```

The reasoning for using `min_stake_fraction/num_shards` as the threshold here is that
we will assign chunk producers to shards later and the algorithm (described below) will try to assign
them in a way that the total stake in each shard is distributed as evenly as possible.
So the total stake in each shard will be roughly be `total_stake_all_chunk_producers / num_shards`.

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

### Output

- `validator_shard_assignments: Vec<Vec<ValidatorStake>>`
  - $i$-th element gives the validators assigned to shard $i$

### Steps

- While any shard has fewer than `min_validators_per_shard` validators assigned to it:
  - Let `cp_i` be the next element of `chunk_producers` (cycle back to the beginning as needed)
    - Note: if there are more shards than chunk producers, then some chunk producers will
      be assigned to multiple shards. This is undesirable because we want each chunk-only producer
      to be a assigned to exactly one shard. However, block producers are also chunk producers,
      so even if we must wrap around a little, chunk-only producers may still not be assigned
      multiple shards. Moreover, we assume that in practice there will be many more chunk producers
      than shards (in particular because block producers are also chunk producers).
  - Let `shard_id` be the shard with the fewest number of assigned validators such that `cp_i` has
    not been assigned to `shard_id`
  - Assign `cp_i` to `shard_id`
- While there are any validators which have not been assigned to any shard:
  - Let `cp_i` be the next validator not assigned to any shard
  - Let `shard_id` be the shard with the least total stake (total stake = sum of stakes of all
    validators assigned to that shard)
  - Assign `cp_i` to `shard_id`
- Return the shard assignments

In addition to the above description, we have a [proof-of-concept (PoC) on
GitHub](https://github.com/birchmd/bp-shard-assign-poc). Note: this PoC has not been updated since
the change to Simple Nightshade, so it assumes we are assigning block producers to shards. However,
the same algorithm works to assign chunk producers to shards; it is only a matter of renaming
variables referencing "block producers" to reference "chunk producers" instead.

## Algorithm for sampling validators proportional to stake

<!-- cspell:ignore Vose's byteorder -->
We sample validators with probability proportional to their stake using the following data structure.

- `weighted_sampler: WeightedIndex`
  - Allow $O(1)$ sampling
  - This structure will be based on the
    [WeightedIndex](https://docs.rs/rand/latest/rand/distr/weighted/struct.WeightedIndex.html)
    implementation (see a description of [Vose's Alias
    Method](https://en.wikipedia.org/wiki/Alias_method) for details)

This algorithm is applied using both chunk-only producers and block producers in the subsequent
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

- (same inputs as selection of block producer at height h)
- `num_shards: usize`
- `chunk_producer_sampler: Vec<WeightedIndex>` (outputs from chunk-only producer selection)
- `validator_shard_assignments: Vec<Vec<ValidatorStake>>`

### Output

- `chunk_producers: Vec<ValidatorStake>`
  - `i`th element gives the validator that will produce the chunk for shard `i`. Note: at least one
    of these will be a block producer, while others will be chunk-only producers.

### Steps

```python
bp = block_producer_at_height(
    h + 1,
    block_producers,
    block_producer_sampler,
    epoch_rng_seed,
)

result = []
for shard_id in range(num_shards):
    # concatenate bytes and take hash to create unique seed
    shard_seed = combine(epoch_rng_seed, h, shard_id)
    # Use selection algorithm defined above
    cp = select_validator(
        rng_seed=shard_seed,
        validators=validator_shard_assignments[shard_id],
        sampler=chunk_producer_sampler[shard_id]
    )
    result.append(cp)

# Ensure the block producer for the next block also produces one of the shards.
# `bp` could already be in the result because block producers are also
# chunk producers (see algorithm for selecting chunk producers from proposals).
if bp not in result:
    # select a random shard for the block producer to also create the chunk for
    rand_shard_seed = combine(epoch_rng_seed, h)
    bp_shard_id = int.from_bytes(rand_shard_seed[0:8], byteorder='little') % num_shards
    result[bp_shard_id] = bp

return result
```
