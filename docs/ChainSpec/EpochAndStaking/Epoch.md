# Epoch

All blocks are split into epochs. Within one epoch, the set of validators is fixed, and validator rotation
happens at epoch boundaries.

Genesis block is in its own epoch. After that, a block is either in its parent's epoch or
starts a new epoch if it meets certain conditions.

Within one epoch, validator assignment is based on block height: each height has a block producer assigned to it, and
each height and shard have a chunk producer.

### End of an epoch

Let `estimated_next_epoch_start = first_block_in_epoch.height + epoch_length`

A `block` is defined to be the last block in its epoch if it's the genesis block or if the following condition is met:

- `block.last_finalized_height + 3 >= estimated_next_epoch_start`

`epoch_length` is defined in `genesis_config` and has a value of `43200` height delta on mainnet (12 hours at 1 block per second).

Since every final block must have two more blocks on top of it, it means that the last block in an epoch will have a height of at least `block.last_finalized_height + 2`, so for the last block it holds that `block.height + 1 >= estimated_next_epoch_start`. Its height will be at least `estimated_next_epoch_start - 1`.

Note that an epoch only ends when there is a final block above a certain height. If there are no final blocks, the epoch will be stretched until the required final block appears. An epoch can potentially be longer than `epoch_length`.

![Diagram of epoch end](/images/epoch_end_diagram.png)

### EpochHeight

Epochs on one chain can be identified by height, which is defined the following way:

- Special epoch that contains genesis block only: undefined
- Epoch starting from the block that's after genesis: `0` (NOTE: in current implementation it's `1` due to a bug, so there are two epochs with height `1`)
- Following epochs: height of the parent epoch plus one

### Epoch id

Every block stores the id of its epoch - `epoch_id`.

Epoch id is defined as

- For special genesis block epoch it's `0`
- For epoch with height `0` it's `0` (NOTE: the first two epochs use the same epoch id)
- For epoch with height `1` it's the hash of genesis block
- For epoch with height `T+2` it's the hash of the last block in epoch `T`

### Epoch end

- After processing the last block of epoch `T`, `EpochManager` aggregates information from block of the epoch, and computes
validator set for epoch `T+2`. This process is described in [EpochManager](EpochManager.md).
- After that, the validator set rotates to epoch `T+1`, and the next block is produced by the validator from the new set
- Applying the first block of epoch `T+1`, in addition to a normal transition, also applies the per-epoch state transitions:
  validator rewards, stake unlocks and slashing.
