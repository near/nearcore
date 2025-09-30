# Block Processing

This section covers how blocks are processed once they arrive from the network.

## Data Structures

Please refer to [this section](../DataStructures/Block.md) for details about blocks and chunks.

## Validity of Block Header

A block header is invalid if any of the following holds:

- `timestamp` is invalid due to one of the following:
  - It is more than 120s ahead of the local time of the machine.
  - It is smaller than the timestamp of the previous block.
- Its signature is not valid, i.e, verifying the signature using the public key of the block producer fails
- `epoch_id` is invalid, i.e, it does not match the epoch id of the block computed locally.
- `next_bp_hash` is invalid. This could mean one of the following:
  - `epoch_id == prev_block.epoch_id && next_bp_hash != prev_block.next_bp_hash`
  - `epoch_id != prev_block.epoch_id && next_bp_hash != compute_bp_hash(next_epoch_id, prev_hash)` where `compute_bp_hash` computes the hash of next epoch's validators.
- `chunk_mask` does not match the size of the chunk mask is not the same as the number of shards
- Approval or finality information is invalid. See [consensus](Consensus.md) for more details.


## Validity of Block

A block is invalid if any of the following holds:

- Any of the chunk headers included in the block has an invalid signature.
- State root computed from chunk headers does not match `state_root` in the header.
- Receipts root computed from chunk headers does not match `chunk_receipts_root` in the header.
- Chunk headers root computed from chunk headers does not match `chunk_headers_root` in the header.
- Transactions root computed from chunk headers does not match `chunk_tx_root` in the header.
- For some index `i`, `chunk_mask[i]` does not match whether a new chunk from shard `i` is included in the block.
- Its vrf output is invalid
- Its gas price is invalid, i.e, gas priced computed from previous gas price and gas usage from chunks included in the block according to the formula described in [economics](../Economics/Economics.md) does not match the gas price in the header.
- Its `validator_proposals` is not valid, which means that it does not match the concatenation of validator proposals from the chunk headers included in the block.

## Process a new block

When a new block `B` is received from the network, the node first check whether it is ready to be processed:

- Its parent has already been processed.
- Header and the block itself are valid
- All the chunk parts are received. More specifically, this means
  - For any shard that the node tracks, if there is a new chunk from that shard included in `B`, the node has the entire chunk.
  - For block producers, they have their chunk parts for shards they do not track to guarantee data availability.
  
Once all the checks are done, the chunks included in `B` can be applied.

If the chunks are successfully applied, `B` is saved and if the height of `B` is higher than the highest known block (head of the chain),
the head is updated.

## Apply a chunk

In order to apply a chunk, we first check that the chunk is valid:

```python
def validate_chunk(chunk):
    # get the local result of applying the previous chunk (chunk_extra)
    prev_chunk_extra = get_chunk_extra(chunk.prev_block_hash, chunk.shard_id)
    # check that the post state root of applying the previous chunk matches the prev state root in the current chunk
    assert prev_chunk_extra.state_root == chunk.state_root
    # check that the merkle root of execution outcomes match
    assert prev_chunk_extra.outcome_root == chunk.outcome_root
    # check that validator proposals match
    assert prev_chunk_extra.validator_proposals == chunk.validator_proposals
    # check that gas usage matches
    assert prev_chunk_extra.gas_used == chunk.gas_used
    # check that balance burnt matches
    assert prev_chunk_extra.balance_burnt == chunk.balance_burnt
    # check outgoing receipt root matches
    assert prev_chunk_extra.outgoing_receipt_root == chunk.outgoing_receipt_root
```

After this we apply transactions and receipts:

```python
# get the incoming receipts for this shard
incoming_receipts = get_incoming_receipts(block_hash, shard_id)
# apply transactions and receipts and obtain the result, which includes state changes and execution outcomes
apply_result = apply_transactions(shard_id, state_root, chunk.transactions, incoming_receipts, other_block_info)
# save apply result locally
save_result(apply_result)
```

## Catchup

If a node validates shard `X` in epoch `T` and needs to validate shard `Y` in epoch `T+1` due to validator rotation, it has to download the state of that shard before epoch `T+1` starts to be able to do so.
To accomplish this, the node will start downloading the state of shard `Y` at the beginning of epoch `T` and, after it has successfully downloaded the state, will apply all the chunks for shard `Y` in epoch `T` until the current block.
From there the node will apply chunks for both shard `X` and shard `Y` for the rest of the epoch.
