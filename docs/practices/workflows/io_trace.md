# IO tracing

## When should I use IO traces?

IO traces can be used to identify slow receipts and to understand why they
are slow. Or to detect general inefficiencies in how we use the DB.

The results give you counts of DB requests and some useful statistics such as
trie node cache hits. It will NOT give you time measurements, use Graphana to
observe those.

The main uses cases in the past were to estimate the performance of new storage
features (prefetcher, flat state) and to find out why specific contracts produce
slow receipts.

## Setup 

When compiling neard (or the paramater estimator) with `feature=io_trace` it
instruments the binary code with fine-grained database operations tracking.

*Aside: We don't enable it by default because we are afraid the overhead could be
too much, since we store some information for very hot paths such as trie node
cache hits. Although we haven't properly evaluated if it really is a performance
problem.*

This allows using the `--record-io-trace=/path/to/output.io_trace` CLI flag on
neard. Run it in combination with the subcommands `neard run`, `neard
view-state`, or `runtime-params-estimator` and it will record an IO trace. Make
sure to provide the flag to `neard` itself, however, not to the subcommands.
(See examples below)

```bash
# Example command for normal node
# (Careful! This will quickly fill your disk if you let it run.)
cargo build --release -p neard --features=io_trace
target/release/neard \
    --record-io-trace=/mnt/disks/some_disk_with_enough_space/my.io_trace \
    run
```

```bash
# Example command for state viewer, applying a range of chunks in shard 0
cargo build --release -p neard --features=io_trace
target/release/neard \
    --record-io-trace=75220100-75220101.s0.io_trace \
    view-state apply-range --start-index 75220100 --end-index 75220101 \
    --sequential --shard-id 0
```

```bash
# Example command for params estimator
cargo run --release -p runtime-params-estimator --features=required,io_trace \
-- --accounts-num 200000 --additional-accounts-num 200000 \
--iters 3 --warmup-iters 1 --metric time \
--record-io-trace=tmp.io \
--costs ActionReceiptCreation
```

## IO trace content

Once you have collected an IO trace, you can inspect its content manually, or
use existing tools to extract statistics. Let's start with the manual approach.

### Simple example trace: Estimator
An estimator trace typical typically starts something like this:

```
commit
  SET DbVersion "'VERSION'" size=2
commit
  SET DbVersion "'KIND'" size=3
apply num_transactions=0 shard_cache_miss=7
  GET State "AAAAAAAAAAB3I0MYevRcExi1ql5PSQX+fuObsPH30yswS7ytGPCgyw==" size=46
  GET State "AAAAAAAAAACGDsmYvNoBGZnc8PzDKoF4F2Dvw3N6XoAlRrg8ezA8FA==" size=107
  GET State "AAAAAAAAAAB3I0MYevRcExi1ql5PSQX+fuObsPH30yswS7ytGPCgyw==" size=46
  GET State "AAAAAAAAAACGDsmYvNoBGZnc8PzDKoF4F2Dvw3N6XoAlRrg8ezA8FA==" size=107
  GET State "AAAAAAAAAAB3I0MYevRcExi1ql5PSQX+fuObsPH30yswS7ytGPCgyw==" size=46
  GET State "AAAAAAAAAACGDsmYvNoBGZnc8PzDKoF4F2Dvw3N6XoAlRrg8ezA8FA==" size=107
  GET State "AAAAAAAAAAB3I0MYevRcExi1ql5PSQX+fuObsPH30yswS7ytGPCgyw==" size=46
...
```

Depending on the source, traces look a bit different at the start. But you
should always see some setup at the beginning and per-chunk workload later on.

Indentation is used to display the call hierarchy. The `commit` keyword shows
when a commit starts, and all `SET` and `UPDATE_RC` commands that follow with
one level deeper indentation belong to that same database transaction commit.

Later, you see a group that starts with an `apply` header. It groups all IO
requests that were performed for a call to [`fn
apply`](https://github.com/near/nearcore/blob/d38c94ac8e78a5a71c592125dfd47803beff58ce/runtime/runtime/src/lib.rs#L1172)
that applies transactions and receipts of a chunk to the previous state root.

In the example, you see a list of `GET` requests that belong to that `apply`,
each with the DB key used and the size of the value read. Further, you can read
in the trace that this specific chunk had 0 transactions and that it
cache-missed all 7 of the DB requests it performed to apply this empty chunk.

### Example trace: Full mainnet node

Next let's look at an excerpt of an IO trace from a real node on mainnet.

```
...
GET State "AQAAAAMAAACm9DRx/dU8UFEfbumiRhDjbPjcyhE6CB1rv+8fnu81bw==" size=9
GET State "AQAAAAAAAACLFgzRCUR3inMDpkApdLxFTSxRvprJ51eMvh3WbJWe0A==" size=203
GET State "AQAAAAIAAACXlEo0t345S6PHsvX1BLaGw6NFDXYzeE+tlY2srjKv8w==" size=299
apply_transactions shard_id=3
  process_state_update 
    apply num_transactions=3 shard_cache_hit=207
      process_transaction tx_hash=C4itKVLP5gBoAPsEXyEbi67Gg5dvQVugdMjrWBBLprzB shard_cache_hit=57
        GET FlatState "AGxlb25hcmRvX2RheS12aW5jaGlrLm5lYXI=" size=36
        GET FlatState "Amxlb25hcmRvX2RheS12aW5jaGlrLm5lYXICANLByB1merOzxcGB1HI9/L60QvONzOE6ovF3hjYUbhA8" size=36
      process_transaction tx_hash=GRSXC4QCBJHN4hmJiATAbFGt9g5PiksQDNNRaSk666WX shard_cache_miss=3 prefetch_pending=3 shard_cache_hit=35
        GET FlatState "AnJlbGF5LmF1cm9yYQIA5iq407bcLgisCKxQQi47TByaFNe9FOgQg5y2gpU4lEM=" size=36
      process_transaction tx_hash=6bDPeat12pGqA3KEyyg4tJ35kBtRCuFQ7HtCpWoxr8qx shard_cache_miss=2 prefetch_pending=1 shard_cache_hit=21 prefetch_hit=1
        GET FlatState "AnJlbGF5LmF1cm9yYQIAyKT1vEHVesMEvbp2ICA33x6zxfmBJiLzHey0ZxauO1k=" size=36
      process_receipt receipt_id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC predecessor=1663adeba849fb7c26195678e1c5378278e5caa6325d4672246821d8e61bb160 receiver=token.sweat id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC shard_cache_too_large=1 shard_cache_miss=1 shard_cache_hit=38
        GET FlatState "AXRva2VuLnN3ZWF0" size=36
        GET State "AQAAAAMAAADVYp4vtlIbDoVhji22CZOEaxVWVTJKASq3iMvpNEQVDQ==" size=206835
        input 
        register_len 
        read_register 
        storage_read READ key='STATE' size=70 tn_db_reads=20 tn_mem_reads=0 shard_cache_hit=21
        register_len 
        read_register 
        attached_deposit 
        predecessor_account_id 
        register_len 
        read_register 
        sha256 
        read_register 
        storage_read READ key=dAAxagYMOEb01+56sl9vOM0yHbZRPSaYSL3zBXIfCOi7ow== size=16 tn_db_reads=10 tn_mem_reads=19 shard_cache_hit=11
          GET FlatState "CXRva2VuLnN3ZWF0LHQAMWoGDDhG9NfuerJfbzjNMh22UT0mmEi98wVyHwjou6M=" size=36
        register_len 
        read_register 
        sha256 
        read_register 
        storage_write WRITE key=dAAxagYMOEb01+56sl9vOM0yHbZRPSaYSL3zBXIfCOi7ow== size=16 tn_db_reads=0 tn_mem_reads=30
        ...
```

Maybe that's a bit much. Let's break it down into pieces.

It start with a few DB get requests that are outside of applying a chunk. It's
quite common that we have these kinds of constant overhead requests that are
independent of what's inside a chunk. If we see too many such requests, we
should take a close look to see if we are wasting performance.

```
GET State "AQAAAAMAAACm9DRx/dU8UFEfbumiRhDjbPjcyhE6CB1rv+8fnu81bw==" size=9
GET State "AQAAAAAAAACLFgzRCUR3inMDpkApdLxFTSxRvprJ51eMvh3WbJWe0A==" size=203
GET State "AQAAAAIAAACXlEo0t345S6PHsvX1BLaGw6NFDXYzeE+tlY2srjKv8w==" size=299
```

Next let's look at `apply_transactions` but limit the depth of items to 3
levels.

```
apply_transactions shard_id=3
  process_state_update 
    apply num_transactions=3 shard_cache_hit=207
      process_transaction tx_hash=C4itKVLP5gBoAPsEXyEbi67Gg5dvQVugdMjrWBBLprzB shard_cache_hit=57
      process_transaction tx_hash=GRSXC4QCBJHN4hmJiATAbFGt9g5PiksQDNNRaSk666WX shard_cache_miss=3 prefetch_pending=3 shard_cache_hit=35
      process_transaction tx_hash=6bDPeat12pGqA3KEyyg4tJ35kBtRCuFQ7HtCpWoxr8qx shard_cache_miss=2 prefetch_pending=1 shard_cache_hit=21 prefetch_hit=1
      process_receipt receipt_id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC predecessor=1663adeba849fb7c26195678e1c5378278e5caa6325d4672246821d8e61bb160 receiver=token.sweat id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC shard_cache_too_large=1 shard_cache_miss=1 shard_cache_hit=38
```

Here you can see that before we even get to `apply`, we go through
`apply_transactions` and `process_state_update`. The excerpt does not show it
but there are DB requests listed further below that belong to these levels but
not to `apply`.

Inside `apply`, we see 3 transactions being converted to receipts as part of
this chunk, and one already existing action receipt getting processed. 

Cache hit statistics for each level are also displayed. For example, the first
transaction has 57 read requests and all of them hit in the shard cache. For
the second transaction, we miss the cache 3 times but the values were already in the
process of being prefetched. This would be account data which we fetch in
parallel for all transactions in the chunk.

Finally, there are several `process_receipt` groups, although the excerpt was
cut to display only one. Here we see the receiving account
(`receiver=token.sweat`) and the receipt ID to potentially look it up on an
explorer, or dig deeper using state viewer commands.

Again, cache hit statistics are included. Here you can see one value
missed the cache because it was too large. Usually that's a contract code. We do
not include it in the shard cache because it would take up too much space.

Zooming in a bit further, let's look at the DB request at the start of the
receipt.

```
    GET FlatState "AXRva2VuLnN3ZWF0" size=36
    GET State "AQAAAAMAAADVYp4vtlIbDoVhji22CZOEaxVWVTJKASq3iMvpNEQVDQ==" size=206835
    input 
    register_len 
    read_register 
    storage_read READ key='STATE' size=70 tn_db_reads=20 tn_mem_reads=0 shard_cache_hit=21
    register_len 
    read_register 
```

`FlatState "AXRva2VuLnN3ZWF0"` reads the `ValueRef` of the contract code, which
is 36 bytes in its serialized format. Then, the `ValueRef` is dereferenced to
read the actual code, which happens to be 206kB in size. This happens in the
`State` column because at the time of writing, we still read the actual values
from the trie, not from flat state.

What follows are host function calls performed by the SDK. It uses `input` to
check the function call arguments and copies it from a register into WASM
memory.

Then the SDK reads the serialized contract state from the hardcoded key
`"STATE"`. Note that we charge 20 `tn_db_reads` for it, since we missed the
chunk cache, but we hit everything in the shard cache. Thus, there are no DB
requests. If there were DB requests for this `tn_db_reads`, you would see them
listed.

The returned 70 bytes are again copied into WASM memory. Knowing the SDK code a
little bit, we can guess that the data is then deserialized into the struct used
by the contract for its root state. That's not visible on the trace though, as
this happens completely inside the WASM VM.

Next we start executing the actual contract code, which again calls a bunch of
host functions. Apparently the code starts by reading the attached deposit and
the predecessor account id, presumably to perform some checks.

The `sha256` call here is used to shorten implicit account ids.
([Link to code for comparison](https://github.com/sweatco/near-sdk-rs/blob/af6ba3cb75e0bbfc26e346e61aa3a0d1d7f5ac7b/near-contract-standards/src/fungible_token/core_impl.rs#L249-L259)).

Afterwards, a value with 16 bytes (a `u128`) is fetched from the trie state.
To serve this, it required reading 30 trie nodes, 19 of them were cached in the
chunk cache and were not charged the full gas cost. And the remaining 11 missed
the chunk cache but they hit the shard cache. Nothing needed to be fetched from
DB because the Sweatcoin specific prefetcher has already loaded everything into
the shard cache.

*Note: We see trie node requests despite flat state being used. This is because
the trace was collected with a binary that performed a read on both the trie and
flat state to do some correctness checks.*

```
    attached_deposit 
    predecessor_account_id 
    register_len 
    read_register 
    sha256 
    read_register 
    storage_read READ key=dAAxagYMOEb01+56sl9vOM0yHbZRPSaYSL3zBXIfCOi7ow== size=16 tn_db_reads=10 tn_mem_reads=19 shard_cache_hit=11
        GET FlatState "CXRva2VuLnN3ZWF0LHQAMWoGDDhG9NfuerJfbzjNMh22UT0mmEi98wVyHwjou6M=" size=36
```

So that is how to read these traces and dig deep. But maybe you
want aggregated statistics instead? Then please continue reading.


## Evaluating an IO trace

When you collect an IO trace over an hour of mainnet traffic, it can quickly be
above 1GB in uncompressed size. You might be able to sample a few receipts and
eyeball them to get a feeling for what's going on. But you can't understand the
whole trace without additional tooling.

The parameter estimator command `replay` can help with that. (See also [this
readme](https://github.com/near/nearcore/tree/master/runtime/runtime-params-estimator/README.md))
Run the following command to see an overview of available commands.

```bash
# will print the help page for the IO trace replay command
cargo run --profile quick-release -p runtime-params-estimator -- \
  replay --help
```

All commands aggregate the information of a trace. Either globally, per chunk,
or per receipt. For example, below is the output that gives a list of RocksDB
columns that were accessed and how many times, aggregated by chunk.


```bash
cargo run --profile quick-release -p runtime-params-estimator -- \
  replay  ./path/to/my.io_trace  chunk-db-stats
```

```
apply_transactions shard_id=3 block=DajBgxTgV8NewTJBsR5sTgPhVZqaEv9xGAKVnCiMiDxV
  GET   12 FlatState  4 State  

apply_transactions shard_id=0 block=DajBgxTgV8NewTJBsR5sTgPhVZqaEv9xGAKVnCiMiDxV
  GET   14 FlatState  8 State  

apply_transactions shard_id=2 block=HTptJFZKGfmeWs7y229df6WjMQ3FGfhiqsmXnbL2tpz8
  GET   2 FlatState  

apply_transactions shard_id=3 block=HTptJFZKGfmeWs7y229df6WjMQ3FGfhiqsmXnbL2tpz8
  GET   6 FlatState  2 State  

apply_transactions shard_id=1 block=HTptJFZKGfmeWs7y229df6WjMQ3FGfhiqsmXnbL2tpz8
  GET   50 FlatState  5 State  

...

apply_transactions shard_id=3 block=AUcauGxisMqNmZu5Ln7LLu8Li31H1sYD7wgd7AP6nQZR
  GET   17 FlatState  3 State  

top-level:
  GET   8854 Block  981 BlockHeader  16556 BlockHeight  59155 BlockInfo  2 BlockMerkleTree  330009 BlockMisc  1 BlockOrdinal  31924 BlockPerHeight  863 BlockRefCount  1609 BlocksToCatchup  1557 ChallengedBlocks  4 ChunkExtra  5135 ChunkHashesByHeight  128788 Chunks  35 EpochInfo  1 EpochStart  98361 FlatState  1150 HeaderHashesByHeight  8113 InvalidChunks  263 NextBlockHashes  22 OutgoingReceipts  131114 PartialChunks  1116 ProcessedBlockHeights  968698 State  
  SET   865 BlockHeight  1026 BlockMerkleTree  12428 BlockMisc  1636 BlockOrdinal  865 BlockPerHeight  865 BlockRefCount  3460 ChunkExtra  3446 ChunkHashesByHeight  339142 FlatState  3460 FlatStateDeltas  3460 FlatStateMisc  865 HeaderHashesByHeight  3460 IncomingReceipts  865 NextBlockHashes  3442 OutcomeIds  3442 OutgoingReceipts  863 ProcessedBlockHeights  340093 StateChanges  3460 TrieChanges  
  UPDATE_RC   13517 ReceiptIdToShardId  13526 Receipts  1517322 State  6059 Transactions  
```

The output contains one `apply_transactions` for each chunk, with the block hash
and the shard id. Then it prints one line for each DB operations observed
(GET,SET,...) together with a list of columns and an OP count.

See the `top-level` output at the end? These are all the DB requests that could
not be assigned to specific chunks. The way we currently count write operations
(SET, UPDATE_RC) they are never assigned to a specific chunk and instead only
show up in the top-level list. Clearly, there is some room for improvement here.
So far we simply haven't worried about RocksDB write performance so the tooling
to debug write performance is naturally lacking.