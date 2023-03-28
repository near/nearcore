# IO tracing

A quick guide on how to dig into IO access patterns.


## Setup 

When compiling neard (or the paramater estimator) with `feature=io_trace` it
instruments the binary code with fine-grained database operations tracking.

This allows using the `--record-io-trace=/path/to/output.io_trace` CLI flag. Run
it on `neard run`, `neard view-state`, or `runtime-params-estimator` and it will
record an IO trace.

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

### Example trace: estimator
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

Depending on the source, you trace might look different at the start.
But you should always see some setup at the beginning and per-chunk workload later on.

Indentation is used to display the call hierarchy. The `commit` keyword shows
when a commit starts, and all `SET` and `UPDATE_RC` commands that follow with
one level deeper indentation belong to that same database transaction commit.

Later, you see a group that start with an `apply` header. It groups all IO
requests that were performed for a call to [`fn
apply`](https://github.com/near/nearcore/blob/d38c94ac8e78a5a71c592125dfd47803beff58ce/runtime/runtime/src/lib.rs#L1172)
that applies transactions and receipts of a chunk to the previous state root.

In the example, you see a list of `GET` requests that belong to that `apply`,
each with the DB key used and the size of the value read. Further, you can read
in the trace that this specific chunk had 0 transactions and that it missed all
7 of the DB requests it performed to apply this empty chunk.

### Example trace: full node

Next let's look at an excerpt of an IO trace from a real node on mainnet.

```
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

Next let's look at `apply_transactions` but limit the depth of items to 3 levels.

```
apply_transactions shard_id=3
  process_state_update 
    apply num_transactions=3 shard_cache_hit=207
      process_transaction tx_hash=C4itKVLP5gBoAPsEXyEbi67Gg5dvQVugdMjrWBBLprzB shard_cache_hit=57
      process_transaction tx_hash=GRSXC4QCBJHN4hmJiATAbFGt9g5PiksQDNNRaSk666WX shard_cache_miss=3 prefetch_pending=3 shard_cache_hit=35
      process_transaction tx_hash=6bDPeat12pGqA3KEyyg4tJ35kBtRCuFQ7HtCpWoxr8qx shard_cache_miss=2 prefetch_pending=1 shard_cache_hit=21 prefetch_hit=1
      process_receipt receipt_id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC predecessor=1663adeba849fb7c26195678e1c5378278e5caa6325d4672246821d8e61bb160 receiver=token.sweat id=GRB3skohuShBvdGAoEoR3SdJJw7MwCxxscJHKLdPoYUC shard_cache_too_large=1 shard_cache_miss=1 shard_cache_hit=38
```

This includes

```
    GET FlatState "AXRva2VuLnN3ZWF0" size=36
    GET State "AQAAAAMAAADVYp4vtlIbDoVhji22CZOEaxVWVTJKASq3iMvpNEQVDQ==" size=206835
    input 
    register_len 
    read_register 
    storage_read READ key='STATE' size=70 tn_db_reads=20 tn_mem_reads=0 shard_cache_hit=21
```

```
    read_register 
    attached_deposit 
    predecessor_account_id 
    register_len 
    read_register 
    sha256 
    read_register 
    storage_read READ key=dAAxagYMOEb01+56sl9vOM0yHbZRPSaYSL3zBXIfCOi7ow== size=16 tn_db_reads=10 tn_mem_reads=19 shard_cache_hit=11
        GET FlatState "CXRva2VuLnN3ZWF0LHQAMWoGDDhG9NfuerJfbzjNMh22UT0mmEi98wVyHwjou6M=" size=36
```


## Evaluating an IO trace

