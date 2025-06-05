# Sharded Benchmark Tool

The `sharded_bm.py` script is used to run benchmarks with heavy transaction load on forknet with many shards.

## Prerequisites

1. [Set up forknet.](../../../../benchmarks/sharded-bm/README.md#forknet)

2. Set environment variables:

* `CASE` - path to cluster configuration in [`../../../../benchmarks/sharded-bm`](../../../../benchmarks/sharded-bm) directory
* `FORKNET_NAME` - unique cluster name set up on previous step
* `NEARD_BINARY_URL` - link to binary which will be used for testing. Note that it must include `tx_generator` feature.

Example:

```bash
export CASE=cases/forknet/realistic_20_cp_1_rpc_20_shard/
export FORKNET_NAME=shardnet
export NEARD_BINARY_URL=https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/tx-generator-neard/neard
```

## Usage

1. Go to `pytest` directory.
2. Start the benchmark:

```bash
python tests/mocknet/sharded_bm.py init && python tests/mocknet/sharded_bm.py start --enable-tx-generator
```

3. Monitor the benchmark:

* [Look at Grafana dashboard](../../../../benchmarks/sharded-bm/README.md#forknet---monitoring)
* Download OpenTelemetry traces corresponding to the latest timeframe for opening in [traviz](https://github.com/jancionear/traviz):

```bash
python tests/mocknet/sharded_bm.py get-traces
```

4. Stop the benchmark:

```bash
python3 tests/mocknet/sharded_bm.py stop --disable-tx-generator
```

5. Reset the benchmark state to start the new one from scratch:

```bash
python tests/mocknet/sharded_bm.py reset
```

## Tweaking

For now, if you want to change load schedule, it is easier to run `python tests/mocknet/sharded_bm.py init` again.
TODO: make it faster.

Changing neard binary also requires calling `init`. You can either:

* give flag `--neard-binary-url URL` which takes the highest priority;
* otherwise, value of env var `NEARD_BINARY_URL` will be taken, if set;
* otherwise, `binary_url` from `CASE` will be taken.

## Other docs

[Documentation for bench.sh](../../../../benchmarks/sharded-bm/README.md) which is in process of migration to `sharded_bm.py`.
