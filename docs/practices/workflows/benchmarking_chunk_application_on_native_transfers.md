# Benchmarking chunk application on the native transfers workload

This doc shows how to:
* Run a single-node localnet with native transfer workload
* Use the `view-state` command to benchmark applying one of the chunks created during the workload

Running the workload creates ~realistic chunks and state and then we can benchmark applying one of the chunks using this.

### Running the initial workload

Clone nearcore and build neard with the tx_generator feature
> Note: Building `neard` with `--profile dev-release` instead of `--release` takes less time and the binary is almost as fast as the release one.

> Note: If you plan to gather `perf` profiles, build with `--config .cargo/config.profiling.toml` (see [profiling docs](profiling.md))

```shell
git clone https://github.com/near/nearcore
cd nearcore
cargo build -p neard --release --features tx_generator
```

Build synth-bm
```shell
cd benchmarks/synth-bm
cargo build --release
```

Go to transaction-generator
```shell
cd ../transactions-generator/
```

Create a link to `neard`
```shell
ln -s ../../target/release/neard
```

Set TPS to 4000

There are two files with the load schedule: `tx-generator-settings.json` and `tx-generator-settings.json.in`.
Transaction generator reads data from the `tx-generator-settings.json` file, not the one with `.in` suffix. The `.in` file is used only to store the default values for transaction generator. If `tx-generator-settings.json` doesn't exist, it's created based on `tx-generator-settings.json.in` during the first run.
This way you can modify `tx-generator-settings.json` locally without merge conflicts.

Create `tx-generator-settings.json`:
```shell
cp tx-generator-settings.json.in tx-generator-settings.json
```

Set `tps = 4000` in `tx-generator-settings.json`. Example jq command (might not work in the future if the file structure changes):
```shell
jq '.tx_generator.schedule[0].tps = 4000' tx-generator-settings.json | sponge tx-generator-settings.json
```

Run the native transfers workload (Ignore "Error: tx generator idle: no schedule provided", this is normal during `just create-accounts`)
```shell
just do-it
```

This will create a `benchmarks/transactions-generator/.near` directory with node state, create accounts and run the workload.
Creating accounts takes a few minutes, be patient.
Sometimes running the workload fails because the database LOCK hasn't been freed in time. If that happens run `just run-localnet` to retry.

### Re-running the workload

To re-run the workload run `just run-localnet`. It's not necessary to do `just do-it`, which would recreate the state from scratch.

To change TPS edit the `tx-generator-settings.json` file (without `.in` !) and run `just enable-tx`

### Preparing state for chunk application benchmark

(Taken from [profiling docs](profiling.md))

First, make sure all deltas in flat storage are applied and written:

```shell
./neard --home .near view-state --read-write apply-range --shard-id 0 --storage flat sequential
```

Then move flat head back 32 blocks to a height that had chunks with high load
```shell
./neard --home .near flat-storage move-flat-head --shard-id 0 --version 0 back --blocks 32
```

### Benchmarking chunk application

Run these commands to benchmark chunk application at the height to which the flat head was moved.

```shell
./neard --home .near view-state apply-range --shard-id 0 --storage flat benchmark
```

or

```shell
./neard --home .near view-state apply-range --shard-id 0 --storage memtrie benchmark
```
