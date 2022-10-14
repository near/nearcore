# Runtime Parameters Estimator

Use this tool to measure the running time of elementary runtime operations that have associated fees.

1. Run the estimator
    ```bash
    cargo run --release --package runtime-params-estimator --features required --bin runtime-params-estimator -- --accounts-num 20000 --additional-accounts-num 200000 --iters 1 --warmup-iters 1 --metric time
    ```

    With the given parameters above estimator will run relatively fast.
    Note the `--metric time` flag: it instructs the estimator to use wall-clock time for estimation, which is quick, but highly variable between runs and physical machines.
    To get more robust estimates, use these arguments:

    ```bash
    --accounts-num 20000 --additional-accounts-num 200000 --iters 1 --warmup-iters 1 \
      --docker --metric icount
    ```

    This will run and build the estimator inside a docker container, using QEMU to precisely count the number of executed instructions.

    We will be using different parameters to do the actual parameter estimation.
    The instructions in [`emu-cost/README.md`](./emu-cost/README.md) should be followed to get the real data.

2. The result of the estimator run is the `costs-$timestamp$.txt` file, which contains human-readable representation of the costs.
   It can be compared with `costs.txt` file in the repository, which contains the current costs we are using.
   Note that, at the moment, `costs.txt` is *not* the source of truth.
   Rather, the costs are hard-codded in the `Default` impl for `RuntimeConfig`.
   You can run `cargo run --package runtime-params-estimator --bin runtime-params-estimator -- --costs-file costs.txt` to convert cost table into `RuntimeConfig`.

3. **Continuous Estimation**: Take a look at [`continuous-estimation/README.md`](./continuous-estimation/README.md) to learn about the automated setup around the parameter estimator.

Note, if you use the plotting functionality you would need to install [gnuplot](http://gnuplot.info/) to see the graphs.

## Replaying IO traces

Compiling `neard` with `--features=io_trace` and then running it with
`--record-io-trace=my_trace.log` produces a trace of all storage and database
accesses. This trace can be replayed by the estimator. For now only to get
statistics. But the plan is that it will also give gas estimations based on
replaying traces.

Example:
```
cargo run -p runtime-params-estimator -- replay my_trace.log cache-stats
  GET   193 Block  193 BlockHeader  101 BlockHeight  100 BlockInfo  2 BlockMisc
        11 CachedContractCode  98 ChunkExtra  95 Chunks  4 EpochInfo  
        98 IncomingReceipts  30092 State  
  SET   1 CachedContractCode  
  DB GET        30987 requests for a total of 391093512 B
  DB SET            1 requests for a total of 10379357 B
  STORAGE READ  153001 requests for a total of  2523227 B
  STORAGE WRITE 151412 requests for a total of  2512012 B
  TRIE NODES    8878276 /375708 /27383  (chunk-cache/shard-cache/DB)
  SHARD CACHE         93.21% hit rate,  93.21% if removing 15 too large nodes from total
  CHUNK CACHE         95.66% hit rate,  99.69% if removing 375708 shard cache hits from total
```

For a list of all options, run `cargo run -p runtime-params-estimator -- replay --help`.

### IO trace tests

The test input files `./res/*.io_trace` have been generated based on real mainnet traffic.

```bash
cargo build --release -p neard --features=io_trace
for shard in 0 1 2 3
do
  target/release/neard \
    --record-io-trace=75220100-75220101.s${shard}.io_trace view-state \
    apply-range --start-index 75220100 --end-index 75220101 \
    --sequential --shard-id ${shard}
done
```

When running these command, make sure to run with `sequential` and to disable
prefetching is disabled, or else the the replaying modes that match requests to
receipts will not work properly.

```js
// config.json
  "store": {
    "enable_receipt_prefetching": false
  }
```
