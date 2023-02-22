# Running the Estimator

This workflow describes how to run the gas estimator byzantine-benchmark suite.
To learn about its background and purpose, refer to [Runtime Parameter
Estimator](../../architecture/gas/estimator.md) in the architecture chapter.

Type this in your console to quickly run estimations on a couple of action costs.

```bash
cargo run -p runtime-params-estimator --features required -- \
    --accounts-num 20000 --additional-accounts-num 20000 \
    --iters 3 --warmup-iters 1 --metric time \
    --costs=ActionReceiptCreation,ActionTransfer,ActionCreateAccount,ActionFunctionCallBase
```

You should get an output like this.

```
[elapsed 00:00:17 remaining 00:00:00] Writing into storage ████████████████████   20000/20000  
ActionReceiptCreation         4_499_673_502_000 gas [  4.499674ms]    (computed in 7.22s) 
ActionTransfer                  410_122_090_000 gas [   410.122µs]    (computed in 4.71s) 
ActionCreateAccount             237_495_890_000 gas [   237.496µs]    (computed in 4.64s) 
ActionFunctionCallBase          770_989_128_914 gas [   770.989µs]    (computed in 4.65s) 


Finished in 40.11s, output saved to:

    /home/you/near/nearcore/costs-2022-11-11T11:11:11Z-e40863c9b.txt
```

This shows how much gas a parameter should cost to satisfy the 1ms = 1Tgas rule.
It also shows how much time that corresponds to and how long it took to compute
each of the estimations.

Note that the above does not produce very accurate results and it can have high
variance as well. It runs an unoptimized binary, the state is small, and the
metric used is wall-clock time which is always prone to variance in hardware and
can be affected by other processes currently running on your system.

Once your estimation code is ready, it is better to run it with a larger state
and an optimized binary.

```bash
cargo run --release -p runtime-params-estimator --features required -- \
    --accounts-num 20000 --additional-accounts-num 2000000 \
    --iters 3 --warmup-iters 1 --metric time \
    --costs=ActionReceiptCreation,ActionTransfer,ActionCreateAccount,ActionFunctionCallBase
```

You might also want to run a hardware-agnostic estimation using the following
command. It uses `docker` and `qemu` under the hood, so it will be quite a bit
slower. You will need to install `docker` to run this command.

```bash
cargo run --release -p runtime-params-estimator --features required -- \
    --accounts-num 20000 --additional-accounts-num 2000000 \
    --iters 3 --warmup-iters 1 --metric icount --docker --full \
    --costs=ActionReceiptCreation,ActionTransfer,ActionCreateAccount,ActionFunctionCallBase
```

Note how the output looks a bit different now. The `i`, `r` and `w` values show
instruction count, read IO bytes, and write IO bytes respectively. The IO byte
count is known to be inaccurate.

```
+ /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so -cpu Westmere-v1 /host/nearcore/target/release/runtime-params-estimator --home /.near --accounts-num 20000 --iters 3 --warmup-iters 1 --metric icount --costs=ActionReceiptCreation,ActionTransfer,ActionCreateAccount,ActionFunctionCallBase --skip-build-test-contract --additional-accounts-num 0 --in-memory-db
ActionReceiptCreation         214_581_685_500 gas [  1716653.48i 0.00r 0.00w]     (computed in 6.11s) 
ActionTransfer                 21_528_212_916 gas [   172225.70i 0.00r 0.00w]     (computed in 4.71s) 
ActionCreateAccount            26_608_336_250 gas [   212866.69i 0.00r 0.00w]     (computed in 4.67s) 
ActionFunctionCallBase         12_193_364_898 gas [    97546.92i 0.00r 0.00w]     (computed in 2.39s) 


Finished in 17.92s, output saved to:

    /host/nearcore/costs-2022-11-01T16:27:36Z-e40863c9b.txt
```

The difference between the metrics is discussed in the [Estimation
Metrics](../../architecture/gas/estimator.md#estimation-metrics) chapter.

You should now be all set up for running estimations on your local machine. Also,
check `cargo run -p runtime-params-estimator --features required -- --help` for
the list of available options.
