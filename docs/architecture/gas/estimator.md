# Runtime Parameter Estimator

The runtime parameter estimator is a byzantine benchmarking suite. Byzantine
benchmarking is not really commonly used term but I feel it describes it quite
well. It measures the performance assuming that up to a third of validators and
all users collude to make the system as slow as possible.

This benchmarking suite is used check that the gas parameters defined in the
protocol are correct. Correct in this context means, a chunk filled with 1 Pgas
will take at most 1 second to be applied. Or more generally, per 1 Tgas of
execution, we spend no more than 1ms wall-clock time. 

For now, nearcore timing is the only one that matters. Things will become more
complicated once there are multiple client implementations. But knowing that
nearcore can serve requests fast enough proofs that it is possible to be at
least as fast. However, we should be careful to not couple costs too tightly
with the specific implementation of nearcore to allow for innovation in new
clients.

The estimator code is part of the nearcore repository in the directory
[runtime/runtime-params-estimator](https://github.com/near/nearcore/tree/master/runtime/runtime-params-estimator).

## Running the Estimator

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

The difference between the metrics will be discussed in the
section on [Estimation Metrics](#estimation-metrics).

You should now be all setup for running estimations on your local machine. Also
check `cargo run -p runtime-params-estimator --features required -- --help` for
the list of available options.

## From Estimations to Parameter Values

To calculate the final gas parameter values, there is more to be done than just
running a single command. After all, these parameters are part of the protocol
specification. They cannot be changed easily. And setting them to a wrong value
can cause severe system instability.

Our current strategy is to run estimations
with two different metrics and do so on standardized cloud hardware. The output
is then sanity checked manually by several people. Based on that, the final gas
parameter value is determined. Usually it will be the higher output of the two
metrics rounded up. More details on the process will be added to this document
in due time.

## Code Structure
The estimator contains a binary and a library module. The
[main.rs](https://github.com/near/nearcore/blob/e40863c9ba61a0de140c869583b2113358605771/runtime/runtime-params-estimator/src/main.rs)
contains the CLI arguments parsing code and logic to fill the test database.

The interesting code lives in
[lib.rs](https://github.com/near/nearcore/blob/e40863c9ba61a0de140c869583b2113358605771/runtime/runtime-params-estimator/src/lib.rs)
and its submodules. The comments on the top of that file provide a
high-level overview of how estimations work. More details on specific
estimations are available as comments on the enum variants of `Cost` in
[costs.rs](https://github.com/near/nearcore/blob/e40863c9ba61a0de140c869583b2113358605771/runtime/runtime-params-estimator/src/cost.rs#L9).

If you roughly understand the three files above, you already have a great
overview around the estimator.
[estimator_context.rs](https://github.com/near/nearcore/blob/e40863c9ba61a0de140c869583b2113358605771/runtime/runtime-params-estimator/src/estimator_context.rs)
is another central file. A full estimation run creates a single
`EstimatorContext`. Each individual estimation will use it to spawn a new
`Testbed` with a fresh database that contains the same data as setup in the
estimator context.

Most estimations fill blocks with transactions to be executed and hands them to
`Testbed::measure_blocks`. To allow for easy repetitions, the block is usually
filled by an instance of the
[`TransactionBuilder`](https://github.com/near/nearcore/blob/e40863c9ba61a0de140c869583b2113358605771/runtime/runtime-params-estimator/src/transaction_builder.rs),
which can be retrieved from a testbed.

But even filling blocks with transactions becomes repetitive since many
parameters are estimated similarly.
[utils.rs](https://github.com/near/nearcore/blob/master/runtime/runtime-params-estimator/src/utils.rs)
has a collection of helpful functions that let you write estimations very
quickly.

## Estimation Metrics

Estimation code is generally not concerned with the metric used to estimate gas.
We use `let clock = GasCost::measure();` and `clock.elapsed()` to measure the
cost in whatever metric has been specified in the CLI argument `--metric`. But
when you run estimations and especially when you want to interpret the results,
you want to understand the metric used. Available metrics are `time` and
`icount`.

Starting with `time`, this is a simple wall-clock time measurement. At the end
of the day, this is what counts in a validator setup. But unfortunately, this
metrics is very dependent on the specific hardware and what else is running on
that hardware right now. Dynamic voltage and frequency scaling (DVFS) also plays
a role here. To a certain degree, all these factors can be controlled. But it
requires full control over a system (often not the case when running on
cloud-hosted VMs) and manual labour to set it up.

The other supported metric `icount` is much more stable. It uses
[qemu](https://www.qemu.org/) to emulate an x86 CPU. We then insert a custom
[TCG plugin](https://qemu.readthedocs.io/en/latest/devel/tcg-plugins.html)
([counter.c](https://github.com/near/nearcore/blob/08c4a1bd4b16847eb1c2fccee36bf16f6efb71fd/runtime/runtime-params-estimator/emu-cost/counter_plugin/counter.c))
that counts the number of executed x86 instructions. It also intercepts system
calls and counts the number of bytes seen in `sys_read`, `sys_write` and their
variations. This gives an approximation for IO bytes, as seen on the interface
between operating system and nearcore. To convert to gas, we use  three
constants to multiply with instruction count, read bytes, and write bytes.

We run qemu inside a Docker container, to make sure the qemu and qemu plugin
versions match with system libraries. Make sure to add `--docker` when running
with `--metric icount`.

The great thing about `icount` is how you can run it on different machines and
it will always return the same result. It is not 100% deterministic but very
close, so it can usually detect code changes that degrade performance in major
ways.

The problem with `icount` is how unrepresentative it is for real-life
performance. First, `x86` instructions are not all equally complex. Second, how
many of them are executed per cycle depends on instruction level pipelining,
branch prediction, memory prefetching, and more CPU features like that which are
just not captured by an emulator like qemu. Third, the time it takes to serve
bytes in system calls depends less on the sum of all bytes and more on data
locality and how it can be cached in the OS page cache. But regardless of all
these inaccuracies, it can still be useful to compare different implementations
both measured using `icount`.

<!-- TODO: how to add a new host function estimation -->
<!-- TODO: state of IO estimations -->
<!-- TODO: CE and Warehouse -->
<!-- TODO: ... -->