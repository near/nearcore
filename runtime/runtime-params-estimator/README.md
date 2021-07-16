# Runtime Parameters Estimator

Use this tool to measure the running time of elementary runtime operations that have associated fees.

1. Create genesis config and generate state dump
    ```bash
    cargo run --release --package neard --bin neard -- --home /tmp/data init --test-seed=alice.near --account-id=test.near --fast
    cargo run --release --package genesis-populate --bin genesis-populate -- --additional-accounts-num=200000 --home /tmp/data
    ```

2. Run the estimator
    ```bash
    cargo run --release --package runtime-params-estimator --features required --bin runtime-params-estimator -- --home /tmp/data --accounts-num 20000 --iters 1 --warmup-iters 1 --metric time
    ```

    With the given parameters above estimator will run relatively fast. We will be using different parameters to do the actual parameter estimation. Also note that the default metric is `icount`, instruction count, but requires using QEMU to emulate the processor. So this example provides a way to get a quick way to test out the estimator based on time, but the instructions in [`emu-cost/README.md`](./emu-cost/README.md) should be followed to get the real data.

Note, if you use the plotting functionality you would need to install [gnuplot](http://gnuplot.info/) to see the graphs.
