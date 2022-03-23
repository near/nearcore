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
