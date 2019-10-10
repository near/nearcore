# Runtime Parameters Estimator

Use this tool to measure the running time of elementary runtime operations that have associated fees.

1. Create genesis config and generate state dump
    ```bash
    cargo run --package near --bin near -- --home /tmp/data init --chain-id= --test-seed=alice.near --account-id=test.near --fast
    cargo run --package genesis-populate --bin genesis-populate -- --additional-accounts-num=200000 --home /tmp/data
    ```

2. 

1. Follow `genesis-toosl/genesis-populate` instructions to generate the state dump (you can use 100000 accounts if you are experimenting only);
2. Place generated files into `/tmp/data` and run this tool.

Note, you would need to install [gnuplot](http://gnuplot.info/) to see the graphs.
