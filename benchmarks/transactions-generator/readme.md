# How to use

## Run the benchmark

- compile the `neard` with the `tx_generator` feature

```
cargo build --release --features tx_generator
```

- create a link to the release binary of neard in the `benchmarks/transactions-generator`

```
cd benchmarks/transactions-generator && ln -s ../../target/release/neard ./
```

- build `synth-bm`

```
cd benchmarks/synth-bm && cargo build --release
```

- run the thing

```
just do-it [<TPS>] [<TIMEOUT>]
```

The last command will init a single-node network, apply customizations to the config, create the accounts and run the
benchmark. All of it.
After running that command once, you can repeat the benchmark without re-executing the setup steps by:

```
just run-localnet
```

For a more fine-grained control feel free to dive into the `justfile` for the commands executing the intermediate steps.

## What you should see

- observe the load reported in the logs

```
...
2025-02-12T16:42:37.351118Z  INFO stats: #    1584 6nxZon12xBTmARmUm3ngtgaA2K7V9dX1J13EtPZ2kEhe Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s 1.60 bps 131 Tgas/s CPU: 70%, Mem: 2.98 GB
...
2025-02-18T13:04:29.790195Z  INFO transaction-generator: total="Stats { pool_accepted: 158247, pool_rejected: 237, processed: 155338, failed:0 }"
2025-02-18T13:04:29.790208Z  INFO transaction-generator: diff="Stats { pool_accepted: 6049, pool_rejected: 0, processed: 6439, failed: 0 }"
...
```

- see the number of processed transactions in the metrics

```
$ curl http://localhost:4040/metrics -s | rg "near_transaction_processed_successfully_total (\\d+)"
near_transaction_processed_successfully_total 852883
```
