# How to use

## Run the benchmark
- compile the `neard` with the `tx_generator` feature
```
cargo build --release --features tx_generator
```
- create a link to the release binary of neard in the `benchmarks/transactions-generator`
```
cd benchmarks/transactions-generator && ln -s ../../release/neard ./
```
- run the thing
```
just do_it 20000
```
The last command will init a single-node network, apply customizations to the config, create the accounts and run the benchmark.
All of it.
For a more fine-grained control feel free to dive into the `justfile` for the commands executing the intermediate steps.

## What you should see
- observe the load reported in the logs
```
...
2025-02-12T16:42:37.351118Z  INFO stats: #    1584 6nxZon12xBTmARmUm3ngtgaA2K7V9dX1J13EtPZ2kEhe Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s 1.60 bps 131 Tgas/s CPU: 70%, Mem: 2.98 GB
...
2025-02-13T16:42:38.175452Z  INFO transaction-generator: transactions pool_accepted=189571 pool_rejected=2 total_processed=186494 total_failed=0
...
```
- see the number of processed transactions in the metrics
```
$ curl http://localhost:3030/metrics -s | rg "near_transaction_processed_successfully_total (\\d+)"
near_transaction_processed_successfully_total 852883
```
