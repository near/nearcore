# Benchmarking `neard`

At the moment there are 3 major benchmarking tools in use: 
- Native token transactions using the RPC node
- Native token transactions directly injecting transaction to the pool
- `apply-range` benchmarks

The first two can be run either single node or multi-node setup.

## Native token transactions

### Using the RPC Node to generate the load
This one includes all major components of the network including the RPC layer.
Which makes it most representative for measuring the realistic throughput of the network while on the same time introduces some noise and makes it difficult to reason about the performance of the specific components.

One needs to compile the `benchmarks/synth-bm`, create the link to the `neard` executable and run the tasks from the `justfile`.
The details are available on the dedicated [page](./benchmarking_synthetic_workloads.md).

### Direct transaction injection
This benchmark bypasses the RPC layer and injects the transactions using direct calls to the ingress handling actor. 
It is representative of the performance of the `neard` excluding the RPC layer.
The single-node setup of this benchmark is a part of the CI.

#### Run the benchmark
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
The last command will init a single-node network, apply customizations to the config, create the accounts and run the benchmark for 3m.
All of it.
For a more fine-grained control feel free to dive into the `justfile` for the commands executing the intermediate steps.

#### What you should see
- observe the load reported in the logs
```
...
2025-02-12T16:42:37.351118Z  INFO stats: #    1584 6nxZon12xBTmARmUm3ngtgaA2K7V9dX1J13EtPZ2kEhe Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s 1.60 bps 131 Tgas/s CPU: 70%, Mem: 2.98 GB
...
2025-02-18T13:04:29.790195Z  INFO transaction-generator: total="Stats { pool_accepted: 158247, pool_rejected: 237, included_in_chunks: 155338, failed:0 }"
2025-02-18T13:04:29.790208Z  INFO transaction-generator: diff="Stats { pool_accepted: 6049, pool_rejected: 0, included_in_chunks: 6439, failed: 0 }, mean_processing_rate=6036"
...
```
- see the number of processed transactions (`near_transaction_processed_successfully_total`) or transactions included to the chunks (`near_chunk_transactions_total`) in the metrics
```
$ curl http://localhost:3030/metrics -s | rg "near_transaction_processed_successfully_total (\\d+)"
near_transaction_processed_successfully_total 852883
```
The first one is prone to double-counting the transactions but may better reflect the performance of the underlying runtime.

[!CAUTION]
When the un-limiting settings are applied to the network config the network is prone to failing in non-obvious ways.
To notice the degradation one can monitor for missing chunks and block production times for example.
When the latter starts to grow that means the network no longer sustains the load although the reported mean rate of transaction processing may remain at a high level (reflecting the performance the runtime subsystem).


### Apply-range benchmark
This measures the performance of the runtime sub-system in isolation applying the block or blocks in multiple configurable ways.
The detailed description of it is provided in the [profiling](./profiling.md) section, `Runtime::apply` subsection.
To evaluate the number of rate of processing the transactions one may query the stats as described above.

### Multi-Node setup
For benchmarking multi-node sharded networks, there is a comprehensive toolset in the [sharded-bm](https://github.com/near/nearcore/tree/master/benchmarks/sharded-bm) directory. This framework allows testing various network configurations with different numbers of shards, validators and traffic profiles.

The sharded benchmark framework supports both localnet testing on a single machine and forknet deployments. Forknet is particularly useful as it allows creating a network running on multiple VMs, providing real-world conditions for performance testing.

To run sharded benchmarks on a forknet, you'll need to:
1. Set up the required infrastructure using terraform
2. Configure environment variables for the benchmark
3. Initialize and start the network
4. Create test accounts and run the benchmark

For detailed instructions and configuration options, refer to the [README](https://github.com/near/nearcore/tree/master/benchmarks/sharded-bm/README.md#forknet).

