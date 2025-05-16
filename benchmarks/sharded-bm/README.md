# Tooling for multishard benchmarks

Set of tools to benchmark the chain's output in terms of transactions per seconds (TPS).

The main objective is to make the benchmarks easy to run and reproducible.

## Requirements

- `cargo` to build synthetic benchmark tools
- `gcloud`, `python` for forknet benchmarks
- a linux VM or machine to run benchmark and build binaries 

## Benchmark cases definition

Benchmark cases or scenarios are defined in the directory `cases`.

Each scenario has a set of overrides for the node configuration and genesis configuration, plus a parameters config file to customize the load generation.

## Localnet

### TL;DR - run a localnet benchmark

In this directory, on a linux machine, run:

```sh
export CASE=cases/local/1_node_5_shard
./bench.sh init
./bench.sh start-nodes
./bench.sh create-accounts
./bench.sh native-transfers
```

### Localnet - Usage

The basic flow is the following:

1. Make sure `neard` binary is built (default path `/home/ubuntu/neard`)
2. Prepare the network

    ```sh
    ./bench.sh init <BENCH CASE>
    ```

3. Start all nodes

    ```sh
    ./bench.sh start-nodes <BENCH CASE>
    ```

    I advice checking that the network started correctly before proceeding, especially if there are multiple nodes.

4. Create the test accounts

    ```sh
    ./bench.sh create-accounts <BENCH CASE>
    ```

5. Run the benchmark

    ```sh
    ./bench.sh native-transfers <BENCH CASE>
    ```

6. Cleanup when finished

    ```sh
    ./bench.sh reset <BENCH CASE>
    ```

### Other commands

- Monitor local benchmark execution (TPS and other stuff):

    ```sh
    ./bench.sh monitor <BENCH CASE>
    ```

- Apply config changes

    ```sh
    ./bench.sh tweak-config <BENCH CASE>
    ```

- Stop all nodes

    ```sh
    ./bench.sh stop-nodes <BENCH CASE>
    ```

### Localnet - Monitoring

`neard` logs are inside `logs` for a localnet or in `journalctl` for a single node.

`synth-bm` logs are inside `logs`

Debug UI works, just use the machine public IP.

## Forknet

### Forknet - Setup infrastructure

The first step is to create VMs to run the nodes.

You should be able to use any valid forknet image with [forknet terraform recipes](https://docs.nearone.org/doc/mocknet-guide-7VnYUXjs2A).
This [setup](https://github.com/Near-One/infra-ops/tree/tpsnet/provisioning/terraform/infra/network/mocknet/tpsnet) is an example infra suitable for the benchmark. You can make a copy then make the following modifications:

1. in `main.tf`: update `unique_id` and `start_height`, then specify the number
of nodes to start & their regions. Number of instances specified in terraform
(when totalled across regions) _MUST_ match the `CASE` you intend to run (eg, 20
CP + 1 RPC = 21) -- see below or [`cases`](https://github.com/near/nearcore/tree/master/benchmarks/sharded-bm/cases) Note all regions may not
support starting all types of instances (you can check in the console)
2. in `resources.tf` modify the bucket prefix to be unique (will store the terraform state)

Next, you can deploy it with:
```sh
terraform init
terraform apply
```

See all the nodes and their IPs with

```sh
gcloud compute instances list --project=nearone-mocknet --filter <UNIQUE ID>
```

### Forknet - Run benchmark

1. Set the correct values in the test case `params.json`. Keep in mind that:
   - Benchmarks run with one RPC node exactly. RPC node will be selected automatically and it will be the 'last' GCP instance.
   - Nodes will run the `neard` binary specified in `forknet.binary_url`
2. Follow these instructions (they work on macOS as well):

<!-- cspell:words BENCHNET -->
```sh
export CASE=cases/forknet/10_cp_1_rpc_10_shard/
export FORKNET_NAME=<unique name of forknet> 
export FORKNET_START_HEIGHT=<forknet start height>

# Export SYNTH_BM_BIN=<absolute path or URL to near-synth-bm binary> if you need it for account creation
# Export UPDATE_BINARIES=true if you want to force neard binary update during init
./bench.sh init
./bench.sh start-nodes

# Check that the network started properly, you can use debug UI with the external IP of any node
# http://debug.nearone.org/<IP>/last_blocks

./bench.sh create-accounts
./bench.sh native-transfers

# Collect latest OTLP traces, if you set tracing_server = true in main.tf for your cluster
./bench.sh get-traces

# If you are using transaction generator and want to stop it
./bench.sh stop-injection
```

### Forknet - Monitoring

Grafana mostly, [Blockchain utilization dashboard](https://grafana.nearone.org/goto/3bS1Lr2Ng?orgId=1).

### Forknet specific commands

- Shortcut to call `mirror.py`:

    ```sh
    ./bench.sh mirror <ARGS>
    ```

### Forknet - How to measure max TPS

Follow these steps to determine the maximum TPS (transactions per second) the network can handle, to build a report similar to [this example](https://github.com/near/nearcore/issues/13130#issuecomment-2797211286):

1. **Setup:** Initialize the nodes and network for benchmarking using the realistic scenario: `CASE=cases/forknet/realistic_20_cp_1_rpc_20_shard/`.
2. **Start Low:** In `params.json`, set `tx_generator.tps` to about 90% of the previously measured max TPS, or to a conservative low value if unknown.
3. **Iterative Testing:** Repeat the following steps:
    - Run `./bench.sh native-transfers`.
    - Let the network run for at least 15 minutes.
    - In the [Blockchain utilization dashboard](https://grafana.nearone.org/goto/3bS1Lr2Ng?orgId=1), note the value for **Transactions included in chunks** (*current TPS*).
    - Check **Blocks per second** in the same dashboard. Make sure it matches expectations for your `neard` config (e.g., with 1.3s block time, you should see ≥ 0.76 block/s).
        - **If blocks per second is as expected:**  
          - Update your *max TPS* to the observed *current TPS*.
          - Increase `tx_generator.tps` by a small value (e.g., 100).
          - Repeat the loop to test the new TPS.
        - **If blocks per second drops below expected:**  
          - Stop the experiment. The previous *max TPS* is the network’s maximum sustainable throughput.

The idea is to gradually increase the load (TPS) in small steps, starting from a safe value, until the network can no longer keep up.

### Known issues

- It is not possible to configure the number of RPC nodes
- There is no support for Chunk Validator nodes at all

## Transaction injection

Transaction injection with `transactions-generator` works slightly different from `synth-bm`. When using the generator, the node creates transactions automatically as long as `neard` runs.

To use transaction injection you must enable it in `params.json`. To stop the injection run:

```sh
./bench.sh stop-injection
```
