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
This [commit](https://github.com/Near-One/infra-ops/commit/f293a6b9ff4d8918925e4fd34a306f4cb7b0144d) is an example infra suitable for the benchmark. You can do something similar and deploy it with:

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
   - Benchmarks require one RPC node exactly. RPC node will be selected automatically and it will be the 'last' GCP instance.
   - The number of nodes deployed in terraform must match the number of nodes expected in the benchmark scenario.
   - Nodes will run the `neard` binary specified in `forknet.binary_url`

2. Make sure you have a `near-synth-bm` binary that can be run on a mainnet node.

3. Follow these instructions (they work on macOS as well):

<!-- cspell:words BENCHNET -->
```sh
export CASE=cases/forknet/10_cp_1_rpc_10_shard/
export VIRTUAL_ENV=<absolute path to virtual env bin directory>
export SYNTH_BM_BIN=<absolute path to near-synth-bm binary>
export GEN_NODES_DIR=<absolute path to directory of your choice to store nodes configs>
export NEARD=<absolute path to neard binary>
export FORKNET_NAME=<unique name of forknet> 
export FORKNET_START_HEIGHT=<forknet start height>

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

### Known issues

- starting and stopping `neard` in forknet could be done with `mirror` commands
- sometimes forknet commands fail, and they must be issues again

## Transaction injection

Transaction injection with `transactions-generator` works slightly different from `synth-bm`. When using the generator, the node creates transactions automatically as long as `neard` runs.

To use transaction injection you must enable it in `params.json`. To stop the injection run:

```sh
./bench.sh stop-injection
```
