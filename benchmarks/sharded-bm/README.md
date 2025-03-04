# Tooling for multishard benchmarks

Set of tools to benchmark the chain's output in terms of transactions per seconds (TPS).

The main objective is to make the benchmarks easy to run and reproducible.

## Requirements

- `cargo` to build synthetic benchmark tools

## Benchmark cases definition

Benchmark cases or scenarios are defined in the directory `cases`.

Each scenario has a set of overrides for the node configuration and genesis configuration, plus a parameters config file to customize the load generation.

## Localnet

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

### TL;DR - run a benchmark

```sh
export CASE=cases/local/1_node_5_shard
./bench.sh init
./bench.sh start-nodes
./bench.sh create-accounts
./bench.sh native-transfers
```

### Monitoring

`neard` logs are inside `logs` for a localnet or in `journalctl` for a single node.

`synth-bm` logs are inside `logs`

Debug UI works, just use the machine public IP.

## Forknet

### Forknet - Usage

You should be able to use any valid forknet image with [forknet terraform recipes](https://docs.nearone.org/doc/mocknet-guide-7VnYUXjs2A).

First, set correct values inside the `forknet` object in `params.json`.

```sh
export CASE=cases/forknet/5_cp_1_rpc_5_shard/
export VIRTUAL_ENV=<absolute path to virtual env bin directory>
export SYNTH_BM_BIN=<absolute path to near-synth-bm>

# either run this command or export an env variable (GEN_NODES_DIR) with the path to its output, 
# which is a directory containing config for node0, node1, etc 
./bench.sh init cases/forknet/5_cp_1_rpc_5_shard/local

./bench.sh init
./bench.sh start-nodes
./bench.sh create-accounts
./bench.sh native-transfers
```

### Known issues

- updating `param.json` is tedious
- the start-node command hangs (must interrupt it)
