# Tooling for multishard benchmarks

Set of tools to benchmark the chain's output in terms of transactions per seconds (TPS).

The main objective is to make the benchmarks easy to run and reproducible.

## Requirements

- `cargo` to build synthetic benchmark tools

## Benchmark cases definition

Benchmark cases or scenarios are defined in the directory `cases`.

Each scenario has a set of overrides for the node configuration and genesis configuration, plus a parameters config file to customize the load generation.

## Usage

The basic flow is the following:

1. Make sure `neard` is built and stopped
2. Clear the database, if the node run beforehand

    ```sh
    ./bench.sh reset
    ```

3. Prepare the network

    ```sh
    ./bench.sh init <BENCH CASE>
    ```

4. Start `neard`
5. Create the test accounts

    ```sh
    ./bench.sh create-accounts <BENCH CASE>
    ```

6. Run the benchmark

    ```sh
    ./bench.sh native-transfers <BENCH CASE>
    ```

### Other commands

- Monitor local benchmark execution (TPS and other stuff):

    ```sh
    ./bench.sh monitor
    ```

- Apply config changes

    ```sh
    ./bench.sh tweak-config <BENCH CASE>
    ```

### TL;DR - run a benchmark

```sh
sudo systemctl stop neard
./bench.sh reset
./bench.sh init 50_shards_synth
sudo systemctl restart neard
./bench.sh create-accounts 50_shards_synth
./bench.sh native-transfers 50_shards_synth
```
