# Tooling for multishard benchmarks

Set of tools to bechmark the network's output in terms of transactions per seconds (TPS). 
The main objective is to make the benchmarks easy to run and reproducible.

## Requirements
- `cargo` to build synthetic benchmark tools

## Benchmark cases definition

Benchmark cases or scenarios are defined in the directory `cases`. 

Each scenario has a set of overrides for the node configuration and genesis configuration, plus a parameters config file to customize the load generation.

## Usage

The basic flow is the following:

1. Make sure `neard` is built and stopped
2. Clear the database, if necessary
    ```
    ./bench.sh reset
    ``` 
3. Prepare the network 
    ```
    ./bench.sh init <BENCH CASE>
    ```
4. Start `neard`
5. Create the test accounts
    ```
    ./bench.sh create-accounts <BENCH CASE>
    ```
6. Run the benchmark
    ```
    ./bench.sh native-transfers <BENCH CASE>
    ```

### Other commands

- Monitor local benchmark execution (TPS):
    ```
    ./bench.sh monitor
    ```