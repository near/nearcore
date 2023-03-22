# Runtime Parameter Estimator Warehouse

A wrapper application around a SQLite database. SQLite uses a single file to store it's data and only requires minimal tools to be installed.

The warehouse acts as middleman between the output of the parameter estimator and analytic tools that work with the data.

Type `cargo run -- help` for an up-to-date list of available commands and their documentation.

## Examples
### estimator-warehouse import
```
$ target/release/runtime-params-estimator --json-output --metric time --iters 5 --warmup-iters 1 --costs WriteMemoryBase \
  | target/release/estimator-warehouse import --commit-hash `git rev-parse HEAD`
```

### estimator-warehouse stats
```
$ cargo run -- --db $SQLI_DB stats

========================= Warehouse statistics =========================

                  metric                 records            last updated
                  ------                 -------            ------------
                  icount                     163     2022-03-23 15:50:58
                    time                      48     2022-03-23 11:14:00
               parameter                       0                   never

============================== END STATS ===============================
```

### estimator-warehouse check
```
$ cargo run -- --db $SQLI_DB check --metric time
RelativeChange(RelativeChange { estimation: "WriteMemoryBase", before: 191132060000.0, after: 130098178000.0 })
```

# Continuous Estimation

This folder contains some scripts for automated parameter estimation and tracking of the results.

## How can I observe results?
1. Check [Zulip # pagoda/contract-runtime/ce](https://near.zulipchat.com/#narrow/stream/319057-pagoda.2Fcontract-runtime.2Fce) for significant changes in gas cost estimations on the master branch.
1. Browse [near.github.io/parameter-estimator-reports](https://near.github.io/parameter-estimator-reports) for a history of gas cost estimations and how it compares to protocol parameters.

## Understanding the Data flow
1. The estimator produces JSON output with gas costs and extra details.
1. JSON output is fed to the `estimator-warehouse`, which is a wrapper around an SQLite database file. This file is stored as a buildkite artifact.
1. The estimator-warehouse pushes notifications to Zulip.
1. (TODO[jakmeier]) The estimator-warehouse pushes JSON reports to near/parameter-estimator-reports.
1. (TODO[jakmeier]) A vanilla JavaScript frontend at reads the JSON files hosted by GitHub pages and displays them at [near.github.io/parameter-estimator-reports](https://near.github.io/parameter-estimator-reports).

## Running in CI
TODO[jakmeier]: Install a daily buildkite job and document the necessary steps to prepare the full environment.

## Running locally
Use `cargo run -- estimate` to run estimations on the current version in your working directory.
Then use [estimator-warehouse](../estimator-warehouse) to interact with the data.


## Configuration
The script running estimations can be configured to use where it should store the estimated data, where 
* SQLI_DB="/path/to/db.sqlite"
* ESTIMATOR_NEAR_HOME="/path/to/near/home"
    * Use this if a persistent near state should be used. Useful for testing with large stores. But make sure the deployed test contracts are up-to-date.
* REPO_UNDER_TEST="/path/to/another/repository"
    * If you want to run the estimator on a repository clone other than the current directory. Useful to run estimation on older commits, which do not have the continuous estimation scripts.