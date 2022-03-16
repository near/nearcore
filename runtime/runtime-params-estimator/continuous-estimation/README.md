# Continuous Estimation

This folder contains some scripts for automated parameter estimation and tracking of the results.

## How can I observe results?
1. Check XXXzulipXXX for significant changes in gas cost estimations.
1. Browse XXX for history of gas cost estimations and how it compares to protocol parameters.

## High-level flow of scripts
1. The full list of available estimations is defined in `runtime/runtime-params-estimator/src/cost.rs` and it is run with the binary in runtime/runtime-params-estimator.
1. The script `XXX.js` serves as an entry point for regular triggers. It is installed as a CRON job and calls all scripts named below.
1. Estimations run on the latest change on github.com/near/nearcore
1. Detailed estimations results are inserted into a SQLite3 DB file.
1. `XXX` exports the data from the DB to JSON files for a webview with a time series of all estimation results.
1. `XXX` compares the latest two estimations and notifies XXXzulipXXX about significant diversions.


## Files overview
- `init.sql`: SQL statement to initialize a table in an sqlite3 database. Usage: `sqlite3 mydb.sqlite3 --init init.sql .exit`
- `run_estimation.sh`: Runs all estimations on currently checked out git HEAD and stores result in a sqlite3 DB. Usage: `SQLI_DB=mydb.sqlite3 ./run_estimation.sh`
- Various awk scripts used as helpers to parse output and format it as sql insertion query. Used inside `run_estimation.sh`.
- TODO: Describe all files