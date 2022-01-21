# Continuous Estimation

This folder contains some scripts for automated parameter estimation and tracking of the results.

- `init.sql`: SQL statement to initialize a table in an sqlite3 database. Usage: `sqlite3 mydb.sqlite3 --init init.sql .exit`
- `run_estimation.sh`: Runs all estimations on currently checked out git HEAD and stores result in a sqlite3 DB. Usage: `SQLI_DB=mydb.sqlite3 ./run_estimation.sh`
- Various awk scripts used as helpers to parse output and format it as sql insertion query. Used inside `run_estimation.sh`.