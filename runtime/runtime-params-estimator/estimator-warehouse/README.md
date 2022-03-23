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