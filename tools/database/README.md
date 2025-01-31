# Database

A set of tools useful when working with the underlying database.

## Analyze data size distribution

The analyze database script provides an efficient way to assess the size distribution
of keys and values within RocksDB.

### Usage

To run the script, use the following example:
```bash
cargo run --bin neard -- --home /home/ubuntu/.near database analyze-data-size-distribution --column State --top_k 50
```
The arguments are as follows:

 - `--home`: The path to the RocksDB directory.
 - `--column`: The specific column to inspect.
 - `--top_k`: The maximum number of counts to display (default is 100).

The resulting output will show the following:

 - Total number of key-value pairs per column family
 - Sizes of each column family
 - Key and value size distribution

### Tips for Handling Large Column Families
As this script is designed to read as many column families as possible at the start,
you may need to adjust the max_open_files limit on your operating system.

For Ubuntu 18.04, you can check the current limit by using the command `ulimit -Hn`.
To adjust this limit, modify the `/etc/security/limits.conf` file and add the following
entry (adjust parameters to suit your needs):
```
* soft nofile 100000
* hard nofile 100000
```

## Adjust-db tool
This is a tool that should only be used for testing purposes.  
It is intended as a collection of commands that perform small db modifications.


### change-db-kind
Changes DbKind of a DB described in config (cold or hot).  
Example usage:
```bash
cargo run --bin neard -- --home /home/ubuntu/.near database change-db-kind --new-kind RPC change-cold
```

In this example we change DbKind of the cold db to RPC (for some reason).  
Notice, that you cannot perform this exact command twice in a row,
because you will not be able to open cold db in the first place.  
If you want to change DbKind of the cold db back, you would have to adjust your config:
- .store.path = `<relative cold path>`
- .cold_store = null
- .archive = false

This way neard would try to open db at cold path as RPC db.  
Then you can call
`neard database change-db-kind --new-kind Cold change-hot`.
Notice that even though in your mind this db is cold, in your config this db hot, so you have to pass `change-hot`.

## Compact database

Run compaction on the SST files. Running this command might increase database read performance.
This is good use case when changing `block_size` and wishing to perform test on how the RocksDB performance has
changed.

Example usage:
```bash
cargo run --bin neard -- database compact-database
```


## Make a DB Snapshot

Makes a copy of a DB (hot store only) at a specified location. If the
destination is within the same filesystem, the copy will be made instantly and
take no additional disk space due to hard-linking all the files.

Example usage:
```bash
cargo run --bin neard -- --home /home/ubuntu/.near database make-snapshot --destination /home/ubuntu/.near/data/snapshot
```

In this example all `.sst` files from `/home/ubuntu/.near/data` will be also
available in `/home/ubuntu/.near/data/snapshot`

This command can be helpful before attempting activities that can potentially
corrupt the database.

### Run DB Migrations

Opens the DB and runs migrations to bring it to the actual version expected by `neard`
Example usage:
```bash
cargo run --bin neard database run-migrations
```

For example, if the binary expects DB version `38`, but the DB is currently
version `36`, the command will open the DB, run migrations that bring the DB
from version `36` to version `38`, and then exits.

## State read perf
A tool for performance testing hot storage RocksDB State column reads.
Use help to get more details: `neard database state-perf --help`
