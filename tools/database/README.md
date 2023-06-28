# Database

A set of tools useful when working with the underlying database.

## Analyse Database

The analyse database script provides an efficient way to assess the size distribution
of keys and values within RocksDB.

### Usage

To run the script, use the following example:
```bash
cargo run --bin neard -- --home /home/ubuntu/.nerd database analyse-data-size-distribution --column State --top_k 50
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

