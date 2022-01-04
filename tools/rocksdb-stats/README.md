## RocksDB Stats

Tool for measuring statistics of the store for each column:
- number of entries
- column size
- total keys size
- total values size

Before running, install `sst_dump` tool as follows:

```shell
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
make sst_dump
sudo cp sst_dump /usr/local/bin/
```

Then simply `cargo run -p rocksdb-stats`. Should take ~2m for RPC node and 45m for archival node as of 4 Jan 2022.

### Output

List of statistics for each column sorted by column size. 
