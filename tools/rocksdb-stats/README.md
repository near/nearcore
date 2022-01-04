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

### Output

List of statistics for each column sorted by column size. 
