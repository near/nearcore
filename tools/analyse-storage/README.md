# Analyse storage

This script serve as a way to analyse RocksDB SST files data, to get a better
sense of data key and value size distribution.

## Usage
Example of running script:
```bash
cargo run --bin analyse-storage <db_path> --column col5 --limit 50
```
where first argument is the path to RocksDB directory, second which column
to inspect and limit how much counts to show.

## Tips
Since the script is written to start reading as much column families as possible one
might need to adjust `max_open_files` on you OS. E.g. for Ubuntu 18.04 you can get
the value by writing `ulimit -Hn`. Adjust it by modifying the file 
`/etc/security/limits.conf` and adding this entry (adjust parameters accordingly):
```
* soft nofile 100000
* hard nofile 100000
```

