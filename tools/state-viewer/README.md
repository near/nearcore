# `neard view_state`

`neard view_state` is a tool that helps you look into the state of the blockchain, which includes:

* apply old blocks with a new version of the code or of the protocol
* generate a genesis file from the current state of the blockchain

## Functions

TODO: Fill out documentation for all available commands

### `apply_range`

Basic example:
```bash
make neard
./target/release/neard --home ~/.near/ view_state apply_range \
        --shard-id=0 --start-index=42376889 --end_index=423770101 \
         --verbose-output --csv-file=./apply_range.csv
```

This command will:
* build `neard` with link-time optimizations
* open the blockchain state at the location provided by `--home`
* for each block with height between `--start-index` and `--end-index`
  * Run `apply_transactions` function
* Print individual outcomes if `--verbose-output` is provided. Useful for finding and debugging differences in replaying
the history.
* Print a csv file if `--csv-file` is provided. The csv file contains per-block statistics such as, timestamp of the
block, gas per block, delayed receipts per block. Useful for debugging performance issues. Don't forget to sort your
data before making charts using this data.

If you want to re-apply all the blocks in the available blockchain then omit both the `--start-index` and `--end-index`
flags. Missing `--start-index` means use chain state starting from the genesis. Missing `--end-index` means using blocks up to the latest block available in the blockchain.

Enable debug output to print extra details such as individual outcomes:

```bash
./target/release/neard view_state apply_range --verbose ...
```

To make more precise time estimations, enable `--sequential` flag, which will also cause slowdown proportional to the 
number of rayon threads.

#### Running for the whole `mainnet` history

As of today you need approximately 2TB of disk space for the whole history of `mainnet`, and the most practical way of
obtaining this whole history is the following:

* Patch <https://github.com/near/near-ops/pull/591> to define your own GCP instance in project `rpc-prod`.
* Make sure to change `machine-name` and `role` to something unique.
* Make a Pull Request and ask Mario (@mhalambek) or Sandi (@chefsale) for review.
* Ask Mario or Sandi to grant you permissions to the GCP project `rpc-prod`.
* Run `terraform init` and `terraform apply` to start an instance. This instance will have a running `neard` systemd
  service, with `/home/ubuntu/.near` as the home directory. Follow the `terraform` CLI
  [installation guide](https://learn.hashicorp.com/tutorials/terraform/install-cli) if needed.
* SSH using `gcloud compute ssh <machine_name>" --project=rpc-prod`. Follow the `gcloud` CLI
  [installation guide](https://cloud.google.com/sdk/docs/install) if needed.
* It is recommended to run all the following commands as user `ubuntu`: `sudo su ubuntu`.
* Install tools be able to compile `neard`:
  * Install development packages: <https://docs.near.org/docs/develop/node/validator/compile-and-run-a-node>
  * Install Rust: <https://rustup.rs/>
  * Clone the git repository: `git clone http://github.com/near/nearcore`
  * `make neard`
* `sudo systemctl stop neard`, because a running node has a LOCK over the database.
* Run `neard view_state` as described above
* Enjoy

#### Checking Predicates

It's hard to know in advance which predicates will be of interest. If you want to check that none of function calls use
more than X gas, feel free to add the check yourself.

### `view_chain`

If called without arguments this command will print the block header of tip of the chain, and chunk extras for that
block.

Flags:

* `--height` gets the block header and chunk extras for a block at a certain height.
* `--block` displays contents of the block itself, such as timestamp, outcome_root, challenges, and many more.
* `--chunk` displays contents of the chunk, such as transactions and receipts.

### `dump_state`

Saves the current state of the network in a new genesis file.

Flags:

* `--height` takes state from the genesis up to and including the given height. By default, dumps all available state.

### `dump_tx`

Saves all transactions of a block to a file.

Flags:

* `--height` specifies the block by its height.

* `--account-ids` specifies the accounts as receivers of the transactions that need to be dumped. By default, all transactions will be dumped if this parameter is not set.

### `rocksdb_stats`

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

Should take ~2m for RPC node and 45m for archival node as of 4 Jan 2022.

#### Output

List of statistics for each column sorted by column size.

#### Running on macOS

```bash
brew install --cask google-cloud-sdk
export PATH=/usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin:$PATH
gcloud beta compute ssh --zone "europe-west4-a" "<machine>"  --project "rpc-prod"
```

Check running instances at <https://console.cloud.google.com/compute/instances?project=rpc-prod> to see the machine
name and datacenter.
