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
  * Install development packages: <https://near-nodes.io/validator/compile-and-run-a-node>
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

* `--height` takes state from the genesis up to and including the given height. By default, the tool dumps all available states.

* `--account-ids`, if set, specifies the only accounts that will appear in the output genesis file, except for validators, who will always be included.

Example:

```shell
./target/release/neard --home ~/.near/mainnet/ view_state dump_state --height 68874690 --account-ids near
```

### `dump_tx`

Saves all transactions of a range of blocks [start, end] to a file.

Flags:

* `--start-height` specifies the start block by its height, inclusive.

* `--end-height` specifies the end block by its height, inclusive.

* `--account-ids` specifies the accounts as receivers of the transactions that need to be dumped. By default, all transactions will be dumped if this parameter is not set.

Example:

```shell
./target/release/neard --home ~/.near/mainnet/ view_state dump_tx --start-height 68701890 --end-height 68701890 --account-ids near
```

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

### contract-accounts

List account names with contracts deployed and additional information about the
contracts.

By default, the command only displays the names of all accounts that have a
contract deployed right now. This should be fairly quick. Using flags, you can
display more information but it will also slow down the process.

To see a list of flags, run 
```ignore
cargo run -p neard -- view-state contract-accounts --help
```

#### Example

The following command lists all (but the skipped) accounts with contracts
deployed with the additional information of how many times the account has been
the receiver of a receipt and how many times a receipt was sent by the contract.
(Note: outgoing counts only sent by the contract, not anything where the signed
was the same account id.)

Additionally, the output contains a list of actions that were in the outgoing
receipts. This is particularly useful to find contracts that call certain
actions on-chain.

```ignore
cargo run -p neard -- view-state contract-accounts \
  --skip-accounts "aurora,relay.aurora,token.sweat,oracle.sweat,tge-lockup.near,sweat_welcome.near" \
  --receipts-in \
  --receipts-out \
  --actions \
  > output.log
```

And the output may look something like thi:
```ignore
ACCOUNT_ID                                                         RCPTS_IN  RCPTS_OUT ACTIONS
0-0.near                                                                 37         14 Transfer
0-1.near                                                                797        117 Transfer
0.app.hipodev.near                                                        8          9 Transfer
0.app2.hipodev.near                                                       4          5 Transfer
0.near                                                                   56          9 Transfer
00.near                                                                  29          5 Transfer
000.near                                                                190         17 Transfer
0000.mintbase1.near                                                      49         68 FunctionCall,Transfer
...
And 18858 errors:
failed loading outgoing receipt DpoPSrAHECYrpntTdYXrp2W2Ad3yEPyMyCav4mXi8kyh
failed loading outgoing receipt BoSv67f3CYsWu9LfLxPNkKGSEnwiH7jwPzEmYwL5c7rm
...
failed loading outgoing receipt D4AEcD6umuJKGjSNA2JEZ4EMxn3GK4Z8Ew1iAQpWYtPS
failed loading outgoing receipt AAht3HUDJeGRJ1N776ZKJ2vRiRBAD9GtsLabgbrdioAC
```
