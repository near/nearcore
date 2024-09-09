# Archival node - Recovery of missing data

# Incident description

In early 2024 there have been a few incidents on archival node storage. As result of these incidents archival node might have lost some data from January to March.

The practical effect of this issue is that requests querying the state of an account may fail, returning instead an internal server error.

# Check if my node has been impacted

The simplest way to check whether or not a node has suffered data loss is to run one of the following queries:

*replace <RPC_URL> with the correct URL (example: http://localhost:3030)*

```bash
curl -X POST <RPC_URL> \
        -H "Content-Type: application/json" \
        -d '
        { "id": "dontcare", "jsonrpc": "2.0", "method": "query", "params": { "account_id": "b001b461c65aca5968a0afab3302a5387d128178c99ff5b2592796963407560a", "block_id": 109913260, "request_type": "view_account" } }'
```

```bash
curl -X POST <RPC_URL> \
        -H "Content-Type: application/json" \
        -d '
        { "id": "dontcare", "jsonrpc": "2.0", "method": "query", "params": { "account_id": "token2.near", "block_id": 114580308, "request_type": "view_account" } }'
```

```bash
curl -X POST <RPC_URL> \
        -H "Content-Type: application/json" \
        -d '
        { "id": "dontcare", "jsonrpc": "2.0", "method": "query", "params": { "account_id": "timpanic.tg", "block_id": 115185110, "request_type": "view_account" } }'
```

```bash
curl -X POST <RPC_URL> \
        -H "Content-Type: application/json" \
        -d '
        { "id": "dontcare", "jsonrpc": "2.0", "method": "query", "params": { "account_id": "01.near", "block_id": 115514400, "request_type": "view_account" } }'
```

If for any of the above requests you get an error of the kind `MissingTrieValue` it means that the node presents the issue.

# Remediation steps

## Option A (recommended): download a new DB snapshot


1. Stop `neard`
2. Delete the existing `hot` and `cold` databases. Example assuming default configuration:
    ```bash
    rm -rf ~/.near/hot-data && rm -rf ~/.near/cold-data
    ```


3. Download an up-to-date snapshot, following this guide: [Storage snapshots](https://near-nodes.io/archival/split-storage-archival#S3%20migration)

## Option B: manually run recovery commands

Follow the instructions below if, for any reason, you prefer to perform the manual recovery steps on your node instead of downloading a new snapshot.

**Requirements:**

* `neard` must be stopped while recovering data
* `cold` storage must be mounted on an SSD disk or better
* The config `resharding_config.batch_delay` must be set to 0.

After the recovery is finished the configuration changes can be undone and a hard disk drive can be used to mount the `cold` storage.

**Important considerations:**

* The recovery procedure will, most likely, take more than one week
  * Since `neard` must be stopped in order to execute the commands, the node won't be functional during this period of time
* The node must catch up to the chain's head after completing the data recovery, this could take days as well
  * During catch up the node can answer RPC queries. However, the chain head is still at the point where the node was stopped; for this reason recent blocks won't be available immediately

We published a [reference recovery script](https://github.com/near/nearcore/blob/master/scripts/recover_missing_archival_data.sh) in the `nearcore` repository. Your `neard` setup might be different, so the advice is to thoroughly check the script before running it. For completeness, here we include the set of commands to run:

```bash
neard view-state -t cold --readwrite apply-range --start-index 109913254 --end-index 110050000 --shard-id 2 --storage trie-free --save-state cold sequential
```

```bash
RUST_LOG=debug neard database resharding --height 114580307 --shard-id 0 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 114580307 --shard-id 1 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 114580307 --shard-id 2 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 114580307 --shard-id 3 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 115185107 --shard-id 0 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 115185107 --shard-id 1 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 115185107 --shard-id 2 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 115185107 --shard-id 3 --restore
```

```bash
RUST_LOG=debug neard database resharding --height 115185107 --shard-id 4 --restore
```

## Verify if remediation has been successful

Run the queries specified in the section: [Check if my node has been impacted](https://docs.nearone.org/doc/archival-node-recovery-of-missing-data-speQFTJc0L#h-check-if-my-node-has-been-impacted). All of them should return a successful response now.