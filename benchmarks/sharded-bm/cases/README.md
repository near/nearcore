# Benchmark cases

Define a benchmark case in a separate location, example `local/1_node_1_shard`.

Keep shared configuration overrides in the base directory, example `base_config_patch.json`.

## Case definition

### `param.json`

```json
{
    // number of chunk producer nodes
    "chunk_producers": 5,
    // number of RPC nodes
    "rpcs": 1,
    // patch file for node config, can be further customize through config_patch.json
    // not used for forknet
    "base_config_patch": "../../base_config_patch.json",
    // patch file for chain genesis, can be further customize through genesis_patch.json
    // not used for forknet
    "base_genesis_patch": "../../50_shards_genesis_patch.json",
    // configuration for synth-bm, check the tool's docs for more details
    "num_accounts": 20,
    "requests_per_second": 6000,
    "num_transfers": 6000,
    // part below is required only for forknet runs
    "forknet": {
        // forknet unique name
        "name": "foo",
        // forknet height (used in combination with unique name)
        "start_height": "138038233",
        // neard binary url 
        "binary_url": "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/master/neard",
        // name or suffix of all chunk producers
        "chunk_producers": ["2467", "cd2b", "0d6a", "7953", "19cb"],
        // name or suffix of RPC node
        "rpc": "60b4",
        // address of RPC node (use internal IP)
        "rpc_addr": "10.132.0.59:3030",
        // boot node string (I use key and internal IP of RPC node)
        "boot_nodes": "ed25519:HJHeHmTGs9aj9oNtXbuZhgGq2aTCcr8it13DLRVp4eZw@10.132.0.59:24567"
    }
}
```

## Log settings

Edit the file `log_patch.json` to change the log configuration on all nodes.
