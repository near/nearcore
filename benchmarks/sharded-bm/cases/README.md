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
    // OPTIONAL: not necessary for forknet
    "base_config_patch": "../../base_config_patch.json",
    // patch file for chain genesis, can be further customize through genesis_patch.json
    // OPTIONAL: not necessary for forknet
    "base_genesis_patch": "../../50_shards_genesis_patch.json",
    // num accounts per shard
    "num_accounts": 20,
    // synth-bm configuration
    // OPTIONAL: not necessary for tx generator
    "requests_per_second": 6000,
    // synth-bm configuration
    // OPTIONAL: not necessary for tx generator
    "num_transfers": 6000,
    // OPTIONAL: required only if using tx generator
    "tx_generator": {
        // true to use tx injection
        "enabled": true,
        "tps": 4000,
        "volume": 0
    }
    // OPTIONAL: required only for forknet runs
    "forknet": {
        // forknet unique name
        "name": "foo",
        // forknet height (used in combination with unique name)
        "start_height": "138038233",
        // neard binary url 
        "binary_url": "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/master/neard",
        // name or suffix of RPC node
        "rpc": "60b4",
    }
}
```

## Log settings

Edit the file `log_patch.json` to change the log configuration on all nodes.
