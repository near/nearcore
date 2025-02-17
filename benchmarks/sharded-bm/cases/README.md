# Benchmark cases

Define a benchmark case in a separate location, example `local/1_node_1_shard`.

Keep shared configuration overrides in the base directory, example `base_config_patch.json`.

## Case definition

### `param.json`

```json
{
    // number of nodes
    "nodes": 1, 
    // patch file for node config, can be further customize through config_patch.json
    "base_config_patch": "../../base_config_patch.json",
    // patch file for chain genesis, can be further customize through genesis_patch.json
    "base_genesis_patch": "../../50_shards_genesis_patch.json",
    // configuration for synth-bm, check the tool's docs for more details
    "num_accounts": 20,
    "requests_per_second": 6000,
    "interval_duration_micros": 23000,
    "num_transfers": 6000
}
```

## Log settings

Edit the file `log_patch.json` to change the log configuration on all nodes.
