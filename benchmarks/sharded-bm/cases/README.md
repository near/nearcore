# Benchmark cases

Define a benchmark case in a separate location, example `forknet/10-shards`.

Keep shared configuration overrides in the base directory, example `base_config_patch.json`.

## Case definition

### Epoch Configs

Create a directory named `epoch_configs` with a file called `template.json`: a template to create an epoch config for the latest protocol version in the binary. See example [forknet/4-shards/epoch_configs/template.json](forknet/4-shards/epoch_configs/template.json).

### `param.json`

```json
{
    // number of chunk producer nodes
    "chunk_producers": 5,
    // OPTIONAL
    // patch file for node config
    "base_config_patch": "../../base_config_patch.json",
    // OPTIONAL
    // patch file for chain genesis
    "base_genesis_patch": "genesis_patch.json",
    // num accounts per shard
    "num_accounts": 50000,
    // OPTIONAL: required only if using tx generator
    "tx_generator": {
        // true to use tx injection
        "enabled": true,
    }
    // REQUIRED only if using bench.sh
    // number of RPC nodes
    "rpcs": 1,
    // REQUIRED only if using bench.sh
    // RPS for account creation transactions
    "account_rps": 100,
    // OPTIONAL: not necessary for tx generator
    // synth-bm configuration
    "channel_buffer_size": 30000,
    // OPTIONAL: not necessary for tx generator
    // synth-bm configuration
    "requests_per_second": 6000,
    // OPTIONAL: not necessary for tx generator
    // synth-bm configuration
    "num_transfers": 6000,
    // REQUIRED only if using bench.sh
    "forknet": {
        // neard binary url 
        "binary_url": "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/master/neard",
    }
}
```

### `config_patch.json`

File to place optional `neard` configuration overrides valid for this scenario.

### `tx-generator-settings.json`

Required if using `tx generator`.

Example:

```json
{
    "tx_generator": {
        "schedule": [
            { "tps": 1000, "duration_s": 180 },
            { "tps": 2000, "duration_s": 180 },
            { "tps": 4000, "duration_s": 60 }
        ],
        "controller": {
            "target_block_production_time_s": 1.5,
            "bps_filter_window_length": 40,
            "gain_proportional": 15,
            "gain_integral": 0.0,
            "gain_derivative": 0.0
        }
    }
}
```

## Log settings

Edit the file `log_patch.json` to change the log configuration on all nodes.
