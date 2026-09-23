Dynamic config helpers for the NEAR codebase.

This crate contains utilities that allow to reconfigure the node while it is running.

## How to:

### Logging and tracing

Logging options are controlled by the `rust_log` entry in the `${NEAR_HOME}/log_config.json`.
The example entry may look like
```json
{
    "rust_log": "transaction-generator=info,garbage_collection=trace"
}
```
where the `transaction-generator` and `garbage_collection` are the `target`s in the `tracing` calls.

Tracing options are similarly controlled by the `opentelemetry` field:
```json
    "opentelemetry": "client=debug,chain=debug,stateless_validation=debug,info"
```

### Apply changes

Make changes to `${NEAR_HOME}/log_config.json` and send `SIGHUP` signal to the `neard` process
```shell
kill -HUP $(pidof neard)
```

### Other config values

Makes changes to `config.json` and send `SIGHUP` signal to the `neard` process.

#### Fields of config that can be changed while the node is running:

- `expected_shutdown`: the specified block height neard will gracefully shutdown at.
- `block_production_tracking_delay`: how often the node checks whether it is time to produce or skip a block.
- `min_block_production_delay`: the shortest time the node waits before producing a block.
- `max_block_production_delay`: how long the node waits for block approvals before producing a block anyway.
- `max_block_wait_delay`: how long the node waits before giving up on a height and skipping it.
- `chunk_wait_mult`: multiplier on how long the node waits for all chunks to arrive before producing a block.
- `doomslug_step_period`: how often the doomslug timer fires.

#### Changing other fields of `config.json`

The changes to other fields of `config.json` will be silently ignored as long as
`config.json` remains a valid json object and passes internal validation.

Please be careful about making changes to `config.json` because when a node
starts (or restarts), it checks the validity of the config files and crashes if
detects any issues.
