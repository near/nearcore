Dynamic config helpers for the NEAR codebase.

This crate contains utilities that allow to reconfigure the node while it is running.

## How to:

### Logging and tracing

Make changes to `log_config.json` and send `SIGHUP` signal to the `neard` process.

### Other config values

Makes changes to `config.json` and send `SIGHUP` signal to the `neard` process.

#### Fields of config that can be changed while the node is running:

- `expected_shutdown`: the specified block height neard will gracefully shutdown at.

#### Changing other fields of `config.json`

The changes to other fields of `config.json` will be silently ignored as long as
`config.json` remains a valid json object and passes internal validation.

Please be careful about making changes to `config.json` because when a node
starts (or restarts), it checks the validity of the config files and crashes if
detects any issues.
