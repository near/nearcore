Dynamic config helpers for the NEAR codebase.

This crate contains utilities that allow to reconfigure the node while it is running.

## How to:

### Logging and tracing

Make changes to `log_config.json` and send `SIGHUP` signal to the `neard` process.

### Other config values

Makes changes to `config.json` and send `SIGHUP` signal to the `neard` process.

#### Fields of config that can be changed while the node is running:

- `expected_shutdown`: the specified block height neard will gracefully shutdown at.
