# State viewer

Simple tool to view the current state

## Example usage

View the current state in a human-readable format:

    cargo run --package state-viewer -- -d <BASE_DIR> -c <CHAIN_SPEC_FILE>

Dump the current state into a chain spec that starts with it as genesis:

    cargo run --package state-viewer -- -d <BASE_DIR> -c <CHAIN_SPEC_FILE> --dump-genesis-file <OUT_FILE>
