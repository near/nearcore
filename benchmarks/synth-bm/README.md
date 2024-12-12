# About

This crate provides tooling for benchmarking synthetic workloads. It is based on [`near-jsonrpc-client`](https://crates.io/crates/near-jsonrpc-client) and having the Rust toolchain installed should be sufficient to get you started. Documentation is available [here](../../docs/practices/workflows/benchmarking_synthetic_workloads.md).

# Roadmap

- [] Automatically measure TPS when transactions are sent with `wait_until: NONE`.
- [] Enable removing `--nonce` parameters by querying the nonce from the network.
- [] Add support for [other workloads](~/pytest/tests/loadtest/locust/):
  - [] ft transfers
