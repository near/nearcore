# Summary

[Introduction](README.md)

# Architecture

- [Overview](./architecture/README.md)
- [How neard works](./architecture/how/README.md)
  - [How Sync Works](./architecture/how/sync.md)
  - [Garbage Collection](./architecture/how/gc.md)
  - [How Epoch Works](./architecture/how/epoch.md)
  - [Transaction Routing](./architecture/how/tx_routing.md)
  - [Transactions And Receipts](./architecture/how/tx_receipts.md)
  - [Cross shard transactions - deep dive](./architecture/how/cross-shard.md)
  - [Serialization: Borsh, Json, ProtoBuf](./architecture/how/serialization.md)
  - [Proofs](./architecture/how/proofs.md)
- [How neard will work](./architecture/next/README.md)
  - [Catchup and state sync improvements](./architecture/next/catchup_and_state_sync.md)
  - [Malicious producers and phase 2](./architecture/next/malicious_chunk_producer_and_phase2.md)
- [Trie](./architecture/trie.md)
- [Network](./architecture/network.md)
- [Gas Cost Parameters](./architecture/gas/README.md)
  - [Parameter Definitions](./architecture/gas/parameter_definition.md)
  - [Gas Profile](./architecture/gas/gas_profile.md)
  - [Runtime Parameter Estimator](./architecture/gas/estimator.md)

# Practices

- [Overview](./practices/README.md)
- [Rust 🦀](./practices/rust.md)
- [Workflows](./practices/workflows/README.md)
  - [Run a Node](./practices/workflows/run_a_node.md)
  - [Deploy a Contract](./practices/workflows/deploy_a_contract.md)
  - [Run Gas Estimations](./practices/workflows/gas_estimations.md)
- [Code Style](./practices/style.md)
- [Documentation](./practices/docs.md)
- [Tracking Issues](./practices/tracking_issues.md)
- [Security Vulnerabilities](./practices/security_vulnerabilities.md)
- [Fast Builds](./practices/fast_builds.md)
- [Testing](./practices/testing/README.md)
  - [Python Tests](./practices/testing/python_tests.md)
  - [Testing Utils](./practices/testing/test_utils.md)
- [Protocol Upgrade](./practices/protocol_upgrade.md)

# Misc

- [Misc](./misc/README.md)
