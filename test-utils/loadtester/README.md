# Load testing tool

This tool can be used to test a local or remote set of nodes. It submits transactions at a given rate and monitors
the output TPS by periodically requesting the most recent block from the leading nodes.

## Example

The following is an example of how to crash a locally running TestNet by saturating it with transactions.
As of 2019-04-16, this has not been fixed.

Start the local TestNet:
```bash
./ops/local_alphanet.sh
```

Host machine needs to have keys that nodes use for their associated accounts. Generate the keys like this:

```bash
cargo run --package keystore --bin keystore keygen --test-seed near.0 -p /tmp/keys/
cargo run --package keystore --bin keystore keygen --test-seed near.1 -p /tmp/keys/
cargo run --package keystore --bin keystore keygen --test-seed near.2 -p /tmp/keys/
cargo run --package keystore --bin keystore keygen --test-seed near.3 -p /tmp/keys/
```

Make sure the load tester is configured to send 700 TPS of monetary transactions. See the `main.rs` file:
```rust
 Executor::spawn(nodes, TransactionType::Monetary, None, None, 700, TrafficType::Regular);
```

Launch the load tester:
```bash
cargo run --package loadtester --bin loadtester -- --key-files-path /tmp/keys \
--addresses 127.0.0.1:3030 127.0.0.1:3031 127.0.0.1:3032 127.0.0.1:3033 \
--public-keys 82M8LNM7AzJHhHKn6hymVW1jBzSwFukHp1dycVcU7MD CTVkQMjLyr4QzoXrTDVzfCUp95sCJPwLJZ34JTiekxMV EJ1DMa6s2ngC5GtZb3Z2DZzat2xFZ34j15VLY37dcdXX 3DToePHssYc75SsxZgzgVLwXE8XQXKjdpdL7CT7D34UE \
--account-ids near.0 near.1 near.2 near.3 2>&1  | tee /tmp/out2.txt
```

Observe that the TestNet produces up to 700 TPS and then after a random amount of time it stops creating new blocks.

