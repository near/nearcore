# Load testing tool

This tool can be used to test a local or remote set of nodes. It submits transactions at a given rate and monitors
the output TPS by periodically requesting the most recent block from the leading nodes.

## Example of a local testnet

The following is an example of starting a local testnet suitable for load testing and load testing it.

Create 10 validator accounts and totally 400 accounts (these are default arguments):
```bash
target/debug/loadtester create_genesis
```

Start the local TestNet (default is 4 node):
```bash
near_ops/start_local_loadtest_network.sh
```

Launch the load tester:
```bash
target/debug/loadtester run --addrs 127.0.0.1:3030 127.0.0.1:3031 127.0.0.1:3032 127.0.0.1:3033
```

Observe that the TestNet produces up to 2000 TPS for 10 seconds in one shard, in my computer loadtester can generate up to 1600 tps and node can process up to 900 tps in the settings mentioned above.

## Example of a launch and load test a remote testnet
Delete `sudo rm -rf ~/.near/near.*/data` if there is any. Create 10 validator accounts and totally 400 accounts (these are default arguments):
```bash
target/debug/loadtester create_genesis
```

Create the testnet in google cloud (default is 10 nodes, 3 in us west, 3 in us east and 4 in us central):
```bash
near_ops/start_gcloud_load_test_network.sh
```

Running loadtester with 100 tps, using 10 of accounts created in the former step. (Due to network latency, loadtest in a single node with higher parameters actually gives worse result in my laptop, but further experiment is welcomed)
```
target/debug/loadtester run --tps 100 --accounts 10 --addrs  `gcloud compute instances list --format="value[terminator=':3030 '](networkInterfaces[0].accessConfigs[0].natIP)" --filter="name~load-test-${USER}"`
```

Observe the result. In my laptop the best result is 10 tps.

## More usages

Some parameters, like tps, number of accounts to create for loadtest network config, etc. is customizable. See them by
`loadtester run --help` and `loadtester create_genesis --help`.

## Further work
- [x] run loadtester with different kind of txns.
- [x] run loadtester from gcloud to a all gcloud network
- [1/2] run loadtester from local to hybrid of local/gcloud network.
- [1/2] exposing peer node information from nearcore, so you can specify only one address to post transaction to all public facing nodes.
