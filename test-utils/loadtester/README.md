# Load testing tool

This tool can be used to test a local or remote set of nodes. It submits transactions at a given rate and monitors
the output TPS by periodically requesting the most recent block from the leading nodes.

## Example of a local testnet

The following is an example of starting a local testnet suitable for load testing and load testing it.

Create 10 validator accounts and totally 400 accounts (these are default arguments):
```bash
target/debug/loadtester create_genesis
```

Start a local testnet from home dir of `~/.near/near.x`

Launch the load tester:
```bash
target/debug/loadtester run --addrs 127.0.0.1:3030 127.0.0.1:3031 127.0.0.1:3032 127.0.0.1:3033
```

## Example of a launch and load test a remote testnet
Delete `sudo rm -rf ~/.near/near.*/data` if there is any. Create 10 validator accounts and totally 400 accounts (these are default arguments):
```bash
target/debug/loadtester create_genesis
```

Launch a testnet in several remote nodes.

Running loadtester with 100 tps, using 10 of accounts created in the first step.
```
target/debug/loadtester run --tps 100 --accounts 10 --addrs <list-of-node-socket-addrs>
```

## More usages

More parameters, like tps, number of accounts to create for loadtest network config, etc. is customizable. See them by
`loadtester run --help` and `loadtester create_genesis --help`.

## Further work
- [x] run loadtester with different kind of txns.
- [x] run loadtester from gcloud to a all gcloud network
- [1/2] run loadtester from local to hybrid of local/gcloud network.
- [1/2] exposing peer node information from nearcore, so you can specify only one address to post transaction to all public facing nodes.