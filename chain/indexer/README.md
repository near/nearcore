# NEAR Indexer

NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network.


NEAR Indexer is useful to handle real-time "events" on the chain.


NEAR Indexer is going to be used to build NEAR Explorer, augment NEAR Wallet, and provide overview of events in Rainbow Bridge.


See the [example](https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example) for further details.


## How to set up and test NEAR Indexer

Before you proceed, make sure you have the following software installed:
* [rustup](https://rustup.rs/) or Rust version that is mentioned in `rust-toolchain` file in the root of nearcore project.

### localnet

Clone [nearcore](https://github.com/nearprotocol/nearcore)

```bash
$ git clone git@github.com:nearprotocol/nearcore.git
$ cd nearcore
$ cargo run --package neard --bin neard init
```

The above commands should initialize necessary configs and keys to run localnet

```bash
$ cd tools/indexer/example
$ cargo run --release
```

After the node is started, you should see logs of every block produced in your localnet. Get back to the code to implement any custom handling of the data flowing into the indexer.

Use [near-shell](https://github.com/near/near-shell) to submit transactions. For example, to create a new user you run the following command:

```
$ env NEAR_ENV=local near --keyPath ~/.near/localnet/validator_key.json create_account new-account.test.near --masterAccount test.near
```


### betanet

To run the NEAR Indexer connected to betanet we need to have configs and keys prepopulated, you can get them with the [nearup](https://github.com/near/nearup). Clone it and follow the instruction to run non-validating node (leaving account ID empty).

Configs for betanet are in the `~/.near/betanet` folder. We need to ensure that NEAR Indexer follows all the necessary shards, so `"tracked_shards"` parameter in `~/.near/betaneta/config.json` needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```
...
"tracked_shards": [0],
...
```

After that we can run NEAR Indexer.

Follow to `nearcore` folder.

```bash
$ cd nearcore/tools/indexer/example
$ cargo run --release
```

After the network is synced, you should see logs of every block produced in Betanet. Get back to the code to implement any custom handling of the data flowing into the indexer.
