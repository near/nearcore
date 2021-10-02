# NEAR Indexer

NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network. It is useful to handle real-time "events" on the chain.

## Rationale

As scaling dApps enter NEAR’s mainnet, an issue may arise: how do they quickly and efficiently access state from our deployed smart contracts, and cut out the cruft? Contracts may grow to have complex data structures and querying the network RPC may not be the optimal way to access state data. The NEAR Indexer Framework allows for streams to be captured and indexed in a customized manner. The typical use-case is for this data to make its way to a relational database. Seeing as this is custom per project, there is engineering work involved in using this framework.

NEAR Indexer is already in use for several new projects, namely, we index all the events for NEAR Blockchain Explorer, and we also dig into Access Keys and index all of them for NEAR Wallet passphrase recovery and multi-factor authentication. With NEAR Indexer you can do high-level aggregation as well as low-level introspection of all the events inside the blockchain.

We are going to build more Indexers in the future, and will also consider building Indexer integrations with streaming solutions like Kafka, RabbitMQ, ZeroMQ, and NoSQL databases. Feel free to [join our discussions](https://github.com/nearprotocol/nearcore/issues/2996).

See the [example](https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example) for further technical details.

## How to set up and test NEAR Indexer

Before you proceed, make sure you have the following software installed:
* [rustup](https://rustup.rs/) or Rust version that is mentioned in `rust-toolchain` file in the root of nearcore project.

### localnet

Clone [nearcore](https://github.com/nearprotocol/nearcore)

To run the NEAR Indexer connected to a network we need to have configs and keys prepopulated. To generate configs for localnet do the following

```bash
$ git clone git@github.com:nearprotocol/nearcore.git
$ cd nearcore/tools/indexer/example
$ cargo run --release -- --home-dir ~/.near/localnet init
```

The above commands should initialize necessary configs and keys to run localnet in `~/.near/localnet`.

```bash
$ cargo run --release -- --home-dir ~/.near/localnet/ run
```

After the node is started, you should see logs of every block produced in your localnet. Get back to the code to implement any custom handling of the data flowing into the indexer.

Use [near-shell](https://github.com/near/near-shell) to submit transactions. For example, to create a new user you run the following command:

```bash
$ NEAR_ENV=local near --keyPath ~/.near/localnet/validator_key.json \
       create_account new-account.test.near --masterAccount test.near
```


### testnet / betanet

To run the NEAR Indexer connected to testnet or betanet we need to have configs and keys prepopulated, you can get them with the NEAR Indexer Example like above with a little change. Follow the instructions below to run non-validating node (leaving account ID empty).

```bash
$ cargo run --release -- --home-dir ~/.near/testnet init --chain-id testnet --download
```

The above code will download the official genesis config and generate necessary configs. You can replace `testnet` in the command above to different network ID `betanet`.

**NB!** According to changes in `nearcore` config generation we don't fill all the necessary fields in the config file. While this issue is open <https://github.com/nearprotocol/nearcore/issues/3156> you need to download config you want and replace the generated one manually.
 - [testnet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/testnet/config.json)
 - [betanet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/betanet/config.json)
 - [mainnet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/config.json)

Replace `config.json` in your `--home-dir` (e.g. `~/.near/testnet/config.json`) with downloaded one.

Configs for the specified network are in the `--home-dir` provided folder. We need to ensure that NEAR Indexer follows all the necessary shards, so `"tracked_shards"` parameters in `~/.near/testnet/config.json` needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```text
...
"tracked_shards": [0],
...
```

Hint: See the Tweaks section below to learn more about further configuration options.

After that we can run NEAR Indexer.


```bash
$ cargo run --release -- --home-dir ~/.near/testnet run
```

After the network is synced, you should see logs of every block produced in Testnet. Get back to the code to implement any custom handling of the data flowing into the indexer.

## Tweaks

By default, nearcore is configured to do as little work as possible while still operating on an up-to-date state. Indexers may have different requirements, so there is no solution that would work for everyone, and thus we are going to provide you with the set of knobs you can tune for your requirements.

As already has been mentioned above, the most common tweak you need to apply is listing all the shards you want to index data from; to do that, you should ensure that `"tracked_shards"` in the `config.json` lists all the shard IDs, e.g. for the current betanet and testnet, which have a single shard:

```json
...
"tracked_shards": [0],
...
```


You can choose Indexer Framework sync mode by setting what to stream:
 - `LatestSynced` - Real-time syncing, always taking the latest finalized block to stream
 - `FromInterruption` - Starts syncing from the block NEAR Indexer was interrupted last time
 - `BlockHeight(u64)` - Specific block height to start syncing from

 Refer to `main()` function in [Indexer Example](https://github.com/nearprotocol/nearcore/blob/master/tools/indexer/example/src/main.rs)

Indexer Framework also exposes access to the internal APIs (see `Indexer::client_actors` method), so you can fetch data about any block, transaction, etc, yet by default, nearcore is configured to remove old data (garbage collection), so querying the data that was observed a few epochs before may return an error saying that the data is not found. If you only need blocks streaming, you don't need this tweak, but if you need access to the historical data right from your Indexer, consider updating `"archive"` setting in `config.json` to `true`:

```json
...
"archive": true,
...
```


## Who is using NEAR Indexer?

*This list is not exhaustive, feel free to submit your project by sending a pull request.*

* [Indexer for NEAR Wallet](https://github.com/near/near-indexer-for-wallet)
* [Indexer for NEAR Explorer](https://github.com/near/near-indexer-for-explorer)
