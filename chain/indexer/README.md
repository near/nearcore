# NEAR Indexer

NEAR Indexer is a micro-framework, which provides you with a stream of blocks that are recorded on NEAR network.


NEAR Indexer is useful to handle real-time "events" on the chain.


NEAR Indexer is going to be used to build NEAR Explorer, augment NEAR Wallet, and provide overview of events in Rainbow Bridge.


See the [example](https://github.com/nearprotocol/nearcore/tree/master/tools/indexer/example) for further details.


## How to set up and test NEAR Indexer

Assuming you have all necessary tools installed (Python, Rust, Cargo etc.)

### Localnet

Clone [nearcore](https://github.com/nearprotocol/nearcore)

```bash
$ git clone git@github.com:nearprotocol/nearcore.git
$ cd nearcore
$ cargo run --package neard --bin neard init
```

The above commands should initialize necessary configs and keys to run localnet

```bash
$ cd tools/indexer/example
$ cargo run
```

After that you should see logs of every block produced by your localnet


### Betanet


Clone [nearcore](https://github.com/nearprotocol/nearcore)

```bash
$ git clone git@github.com:nearprotocol/nearcore.git
$ cd nearcore
$ make release
```

This will take awhile to build `nearcore` (we are building `release` as it works a bit faster than debug)

To connect to Betanet we need to have a proper configs and keys.

Clone [nearup](https://github.com/near/nearup)

```bash
$ git clone git@github.com:near/nearup.git
$ cd nearup
$ ./nearup betanet --nodocker --binary-path path/to/nearcore/target/release
```

After that you will be asked for your validator account ID, you can leave it empty as we are not going to validate.

After some time you'll see a success message that says the node is running.

Now we need to stop the node.

```bash
$ ./nearup stop
```

Configs for beta net are in the `~/.near/betanet` folder. We need to ensure `"tracked_shards"` parameter in `~/.near/betaneta/config.json` set up properly.

It has to be

```
...
"tracked_shards": [0],
...
```

After that we can run NEAR Indexer.

Follow back to `nearcore` folder.

```bash
$ cd nearcore/tools/indexer/example
$ cargo run
```

After that you should see logs of every block produced by Betanet
