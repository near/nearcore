# Speedy sync (a.k.a PoorMan's EpochSync)

The goal of the speedy sync is to allow people to cathup quickly with mainnet, before we have fully implemented the EpochSync feature.

Currently, in order to catchup with mainnet there are two possible options:
* download a DB backup that Pagoda provides (around 200GB)
* sync from scrach - which can take couple days.

With SpeedySync, you're able to catchup with mainnet in around 2-3 hours.

# How does it work?

With regular sync, your job needs to download all the headers from the genesis block until now (so around 60 million headers - as of May 2022).

This of course will take a lot of time (possibly even days). The real fix, will come once we finish building EpochSync - which would require the system to load only a single block per epoch (therefore would limit number of blocks needed by a factor of 40k - to around 12k blocks).

But as EpochSync is not there yet, you can use SpeedySync in the meantime.

SpeedySync uses a small checkpoint (around 50kb), that contains the necessary information about the state of the chain at a given epoch. Therefore your job can continue syncing from that moment, rather than directly from genesis.

## Is it safe?

Yes, but with small caveat: If someone provides you with a fake checkpoint, your future block hashes will not match, that's why **You should verify the block headers after your job is synced, to make sure that they match other blocks on the mainnet**.


# How do I use it?


## Creating a checkpoint
To create a checkpoint, please run:

```
cargo build -p speedy_sync 

./speedy_sync create --home $PATH_TO_RUNNING_NEAR_NODE --destination-dir $PATH_TO_PLACE_WHERE_TO_PUT_CHECKPOINT
```

## Loading a checkpoint
If your new HOME dir doesn't have a node_key.json file, you can generate a random one using:
```
cargo run -p keypair-generator -- --home /tmp/bar --generate-config node-key
```


To load a checkpoint, please run:
```
cargo build -p speedy_sync 
./speedy_sync load --source-dir $PATH_TO_CHECKPOINT_DIR --target-home $PATH_TO_HOME_DIR_OF_A_NEW_NODE
```


### After running speedy

**Important:** After running the 'load' command, you must still copy the 'node_key.json' file into that directory, before running neard.

Please also check and verify the config.json file.

Afterwards you can start the neard with the new homedir and let it sync:

```
./neard --home $PATH_TO_HOME_DIR_OF_A_NEW_NODE
```
