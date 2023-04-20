# How Epoch Works

This short document will tell you all you need to know about Epochs in NEAR
protocol.

You can also find additional information about epochs in
[nomicon](https://nomicon.io/BlockchainLayer/EpochManager/).

## What is an Epoch?

Epoch is a sequence of consecutive blocks.
Within one epoch, the set of validators is fixed, and validator rotation
happens at epoch boundaries.

Basically almost all the changes that we do are happening at epoch boundaries:

* sharding changes
* protocol version changes
* validator changes
* changing tracking shards
* state sync

## Where does the Epoch Id come from?

`EpochId` for epoch T+2 is the last hash of the block of epoch T.

![image](https://user-images.githubusercontent.com/1711539/195907256-c4b1d956-632c-4c11-aa38-17603b1fcc40.png)

Situation at genesis is interesting. We have three blocks:

dummy ← genesis ← first-block

## Where do we set the epoch length?

Epoch length is set in the genesis config. Currently in mainnet it is set to 43200 blocks:

```json
  "epoch_length": 43200
```

<!-- TODO: Where is this supposed to point to? -->
See http://go/mainnet-genesis for more details.

This means that each epoch lasts around 15 hours.

**Important:** sometimes there might be ‘troubles’ on the network, that might result
in epoch lasting a little bit longer (if we cannot get enough signatures on the
last blocks of the previous epoch).

You can read specific details on our
[nomicon page](https://nomicon.io/BlockchainLayer/EpochManager/Epoch).

## How do we pick the next validators?

**TL;DR:** in the last block of the epoch T, we look at the accounts that have
highest stake and we pick them to become validators in **T+2**.

We are deciding on validators for T+2 (and not T+1) as we want to make sure that
validators have enough time to prepare for block production and validation (they
have to download the state of shards etc).

For more info on how we pick validators please look at
[nomicon](https://nomicon.io/Economics/Economic#validator-selection).

## Epoch and Sharding

Sharding changes happen only on epoch boundary - that’s why many of the requests
(like which shard does my account belong to), require also an `epoch_id` as a
parameter.

As of April 2022 we don’t have dynamic sharding yet, so the whole chain is
simply using 4 shards.

### How can I get more information about current/previous epochs?

We don’t show much information about Epochs in Explorer. Today, you can use
`state_viewer` (if you have access to the network database).

At the same time, we’re working on a small debug dashboard, to show you the
basic information about past epochs - stay tuned.

## Technical details

### Where do we store epoch info?

We use a couple columns in the database to store epoch information:

* **ColEpochInfo = 11** - is storing the mapping from EpochId to EpochInfo
  structure that contains all the details.
* **ColEpochStart = 23** - has a mapping from EpochId to the first block height
  of that epoch.
* **ColEpochValidatorInfo = 47** - contains validator statistics (blocks
  produced etc.) for each epoch.

### How does epoch info look like?

Here’s the example epoch info from a localnet node. As you can see below,
EpochInfo mostly contains information about who is the validator and in which
order should they produce the blocks.

```
EpochInfo.V3(
  epoch_height=7,
  validators=ListContainer([
    validator_stake.V1(account_id='node0', public_key=public_key.ED25519(tuple_data=ListContainer([b'7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX'])), stake=51084320187874404740382878961615),
    validator_stake.V1(account_id='node2', public_key=public_key.ED25519(tuple_data=ListContainer([b'GkDv7nSMS3xcqA45cpMvFmfV1o4fRF6zYo1JRR6mNqg5'])), stake=51084320187874404740382878961615),
    validator_stake.V1(account_id='node1', public_key=public_key.ED25519(tuple_data=ListContainer([b'6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e'])), stake=50569171534262067815663761517574)]),

  validator_to_index={'node0': 0, 'node1': 2, 'node2': 1},

  block_producers_settlement=ListContainer([0, 1, 2]),
  chunk_producers_settlement=ListContainer([ListContainer([0, 1, 2]), ListContainer([0, 1, 2]), ListContainer([0, 1, 2]), ListContainer([0, 1, 2]), ListContainer([0, 1, 2])]),

  hidden_validators_settlement=ListContainer([]),
  fishermen=ListContainer([]),
  fishermen_to_index={},
  stake_change={'node0': 51084320187874404740382878961615, 'node1': 50569171534262067815663761517574, 'node2': 51084320187874404740382878961615},
  validator_reward={'near': 37059603312899067633082436, 'node0': 111553789870214657675206177, 'node1': 110428850075662293347329569, 'node2': 111553789870214657675206177},
  validator_kickout={},
  minted_amount=370596033128990676330824359,
  seat_price=24438049905601740367428723111,
  protocol_version=52
)
```
