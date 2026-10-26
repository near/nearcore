# Resharding V2

**DISCLAIMER**: This document describes Resharding V2, which `nearcore` no longer implements. nearcore 2.5.0 (protocol version 75) replaced it with Resharding V3, and nearcore 2.7.0 removed the Resharding V2 code. From protocol version 85 (nearcore 2.13.0), dynamic resharding chooses shard splits automatically, up to the `max_number_of_shards` limit in the epoch config. See [Dynamic Resharding](https://github.com/near/nearcore/blob/master/docs/architecture/how/dynamic_resharding.md) for the current design.

## Resharding

Resharding is the process in which the shard layout changes. The primary purpose
of resharding is to keep the shards small so that a node meeting minimum hardware
requirements can safely keep up with the network while tracking some set minimum
number of shards.

## Specification

The resharding is described in more detail in the following NEPs:

* [NEP-0040](https://github.com/near/NEPs/blob/master/neps/archive/0040-split-states.md)
* [NEP-0508](https://github.com/near/NEPs/blob/master/neps/nep-0508.md)

## Shard layout

The shard layout determines the number of shards and the assignment of accounts
 to shards (as single account cannot be split between shards).

There are four versions of the ShardLayout enum.

* v0 - maps the account to a shard taking hash of the account id modulo number of shards
* v1 - maps the account to a shard by looking at a set of predefined boundary accounts
and selecting the shard where the accounts fits by using alphabetical order
* v2 - uses boundary accounts like v1, but the shard ids do not need to be contiguous
* v3 - uses boundary accounts like v2, and also keeps the full history of shard splits. Dynamic resharding creates v3 layouts.

**IMPORTANT**: Using alphabetical order applies to the full account name, so ``a.near`` could belong to
 shard 0, while ``z.a.near`` to shard 3.

Mainnet and testnet use a v3 shard layout with 10 shards. From protocol version 85 (nearcore 2.13.0),
dynamic resharding can split a shard at an epoch boundary. Both networks allow at most 10 shards
(``max_number_of_shards`` in the epoch config), so no further split happens until a later protocol
version raises that limit. Read the current layout from the ``EXPERIMENTAL_protocol_config`` RPC method
(field ``shard_layout``) instead of relying on a fixed list.

## Shard layout changes

Before protocol version 85, the shard layout of an epoch comes from the epoch config of its
protocol version in the ``AllEpochConfig``. To change the shard layout, developers had to
choose the new shard layout by hand and set it for a new protocol version.

From protocol version 85, the shard layout of each epoch is stored in its ``EpochInfo``. At the
end of epoch ``T``, the epoch manager computes the shard layout of epoch ``T+2``. It splits at most
one shard, based on the split proposals in the chunk headers of the last block of epoch ``T``.
See [Dynamic Resharding](https://github.com/near/nearcore/blob/master/docs/architecture/how/dynamic_resharding.md) for the details.

In both cases, the shard layout can change only at an epoch boundary.

### Deeper technical details

It all starts in ``preprocess_block`` - if the node sees, that the block it is
about to preprocess is the first block of the epoch (X+1)  - it calls
``get_state_sync_info``, which is responsible for figuring out which shards will
be needed in next epoch (X+2).

This is the moment, when node can request new shards that it didn't track before (using StateSync) - and if it detects that the shard layout would change in the next epoch, it also involves the StateSync - but skips the download part (as it already has the data) - and starts from resharding.

StateSync in this phase would send the ``ReshardingRequest`` to the ``SyncJobsActor`` (you can think about the ``SyncJobsActor`` as a background thread).

We'd use the background thread to perform resharding: the goal is to change the one trie (that represents the state of the current shard) - to multiple tries (one for each of the new shards).

In order to split a trie into children tries we use a snapshot of the flat storage. We iterate over all of the entries in the flat storage and we build the children tries by inserting the parent entry into either of the children tries.

Extracting of the account from the key happens in ``parse_account_id_from_raw_key`` - and we do it for all types of data that we store in the trie (contract code, keys, account info etc) EXCEPT for Delayed receipts. Then, we figure out the shard that this account is going to belong to, and we add this key/value to that new trie.

This way, after going over all the key/values from the original trie, we end up with X new tries (one for each new shard).

IMPORTANT: in the current code, we only support such 'splitting' (so a new shard can have just one parent).

### Why delayed receipts are special?

For all the other columns, there is no dependency between entries, but in case of delayed receipts - we are forming a 'queue'. We store the information about the first index and the last index (in DelayedReceiptIndices struct).

Then, when receipt arrives, we add it as the 'DELAYED_RECEIPT + last_index' key (and increment last_index by 1).

That is why we cannot move this trie entry type in the same way as others where account id is part of the key. Instead we do it by iterating over this queue and inserting entries to the queue of the relevant child shard.

## Constraints

The state sync of the parent shard, the resharding and the catchup of the children shards must all complete within a single epoch.

## Rollout

### Flow

The resharding will be initiated by having it included in a dedicated protocol version together with neard. Here is the expected flow of events:

* A new neard release is published and protocol version upgrade date is set to D, roughly a week from the release.
* All node operators upgrade their binaries to the newly released version within the given time frame, ideally as soon as possible but no later than D.
* The protocol version upgrade voting takes place at D in an epoch E and nodes vote in favour of switching to the new protocol version in epoch E+2.
* The resharding begins at the beginning of epoch E+1.
* The network switches to the new shard layout in the first block of epoch E+2.

### Monitoring

The metrics in this section are the current ones, which Resharding V3 and dynamic resharding use.

Resharding exposes a number of metrics and logs that allow for monitoring the resharding process as it is happening. Resharding requires manual recovery in case anything goes wrong and should be monitored in order to ensure smooth node operation.

* near_resharding_status is the primary metric that should be used for tracking the progress of resharding. It's tagged with a shard_uid label of the parent shard. No series is exported for a shard until a resharding is scheduled for it. It's set to corresponding ReshardingStatus enum and can take one of the following values
  * -2 - Cancelled - resharding stopped before it completed, for example on node shutdown or when the child shards are no longer tracked.
  * -1 - Failed - resharding failed at some stage.
  * 1 - Scheduled - the node received the resharding event and waits for the resharding block to become final.
  * 2 - Splitting flat storage - the flat storage of the parent shard is being split.
  * 3 - Catching up - the flat storage of the child shards is catching up.
  * 4 - Resharding trie state - the State column is being rewritten for the child shards.
  * 5 - Done - resharding finished.
* near_flat_storage_resharding_split_shard_processed_batches and near_flat_storage_resharding_split_shard_processed_bytes show the progress of the flat storage split. near_flat_storage_resharding_split_shard_batch_size is the size in bytes of each batch. near_flat_storage_resharding_status shows the flat storage resharding status of each shard.
* near_state_col_resharding_processed_batches shows the progress of rewriting the State column.
* near_resharding_start_timestamp_seconds is the Unix time when the resharding started. near_resharding_total_duration_seconds is the total time from the start until the State column step finishes. near_resharding_memtrie_split_duration_seconds is the time of the memtrie split at the resharding block. This split runs during block processing, so a slow split delays the block.

The monitoring section of [Dynamic Resharding](https://github.com/near/nearcore/blob/master/docs/architecture/how/dynamic_resharding.md#8-monitoring) also lists the metrics of the earlier steps, from the split proposal to the memtrie preload.

The tables and screenshots below are Resharding V2 history. They use Resharding V2 metrics, which no longer exist. The duration column reflects how long Resharding V2 took to build the state of the new shards. The values were captured in a production-like environment before the Resharding V2 rollout.

| mainnet | duration | batch count | batch size |
| ------- | -------- | ----------- | ---------- |
| total   | 2h23min  |             |
| shard 0 | 32min    | 12,510      |  6.6GB     |
| shard 1 | 30min    | 12,203      |  6.1GB     |
| shard 2 | 26min    | 10,619      |  6.0GB     |
| shard 3 | 55min    | 21,070      | 11.5GB     |

| testnet | duration | batch count | batch size |
| ------- | -------- | ----------- | ---------- |
| total   | 5h32min  |             |
| shard 0 | 21min    | 10,259      | 10.9GB     |
| shard 1 | 18min    |  7,034      |  3.5GB     |
| shard 2 | 2h31min  | 75,529      | 75.6GB     |
| shard 3 | 2h22min  | 63,621      | 49.2GB     |

Here is an example of what that may look like in a grafana dashboard. Please keep in mind that the values and duration is not representative as the sample data below is captured in a testing environment with different configuration.

<img width="941" alt="Screenshot 2023-12-01 at 10 10 20" src="https://github.com/near/nearcore/assets/1555986/42824d5a-af16-4a06-9727-a04b1b9d7c03">

<img width="941" alt="Screenshot 2023-12-01 at 10 10 50" src="https://github.com/near/nearcore/assets/1555986/06a2c6f1-1daf-4220-b3fe-e21992e2d62c">

<img width="941" alt="Screenshot 2023-12-01 at 10 10 42" src="https://github.com/near/nearcore/assets/1555986/fea2ad6b-2fa4-4862-875e-a3ca5d61d849">

### Throttling

The resharding process can be quite resource intensive and affect the regular operation of a node. In order to mitigate that as well as limit any need for increasing hardware specifications of the nodes throttling was added. Throttling slows down resharding to not have it impact other node operations. Throttling can be configured by adjusting the resharding_config in the node config file.

* batch_size - controls the size of batches in which resharding moves data around. Setting a smaller batch size will slow down the resharding process and make it less resource-consuming.
* batch_delay - controls the delay between processing of batches. Setting a smaller batch delay will speed up the resharding process and make it more resource-consuming.

The remaining fields in the ReshardingConfig are only intended for testing purposes and should remain set to their default values.

The default configuration for ReshardingConfig should provide a good and safe setting for resharding in the production networks. There is no need for node operators to make any changes to it unless they observe issues.

The resharding config can be adjusted at runtime, without restarting the node. The config needs to be updated first and then a SIGHUP signal should be sent to the neard process. When received the signal neard will update the config and print a log message showing what fields were changed. It's recommended to check the log to make sure the relevant config change was correctly picked up.

## Future possibilities

### Localize resharding to a single shard

Resharding V3 implements this: it changes only the data of the shard that is split. The text below is the original plan.

Currently when resharding we need to move the data for all shards even if only a single shard is being split. That is due to having the version field in the storage key that needs to be updated when changing shard layout version.

This can be improved by changing how ShardUId works e.g. removing the version and instead using globally unique shard ids.

### Dynamic resharding

Dynamic resharding is implemented from protocol version 85 (nearcore 2.13.0). See [Dynamic Resharding](https://github.com/near/nearcore/blob/master/docs/architecture/how/dynamic_resharding.md). The text below is the original plan.

The current implementation relies on having the shard layout determined offline and manually added to the node implementation.

The dynamic resharding would mean that the network itself can automatically determine that resharding is needed, what should be the new shard layout and schedule the resharding.  

### Support different changes to shard layout

The current implementation only supports splitting a shard. In the future we can consider adding support for other operations such as merging two shards or moving an existing boundary account.
