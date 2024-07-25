# Resharding

## Resharding

Resharding is the process in which the shard layout changes. The primary purpose
of resharding is to keep the shards small so that a node meeting minimum hardware
requirements can safely keep up with the network while tracking some set minimum 
number of shards. 

## Specification

The resharding is described in more detail in the following NEPs:
* [NEP-0040](https://github.com/near/NEPs/blob/master/specs/Proposals/0040-split-states.md)
* [NEP-0508](https://github.com/near/NEPs/blob/master/neps/nep-0508.md)

## Shard layout

The shard layout determines the number of shards and the assignment of accounts
 to shards (as single account cannot be split between shards). 

There are two versions of the ShardLayout enum. 
* v0 - maps the account to a shard taking hash of the account id modulo number of shards
* v1 - maps the account to a shard by looking at a set of predefined boundary accounts
and selecting the shard where the accounts fits by using alphabetical order

At the time of writing there are three pre-defined shard layouts but more can
be added in the future. 

* v0 - The first shard layout that contains only a single shard encompassing all the accounts. 
* simple nightshade - Splits the accounts into 4 shards. 
* simple nightshade v2 - Splits the accounts into 5 shards. 

**IMPORTANT**: Using alphabetical order applies to the full account name, so ``a.near`` could belong to
 shard 0, while ``z.a.near`` to shard 3.

Currently in mainnet & testnet, we use the fixed shard split (which is defined in ``get_simple_nightshade_layout``):

``vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]``

In the near future we are planning on switching to simple nightshade v2 (which is defined in ``get_simple_nightshade_layout_v2``)

``vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near", "tge-lockup.sweat"]``


## Shard layout changes

Shard Layout is determined at epoch level in the AllEpochConfig based on the protocol version of the epoch. 

The shard layout can change at the epoch boundary. Currently in order to change the 
shard layout it is necessary to manually determine the new shard layout and setting it
for the desired protocol version in the ``AllEpochConfig``.


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

The state sync of the parent shard, the resharing and the catchup of the children shards must all complete within a single epoch. 

## Rollout

### Flow

The resharding will be initiated by having it included in a dedicated protocol version together with neard. Here is the expected flow of events:

* A new neard release is published and protocol version upgrade date is set to D, roughly a week from the release. 
* All node operators upgrade their binaries to the newly released version within the given timeframe, ideally as soon as possible but no later than D. 
* The protocol version upgrade voting takes place at D in an epoch E and nodes vote in favour of switching to the new protocol version in epoch E+2. 
* The resharding begins at the beginning of epoch E+1. 
* The network switches to the new shard layout in the first block of epoch E+2. 


### Monitoring

Resharding exposes a number of metrics and logs that allow for monitoring the resharding process as it is happening. Resharding requires manual recovery in case anything goes wrong and should be monitored in order to ensure smooth node operation. 

* near_resharding_status is the primary metric that should be used for tracking the progress of resharding. It's tagged with a shard_uid label of the parent shard. It's set to corresponding ReshardingStatus enum and can take one of the following values
  * 0 - Scheduled - resharding is scheduled and waiting to be executed. 
  * 1 - Building - resharding is running. Only one shard at a time can be in that state while the rest will be either finished or waiting in the Scheduled state. 
  * 2 - Finished - resharding is finished. 
  * -1 - Failed - resharding failed and manual recovery action is required. The node will operate as usual until the end of the epoch but will then stop being able to process blocks. 
* near_resharding_batch_size and near_resharding_batch_count - those two metrics show how much data has been resharded. Both metrics should progress with the near_resharding_status as follows. 
  * While in the Scheduled state both metrics should remain 0. 
  * While in the Building state both metrics should be gradually increasing. 
  * While in the Finished state both metrics should remain at the same value. 
* near_resharding_batch_prepare_time_bucket, near_resharding_batch_apply_time_bucket and near_resharding_batch_commit_time_bucket - those three metrics can be used to track the performance of resharding and fine tune throttling if needed. As a rule of thumb the combined time of prepare, apply and commit for a batch should remain at the 100ms-200ms level on average. Higher batch processing time may lead to disruptions in block processing, missing chunks and blocks. 

Here are some example metric values when finished for different shards and networks. The duration column reflects the duration of the building phase. Those were captured in production like environment in November 2023 and actual times at the time of resharding in production may be slightly higher. 

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

Currently when resharding we need to move the data for all shards even if only a single shard is being split. That is due to having the version field in the storage key that needs to be updated when changing shard layout version. 

This can be improved by changing how ShardUId works e.g. removing the version and instead using globally unique shard ids. 

### Dynamic resharding

The current implementation relies on having the shard layout determined offline and manually added to the node implementation. 

The dynamic resharding would mean that the network itself can automatically determine that resharding is needed, what should be the new shard layout and schedule the resharding.  


### Support different changes to shard layout

The current implementation only supports splitting a shard. In the future we can consider adding support for other operations such as merging two shards or moving an existing boundary account. 
