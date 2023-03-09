# Experimental: Dump of state to External Storage

## Purpose

Current implementation of state sync (see
https://github.com/near/nearcore/blob/master/docs/architecture/how/sync.md for
details) doesn't allow the nodes to reliably perform state sync for testnet or
mainnet.

That's why a new solution for state sync is being designed.
The experimental code is likely going to be a part of solution to greatly
improve both reliability and speed of state sync.

The new solution will probably involve making the state available on external
storage, making downloading the state both low latency and reliable process, 
thanks to the robust infrastructure of external storage such as S3.

## How-to

[#8661](https://github.com/near/nearcore/pull/8661) adds an experimental option
to dump state of every epoch to external storage. At the moment only S3 is
supported as external storage.

To enable, add this to your `config.json` file:

```json
"state_sync": {
  "s3_bucket": "my-bucket",
  "s3_region": "eu-central-1",
  "dump_enabled": true
}
```

And run your node with environment variables `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`:
```shell
AWS_ACCESS_KEY_ID="MY_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="MY_AWS_SECRET_ACCESS_KEY" ./neard run
```

## Implementation Details

The experimental option spawns a thread for each of the shards tracked by a node.
Each of the threads acts independently. Each thread determines the last
complete epoch, and starts the process of dumping the state.

To dump the state a thread does the following:
* Get the size of the trie to determine the number of state parts
* Obtain each state part
* Upload each state part to S3

State parts are uploaded as individual objects. Location of those objects is
computed as follows:
```
"chain_id={chain_id}/epoch_height={epoch_height}/shard_id={shard_id}/state_part_{part_id:06}_of_{num_parts:06}",
```
for example `chain_id=testnet/epoch_height=1790/shard_id=2/state_part_032642_of_065402`

Currently, using multiple nodes for dumping state doesn't make the process go
any faster. The nodes will simpler duplicate the work overwriting files created
by each other.

Future improvement can be to make the nodes cooperate. To avoid introducing a
complicated consensus process, we can suggest the following simple process:
* Get a list of state parts already dumped for an epoch
* Pick 100 random state parts that are not yet random
* Obtain and upload that 100 state parts
* Repeat until all state parts are complete

The process of dumping state parts is managed as a state machine with 2
possible states. The state is stored in the `BlockMisc` column with row key
`STATE_SYNC_DUMP:X` for shard X. Note that epoch id is not included in the row
key, because epoch id is not needed for managing the state machine, because only
one epoch per shard can be dumped at a time.
