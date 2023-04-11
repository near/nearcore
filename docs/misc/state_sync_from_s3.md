# Experimental: Sync state from External Storage

## Purpose

Current implementation of state sync (see
https://github.com/near/nearcore/blob/master/docs/architecture/how/sync.md for
details) doesn't allow the nodes to reliably perform state sync for testnet or
mainnet.

That's why a new solution for state sync is being designed.
This is a short-term solution that is needed to let nodes sync and let chunk
only producers to switch tracked shards.
The experimental code is will not be kept for long and will be replaced with a
decentralized solution.

## How-to

[#8789](https://github.com/near/nearcore/pull/8789) adds an experimental option
to sync state from external storage. At the moment only S3 is
supported as external storage.

To enable, add this to your `config.json` file:

```json
"state_sync_enabled": true,
"state_sync": {
  "s3_bucket": "my-bucket",
  "s3_region": "eu-central-1",
  "sync_from_s3_enabled": true
}
```

And run your node with environment variables `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`:
```shell
AWS_ACCESS_KEY_ID="MY_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="MY_AWS_SECRET_ACCESS_KEY" ./neard run
```

## Implementation Details

The new implementation expects to find state parts as files on an S3 storage.

The sync mechanism proceeds to download state parts mostly-sequentially from S3.
In case the state part is not available, the request will be retried after a
delay defined by `state_sync_timeout`, which by default is 1 minute.

State parts are location on S3 at the following location:
```
"chain_id={chain_id}/epoch_height={epoch_height}/shard_id={shard_id}/state_part_{part_id:06}_of_{num_parts:06}",
```
for example `chain_id=testnet/epoch_height=1790/shard_id=2/state_part_032642_of_065402`

After all state parts are downloaded, the node applies them, which replaces the existing State of the node.

Currently, both downloading and applying state parts work rather quickly.
