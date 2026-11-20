## Transaction Mirror

This is some code that tries to help with the following: We have some
chain, let's call it the "source chain", producing blocks and chunks
with transactions as usual, and we have another chain, let's call it
the "target chain" that starts from state forked from the source
chain. Usually this would be done by using the `neard view-state
dump-state` command, and using the resulting genesis and records file
as the start of the target chain. What we want is to then periodically
send the transactions appearing in the source chain after the fork
point to the target chain. Ideally, the traffic we see in the target
chain will be very similar to the traffic in the source chain.

The first approach we might try is to just send the source chain
transactions byte-for-byte unaltered to the target chain. This almost
works, but not quite, because the `block_hash` field in the
transactions will be rejected. This means we have no choice but to
replace the accounts' public keys in the original forked state, so
that we can sign transactions with a valid `block_hash` field. So the
way we'll use this is that we'll generate the forked state from the
source chain using the usual `dump-state` command, and then run:

```
$ mirror prepare --records-file-in "~/.near/output/records.json" --records-file-out "~/.near/output/mapped-records.json"
```

This command will output a records file where the keys have been
replaced. And then the logic we end up with when running the
transaction generator is something like this:

```
loop {
	sleep(ONE_SECOND);
	source_block = fetch_block(source_chain_view_client, height);
	for chunk in block:
		for tx in chunk:
			private_key = map_key(tx.public_key)
			block_hash = fetch_head_hash(target_chain_view_client)
			new_tx = sign_tx(private_key, tx.actions, block_hash)
			send_tx(target_chain_client, new_tx)
}
```

So then the question is what does `map_key()` do?. If we don't care
about the security of these accounts in the target chain (for example
if the target chain is just some throwaway test chain that nobody
would have any incentive to mess with), we can just use the bytes of
the public key directly as the private key. If we do care somewhat
about security, then we pass a `--secret-key-file` argument to the
`prepare` command, and pass it as an argument to `map_key()`. Using
that makes things a little bit more delicate, since if the generated
secret is ever lost, then it will no longer be possible to mirror any
traffic to the target chain.

ML-DSA-65 keys work the same way, except that the derivation input is
the on-trie key handle - the SHA3-256 digest of the pubkey - rather than
the pubkey itself, since the full pubkey is never stored in state.

## Source chain modes

`neard mirror run --source-home <dir> --target-home <dir> --no-secret` reads the
source chain's blocks out of the store under `--source-home` as it is, and stops
once it has sent the transactions of the last block in it. `--online-source`
instead starts a node in `--source-home` and follows the live chain.

### Streaming the source chain from a cloud archive

`--source-cloud-archive` streams the source chain out of a cloud archive bucket
written by `cloud_archival.writer` nodes. `--source-home` is then a cloud archive
reader home, like the one `neard cloud-archive bootstrap` and `follow` work in: the
source chain's `genesis.json`, a `node_key.json`, and a `config.json` that enables
archival, tracks every shard (the mirror needs every chunk) and names the bucket:

```json
{
  "archive": true,
  "tracked_shards_config": "AllShards",
  "cloud_archival": {
    "location": { "GCS": { "bucket": "my-cloud-archive" } },
    "reader": {}
  }
}
```

`cloud_archival.reader.polling_interval` (5 seconds by default) is how often the
bucket is polled for a new batch; `cloud_archival.credentials_file` can name a GCS
key file. A `{"Filesystem": {"root_dir": "/path"}}` location works for a local test.

The mirror opens that store once and shares it between its own reads and a copy of
the recent reader loop it runs in the background, so it sees new batches as they
land without another process on the store:

- A fresh home is bootstrapped first, from where the mirror goes on (the height
  after the last one it sent, or the target chain's genesis height on a first run)
  up 3200 blocks, or to the bucket's block head if that is lower. The reader loop
  then stays at most 3200 blocks above the heights the mirror asks for, so a fork
  far behind the bucket's head does not copy the whole bucket before the first
  transaction. A bucket that starts above that height is opened
  on its first archived block instead (the heights in between are not in the bucket,
  and the mirror skips them); a bucket whose head has not reached that height
  yet is polled until it has. Failed bucket reads are retried; a bootstrap that
  fails part way is taken on by the reader loop. The log says which range got
  bootstrapped. A home that already has a reader head, from a
  `cloud-archive bootstrap` or an earlier run, is followed from that head, unless the
  head is more than 3200 blocks below where the mirror goes on: the mirror then
  stops and asks for the home's `data` directory to be removed, so the next start
  bootstraps at the right height instead of copying everything in between.
- `--source-skip-state` copies no state out of the bucket, like
  `cloud-archive bootstrap --skip-state`, for a bucket without state snapshots. The
  full access keys of an account cannot be looked up then, and every mirrored
  transaction is signed with the default extra key. A home bootstrapped without
  state (by that flag, or by `cloud-archive bootstrap --skip-state`) has to be
  followed with `--source-skip-state` every time: without it the reader loop fails
  its state checks at startup and the mirror stops.
- Without `--stop-height` the mirror runs until stopped; when it catches up with the
  reader head it waits for the next batch. A `--stop-height` above the bucket's head
  makes it wait the same way (not fail) until the bucket gets there. A `SIGTERM` or
  `SIGINT` drains the transactions already sent and stops the reader loop after the
  batch in flight.
- Progress is logged every 16 batches, and the `near_mirror_source_reader_head` and
  `near_mirror_source_bucket_head` gauges report the reader head and the bucket's
  block head as last read.
