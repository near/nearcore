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
