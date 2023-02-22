# Transaction Routing

We all know that transactions are ‘added’ to the chain - but how do they get
there?

Hopefully by the end of this article, the image below should make total sense.

![image](https://user-images.githubusercontent.com/1711539/196204937-d6828382-16df-42bd-b59b-50eb2e6f07af.png)

## Step 1: Transaction creator/author

The journey starts with the author of the transaction - who creates the
transaction object (basically list of commands) - and signs them with their
private key.

Basically, they prepare the payload that looks like this:

```rust
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: Signature,
}
```

With such a payload, they can go ahead and send it as a JSON-RPC request to ANY
node in the system (they can choose between using ‘sync’ or ‘async’ options).

From now on, they’ll also be able to query the status of the transaction - by
using the hash of this object.

**Fun fact:** the `Transaction` object also contains some fields to prevent
attacks: like `nonce` to prevent replay attack, and `block_hash` to limit the
validity of the transaction (it must be added within
`transaction_validity_period` (defined in genesis) blocks of `block_hash`).

## Step 2: Inside the node

Our transaction has made it to a node in the system - but most of the nodes
are not validators - which means that they cannot mutate the chain.

That’s why the node has to forward it to someone who can - the upcoming
validator.

The node, roughly, does the following steps:

* verify transaction’s metadata - check signatures etc. (we want to make sure
  that we don’t forward bogus data)
* forward it to the ‘upcoming’ validator - currently we pick the validators that
  would be a chunk creator in +2, +3, +4 and +8 blocks (this is controlled by
  `TX_ROUTING_HEIGHT_HORIZON`) - and send the transaction to all of them.

## Step 3: En-route to validator/producer

Great, the node knows to send (forward) the transaction to the validator, but
how does the routing work? How do we know which peer is hosting a validator?

Each validator is regularly (every `config.ttl_account_id_router`/2 seconds == 30
minutes in production) broadcasting so called `AnnounceAccount`, which is
basically a pair of `(account_id, peer_id)`, to the whole network. This way each
node knows which `peer_id` to send the message to.

Then it asks the routing table about the shortest path to the peer, and sends
the `ForwardTx` message to the peer.

## Step 4: Chunk producer

When a validator receives such a forwarded transaction, it double-checks that it is
about to produce the block, and if so, it adds the transaction to the mempool
(`TransactionPool`) for this shard, where it waits to be picked up when the chunk
is produced.

What happens afterwards will be covered in future episodes/articles.

## Additional notes:

### Transaction being added multiple times

But such a approach means, that we’re forwarding the same transaction to multiple
validators (currently 4) - so can it be added multiple times?

No. Remember that a transaction has a concrete hash which is used as a global
identifier. If the validator sees that the transaction is present in the chain,
it removes it from its local mempool.

### Can transaction get lost?

Yes - they can and they do. Sometimes a node doesn’t have a path to a given
validator or it didn’t receive an `AnnouceAccount` for it, so it doesn’t know
where to forward the message. And if this happens to all 4 validators that we
try to send to, then the message can be silently dropped.

We’re working on adding some monitoring to see how often this happens.
