# Proofs

“Don’t trust, but verify” - let’s talk about proofs

## Was your transaction included?

How do you know that your transaction was actually included in the blockchain?
Sure, you can “simply” ask the RPC node, and it might say “yes”, but is it
enough?

The other option would be to ask many nodes - hoping that at least one of them
would be telling the truth. But what if that is not enough?

The final solution would be to run your own node - this way you’d check all the
transactions yourself, and then you could be sure - but this can become a quite
expensive endeavour - especially when many shards are involved.

But there is actually a better solution - that doesn’t require you to trust the
single (or many) RPC nodes, and to verify, by yourself, that your transaction was
actually executed.

## Let’s talk about proofs (merkelization):

Imagine you have 4 values that you’d like to store, in such a way, that you can
easily prove that a given value is present.

![image](https://user-images.githubusercontent.com/1711539/198579560-923f1f97-a8df-486d-b68e-8796c6aaa300.png)

One way to do it, would be to create a binary tree, where each node would hold a
hash:

* leaves would hold the hashes that represent the hash of the respective value.
* internal nodes would hold the hash of “concatenation of hashes of their children”
* the top node would be called a root node (in this image it is the node n7)

With such a setup, you can prove that a given value exists in this tree, by
providing a “path” from the corresponding leaf to the root, and including all
the siblings.

For example to prove that value v[1] exists, we have to provide all the nodes
marked as green, with the information about which sibling (left or right) they
are:

![image](https://user-images.githubusercontent.com/1711539/198579596-488c540b-cd24-4d38-bc07-4dc3378c53d0.png)

```
# information needed to verify that node v[1] is present in a tree
# with a given root (n7)
[(Left, n0), (Right, n6)]

# Verification
assert_eq!(root, hash(hash(n0, hash(v[1])), n6))
```

We use the technique above (called merkelization) in a couple of places in our
protocol, but for today’s article, I’d like to focus on receipts & outcome
roots.

## Merkelization, receipts and outcomes

In order to prove that a given receipt belongs to a given block, we will need to
fetch some additional information.

As NEAR is sharded, the receipts actually belong to “Chunks” not Blocks
themselves, so the first step is to find the correct chunk and fetch its
`ChunkHeader`.

```
ShardChunkHeaderV3 {
    inner: V2(
        ShardChunkHeaderInnerV2 {
            prev_block_hash: `C9WnNCbNvkQvnS7jdpaSGrqGvgM7Wwk5nQvkNC9aZFBH`,
            prev_state_root: `5uExpfRqAoZv2dpkdTxp1ZMcids1cVDCEYAQwAD58Yev`,
            outcome_root: `DBM4ZsoDE4rH5N1AvCWRXFE9WW7kDKmvcpUjmUppZVdS`,
            encoded_merkle_root: `2WavX3DLzMCnUaqfKPE17S1YhwMUntYhAUHLksevGGfM`,
            encoded_length: 425,
            height_created: 417,
            shard_id: 0,
            gas_used: 118427363779280,
            gas_limit: 1000000000000000,
            balance_burnt: 85084341232595000000000,
            outgoing_receipts_root: `4VczEwV9rryiVSmFhxALw5nCe9gSohtRpxP2rskP3m1s`,
            tx_root: `11111111111111111111111111111111`,
            validator_proposals: [],
        },
    ),
}
```

The field that we care about is called `outcome_root`. This value represents the
root of the binary merkle tree, that is created based on all the receipts that
were processed in this chunk.

**Note:** You can notice that we also have a field here called
`encoded_merkle_root` - this is another case where we use merkelization in our
chain - this field is a root of a tree that holds hashes of all the "partial
chunks" into which we split the chunk to be distributed over the network.

So, in order to verify that a given receipt/transaction was really included, we
have to compute its hash (see details below), get the path to the root, and
voila, we can confirm that it was really included.

But how do we get the siblings on the path to the root? This is actually
something that RPC nodes do return in their responses.

If you ever looked closely at NEAR’s tx-status response, you can notice a
"proof" section there. For every receipt, you'd see something like this:

```
proof: [
    {
        direction: 'Right',
        hash: '2wTFCh2phFfANicngrhMV7Po7nV7pr6gfjDfPJ2QVwCN'
    },
    {
        direction: 'Right',
        hash: '43ei4uFk8Big6Ce6LTQ8rotsMzh9tXZrjsrGTd6aa5o6'
    },
    {
        direction: 'Left',
        hash: '3fhptxeChNrxWWCg8woTWuzdS277u8cWC9TnVgFviu3n'
    },
    {
        direction: 'Left',
        hash: '7NTMqx5ydMkdYDFyNH9fxPNEkpgskgoW56Y8qLoVYZf7'
    }
]
```

And the values in there are exactly the siblings (plus info on which side of the
tree the sibling is), on the path to the root.

**Note:** proof section doesn’t contain the root itself and also doesn’t include
the hash of the receipt.


## [Advanced section]: Let’s look at a concrete example

Imagine that we have the following receipt:

```
{
  block_hash: '7FtuLHR3VSNhVTDJ8HmrzTffFWoWPAxBusipYa2UfrND',
  id: '6bdKUtGbybhYEQ2hb2BFCTDMrtPBw8YDnFpANZHGt5im',
  outcome: {
    executor_id: 'node0',
    gas_burnt: 223182562500,
    logs: [],
    metadata: { gas_profile: [], version: 1 },
    receipt_ids: [],
    status: { SuccessValue: '' },
    tokens_burnt: '0'
  },
  proof: [
    {
      direction: 'Right',
      hash: 'BWwZ4wHuzaUxdDSrhAEPjFQtDgwzb8K4zoNzfX9A3SkK'
    },
    {
      direction: 'Left',
      hash: 'Dpg4nQQwbkBZMmdNYcZiDPiihZPpsyviSTdDZgBRAn2z'
    },
    {
      direction: 'Right',
      hash: 'BruTLiGx8f71ufoMKzD4H4MbAvWGd3FLL5JoJS3XJS3c'
    }
  ]
}
```

Remember that the outcomes of the execution will be added to the NEXT block, so
let’s find the next block hash, and the proper chunk.

(in this example, I’ve used the `view-state chain` from neard)

```
417 7FtuLHR3VSNhVTDJ8HmrzTffFWoWPAxBusipYa2UfrND |      node0 | parent: 416 C9WnNCbNvkQvnS7jdpaSGrqGvgM7Wwk5nQvkNC9aZFBH | .... 0: E6pfD84bvHmEWgEAaA8USCn2X3XUJAbFfKLmYez8TgZ8 107 Tgas |1: Ch1zr9TECSjDVaCjupNogLcNfnt6fidtevvKGCx8c9aC 104 Tgas |2: 87CmpU6y7soLJGTVHNo4XDHyUdy5aj9Qqy4V7muF5LyF   0 Tgas |3: CtaPWEvtbV4pWem9Kr7Ex3gFMtPcKL4sxDdXD4Pc7wah   0 Tgas
418 J9WQV9iRJHG1shNwGaZYLEGwCEdTtCEEDUTHjboTLLmf |      node0 | parent: 417 7FtuLHR3VSNhVTDJ8HmrzTffFWoWPAxBusipYa2UfrND | .... 0: 7APjALaoxc8ymqwHiozB5BS6mb3LjTgv4ofRkKx2hMZZ   0 Tgas |1: BoVf3mzDLLSvfvsZ2apPSAKjmqNEHz4MtPkmz9ajSUT6   0 Tgas |2: Auz4FzUCVgnM7RsQ2noXsHW8wuPPrFxZToyLaYq6froT   0 Tgas |3: 5ub8CZMQmzmZYQcJU76hDC3BsajJfryjyShxGF9rzpck   1 Tgas
```

I know that the receipt should belong to Shard 3 <!-- TODO: how? :) --> so let’s fetch
the chunk header:

```console
$ neard view-state chunks --chunk-hash 5ub8CZMQmzmZYQcJU76hDC3BsajJfryjyShxGF9rzpck
```

```
ShardChunkHeaderV3 {
  inner: V2(
      ShardChunkHeaderInnerV2 {
          prev_block_hash: `7FtuLHR3VSNhVTDJ8HmrzTffFWoWPAxBusipYa2UfrND`,
          prev_state_root: `6rtfqVEXx5STLv5v4zwLVqAfq1aRAvLGXJzZPK84CPpa`,
          outcome_root: `2sZ81kLj2cw5UHTjdTeMxmaWn2zFeyr5pFunxn6aGTNB`,
          encoded_merkle_root: `6xxoqYzsgrudgaVRsTV29KvdTstNYVUxis55KNLg6XtX`,
          encoded_length: 8,
          height_created: 418,
          shard_id: 3,
          gas_used: 1115912812500,
          gas_limit: 1000000000000000,
          balance_burnt: 0,
          outgoing_receipts_root: `8s41rye686T2ronWmFE38ji19vgeb6uPxjYMPt8y8pSV`,
          tx_root: `11111111111111111111111111111111`,
          validator_proposals: [],
      },
  ),
  height_included: 0,
  signature: ed25519:492i57ZAPggqWEjuGcHQFZTh9tAKuQadMXLW7h5CoYBdMRnfY4g7A749YNXPfm6yXnJ3UaG1ahzcSePBGm74Uvz3,
  hash: ChunkHash(
      `5ub8CZMQmzmZYQcJU76hDC3BsajJfryjyShxGF9rzpck`,
  ),
}
```

So the `outcome_root` is `2sZ81kLj2cw5UHTjdTeMxmaWn2zFeyr5pFunxn6aGTNB` - let’s
verify it then.

Our first step is to compute the hash of the receipt, which is equal to
`hash([receipt_id, hash(borsh(receipt_payload)])`

```python
# this is a borsh serialized ExecutionOutcome struct.
# computing this, we leave as an exercise for the reader :-)
receipt_payload_hash = "7PeGiDjssz65GMCS2tYPHUm6jYDeBCzpuPRZPmLNKSy7"

receipt_hash = base58.b58encode(hashlib.sha256(struct.pack("<I", 2) + base58.b58decode("6bdKUtGbybhYEQ2hb2BFCTDMrtPBw8YDnFpANZHGt5im") + base58.b58decode(receipt_payload_hash)).digest())
```

And then we can start reconstructing the tree:

```python
def combine(a, b):
   return hashlib.sha256(a + b).digest()

# one node example
# combine(receipt_hash, "BWwZ4wHuzaUxdDSrhAEPjFQtDgwzb8K4zoNzfX9A3SkK")
# whole tree
combine(combine("Dpg4nQQwbkBZMmdNYcZiDPiihZPpsyviSTdDZgBRAn2z", combine(receipt_hash, "BWwZ4wHuzaUxdDSrhAEPjFQtDgwzb8K4zoNzfX9A3SkK")), "BruTLiGx8f71ufoMKzD4H4MbAvWGd3FLL5JoJS3XJS3c")
# result == 2sZ81kLj2cw5UHTjdTeMxmaWn2zFeyr5pFunxn6aGTNB
```

And success - our result is matching the outcome root, so it means that our
receipt was indeed processed by the blockchain.
