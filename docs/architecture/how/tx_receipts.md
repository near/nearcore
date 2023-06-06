# Transactions, Receipts and Chunk Surprises

We finished the previous article ([Transaction routing](./tx_routing.md))
where a transaction was successfully added to the soon-to-be block
producer’s mempool.

In this article, we’ll cover what happens next:
How it is changed into a receipt and executed, potentially creating even
more receipts in the process.

First, let’s look at the ‘high level view’:

![image](https://user-images.githubusercontent.com/1711539/198282472-3883dcc1-77ca-452c-b21e-0a7af1435ede.png)

## Transaction vs Receipt

As you can see from the image above:

**Transactions** are ‘external’ communication - they are coming from the
outside.

**Receipts** are used for ‘internal’ communication (cross shard, cross
contract) - they are created by the block/chunk producers.

## Life of a Transaction

If we ‘zoom-in', the chunk producer's work looks like this:

![image](https://user-images.githubusercontent.com/1711539/198282518-cdeb375e-8f1c-4634-842c-6490020ad9c0.png)

### Step 1: Process Transaction into receipt

Once a chunk producer is ready to produce a chunk, it will fetch the
transactions from its mempool, check that they are valid, and if so, prepare to
process them into receipts.

**Note:** There are additional restrictions (e.g. making sure that we take them in
the right order, that we don’t take too many, etc.) - that you can see in
nomicon’s [transaction page](https://nomicon.io/ChainSpec/Transactions).

You can see this part in explorer:

![image](https://user-images.githubusercontent.com/1711539/198282561-c97235a1-93a1-4dc8-b6bc-ee9983376b2c.png)

### Step 2: Sending receipt to the proper destination

Once we have a receipt, we have to send it to the proper destination - by adding
it to the `outgoing_receipt` list, which will be forwarded to the chunk
producers from the next block.

**Note:** There is a special case here - if the sender of the receipt is the
same as the receiver, then the receipt will be added to the `local_receipts`
queue and executed in the same block.

### Step 3: When an incoming receipt arrives

(**Note:** this happens in the ‘next’ block)

When a chunk producer receives an incoming receipt, it will try to execute its
actions (creating accounts, executing function calls etc).

Such actions might generate additional receipts (for example a contract might
want to call other contracts). All these outputs are added to the outgoing
receipt queue to be executed in the next block.

If the incoming receipt queue is too large to execute in the current chunk,
the producer will put the remaining receipts onto the ‘delayed’ queue.

### Step 4: Profit

When all the ‘dependant’ receipts are executed for a given transaction, we can
consider the transaction to be successful.

### [Advanced] But reality is more complex

**Caution:** In the section below, some things are simplified and do not match exactly 
how the current code works.

Let’s quickly also check what’s inside a Chunk:

```rust
pub struct ShardChunkV2 {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>, // outgoing receipts from 'previous' block
}
```

Yes, it is a little bit confusing, that receipts here are NOT the ‘incoming’
ones for this chunk, but instead the ‘outgoing’ ones from the previous block, i.e. all receipts from shard 0, block B are actually found in shard 0, block B+1.  Why?!?!

This has to do with performance.

The steps usually followed for producing a block are as follows
1. Chunk producer executes the receipts and creates a chunk. It sends the chunk to other validators. Note that it's the execution/processing of the receipts that usually takes the most time.
2. Validators receive the chunk and validate it before signing the chunk. Validation involves executing/processing of the receipts in the chunk.
3. Once the next block chunk producer receives the validation (signature), only then can it start producing the next chunk.

#### Simple approach

First, let’s imagine how the system would look like, if chunks contained things
that we’d expect:

* list of transactions
* list of incoming receipts
* list of outgoing receipts
* hash of the final state

This means, that the chunk producer has to compute all this information first,
before sending the chunk to other validators.

![image](https://user-images.githubusercontent.com/1711539/198282601-383977f1-08dd-45fe-aa19-70556d585034.png)

Once the other validators receive the chunk, they can start their own processing to
verify those outgoing receipts/final state - and then do the signing. Only then,
can the next chunk producer start creating the next chunk.

While this approach does work, we can do it faster.

#### Faster approach

What if the chunk didn’t contain the ‘output’ state? This changes our ‘mental’ model
a little bit, as now when we’re singing the chunk, we’d actually be
verifying the previous chunk - but that’s the topic for the next article (to be added).
<!-- TODO: add future link to article about signatures and verification -->

For now, imagine if the chunk only had:

* a list of transactions
* a list of incoming receipts

In this case, the chunk producer could send the chunk a lot earlier, and
validators (and chunk producer) could do their processing at the same time:

![image](https://user-images.githubusercontent.com/1711539/198282641-1e728088-6f2b-4cb9-90c9-5eb09304e72a.png)

Now the last mystery:
Why do we have ‘outgoing’ receipts from previous chunks rather than incoming
to the current one?

This is yet another optimization. This way the chunk producer can send out the
chunk a little bit earlier - without having to wait for all the other shards.

But that’s a topic for another article (to be added).
<!-- TODO: add future link to article about chunk fragments etc. -->
