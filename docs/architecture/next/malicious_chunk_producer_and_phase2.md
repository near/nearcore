# Malicious producers in phase 2 of sharding.

In this document, we'll compare the impact of the hypothethical malicious producer on the NEAR system (both in the current setup and how it will work when phase2 is implemented).

## Current state (Phase 1)

Let's assume that a malicious chunk producer ``C1`` has produced a bad chunk 
and sent it to the block producer at this height ``B1``. 

The block producer IS going to add the chunk to the block (as we don't validate 
the chunks before adding to blocks - but only when signing the block - see 
[Transcations and receipts - last section](./../how/tx_receipts.md)).

After this block is produced, it is sent to all the validators to get the 
signatures.

As currently all the validators are tracking all the shards - they will quickly 
notice that the chunk is invalid, so they will not sign the block.

Therefore the next block producer ``B2`` is going to ignore ``B1``'s block, and 
select block from ``B0`` as a parent instead.

So TL;DR - **a bad chunk would not be added to the chain.**

## Phase 2 and sharding

Unfortunately things get a lot more complicated, once we scale.

Let's assume the same setup as above (a single chunk producer ``C1`` being 
malicious). But this time, we have 100 shards - each validator is tracking just 
a few (they cannot track all - as today - as they would have to run super 
powerful machines with > 100 cores).

So in the similar scenario as above - ``C1`` creates a malicious chunks, and 
sends it to ``B1``, which includes it in the block.

And here's where the complexity starts - as most of the valiators will NOT 
track the shard which ``C1`` was producing - so they will still sign the block.

The validators that do track that shard will of course (assuming that they are non-malicious) refuse the sign. But overall, they will be a small majority - so the block is going to get enough signatures and be added to the chain.

### Challenges, Slashing and Rollbacks

So we're in a pickle - as a malicious chunk was just added to the chain. And
that's why need to have mechanisms to automatically recover from such situations:
Challenges, Slashing and Rollbacks.

#### Challenge

Challenge is a self-contained proof, that something went wrong in the chunk 
processing. It must contain all the inputs (with their merkle proof), the code
that was executed, and the outputs (also with merkle proofs).

Such a challenge allows anyone (even nodes that don't track that shard or have 
any state) to verify the validity of the challenge.

When anyone notices that a current chain contains a wrong transition - they 
submit such challenge to the next block producer, which can easily verify it 
and it to the next block.

Then the validators do the verification themselves, and if successful, they 
sign the block.

When such block is succesfully signed, the protocol automatically slashes 
malicious nodes (more details below) and initiates the rollback to bring the 
state back to the state before the bad chunk (so in our case, back to the block 
produced by `B0`).


#### Slashing

Slashing is the process of taking away the part of the stake from validators
that are considered malicious.

In the example above, we'll definately need to slash the ``C1`` - and potentially also any validators that were tracking that shard and did sign the bad block.

Things that we'll have to figure out in the future:
* how much do we slash? all of the stake? some part?
* what happens to the slashed stake? is it burned? does it go to some pool?

#### State rollbacks

// TODO: add


## Problems with the current Phase 2 design

### Is slashing painful enough?
In the example above, we'd succesfully slash the ``C1`` producer - but was it  
enough?

Currently (with 4 shards) you need around 20k NEAR to become a chunk producer. 
If we increase the number of shards to 100, it would drop the minimum stake to 
around 1k NEAR.

In such scenario, by sacrificing 1k NEAR, the malicious node can cause the 
system to rollback a couple blocks (potentially having bad impact on the bridge 
contracts etc).

On the other side, you could be a non-malicious chunk producer with a corrupted 
database (or a nasty bug in the code) - and the effect would be the same - the 
chunk that you produced would be marked as malicious, and you'd lose your stake 
(which will be a super-scary even for any legitimate validator).


So the open question is - can we do something 'smarter' in the protocol to
detect the case, where there is 'just a single' malicious (or buggy) chunk 
producer and avoid the expensive rollback?