This document is still a DRAFT.



This document covers our improvement plans for state sync and catchup.
**Before reading this doc, you should take a look at [How sync works](../how/sync.md)**



State sync is used in two situations:
* when your node is behind for more than 2 epochs (and it is not an archival node) - then rather than trying to apply block by block (that can take hours) - you 'give up' and download the fresh state (a.k.a state sync) and apply blocks from there.
* when you're a block (or chunk) producer - and in the upcoming epoch, you'll have to track a shard that you are not currently tracking.

In the past (and currently) - the state sync was mostly used in the first scenario (as all block & chunk producers had to track all the shards for security reasons - so they didn't actually have to do catchup at all).

As we progress towards phase 2 and keep increasing number of shards - the catchup part starts being a lot more critical. When we're running a network with a 100 shards, the single machine is simply not capable of tracking (a.k.a applying all transactions) of all shards - so it will have to track just a subset. And it will have to change this subset almost every epoch (as protocol rebalances the shard-to-producer assignment based on the stakes).

This means that we have to do some larger changes to the state sync design, as requirements start to differ a lot:
* catchups are high priority (the validator MUST catchup within 1 epoch - otherwise it will not be able to produce blocks for the new shards in the next epoch - and therefore it will not earn rewards).
* a lot more catchups in progress (with lots of shards basically every validator would have to catchup at least one shard at each epoch boundary) - this leads to a lot more potential traffic on the network
* malicious attacks & incentives - the state data can be large and can cause a lot of network traffic. At the same time it is quite critical (see point above), so we'll have to make sure that the nodes are incetivised to provide the state parts upon request.
* only a subset of peers will be available to request the state sync from (as not everyone from our peers will be tracking the shard that we're interested in).


## Things that we're actively analysing

### Performance of state sync on the receiver side
We're looking at the performance of state sync:
* how long does it take to create the parts,
* pro-actively creating the parts as soon as epoch starts
* creating them in parallel
* allowing user to ask for many at once
* allowing user to provide a bitmask of parts that are required (therefore allowing the server to return only the ones that it already cached).

### Better performance on the requestor side

Currently the parts are applied only once all them are downloaded - instead we should try to apply them in parallel - after each part is received.

When we receive a part, we should announce this information to our peers - so that they know that they can request it from us if they need it.

## Ideas - not actively working on them yet

### Better networking (a.k.a Tier 3)
Currently our networking code is picking the peers to connect at random (as most of them are tracking all the shards). With phase2 it will no longer be the case, so we should work on improvements of our peer-selection mechanism.

In general - we should make sure that we have direct connection to at least a few nodes that are tracking the same shards that we're tracking right now (or that we'll want to track in the near future).

### Dedicated nodes optimized towards state sync responses
The idea is to create a set of nodes that would specialize in state sync responses (similar to how we have archival nodes today).

The sub-idea of this, is to store such data on one of the cloud providers (AWS, GCP).

### Sending deltas instead of full state syncs
In case of catchup, the requesting node might have tracked that shard in the past. So we could consider just sending a delta of the state rather than the whole state.

While this helps with the amount of data being sent - it might require the receiver to do a lot more work (as the data that it is about to send cannot be easily cached).



