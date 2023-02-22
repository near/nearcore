# How neard works

This chapter describes how neard works with a focus on implementation details
and practical scenarios. To get a better understanding of how the protocol
works, please refer to [nomicon](https://nomicon.io). For a high-level code map
of nearcore, please refer to this [document](../).

## High level overview

On the high level, neard is a daemon that periodically receives messages from
the network and sends messages to peers based on different triggers. Neard is
implemented using an [actor
framework](https://en.wikipedia.org/wiki/Actor_model) called
[actix](https://docs.rs/actix).

**Note:** Using actix was decided in the early days of the implementation of
nearcore and by no means represents our confidence in actix. On the contrary, we
have noticed a number of issues with actix and are considering implementing an
actor framework in house.

There are several important actors in neard:

* `PeerActor` - Each peer is represented by one peer actor and runs in a separate
  thread. It is responsible for sending messages to and receiving messages from
  a given peer. After `PeerActor` receives a message, it will route it to
  `ClientActor`, `ViewClientActor`, or `PeerManagerActor` depending on the type
  of the message.

* `PeerManagerActor` - Peer Manager is responsible for receiving messages to send
  to the network from either `ClientActor` or `ViewClientActor` and routing them to
  the right `PeerActor` to send the bytes over the wire. It is also responsible for
  handling some types of network messages received and routed through `PeerActor`.
  For the purpose of this document, we only need to know that `PeerManagerActor`
  handles `RoutedMessage`s. Peer manager would decide whether the `RoutedMessage`s
  should be routed to `ClientActor` or `ViewClientActor`.

* `ClientActor` - Client actor is the “core” of neard. It contains all the main
  logic including consensus, block and chunk processing, state transition, garbage
  collection, etc. Client actor is single threaded.

* `ViewClientActor` - View client actor can be thought of as a read-only interface
  to **client**. It only accesses data stored in a node’s storage and does not mutate
  any state. It is used for two purposes:

    * Answering RPC requests by fetching the relevant piece of data from storage.
    * Handling some network requests that do not require any changes to the
      storage, such as header sync, state sync, and block sync requests.

  `ViewClientActor` runs in four threads by default but this number is configurable.

## Data flow within `neard`

Flow for incoming messages:

![](https://user-images.githubusercontent.com/1711539/195619986-25798cde-8a91-4721-86bd-93fa924b483a.png)


Flow for outgoing messages:

![](https://user-images.githubusercontent.com/1711539/195626792-7697129b-7f9c-4953-b939-0b9bcacaf72c.png)


## How neard operates when it is fully synced

When a node is fully synced, the main logic of the node operates in the
following way (the node is assumed to track all shards, as most nodes on mainnet
do today):

1. A block is produced by some block producer and sent to the node through
   broadcasting.
2. The node receives a block and tries to process it. If the node is synced it
   presumably has the previous block and the state before the current block to
   apply. It then checks whether it has all the chunks available. If the node is
   not a validator node, it won’t have any chunk parts and therefore won’t have
   the chunks available. If the node is a validator node, it may already have
   chunk parts through chunk parts forwarding from other nodes and therefore may
   have already reconstructed some chunks. Regardless, if the node doesn’t have all
   chunks for all shards, it will request them from peers by parts.
3. The chunk requests are sent and the node waits for enough chunk parts to be
   received to reconstruct the chunks. For each chunk, 1/3 of all the parts
   <!-- TODO: Is 100 the number of all the parts or one third of all the parts? -->
   (100) is sufficient to reconstruct a chunk. If new blocks arrive while waiting
   for chunk parts, they will be put into a `OrphanPool`, waiting to be processed.
   If a chunk part request is not responded to within `chunk_request_retry_period`,
   which is set to 400ms by default, then a request for the same chunk part
   would be sent again.
4. After all chunks are reconstructed, the node processes the current block by
   applying transactions and receipts from the chunks. Afterwards, it will
   update the head according to the fork choice rule, which only looks at block
   height. In other words, if the newly processed block is of higher height than
   the current head of the node, the head is updated.
5. The node checks whether any blocks in the `OrphanPool` are ready to be
   processed in a BFS order and processes all of them until none can be
   processed any more. Note that a block is put into the `OrphanPool` if and
   only if its previous block is not accepted.
6. Upon acceptance of a block, the node would check whether it needs to run
   garbage collection. If it needs to, it would garbage collect two blocks worth
   of data at a time. The logic of garbage collection is complicated and could
   be found [here](./gc.md).
7. If the node is a validator node, it would start a timer after the current
   block is accepted. After `min_block_production_delay` which is currently
   configured to be 1.3s on mainnet, it would send an approval to the block
   producer of the next block (current block height + 1).

The main logic is illustrated below:

![](https://user-images.githubusercontent.com/1711539/195635652-f0c7ebae-a2e5-423f-8e62-b853b815fcec.png)


## How neard works when it is synchronizing

`PeerManagerActor` periodically sends a `NetworkInfo` message to `ClientActor`
to update it on the latest peer information, which includes the height of each
peer. Once `ClientActor` realizes that it is more than `sync_height_threshold`
(which by default is set to 1) behind the highest height among peers, it starts
to sync. The synchronization process is done in three steps:

1. Header sync. The node first identifies the headers it needs to sync through a
   [`get_locator`](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/*client/src/sync.rs#L332)
   calculation. This is essentially an exponential backoff computation that
   tries to identify commonly known headers between the node and its peers. Then
   it would request headers from different peers, at most
   `MAX_BLOCK_HEADER_HASHES` (which is 512) headers at a time.
2. After the headers are synced, the node would determine whether it needs to
   run state sync. The exact condition can be found
   [here](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/client/src/sync.rs#L458)
   but basically a node would do state sync if it is more than 2 epochs behind
   the head of the network. State sync is a very complex process and warrants
   its own section. We will give a high level overview here.

   1. First, the node computes `sync_hash` which is the hash of the block that
      identifies the state that the node wants to sync. This is guaranteed to be
      the first block of the most recent epoch. In fact, there is a
      [check](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/chain/src/chain.rs#L4292)
      on the receiver side that this is indeed the case. The node would also
      request the block whose hash is `sync_hash`
   2. The node [deletes basically all data (blocks, chunks, state) from its
      storage](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/chain/src/chain.rs#L1809).
      This is not an optimal solution, but it makes the implementation for
      combining state easier when there is no stale data in storage.
   3. For the state of each shard that the node needs to download, it first
      requests a
      [header](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/core/primitives/src/syncing.rs#L40)
      that contains some metadata the node needs to know about. Then the node
      computes the number of state parts it needs to download and requests those
      parts from different peers who track the shard.
    4. After all parts are downloaded, the node [combines those state
       parts](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/client/src/client_actor.rs#L1877)
       and then
       [finalizes](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/chain/src/chain.rs#L3065)
       the state sync by applying the last chunk included in or before the sync
       block so that the node has the state after applying sync block to be able
       to apply the next block.
     5. The node [resets
        heads](https://github.com/near/nearcore/blob/279044f09a7e6e5e3f26db4898af3655dae6eda6/chain/chain/src/chain.rs#L1874)
        properly after state sync.

3. Block Sync. The node first gets the block with highest height that is on the
   canonical chain and request from there `MAX_BLOCK_REQUESTS` (which is set to 5)
   blocks from different peers in a round robin order. The block sync routine
   runs again if head has changed (progress is made) or if a timeout (which is
   set to 2s) has happened.

**Note:** when a block is received and its height is no more than 500 + the
node’s current head height, then the node would request its previous block
automatically. This is called orphan sync and helps to speed up the syncing
process. If, on the other hand, the height is more than 500 + the node’s current
head height, the block is simply dropped.
<!-- TODO: Either this note is incorrect or the block processing diagram is. -->

## How `ClientActor` works

ClientActor has some periodically running routines that are worth noting:

* [Doomslug
  timer](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/client/src/client_actor.rs#L1198) - 
  This routine runs every `doosmslug_step_period` (set to 100ms by default) and
  updates consensus information. If the node is a validator node, it also sends
  approvals when necessary.
* [Block
  production](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/client/src/client_actor.rs#L991) - 
  This routine runs every `block_production_tracking_delay` (which is set to
  100ms by default) and checks if the node should produce a block.
* [Log
  summary](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/client/src/client_actor.rs#L1790) - 
  Prints a log line that summarizes block rate, average gas used, the height of
  the node, etc. every 10 seconds.
* [Resend chunk
  requests](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/chunks/src/lib.rs#L910) - 
  This routine runs every `chunk_request_retry_period` (which is set to 400ms).
  It resends the chunk part requests for those that are not yet responded to.
* [Sync](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/client/src/client_actor.rs#L1629) - 
  This routine runs every `sync_step_period` (which is set to 10ms by default)
  and checks whether the node needs to sync from its peers and, if needed, also
  starts the syncing process.
* [Catch
  up](https://github.com/near/nearcore/blob/fa78002a1b4119e5efe277c3073b3f333f451ffc/chain/client/src/client_actor.rs#L1581) - 
  This routine runs every `catchup_step_period` (which is set to 100ms by
  default) and runs the catch up process. This only applies if a node validates
  shard A in epoch X and is going to validate a different shard B in epoch X+1.
  In this case, the node would start downloading the state for shard B at the
  beginning of epoch X. After the state downloading is complete, it would apply
  all blocks in the current epoch (epoch X) for shard B to ensure that the node
  has the state needed to validate shard B when epoch X+1 starts.
