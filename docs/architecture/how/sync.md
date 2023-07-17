# How Sync Works

## Basics

While Sync and Catchup sounds similar - they are actually describing two
completely different things.

**Sync** - is used when your node falls ‘behind’ other nodes in the network (for
example because it was down for some time or it took longer to process some
blocks etc).

**Catchup** - is used when you want (or have to) start caring about (a.k.a.
tracking) additional shards in the future epochs. Currently it should be a no-op
for 99% of nodes (see below).


**Tracking shards:** as you know our system has multiple shards (currently 4).
Currently 99% of nodes are tracking all the shards: validators have to - as they
have to validate the chunks from all the shards, and normal nodes mostly also
track all the shards as this is default.

But in the future - we will have more and more people tracking only a subset of
the shards, so the catchup will be increasingly important.

## Sync

If your node is behind the head - it will start the sync process (this code is
running periodically in the client_actor and if you’re behind for more than
`sync_height_threshold` (currently 50) blocks - it will enable the sync.

The Sync behavior differs depending on whether you’re an archival node (which
means you care about the state of each block) or ‘normal’ node - where you care
mostly about the Tip of the network.

### Step 1: Header Sync [archival node & normal node*] (“downloading headers”)

The goal of the header sync is to get all the block headers from your current
HEAD all the way to the top of the chain.

As headers are quite small, we try to request multiple of them in a single call
(currently we ask for 512 headers at once).

![image](https://user-images.githubusercontent.com/1711539/195892312-2fbd8241-87ce-4241-a44d-ff3056b12bab.png)

### Step 1a: Epoch Sync [normal node*] // not implemented yet


While currently normal nodes are using Header sync, we could actually allow them
to do something faster - “light client sync” a.k.a “epoch sync”.

The idea of the epoch sync, is to read “just” a single block header from each
epoch - that has to contain additional information about validators.

This way it would drastically reduce both the time needed for the sync and the
db resources.

Implementation target date is TBD.

![image](https://user-images.githubusercontent.com/1711539/195892336-cc117c08-d3ad-43f7-9304-3233b25e8bb1.png)

Notice that in the image above - it is enough to only get the ‘last’ header from
each epoch. For the ‘current’ epoch, we still need to get all the headers.

### Step 2: State sync [normal node]

After header sync - if you notice that you’re too far behind (controlled by
`block_fetch_horizon` config option) **AND** that the chain head is in a different
epoch than your local head - the node will try to do the ‘state sync’.

The idea of the state sync - is rather than trying to process all the blocks -
try to ‘jump’ ahead by downloading the freshest state instead - and continue
processing blocks from that place in the chain. As a side effect, it is going to
create a ‘gap’ in the chunks/state on this node (which is fine - as the data
will be garbage collected after 5 epochs anyway). State sync will ONLY sync to
the beginning of the epoch - it cannot sync to any random block.

This step is never run on the archival nodes - as these nodes want to have whole
history and cannot have any gaps.

![image](https://user-images.githubusercontent.com/1711539/195892354-cf2befed-98e9-40a2-9b81-b5cf738406e0.png)

In this case, we can skip processing transactions that are in the blocks 124 - 128, and start from 129 (after sync state finishes)

### Step 3: Block sync (a.k.a Body sync) [archival node, normal node] (“downloading blocks”)

The final step is to start requesting and processing blocks as soon as possible,
hoping to catch up with the chain.

Block sync will request up to 5  (`MAX_BLOCK_REQUESTS`) blocks at a time - sending
explicit Network BlockRequests for each one.

After the response (Block) is received - the code will execute the ‘standard’ path
that tries to add this block to the chain (see section below).

![image](https://user-images.githubusercontent.com/1711539/195892370-b177228b-2520-486a-94fc-67a91978cb58.png)

In this case, we are processing each transaction for each block - until we catch
up with the chain.

## Side topic: how blocks are added to the chain?

A node can receive a Block in two ways:

* Either by broadcasting - when a new block is produced, its contents are
  broadcasted within the network by the nodes
* Or by explicitly sending a BlockRequest to another peer - and getting a Block
  in return.

(in case of broadcasting, the node will automatically reject any Blocks that are
more than 500 (`BLOCK_HORIZON`) blocks away from the current HEAD).

When a given block is received, the node checks if it can be added to the
current chain.

If block’s “parent” (`prev_block`) is not in the chain yet - the block gets added
to the orphan list.

If the parent is already in the chain - we can try to add the block as the head
of the chain.

Before adding the block, we want to download the chunks for the shards that we
are tracking - so in many cases, we’ll call `missing_chunks` functions that will
try to go ahead and request those chunks.

**Note:** as an optimization, we’re also sometimes trying to fetch chunks for
the blocks that are in the orphan pool – but only if they are not more than 3
(`NUM_ORPHAN_ANCESTORS_CHECK`) blocks away from our head.

We also keep a separate job in client_actor that keeps retrying chunk fetching
from other nodes if the original request fails.

After all the chunks for a given block are received (we have a separate HashMap
that checks how many chunks are missing for each block) - we’re ready to
process the block and attach it to the chain.

Afterwards, we look at other entries in the orphan pool to see if any of them
are a direct descendant of the block that we just added - and if yes, we repeat
the process.

## Catchup

### The goal of catchup

Catchup is needed when not all nodes in the network track all shards and nodes
can change the shard they are tracking during different epochs.

For example, if a node tracks shard 0 at epoch T and tracks shard 1 at epoch T+1,
it actually needs to have the state of shard 1 ready before the beginning of
epoch T+1. We make sure this happens by making the node start downloading
the state for shard 1 at the beginning of epoch T and applying blocks during
epoch T to shard 1’s state. Because downloading state can take time, the
node may have already processed some blocks (for shard 0 at this epoch), so when
the state finishes downloading, the node needs to “catch up” processing these
blocks for shard 1.

Right now, all nodes do track all shards, so technically we shouldn’t need the
catchup process, but it is still implemented for the future.

Image below: Example of the node, that tracked only shard 0 in epoch T-1, and
will start tracking shard 0 & 1 in epoch T+1.

At the beginning of the epoch T, it will initiate the state download (green) and
afterwards will try to ‘catchup’ the blocks (orange). After blocks are caught
up, it will continue processing as normal.

![image](https://user-images.githubusercontent.com/1711539/195892395-2e12808e-002b-4c04-9505-611288386dc8.png)

### How catchup interact with normal block processing

The catchup process has two phases: downloading states for shards that we are
going to care about in epoch T+1 and catching up blocks that have already been
applied.

When epoch T starts, the node will start downloading states of shards that it
will track for epoch T+1, which it doesn't track already. Downloading happens in
a different thread so `ClientActor` can still process new blocks. Before the
shard states for epoch T+1 are ready, processing new blocks only applies chunks
for the shards that the node is tracking in epoch T. When the shard states for
epoch T+1 finish downloading, the catchup process needs to reprocess the
blocks that have already been processed in epoch T to apply the chunks for the
shards in epoch T+1. We assume that it will be faster than regular block
processing, because blocks are not full and block production has its own delays,
so catchup can finish within an epoch.

In other words, there are three modes for applying chunks and two code paths,
either through the normal `process_block` (blue) or through `catchup_blocks`
(orange). When `process_block`, either that the shard states for the next epoch
are ready, corresponding to `IsCaughtUp` and all shards the node is tracking in
this, or will be tracking in the next, epoch will be applied, or when the
states are not ready, corresponding to `NotCaughtUp`, then only the shards for
this epoch will be applied. When `catchup_blocks`, shards for the next epoch
will be applied.


```rust
enum ApplyChunksMode {
    IsCaughtUp,
    CatchingUp,
    NotCaughtUp,
}
```

### How catchup works

The catchup process is initiated by `process_block`, where we check if the block
is caught up and if we need to download states. The logic works as follows:

* For the first block in an epoch T, we check if the previous block is caught
  up, which signifies if the state of the new epoch is ready. If the previous
  block is not caught up, the block will be orphaned and not processed for now
  because it is not ready to be processed yet. Ideally, this case should never
  happen, because the node will appear stalled until the blocks in the previous
  epoch are catching up.
* Otherwise, we start processing blocks for the new epoch T. For the first
  block, we always mark it as not caught up and will initiate the process
  for downloading states for shards that we are going to care about in epoch
  T+1. Info about downloading states is persisted in `DBCol::StateDlInfos`.
* For other blocks, we mark them as not caught up if the previous block is not
  caught up. This info is persisted in `DBCol::BlocksToCatchup` which stores
  mapping from previous block to vector of all child blocks to catch up.
* Chunks for already tracked shards will be applied during `process_block`, as 
  we said before mentioning `ApplyChunksMode`.
* Once we downloaded state, we start catchup. It will take blocks from 
  `DBCol::BlocksToCatchup` in breadth-first search order and apply chunks for 
  shards which have to be tracked in the next epoch.
* When catchup doesn't see any more blocks to process, `DBCol::BlocksToCatchup`
  is cleared, which means that catchup process is finished.


The catchup process is implemented through the function `Client::run_catchup`.
`ClientActor` schedules a call to `run_catchup` every 100ms. However, the call
can be delayed if ClientActor has a lot of messages in its actix queue.

Every time `run_catchup` is called, it checks `DBCol::StateDlInfos` to see 
if there are any shard states that should be downloaded. If so, it
initiates the syncing process for these shards. After the state is downloaded,
`run_catchup` will start to apply blocks that need to be caught up.

One thing to note is that `run_catchup` is located at `ClientActor`, but
intensive work such as applying state parts and applying blocks is actually
offloaded to `SyncJobsActor` in another thread, because we don’t want
`ClientActor` to be blocked by this. `run_catchup` is simply responsible for
scheduling `SyncJobsActor` to do the intensive job. Note that `SyncJobsActor` is
state-less, it doesn’t have write access to the chain. It will return the changes
that need to be made as part of the response to `ClientActor`, and `ClientActor`
is responsible for applying these changes. This is to ensure only one thread
(`ClientActor`) has write access to the chain state. However, this also adds a
lot of limits, for example, `SyncJobsActor` can only be scheduled to apply one
block at a time. Because `run_catchup` is only scheduled to run every 100ms, the
speed of catching up blocks is limited to 100ms per block, even when blocks
applying can be faster. Similar constraints happen to apply state parts.

### Improvements

There are three improvements we can make to the current code.

First, currently we always initiate the state downloading process at the first
block of an epoch, even when there are no new states to be downloaded for the
new epoch. This is unnecessary.

Second, even though `run_catchup` is scheduled to run every 100ms, the call can
be delayed if ClientActor has messages in its actix queue. A better way to do
this is to move the scheduling of `run_catchup` to `check_triggers`.

Third, because of how `run_catchup` interacts with `SyncJobsActor`, `run_catchup`
can catch up at most one block every 100 ms. This is because we don’t want to
write to `ChainStore` in multiple threads. However, the changes that catching up
blocks make do not interfere with regular block processing and they can be
processed at the same time. However, to restructure this, we will need to
re-implement `ChainStore` to separate the parts that can be shared among threads
and the part that can’t.
