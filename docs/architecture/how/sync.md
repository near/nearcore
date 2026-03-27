# How Sync Works

## Basics

While sync and catchup sound similar, they describe two completely different
things.

**Sync** is used when your node falls behind other nodes in the network (for
example because it was down for some time or it took longer to process some
blocks).

**Catchup** is used when you want (or have to) start caring about (a.k.a.
tracking) additional shards in future epochs.

**Tracking shards:** our system has multiple shards. Validators must validate
chunks from all shards, but other nodes may track only a subset. Shard tracking
changes across epochs as the protocol rebalances assignments, making catchup a
regular part of normal operation.

## Sync

If your node is behind the head, it starts the sync process. This runs
periodically in the client actor — if the node is behind by more than
`sync_height_threshold` (default 1) blocks, sync is enabled.

The sync handler classifies the node into one of two categories based on how
far behind it is, and routes it through the appropriate path.

### The two horizons

```
         ┌───────────────┐
         │  Sync Needed  │
         └────────┬──────┘
                  │
                  ▼
          ┌──────────────┐
          │  Need epoch  │
          │    sync?     │
          └───┬───────┬──┘
              │       │
         yes  │       │ no
              ▼       │
      ┌────────────┐  │
      │ EpochSync  │  │
      └──────┬─────┘  │
             │        │
             ▼        │
      ┌────────────┐  │
      │ HeaderSync │  │
      └──────┬─────┘  │
             │        │
             ▼        │
      ┌────────────┐  │
      │ StateSync  │  │
      └──────┬─────┘  │
             │        │
             ▼        ▼
      ┌───────────────────┐
      │     BlockSync     │
      │  (+ header sync)  │
      └─────────┬─────────┘
                │
                ▼
       ┌─────────────────┐
       │  NoSync (Done)  │
       └─────────────────┘
```

The threshold is `epoch_sync_horizon_num_epochs` (default 2, configurable). This
must not exceed `gc_num_epochs_to_keep` (minimum 3, default 5) — otherwise a
near-horizon node could need blocks that peers have already garbage collected.

The decision is based on the node's block head (last fully processed block),
not header head. Archival nodes always take the near-horizon path regardless of
how far behind they are — they need the full block history and cannot use epoch
sync. This means an archival node that falls far behind will do header + block
sync over the entire gap, which can be slow.

The entry decision is made once when sync starts. It is not re-evaluated during
sync — the current path completes naturally.

### Far horizon: full pipeline

When the node is too far behind for block-by-block sync, it follows the full
pipeline:

#### Step 1: epoch sync

Epoch sync is a lightweight mechanism that bootstraps a node to a recent epoch
without downloading every block header in the chain. Instead of syncing headers
one-by-one across potentially millions of blocks, the node downloads a single
epoch sync proof that contains just enough information to verify the chain of
block producers from genesis to a recent epoch.

The proof is structured inductively: genesis epoch's block producers are known,
and each subsequent epoch's producers are verified via `next_bp_hash` from the
previous epoch's last final block. The proof contains one
`EpochSyncProofEpochData` entry per epoch (block producers, last final block
header, endorsements) plus extra data for the last two epochs.

Nodes maintain the epoch sync proof incrementally — at the beginning of every
epoch, the proof is extended by one epoch and stored in compressed form. When a
peer requests a proof, the node returns the pre-computed proof directly without
any on-the-fly derivation. The proof targets epoch T-2 (not T-1) because
`transaction_validity_period` requires approximately two epochs worth of block
headers for transaction validation. Proofs older than 3 epochs are rejected
by the receiving node (this bound matches `MIN_GC_NUM_EPOCHS_TO_KEEP`).

Archival nodes skip epoch sync entirely, since they need the full history.

#### Step 2: header sync

After epoch sync, the node downloads block headers from the epoch sync boundary
forward. This is needed to find a valid state sync hash for the current epoch.
Headers are requested in batches of up to 512 at a time.

Header sync continues until the header chain is close to the network tip
(within ~50 blocks).

#### Step 3: state sync

After headers are synced, the node downloads the full state snapshot at a
sync hash (near the beginning of the most recent epoch). For each shard the
node tracks, it downloads state parts from peers and assembles the full state.

State sync creates a gap in the chain data on this node — blocks between the
epoch sync boundary and the sync point are not processed. This is fine for
non-archival nodes, as that data would be garbage collected after a few epochs
anyway.

This step never runs on archival nodes — they need the complete history and
cannot have gaps.

If state sync takes too long and the network advances past the sync hash's
epoch, state parts become unavailable from peers. When the node detects this
(the network tip is more than one epoch ahead of the sync hash), it triggers
the same data-reset-and-restart flow used for stale nodes.

See [how-to](../../misc/state_sync_from_external_storage.md) to learn how to
configure your node for state sync from external storage (GCS, S3).

#### Step 4: block sync

The final step downloads and applies blocks from the sync point to the network
tip. Header sync runs alongside block sync on every tick. Block sync requests
up to 10 blocks at a time.

After each block is received, the node executes the standard block processing
path (see section below). When the node's head reaches the highest known
height, sync is complete.

### Near horizon: block sync

When the node is close enough to the network tip, it enters the block sync
phase directly — the same final step as the far-horizon pipeline above. Header
sync and block sync run together on every tick, and the node has recent enough
state to apply blocks as they arrive. No epoch sync or state sync is needed.

### Stale node handling

A "stale node" is a non-genesis node that was running but fell far enough
behind to need epoch sync (e.g., it was offline for days). Such a node has
stale data in its DB that is incompatible with the epoch sync bootstrapping
process.

The node handles this by:

1. Downloading and validating the epoch sync proof
2. Writing a `.EPOCH_SYNC_DATA_RESET` marker file
3. Shutting down actors and re-executing the process (via `exec` on Unix;
   on non-Unix platforms, the operator must restart manually)
4. On the new startup, the marker is detected, the data directory is wiped,
   and the node starts fresh on the far-horizon path from genesis

### Restart recovery

If a node crashes mid-sync (e.g., during state sync on the far-horizon path),
the handler detects the situation on restart. If an epoch sync proof exists on
disk and the node's head is still at genesis, the handler checks whether the
header head is within the epoch sync horizon: if so, it re-enters at header
sync (headers are already downloaded); if not, it redoes epoch sync with a
fresh proof. Previously downloaded state parts are preserved in the DB, so
the node does not re-download parts that were already saved before the crash.

## Side topic: how blocks are added to the chain?

A node can receive a Block in two ways:

* Either by broadcasting — when a new block is produced, its contents are
  broadcasted within the network by the nodes
* Or by explicitly sending a BlockRequest to another peer and getting a Block
  in return.

(In case of broadcasting, the node will automatically reject any Blocks that
are more than 500 (`BLOCK_HORIZON`) blocks away from the current HEAD).

When a given block is received, the node checks if it can be added to the
current chain.

If block's "parent" (`prev_block`) is not in the chain yet, the block gets
added to the orphan list.

If the parent is already in the chain, we can try to add the block as the head
of the chain.

Before adding the block, we want to download the chunks for the shards that we
are tracking, so in many cases, we'll call `missing_chunks` functions that will
try to go ahead and request those chunks.

**Note:** as an optimization, we're also sometimes trying to fetch chunks for
the blocks that are in the orphan pool — but only if they are not more than 3
(`NUM_ORPHAN_ANCESTORS_CHECK`) blocks away from our head.

We also keep a separate job in client_actor that keeps retrying chunk fetching
from other nodes if the original request fails.

After all the chunks for a given block are received (we have a separate HashMap
that checks how many chunks are missing for each block), we're ready to
process the block and attach it to the chain.

Afterwards, we look at other entries in the orphan pool to see if any of them
are a direct descendant of the block that we just added, and if yes, we repeat
the process.

## Catchup

### The goal of catchup

Catchup is needed when not all nodes in the network track all shards and nodes
can change the shard they are tracking during different epochs.

For example, if a node tracks shard 0 at epoch T and tracks shard 1 at epoch T+1,
it actually needs to have the state of shard 1 ready before the beginning of
epoch T+1. We make sure this happens by making the node start downloading
the state for shard 1 at the beginning of epoch T and applying blocks during
epoch T to shard 1's state. Because downloading state can take time, the
node may have already processed some blocks (for shard 0 at this epoch), so when
the state finishes downloading, the node needs to "catch up" processing these
blocks for shard 1.

With dynamic resharding, shard assignments change across epochs, making catchup
a regular part of normal operation.

Image below: Example of the node, that tracked only shard 0 in epoch T-1, and
will start tracking shard 0 & 1 in epoch T+1.

At the beginning of the epoch T, it will initiate the state download (green) and
afterwards will try to 'catchup' the blocks (orange). After blocks are caught
up, it will continue processing as normal.

![image](https://user-images.githubusercontent.com/1711539/195892395-2e12808e-002b-4c04-9505-611288386dc8.png)

### How catchup interacts with normal block processing

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
can be delayed if ClientActor has a lot of messages in its actor queue.

Every time `run_catchup` is called, it checks `DBCol::StateDlInfos` to see
if there are any shard states that should be downloaded. If so, it
initiates the syncing process for these shards. After the state is downloaded,
`run_catchup` will start to apply blocks that need to be caught up.

One thing to note is that `run_catchup` is located at `ClientActor`, but
intensive work such as applying state parts and applying blocks is actually
offloaded to `SyncJobsActor` in another thread, because we don't want
`ClientActor` to be blocked by this. `run_catchup` is simply responsible for
scheduling `SyncJobsActor` to do the intensive job. Note that `SyncJobsActor` is
state-less, it doesn't have write access to the chain. It will return the changes
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
be delayed if ClientActor has messages in its actor queue. A better way to do
this is to move the scheduling of `run_catchup` to `check_triggers`.

Third, because of how `run_catchup` interacts with `SyncJobsActor`, `run_catchup`
can catch up at most one block every 100 ms. This is because we don't want to
write to `ChainStore` in multiple threads. However, the changes that catching up
blocks make do not interfere with regular block processing and they can be
processed at the same time. However, to restructure this, we will need to
re-implement `ChainStore` to separate the parts that can be shared among threads
and the part that can't.