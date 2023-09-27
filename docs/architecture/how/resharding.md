# Resharding - current state.

## Shard layout

The shard layout determines the number of shards and the assignment of accounts
 to shards (as single account cannot be split between shards). 


There is a v0 version (that was taking a hash of account to assign account to
 shard), and the v1 version that supports 2 options:
* fixed-shards - where you set a top-domain, and all the subaccounts from this
 domain automatically belong to this shard
* boundary accounts - where accounts are assigned to shards in alphabetical
 order and these account determine the boundaries.

**IMPORTANT**: this applies to full account name, so ``a.near`` could belong to
 shard 0, while ``z.a.near`` to shard 3.

Currently in mainnet & testnet, we use the fixed shard split (you can look at
 ``get_simple_nightshade_layout``):

``vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]``



## Shard layout changes

Theoretically shard layout can change on epoch boundary. But currently we don't
support this in code (the shard_layout is read from the genesis file, and
simply applied to all the epochs).

**Discussion point**: how should we decide (and propagate) shard layout
changes? Dynamic config? some separate message? how do validators vote? can we
do it more often than on new releases?


### Deeper technical details

It all starts in ``preprocess_block`` - if the node sees, that the block it is
about to preprocess is the first block of the epoch (X+1)  - it calls
``get_state_sync_info``, which is responsible for figuring out which shards will 
be needed in next epoch (X+2).

This is the moment, when node can request new shards that it didn't track before (using StateSync) - and if it detects that the shard layout would change in the next epoch, it also involves the StateSync - but skips the download part (as it already has the data) - and starts from state splitting.

StateSync in this phase would send the ``StateSplitRequest`` to the ``SyncJobsActor`` (you can think about the ``SyncJobsActor`` as a background thread).

We'd use the background thread to do the state splitting: the goal is to change the one trie (that represents the state of the current shard) - to multiple tries (one for each of the new shards).

Currently we re-use some of the code that we have for state sync (for example to figure out the number of parts) - and the we take all the items for a given part, which a list of trie **items** (key-value pairs that are stored in the trie - NOT trie nodes) - and for each one, we try to extract the account id that this key belongs to.

**TODO**: It seems that the code was written with the idea of persisting the "progress" (that's why have parts etc) - but AFAIK currently we never persist the progress of the resharding - so if the node restarts, we're starting from scratch.


Extracting of the account from the key happens in ``parse_account_id_from_raw_key`` - and we do it for all types of data that we store in the trie (contract code, keys, account info etc) EXCEPT for Delayed receipts. Then, we figure out the shard that this account is going to belong to, and we add this key/value to that new trie.

This way, after going over all the key/values from the original trie, we end up with X new tries (one for each new shard).


IMPORTANT: in the current code, we only support such 'splitting' (so a new shard can have just one parent).

**Discussion point**: When should we start supporting the 'merge' operation ? And when the combination (split & merge)?


### Why delayed receipts are special?
For all the other columns, there is no dependency between entries, but in case of delayed receipts - we are forming a 'queue'. We store the information about the first index and the last index (in DelayedReceiptIndices struct).

Then, we receipt arrives, we add it as the 'DELAYED_RECEIPT + last_index' key (and increment last_index by 1).

That's why we cannot move it column 'in trie parts' (like we did for others), but we do it by 'iterating' over this queue.



## What should we improve to achieve 'stressless' resharding?

Currently trie is split sequencially (shard by shard), and also sequancially within a shard - by iterating over all the elements. 

This process must finish within a single epoch - which is might be challenging (especially for larger archival nodes).

### Each shard should be handled independently
This is a low-hanging fruit - but each shard's splitting should be done in a separate thread.


### Do trie splitting on the level of accounts, not key/values

Most of the keys in our tries, are in the form of ``${some short prefix}${account_id}${long suffix}``, we could use this fact, and move the whole 'trie subtrees' around.

In theory, this should be super-fast - as the hash of such tree doesn't change (so all the trie nodes below have the same hashes etc).

Unfortunately we add the ShardUid prefix to the trie keys, when storing them in database - which means, that we'd have to iterate over all of them to copy the values to a new row (with a new sharduid) - which kills all the benefits.

#### should we remove the sharduid prefix?

one advantage of sharduid, is that it allows us to quickly remove the contents of the shard (which we do when we're about to do statesync - as we want to start with a clear slate).

#### Or should we replace it with 'account' sharduid? 

Another alternative, is to use the account hash as a prefix for the trie nodes instead. This allow relatively quick shard-level data removal (assuming couple million accounts per shard), and it means that we could do the resharding a lot more efficiently.

We could also do something special for the 'simple' (zero-cost) accounts - the ones that just have a few keys added and don't have any other state (if we assume that we're going to get a lot of them).


### Add support for (somehow) allowing to change resharding on each epoch boundary
Add an option in the config (maybe + some simple voting??) - so that we can easily change the shard layout.

### Allow shard merging (and merging/splitting)
Currently shards can only be split. We should allow also merges - so that we can achieve more flexibility.
