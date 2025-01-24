# Trie

We use Merkle-Patricia Trie to store blockchain state. Trie is persistent, which
means that insertion of new node actually leads to creation of a new path to this
node, and thus root of Trie after insertion will also be represented by a new
object.

Here we describe its implementation details which are closely related to
Runtime.

## Main structures

### Trie

Trie stores the state - accounts, contract codes, access keys, etc. Each state
item corresponds to the unique trie key. You can read more about this structure on
[Wikipedia](https://en.wikipedia.org/wiki/Trie).

There are two ways to access trie - from memory and from disk. The first one is
currently the main one, where only the loading stage requires disk, and the
operations are fully done in memory. The latter one relies only on disk with
several layers of caching. Here we describe the disk trie.

Disk trie is stored in the RocksDB, which is persistent across node restarts. Trie
communicates with database using `TrieStorage`. On the database level, data is
stored in key-value format in `DBCol::State` column. There are two kinds of
records:

* trie nodes, for the which key is constructed from shard id and
  `RawTrieNodeWithSize` hash, and value is a `RawTrieNodeWithSize` serialized by
  a custom algorithm;
* values (encoded contract codes, postponed receipts, etc.), for which the key is
  constructed from shard id and the hash of value, which maps to the encoded value.

So, value can be obtained from `TrieKey` as follows:

* start from the hash of `RawTrieNodeWithSize` corresponding to the root;
* descend to the needed node using nibbles from `TrieKey`;
* extract underlying `RawTrieNode`;
* if it is a `Leaf` or `Branch`, it should contain the hash of the value;
* get value from storage by its hash and shard id.

Note that `Trie` is almost never called directly from `Runtime`, modifications
are made using `TrieUpdate`.

### TrieUpdate

Provides a way to access storage and record changes to commit in the future.
Update is prepared as follows:

* changes are made using `set` and `remove` methods, which are added to
  `prospective` field,
* call `commit` method which moves `prospective` changes to `committed`,
* call `finalize` method which prepares `TrieChanges` and state changes based on
  `committed` field.

Prospective changes correspond to intermediate state updates, which can be
discarded if the transaction is considered invalid (because of insufficient
balance, invalidity, etc.). While they can't be applied yet, they must be cached
this way if the updated keys are accessed again in the same transaction.

Committed changes are stored in memory across transactions and receipts.
Similarly, they must be cached if the updated keys are accessed across
transactions. They can be discarded only if the chunk is discarded.

Note that `finalize`, `Trie::insert` and `Trie::update` do not update the
database storage. These functions only modify trie nodes in memory. Instead,
these functions prepare the `TrieChanges` object, and `Trie` is actually updated
when `ShardTries::apply_insertions` is called, which puts new values to
`DBCol::State` part of the key-value database.

### TrieStorage

Stores all `Trie` nodes and allows to get serialized nodes by `TrieKey` hash
using the `retrieve_raw_bytes` method.

There are two major implementations of `TrieStorage`:

* `TrieCachingStorage` - caches all big values ever read by `retrieve_raw_bytes`.
* `TrieMemoryPartialStorage` - used for validating recorded partial storage.

Note that these storages use database keys, which are retrieved using hashes of
trie nodes using the `get_key_from_shard_id_and_hash` method.

### ShardTries

This is the main struct that is used to access all Tries. There's usually only a single instance of this and it contains stores and caches. We use this to gain access to the `Trie` for a single shard by calling the `get_trie_for_shard` or equivalent methods.

Each shard within `ShardTries` has their own `cache` and `view_cache`. The `cache` stores the most frequently accessed nodes and is usually used during block production. The `view_cache` is used to serve user request to get data, which usually come in via network. It is a good idea to have an independent cache for this as we can have patterns in accessing user data independent of block production.

## Primitives

### TrieChanges

Stores result of updating `Trie`.

* `old_root`: root before updating `Trie`, i.e. inserting new nodes and deleting
  old ones,
* `new_root`: root after updating `Trie`,
* `insertions`, `deletions`: vectors of `TrieRefcountChange`, describing all
  inserted and deleted nodes.

This way to update trie allows to add new nodes to storage and remove old ones
separately. The former corresponds to saving new block, the latter - to garbage
collection of old block data which is no longer needed.

### TrieRefcountChange

Because we remove unused nodes during garbage collection, we need to track
the reference count (`rc`) for each node. Another reason is that we can dedup
values. If the same contract is deployed 1000 times, we only store one contract
binary in storage and track its count.

This structure is used to update `rc` in the database:

* `trie_node_or_value_hash` - hash of the trie node or value, used for uniting
  with shard id to get DB key,
* `trie_node_or_value` - serialized trie node or value,
* `rc` - change of reference count.

Note that for all reference-counted records, the actual value stored in DB is
the concatenation of `trie_node_or_value` and `rc`. The reference count is
updated using a custom merge operation `refcount_merge`.
