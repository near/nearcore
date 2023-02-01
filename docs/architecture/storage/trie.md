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
item corresponds to the unique trie key. All types of trie keys are described in
the [TrieKey](#triekey) section. You can read more about this structure on
[Wikipedia](https://en.wikipedia.org/wiki/Trie).

Trie is stored in the RocksDB, which is persistent across node restarts. Trie
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

Note that `finalize`, `Trie::insert` and `Trie::update` do not update the
database storage. These functions only modify trie nodes in memory. Instead,
these functions prepare the `TrieChanges` object, and `Trie` is actually updated
when `ShardTries::apply_insertions` is called, which puts new values to
`DBCol::State` part of the key-value database.

### TrieStorage

Stores all `Trie` nodes and allows to get serialized nodes by `TrieKey` hash
using the `retrieve_raw_bytes` method.

There are three implementations of `TrieStorage`:

* `TrieCachingStorage` - caches all big values ever read by `retrieve_raw_bytes`.
* `TrieRecordingStorage` - records all key-value pairs ever read by
  `retrieve_raw_bytes`. Used for obtaining state parts (and challenges in the
  future).
* `TrieMemoryPartialStorage` - used for validating recorded partial storage.

Note that these storages use database keys, which are retrieved using hashes of
trie nodes using the `get_key_from_shard_id_and_hash` method.

### ShardTries

Contains stores and caches and allows to get `Trie` object for any shard.

## Primitives

### TrieKey

Describes all keys which may be inserted to `Trie`:

* `Account`
* `ContractCode`
* `AccessKey`
* `ReceivedData`
* `PostponedReceiptId`
* `PendingDataCount`
* `PostponedReceipt`
* `DelayedReceiptIndices`
* `DelayedReceipt`
* `ContractData`

Each key is uniquely converted to `Vec<u8>`. Internally, each such vector is
converted to `NibbleSlice` (nibble is a half of a byte), and each its item
corresponds to one step down in `Trie`.

### TrieChanges

Stores result of updating `Trie`.

* `old_root`: root before updating `Trie`, i.e. inserting new nodes and deleting
  old ones,
* `new_root`: root after updating `Trie`,
* `insertions`, `deletions`: vectors of `TrieRefcountChange`, describing all
  inserted and deleted nodes.

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
updated using a custom merge operation `merge_refcounted_records`.
