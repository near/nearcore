# Trie

We use Merkle-Patricia Trie to store blockchain state.
Trie is persistent, which means that insertion of new node actually leads to creation of new path to this node, and thus root of Trie after insertion will also be presented by new object.

Here we describe its implementation details which are closely related to Runtime.

## Main structures

### Trie

Base structure. Almost never called directly from `Runtime`, modifications are made using `TrieUpdate`.

### TrieUpdate

Provides a way to access Storage and record changes to commit in the future. Update is prepared as follows:

- changes are made using `set` and `remove` methods, which are added to `prospective` field,
- call `commit` method which moves `prospective` changes to `committed`,
- call `finalize` method which prepares `TrieChanges` and state changes based on `committed` field.

Note that `finalize`, `Trie::insert` and `Trie::update` do not update the database storage. 
These functions only modify trie nodes in memory.
Instead, these functions prepares `TrieChanges` object, and `Trie` is actually updated when `ShardTries::apply_insertions` is called, which puts new values to `ColState` part of key-value database.

### TrieStorage

Stores all `Trie` nodes and allows to get serialized nodes by `TrieKey` hash using `retrieve_raw_bytes` method.

There are three implementations of `TrieStorage`:
- `TrieCachingStorage` - caches big values ever read by `retrieve_raw_bytes`.
- `TrieRecordingStorage` - records all key-value pairs ever read by `retrieve_raw_bytes`. Used for obtaining state parts (and challenges in the future).
- `TrieMemoryPartialStorage` - used for validating recorded partial storage.

Note that these storages use database keys, which are retrieved using hashes of trie keys using `get_key_from_shard_id_and_hash` method.

### ShardTries

Contains stores and caches and allows to get `Trie` object for any shard.

## Primitives

### TrieKey

Describes all keys which may be inserted to `Trie`:

- `Account`
- `ContractCode`
- `AccessKey`
- `ReceivedData`
- `PostponedReceiptId`
- `PendingDataCount`
- `PostponedReceipt`
- `DelayedReceiptIndices`
- `DelayedReceipt`
- `ContractData`

Each key is uniquely converted to `Vec<u8>`. Internally, each such vector is converted to `NibbleSlice` (nibble is a half of a byte), and each its item corresponds to one step down in `Trie`.

### TrieChanges

Stores result of updating `Trie`. 

- `old_root`: root before updating `Trie`, i.e. inserting new nodes and deleting old ones,
- `new_root`: root after updating `Trie`,
- `insertions`, `deletions`: vectors of `TrieRefcountChange`, describing all inserted and deleted nodes.

### TrieRefcountChange

Because we remove unused nodes using garbage collector, we need to track reference count (`rc`) for each node. 
This structure is used to update `rc` in the database:

- `key_hash` - hash of `TrieKey`,
- `value` - value corresponding to trie key, e.g. contract code,
- `rc`.

