# Primitives

## TrieKey

Describes all keys which may be inserted to storage:

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
* `PromiseYieldIndices`
* `PromiseYieldTimeout`
* `PromiseYieldReceipt`
* `BufferedReceiptIndices`
* `BufferedReceipt`
* `BandwidthSchedulerState`

Each key is uniquely converted to `Vec<u8>`. Internally, each such vector is
converted to `NibbleSlice` (nibble is a half of a byte), and each its item
corresponds to one step down in trie.

## ValueRef

```
ValueRef {
    length: u32,
    hash: CryptoHash,
}
```

Reference to value corresponding to trie key (Account, AccessKey, ContractData...).
Contains value length and value hash. The full value must be read from disk from
`DBCol::State`, where DB key is the `ValueRef::hash`.

It is needed because reading long values has a significant latency. Therefore for
such values we read `ValueRef` first, charge user for latency needed to read value
of length which is known now, and then read the full value.

## OptimizedValueRef

However, majority of values is short, and it would be a waste of resources to
always spend two reads on them. That's why the read result is `OptimizedValueRef`,
which contains

* `AvailableValue` if value length is <= `INLINE_DISK_VALUE_THRESHOLD` which
is 4000;
* `Ref` which is `ValueRef` otherwise.

## get_optimized_ref

Finally, the main method to serve reads is

`Trie::get_optimized_ref(key: &[u8], mode: KeyLookupMode) -> Option<OptimizedValueRef>`.

Key is encoded trie key. For example, `TrieKey::Account { account_id }` to read
account info.

`Option<OptimizedValueRef>` is a reference for value, which we discussed before.

`mode` is a costs mode used for serving the read.

To get actual value, contract runtime calls `Trie::deref_optimized(optimized_value_ref: &OptimizedValueRef)`
which either makes a full value read or returns already stored value.

## KeyLookupMode

Cost mode used to serve the read.

* `KeyLookupMode::Trie` - user must pay for every trie node read needed to find the key.
* `KeyLookupMode::FlatStorage` - user must pay only for dereferencing the value.

It is based on the need to write new nodes to storage. For read queries it is
enough to get the value; for write queries we also have to update the value and
hashes of all the nodes on the path from root to the leaf corresponding to key.

However, this way to distinguish reads and writes is outdated, because in the
stateless validation world, chunk producers have to read nodes anyway to generate
a proof for chunk validators. But we still maintain it because it helps RPC nodes
which don't generate proofs. Also, getting a cost for `Trie` mode is *itself* a
challenging task because node has to keep reading nodes until it reaches the leaf.

## lookup

Family of functions accepting `key: &[u8], side_effects: bool`. They find node
corresponding to the key in trie and return a value which is later converted
to `Option<OptimizedValueRef>`. Implementation depends on the main storage source:

* `lookup_from_memory`
* `lookup_from_flat_storage`
* `lookup_from_state_column`

Expected side effects are

* changing accessed node counters so that contract runtime can charge caller based on them,
* recording accessed nodes for a proof for chunk validators.
