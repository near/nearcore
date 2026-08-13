# Plan: State & storage

Generation brief for `spec/state-storage.md`. Follow `../CONVENTIONS.md`.

## Scope
How state is represented and persisted: the Merkle-Patricia trie (nodes, keys, state
root), flat storage as a fast read path, `TrieUpdate` batching, state parts and
snapshots for sync/resharding, partial storage (proofs) for stateless validation, the
RocksDB column layout, refcounting, and hot/cold (archival) splitting.

## Out of scope
- The witness *protocol* → [stateless-validation](../spec/stateless-validation.md)
  (proof/`PartialStorage` structure covered here; its use there).
- The sync *protocol* → [sync](../spec/sync.md) (state-part format here).
- Resharding orchestration → [sharding-chunks](../spec/sharding-chunks.md) (the
  trie/flat split mechanics covered here).

## Code to read
- `core/store/src/trie/mod.rs` — `Trie`, node types, `TrieChanges`, `PartialStorage`,
  lookup modes.
- `core/store/src/trie/{trie_storage,trie_storage_update,raw_node,iterator}.rs`,
  `core/store/src/trie/ops/`, `core/store/src/trie/mem/`.
- `core/store/src/trie/{state_parts,state_snapshot,split}.rs`.
- `core/store/src/flat/{mod,storage,manager,delta,chunk_view}.rs`.
- `core/store/src/db/{rocksdb,splitdb,colddb,mixeddb,refcount}.rs`,
  `core/store/src/columns.rs`.
- `core/primitives/src/trie_key.rs` (TrieKey layout), `core/primitives/src/state.rs`.
- `docs/architecture/storage/*` (flow, trie_storage, database, flat_storage; cross-check).

## Questions the spec must answer
- How is the trie structured (branch/leaf/extension), how is a node hashed, and what is
  the state root?
- How are state keys laid out (`TrieKey` for account, contract code/data, access key,
  delayed/buffered receipts, etc.)?
- What is flat storage, what does it map, how are per-block deltas maintained, and when
  is it used vs falling back to the trie?
- How are state changes applied (`TrieUpdate`) and committed (`TrieChanges` → DB), and
  how does refcounting drive GC of trie nodes?
- How are state parts produced/validated for sync, and what is a state snapshot?
- What is `PartialStorage` and how does it serve as a proof for stateless validation?
- RocksDB column families: the important columns and what they hold; hot/cold split.

## Cross-component edges
- Read/written by [runtime-execution](../spec/runtime-execution.md) and the VM host
  functions; parts feed [sync](../spec/sync.md); proofs feed
  [stateless-validation](../spec/stateless-validation.md); split feeds resharding.

## Relevant ProtocolFeatures
- Flat storage (NEP-399), in-memory trie, state-part/witness-related features. Verify
  against `version.rs`.
