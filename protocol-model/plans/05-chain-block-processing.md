# Plan: Chain & block-processing pipeline

Generation brief for `spec/chain-block-processing.md`. Follow `../CONVENTIONS.md`.

## Scope
The block lifecycle inside a node: receiving a block, validating it, applying chunks,
the `ChainUpdate` transaction, fork choice / head update, orphan handling, and garbage
collection. This is the orchestration layer that ties consensus, sharding, and runtime
together.

## Out of scope
- Doomslug rule itself → [consensus-finality](../spec/consensus-finality.md).
- Chunk apply internals → [runtime-execution](../spec/runtime-execution.md).
- Chunk witness validation → [stateless-validation](../spec/stateless-validation.md).
- Sync/catchup → [sync](../spec/sync.md).

## Code to read
- `chain/chain/src/chain.rs` — `Chain`, `process_block`, block acceptance pipeline,
  preprocessing, postprocessing, `apply_chunks`.
- `chain/chain/src/chain_update.rs` — `ChainUpdate`, atomic state commit, head update.
- `chain/chain/src/validate.rs` — block/header validation rules.
- `chain/chain/src/orphan.rs` — orphan pool and resolution.
- `chain/chain/src/store/` — `ChainStore`, `ChainStoreUpdate`.
- GC: `chain/chain/src/garbage_collection.rs` (or search `gc`); `docs/architecture/how/gc.md`.
- `chain/client/src/client.rs` and `client_actor.rs` for how processing is driven.

## Questions the spec must answer
- End-to-end: what are the ordered phases from "block received" to "head updated"?
  (preprocess → apply chunks → postprocess → ChainUpdate commit.)
- What validations gate acceptance, and which run before vs after chunk application?
- How are missing chunks / not-yet-available parents handled (orphans, blocks with
  missing chunks)?
- How is the canonical head chosen on forks? What is reverted on a reorg?
- What does `ChainUpdate` commit atomically, and what is the relationship to
  `ChainStoreUpdate`?
- How and when does garbage collection prune old data; what is kept (archival)?

## Cross-component edges
- Pulls producer/epoch info, Doomslug finality, chunk validation, and runtime apply.
  Persists via [state-storage](../spec/state-storage.md). Link heavily.

## Relevant ProtocolFeatures
- Block ordinal / epoch-sync-data-hash validation, optimistic block. Check `version.rs`.
