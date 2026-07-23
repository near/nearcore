# Plan: Sharding & chunk lifecycle

Generation brief for `spec/sharding-chunks.md`. Follow `../CONVENTIONS.md`.

## Scope
Shards and the chunk lifecycle: shard layouts, chunk production for an assigned shard,
Reed-Solomon erasure encoding into parts, distribution of partial encoded chunks via
the `ShardsManager`, reconstruction, and resharding (shard-layout change + state
migration) at epoch boundaries.

## Out of scope
- Chunk *validation* via witnesses → [stateless-validation](../spec/stateless-validation.md).
- The state transition a chunk applies → [runtime-execution](../spec/runtime-execution.md).
- Trie/flat-storage migration internals → [state-storage](../spec/state-storage.md)
  (resharding orchestration lives here; the storage mechanics there).

## Code to read
- `core/primitives/src/shard_layout.rs` — `ShardLayout`, shard ids, account→shard mapping.
- `chain/chunks/src/shards_manager_actor.rs` — chunk request/response, part tracking,
  forwarding, reconstruction.
- `chain/chunks/src/{chunk_cache,logic,client}.rs`.
- `chain/client/src/chunk_producer.rs` — chunk creation, encoding, part assignment.
- `chain/chain/src/sharding.rs` — shard/chunk helpers.
- Resharding: `chain/chain/src/resharding/{manager,resharding_actor,event_type,types}.rs`.
- `docs/architecture/how/dynamic_resharding.md`, `resharding_v2.md` (READ FIRST per
  repo guidance; cross-check).

## Questions the spec must answer
- How is an account mapped to a shard; how does `ShardLayout` define boundaries?
- How is a chunk produced for a shard: what inputs, how is it encoded into data+parity
  parts, and how are parts assigned to validators?
- How are partial encoded chunks distributed and how/when is a chunk reconstructed?
- What happens when a chunk is missing (skipped chunk semantics)?
- Dynamic resharding: what triggers it, how do old/new shards run during the transition,
  and at what point does the new layout take effect?

## Cross-component edges
- Producer assignment ← epoch. Parts move over networking. Reconstructed chunks feed
  block processing and stateless validation. Resharding state work →
  [state-storage](../spec/state-storage.md).

## Relevant ProtocolFeatures
- `DynamicResharding`, `StickyReshardingValidatorAssignment`, shard-layout version
  bumps. Verify against `version.rs`.
