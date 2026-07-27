# Plan: Data structures & serialization

Generation brief for `spec/data-structures-serialization.md`. Follow `../CONVENTIONS.md`.

## Scope
The core wire/storage data structures shared across the protocol and how they are
serialized. Focus on shape, versioning (V0/V1/… enums), and hashing — not on the
algorithms that consume them.

## Out of scope
- Behavior of components that produce/consume these types (link to them).
- Account/AccessKey *semantics* → [accounts-keys](../spec/accounts-keys.md) (but the
  struct shape can be summarized here).

## Code to read
- `core/primitives/src/block.rs`, `block_header.rs`, `block_body.rs` — Block, header
  versions, approvals embedded.
- `core/primitives/src/sharding.rs` — ShardChunk, ChunkHeader versions, encoded chunks.
- `core/primitives/src/transaction.rs` — Transaction V0/V1, SignedTransaction,
  ExecutionOutcome.
- `core/primitives/src/receipt.rs` — Receipt, ReceiptEnum (Action/Data), versioned
  storage (`StateStoredReceipt`).
- `core/primitives-core/src/account.rs` — Account, AccessKey (shape only).
- `core/primitives/src/state.rs`, `core/primitives-core/src/hash.rs` — StateRoot,
  CryptoHash.
- `docs/architecture/how/serialization.md`, `docs/architecture/how/proofs.md` (cross-check).
- Borsh derives across the above; `chain/network/src/network_protocol/network.proto`
  for proto-serialized network types.

## Questions the spec must answer
- What is the structure of a Block (header + body) and how are the versions laid out?
- How are chunks represented (full vs encoded/partial), and what does a ChunkHeader commit to?
- Transaction vs Receipt vs ExecutionOutcome — fields and relationships.
- Which serialization format is used where (Borsh for storage/consensus hashing,
  Proto for network, JSON for RPC)? Why does the distinction matter for hashing?
- How are IDs/hashes derived (block hash, chunk hash, receipt id, tx hash)?
- How does versioning enable forward/backward compatibility?

## Cross-component edges
- Consumed by nearly everything; link out rather than describe behavior.

## Relevant ProtocolFeatures
- Any feature introducing a new struct version (e.g. block/header/chunk/receipt/account
  version bumps). Check `version.rs` for `*V2`/`*V3`-style additions.
