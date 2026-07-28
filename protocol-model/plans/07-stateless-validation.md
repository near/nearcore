# Plan: Stateless validation

Generation brief for `spec/stateless-validation.md`. Follow `../CONVENTIONS.md`.

## Scope
NEP-509 stateless validation: how a chunk producer builds a state witness, how it is
(partially) distributed to chunk validators, how validators re-execute the chunk
against the witness without holding full state, and how chunk endorsements are
produced and aggregated.

## Out of scope
- The state transition itself → [runtime-execution](../spec/runtime-execution.md).
- Trie proof/`PartialStorage` mechanics → [state-storage](../spec/state-storage.md)
  (cross-link; this spec covers the witness *protocol*, that one the proof structure).
- Chunk encoding/distribution → [sharding-chunks](../spec/sharding-chunks.md).

## Code to read
- `chain/client/src/stateless_validation/` — `chunk_validation_actor.rs`,
  `validate.rs`, `state_witness_producer.rs`, `partial_witness/`, `chunk_validator/`.
- `chain/chain/src/stateless_validation/` — `chunk_validation.rs`,
  `chunk_endorsement.rs`, `state_witness.rs`.
- `core/primitives/src/stateless_validation/` — witness, endorsement, partial-witness
  types.
- `docs/misc/state_witness_size_limits.md`, `docs/misc/contract_distribution.md`
  (cross-check).
- Witness size/limits in `core/parameters` (witness_config).

## Questions the spec must answer
- What is in a `ChunkStateWitness` (main transition, implicit transitions, source
  receipts proof, new transactions, contract code)?
- How is the witness produced after applying a chunk, and how is it encoded/split into
  parts for distribution (partial witness, erasure coding)?
- Who validates a chunk (the chunk-validator assignment), and what does validation do
  step by step (re-apply against witness, compare state root/outgoing receipts)?
- How are chunk endorsements created, signed, gossiped, and aggregated; how many are
  needed for a chunk to be included?
- Size limits and contract-distribution optimization (avoid shipping code in every
  witness; `SignedContractCodeResponse`).
- Failure modes: invalid witness, validation mismatch, missing endorsements.

## Cross-component edges
- Chunk validator assignment ← epoch. Witnesses/endorsements move over networking
  (often T1). Endorsements gate chunk inclusion in block processing. Re-execution uses
  the runtime against a partial trie from state-storage.

## Relevant ProtocolFeatures
- `StatelessValidation`/NEP-509 family, `SignedContractCodeResponse`,
  witness-size/limit features, `ExecutionMetadataV4`. Verify against `version.rs`.
