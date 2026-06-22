# Stateless validation

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `core/primitives/src/stateless_validation/`, `chain/chain/src/stateless_validation/`, `chain/client/src/stateless_validation/`

## Role

Stateless validation (NEP-509) is how the protocol verifies a chunk's state transition without every validator holding the full shard state. After a chunk producer applies a chunk, it packages the *recorded* trie reads (a Merkle proof) plus the transactions and source receipts into a **`ChunkStateWitness`**. The witness is compressed, Reed-Solomon erasure-coded, and distributed part-by-part to the chunk's assigned **chunk validators**. Each validator reconstructs the witness, re-applies the chunk against the partial trie (no full state needed), checks the resulting state root / outgoing-receipts root against the chunk header, and emits a signed **`ChunkEndorsement`**. A chunk may be included in a block only once endorsements covering > 2/3 of the chunk-validator stake are collected. This component sits downstream of [sharding-chunks](sharding-chunks.md) (which produces the chunk) and [runtime-execution](runtime-execution.md) (which is replayed during validation against a partial trie from [state-storage](state-storage.md)); chunk-validator assignment comes from [epoch-validators-staking](epoch-validators-staking.md); witnesses/endorsements travel over [networking-p2p](networking-p2p.md); endorsements gate chunk inclusion in [chain-block-processing](chain-block-processing.md).

## Key data structures

- **`ChunkStateWitness`** — `core/primitives/src/stateless_validation/state_witness.rs:96` — versioned enum, only live variant `V2(Box<ChunkStateWitnessV2>) = 1` (V1 deprecated). `ChunkStateWitnessV2` (`:104`) fields: `epoch_id`; `chunk_header` (the `ShardChunkHeader` being attested, kept so the validator knows what it endorses); `main_state_transition: ChunkStateTransition` (the new-chunk transition of the *last new chunk* of this shard); `source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>` (a non-strict superset of the receipts to apply, with proofs against outgoing-receipt roots); `applied_receipts_hash` (redundant hash of the ordered `Vec<Receipt>` to apply, for debugging); `transactions` (the transactions the last new chunk converted to receipts); `implicit_transitions: Vec<ChunkStateTransition>` (one per missing chunk between the last new chunk and this chunk, forward chronological); `new_transactions` (transactions in *this* chunk).
- **`ChunkStateTransition`** — `core/primitives/src/stateless_validation/state_witness.rs:282` — `block_hash`, `base_state: PartialState` (recorded trie nodes = the proof), `post_state_root` (redundant; the real check is re-deriving it). See [state-storage](state-storage.md) for `PartialState`/proof mechanics.
- **`EncodedChunkStateWitness`** — `core/primitives/src/stateless_validation/state_witness.rs:37` — zstd-compressed borsh of the witness. Raw cap `MAX_UNCOMPRESSED_STATE_WITNESS_SIZE = 64 MiB` (`:19`, 512 MiB under `test_features`); compression level 1 (`:21`).
- **`PartialEncodedStateWitness`** / **`VersionedPartialEncodedStateWitness`** — `core/primitives/src/stateless_validation/partial_witness.rs:26` / `:254` — one Reed-Solomon part: `epoch_id`, `shard_id`, `height_created`, `part_ord`, `part: Box<[u8]>`, `encoded_length`, a `signature_differentiator`, and the chunk-producer signature. `VersionedPartialEncodedStateWitness` is `V1 = 0` / `V2 = 1`; at v86 only V1 is emitted/accepted — `new` ignores `protocol_version` and always returns V1 (`:269`), and handlers drop V2 (V2 wiring is inert, gated for a future `EarlyKickout` rollout). Compressed cap `MAX_COMPRESSED_STATE_WITNESS_SIZE = 48 MiB` (`:18`).
- **`ChunkEndorsement`** — `core/primitives/src/stateless_validation/chunk_endorsement.rs:17` — versioned enum, live variant `V2(ChunkEndorsementV2) = 1`. `ChunkEndorsementV2` (`:100`) has `inner: ChunkEndorsementInnerV1` (the `chunk_hash` + `"ChunkEndorsement"` differentiator that is signed into the block header), `signature` over `inner`, `metadata: ChunkEndorsementMetadata` (`account_id`, `shard_id`, `epoch_id`, `height_created`), and `metadata_signature` over the metadata. The `inner`+`signature` are what land in the block header; the metadata pair is used only for validation routing.
- **`ChunkValidatorAssignments`** — `core/primitives/src/stateless_validation/validator_assignment.rs:33` — ordered `Vec<(AccountId, Balance)>` of validators + their stake for one chunk. `compute_endorsement_state` (`:60`) walks the ordered list, sums `total_stake` and `endorsed_stake` for accounts that supplied a signature, and sets `is_endorsed` via `has_enough_stake` = `endorsed_stake >= total_stake*2/3 + 1` (`:18`, `:22`). If not endorsed the signature vector is cleared.
- **`ChunkProductionKey`** — `core/primitives/src/stateless_validation/mod.rs:19` — `(shard_id, epoch_id, height_created)`; the routing key for every witness/endorsement/contract message.
- **`StoredChunkStateTransitionData`** — `core/primitives/src/stateless_validation/stored_chunk_state_transition_data.rs:12` — persisted per chunk (incl. missing chunks) in `DBCol::StateTransitionData`: `base_state` (the recorded proof), `receipts_hash`, `contract_accesses`, `contract_deploys`. This is the producer-side source from which a witness is later assembled.
- **Contract-distribution types** — `core/primitives/src/stateless_validation/contract_distribution.rs` — `ContractUpdates` (`:462`: `contract_accesses: HashSet<CodeHash>`, `contract_deploys: Vec<ContractCode>`), `ChunkContractAccesses` (`:30`, signed code-hash list), `ContractCodeRequest` (`:136`), `ContractCodeResponse` (`:248`, `V1` unsigned / `V2` signed), `ChunkContractDeploys` + `PartialEncodedContractDeploys` (`:479`/`:502`, erasure-coded deploy distribution). `MAX_CONTRACTS_PER_REQUEST = 1282` (`:20`).

## Behavior

### 1. Witness production (chunk producer, after applying its chunk)

`Client::send_chunk_state_witness_to_chunk_validators` (`chain/client/src/stateless_validation/state_witness_producer.rs:15`) drives production for a newly produced chunk:

1. Early-return if `ProtocolFeature::Spice` is enabled (`:23`) — SPICE has its own path.
2. Build the witness via `ChainStore::create_state_witness` (`chain/chain/src/stateless_validation/state_witness.rs:45`).
3. If `save_latest_witnesses` is set, persist it for debugging (`:54`).
4. **Self-endorse shortcut**: if the producer is itself in the chunk-validator set, it endorses immediately (bypassing validation, since it just applied the chunk) by calling `send_chunk_endorsement_to_block_producers` and feeding the result to its own endorsement tracker (`:57`–`:72`).
5. Send a `DistributeStateWitnessRequest` to the `PartialWitnessActor` (`:74`).

`create_state_witness` (`state_witness.rs:45`) assembles the witness:
- `collect_state_transition_data` (`:100`) walks backwards from the chunk's prev block to the block at `prev_chunk_header.height_included`, reading each block's stored `StateTransitionData` as an *implicit transition* (one per missing chunk, plus an extra one when a resharding shard-id change happens). The transitions are collected newest-first then `reverse()`d to forward order (`:165`). The **main transition** is the last new chunk's transition (`:168`), read via `get_state_transition` (`:194`) from `DBCol::StateTransitionData` keyed by `(block_hash, shard_id)`, with `post_state_root` taken from that block's `ChunkExtra`.
- `collect_source_receipt_proofs` (`:264`) gathers all incoming receipts for the *previous* chunk and their proofs (so the witness self-proves which receipts to apply), keyed by source `ChunkHash`. Genesis prev-chunk yields an empty map.
- The witness carries `prev_chunk.to_transactions()` as `transactions` and the current `chunk.to_transactions()` as `new_transactions` (`:89`, `:91`).

### 2. Encoding and distribution (`PartialWitnessActor`)

`handle_distribute_state_witness_request` (`chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:205`) sends three streams in priority order (`:225`–`:277`):
1. **Contract accesses first** (if any contracts were called): `send_contract_accesses_to_chunk_validators` (`:727`) sends a signed `ChunkContractAccesses` (code-hashes only, not code) to chunk validators *except* chunk producers tracking the same shard, so validators can request missing code while witness parts are still in flight.
2. **Witness parts** (on `witness_creation_spawner` to avoid blocking the actor, `:260`): `compress_and_distribute_witness` (`:280`) compresses (`compress_witness`, `:1060`), then `send_state_witness_parts` (`:344`) → `generate_state_witness_parts` (`:1016`) erasure-codes the compressed bytes into **one part per chunk validator** using a Reed-Solomon encoder sized to `chunk_validators.len()` with `WITNESS_RATIO_DATA_PARTS = 0.6` (`partial_witness/encoding.rs:6`). Part `i` is signed and addressed to chunk validator `i` (the part owner); sent as `NetworkRequests::PartialEncodedStateWitness`.
3. **Contract deploys** (lower priority): `send_chunk_contract_deploys_parts` (`:762`) compresses newly-deployed code and erasure-codes it to non-producer validators, who will need it in later turns.

### 3. Part forwarding and reconstruction (chunk validator)

- A part owner receives its part via `handle_partial_encoded_state_witness` (`:400`). It validates the part (`validate_partial_encoded_state_witness`, `validate.rs:64`), then **forwards** it to all other chunk validators (`NetworkRequests::PartialEncodedStateWitnessForward`, `:464`) and stores its own copy (`:472`). V2 parts are dropped (`:415`).
- Forwarded parts arrive at `handle_partial_encoded_state_witness_forward` (`:499`), are validated and stored (`:531`–`:540`).
- `PartialEncodedStateWitnessTracker` (`partial_witness/partial_witness_tracker.rs:345`) accumulates parts per `ChunkProductionKey` in a per-shard LRU `CacheEntry`. When `ReedSolomonPartsTracker` collects enough data parts it decodes (`process_witness_part` → `InsertPartResult::Decoded`, `:210`). `try_finalize` (`:273`) waits for **both** the witness parts (decoded) and the accessed-contract state to be ready; `AccessedContractsState::Unknown` counts as ready (chunk may have no contracts, or the accesses message was lost — best-effort). On finalize, the decoded witness's main-transition `base_state` is **augmented** with the separately-fetched contract codes (`:493`–`:495`), and the full `ChunkStateWitness` is sent to the `ChunkValidationActor` as a `ChunkStateWitnessMessage` (`:509`).

### 4. Validation (`ChunkValidationActor`)

`process_chunk_state_witness_message` (`chunk_validation_actor.rs:513`):
1. Reject if not a validator (`:520`); send a `ChunkStateWitnessAck` back to the chunk producer for RTT metrics (`:530`).
2. If the witness's `prev_block_hash` is not yet known, park it as an **orphan** in `OrphanStateWitnessPool` (only if within `ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD = 2..6` of head and under `max_orphan_witness_size`, `:239`/`:253`); it is reprocessed when the block arrives via `process_ready_orphan_witnesses` (`:275`).
3. Otherwise `process_chunk_state_witness` (`:323`) checks `prev_block_hash` matches, then `start_validating_chunk` (`:359`).

`start_validating_chunk` (`:359`):
1. Confirm the witness `epoch_id` equals `get_epoch_id_from_prev_block` (`:384`).
2. `pre_validate_chunk_state_witness` (`chain/chain/src/stateless_validation/chunk_validation.rs:310`) — synchronous, cheap checks (below).
3. **Chunk-extra shortcut**: if the validator already has the prev block's `ChunkExtra` on disk (e.g. it produced/tracked the shard), it validates the chunk header directly via `validate_chunk_with_chunk_extra_and_roots` and endorses without replaying (`:425`–`:445`).
4. Otherwise spawn the heavy `validate_chunk_state_witness` on the validation thread pool (`:473`); on `Ok`, call `send_chunk_endorsement_to_block_producers` (`:488`).

`pre_validate_chunk_state_witness` (`chunk_validation.rs:310`):
- Check the chunk header version is allowed at the epoch's protocol version (`:320`) and that every `new_transaction` is valid for that protocol version (`:323`).
- `get_state_witness_block_range` (`:144`) walks backwards from the witness's prev block, classifying each block by `num_new_chunks_seen` for the shard: 0 → contributes an `ApplyOldChunk` implicit transition; 1 → contributes a source-receipt block; 2 → stop. Resharding boundaries inject a `Resharding` implicit transition (`get_resharding_transition`, `:258`).
- `validate_source_receipt_proofs` (`:449`) extracts the receipts to apply from the proofs, validating each proof's `from_shard_id`/`to_shard_id` and Merkle path against the source chunk's outgoing-receipts root (`validate_receipt_proof`, `:529`), filtering for the target shard, and shuffling into application order. The hash of the ordered receipts must equal `state_witness.applied_receipts_hash()` (`:361`), and the witness's `transactions` must merklize to the last new chunk's `tx_root` (`:375`).
- Builds `MainTransition::Genesis` or `MainTransition::NewChunk` with `StorageDataSource::Recorded(PartialStorage { nodes: base_state })` — i.e. the runtime will read from the recorded proof, not a full trie (`:432`).

`validate_chunk_state_witness_impl` (`chunk_validation.rs:565`) — the consensus-critical re-execution:
1. **Main transition**: replay the last new chunk via `apply_new_chunk` against the recorded partial storage (`:603`), producing a `ChunkExtra` and outgoing receipts. (Genesis skips replay; a per-shard LRU cache can short-circuit repeats, `:594`.)
2. Check the resulting `state_root` equals `main_state_transition.post_state_root` (`:622`) — an early debug check; the binding check is the chunk header at the end.
3. **Implicit transitions**: the count must equal `state_witness.implicit_transitions().len()` (`:662`). For each, replay an old chunk (`apply_old_chunk`, `:690`) or apply a resharding split (`retain_split_shard`, `:730`) against that transition's recorded `base_state`, updating `state_root` + `congestion_info`, and checking each against the transition's `post_state_root` (`:738`).
4. **Final binding checks** (in parallel, `:752`): merklize the outgoing-receipt hashes and call `validate_chunk_with_chunk_extra_and_receipts_root` to bind `chunk_extra` (state root, gas, congestion) and the outgoing-receipts root to the **witness chunk header**; and check `new_transactions` merklize to the header `tx_root` and the encoded-merkle-root via `validate_chunk_with_encoded_merkle_root`.

On any failure `Error::InvalidChunkStateWitness` is returned; if `save_witness_if_invalid` is set the witness is persisted for debugging (`validate_chunk_state_witness`, `:782`).

### 5. Endorsement creation, gossip, aggregation

- `send_chunk_endorsement_to_block_producers` (`chain/client/src/stateless_validation/chunk_validator/mod.rs:22`) builds a `ChunkEndorsement::new` (`:62`) and sends it to the next `NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT = 5` unique block producers (`:16`, `:47`) — covering the case where the height-`h` block producer is skipped. If the signer is itself one of those producers, the endorsement is returned for immediate local processing (`:63`).
- A block producer validates and stores received endorsements in `ChunkEndorsementTracker::process_chunk_endorsement` (`chain/client/src/stateless_validation/chunk_endorsement.rs:43`): dedup per validator, `validate_chunk_endorsement` (relevance + both signatures), then cache by `ChunkProductionKey` (`:57`).
- When producing a block, `collect_chunk_endorsements` (`:82`) filters cached endorsements to those whose `chunk_hash` matches the header, then `compute_endorsement_state` decides inclusion: a chunk is includable iff `is_endorsed` (> 2/3 stake). The ordered signature vector (or empty) is what goes into the block body.
- `validate_chunk_endorsements_in_block` (`chain/chain/src/stateless_validation/chunk_endorsement.rs:19`) re-validates at block processing: chunk count == endorsement-vector count; for each new chunk, the signature list length equals the ordered chunk-validator count, each present signature verifies against that validator's key over `ChunkEndorsement::validate_signature` (the `inner` bytes), > 2/3 stake is endorsed, and the header's endorsement **bitmap** is consistent (length, bit-per-signature, trailing zeros). Old chunks (`height_included != block height`) must carry empty endorsements (`:50`). `validate_chunk_endorsements_in_header` (`:156`) checks the bitmap shape against the chunk mask.

### 6. Contract-code distribution (avoid shipping code in every witness)

Contract code is excluded from the witness and distributed out-of-band:
- Producer sends `ChunkContractAccesses` (hashes) to validators (step 2.1).
- Validator, in `handle_chunk_contract_accesses` (`partial_witness_actor.rs:675`), filters out hashes already in its compiled-contract cache, stores the missing set in the tracker, and sends a signed `ContractCodeRequest` to a *random* chunk producer for the shard (`:706`–`:720`).
- Producer answers in `handle_contract_code_request` (`:862`): dedups per `(key, requester)`, re-derives the `MainTransitionKey` (`derive_main_transition_key`, `:787`) and confirms it matches the request, confirms each requested hash was actually accessed (`valid_accesses`, `:917`), reads the raw code from the trie, and replies with `ContractCodeResponse::encode` — **V2 (signed) iff `SignedContractCodeResponse` is enabled, else V1 (unsigned)** (`contract_distribution.rs:254`, gate at `:260`).
- Validator stores the received code (`handle_contract_code_response`, `:964`); the tracker merges it into the witness base state at finalize (step 3).
- Newly-deployed contracts ride the separate `PartialEncodedContractDeploys` erasure-coded path so future-turn validators have them ahead of time.

## Interactions

| Consumes | Produces |
|---|---|
| Produced chunk + applied `StateTransitionData` from [sharding-chunks](sharding-chunks.md) / [runtime-execution](runtime-execution.md) | `ChunkStateWitness` (compressed, erasure-coded parts) |
| Chunk-validator assignment & validator keys from [epoch-validators-staking](epoch-validators-staking.md) (`get_chunk_validator_assignments`, `get_chunk_producer_info`) | `ChunkEndorsement`s sent to upcoming block producers |
| Recorded `PartialState` proof from [state-storage](state-storage.md); runtime re-execution from [runtime-execution](runtime-execution.md) | Endorsement aggregate gating chunk inclusion in [chain-block-processing](chain-block-processing.md) |
| Witness/endorsement/contract messages over [networking-p2p](networking-p2p.md) | Contract-code request/response & deploy parts |

Re-execution during validation calls the runtime (`apply_new_chunk`/`apply_old_chunk`) exactly as block processing does, but with `StorageDataSource::Recorded` (a partial trie) instead of full state — see [state-storage](state-storage.md) for proof verification and [runtime-execution](runtime-execution.md) for the state-transition itself. This spec does not re-document those.

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs`. The whole NEP-509 family (`_DeprecatedStatelessValidation` @ 69, `_DeprecatedChunkEndorsementV2` / `_DeprecatedChunkEndorsementsInBlockHeader` @ 72, `_DeprecatedExcludeContractCodeFromStateWitness` @ 73, `_DeprecatedRelaxedChunkValidation` @ 74, `_DeprecatedVersionedStateWitness` @ 78) is **deprecated and always-on** at v86 (`MIN_SUPPORTED_PROTOCOL_VERSION = 83`, `version.rs:596`) — these behaviors are now unconditional in the code paths above.

Still-gated at v86:

| Feature | Activates | Effect on this component |
|---|---|---|
| `SignedContractCodeResponse` | 85 (`version.rs:567`) | `ContractCodeResponse::encode` emits the signed `V2` variant; `validate_contract_code_response` requires & verifies the responder signature (`validate.rs:217`, `:371`). Below 85: unsigned `V1`. Enabled at v86. |
| `ExecutionMetadataV4` | 85 (`version.rs:566`) | Execution-metadata/profile shape produced during re-execution (consumed via [runtime-execution](runtime-execution.md)); the witness validation replays under whatever metadata version the epoch's runtime config selects. Enabled at v86. |
| `EarlyKickout` | 152 (nightly, `version.rs:579`) | Future rollout switch for `VersionedPartialEncodedStateWitness` V1→V2 (adds `prev_block_hash` for hash-based chunk-producer lookup). **Inert at v86**: `new` always emits V1 and handlers drop V2 (`partial_witness.rs:269`, `partial_witness_actor.rs:415`). |
| `Spice` | 180 (nightly, `version.rs:582`) | When enabled, the non-SPICE witness path is skipped entirely (`state_witness_producer.rs:23`); SPICE uses the `SpiceChunk*` contract-distribution types (`contract_distribution.rs:602`+). Not enabled at v86. |

Note: the unconditional 64/48 MiB witness size caps (`MAX_UNCOMPRESSED_STATE_WITNESS_SIZE`, `MAX_COMPRESSED_STATE_WITNESS_SIZE`) are compile-time constants, not protocol-feature-gated; the per-receipt / combined-transaction storage-proof soft limits live in the runtime config (`core/parameters`) and are read during re-execution via [runtime-execution](runtime-execution.md).

## Invariants & failure modes

- **> 2/3 stake to endorse.** `has_enough_stake` requires `endorsed_stake >= total_stake*2/3 + 1` (`validator_assignment.rs:18`); below threshold `compute_endorsement_state` clears the signature vector so the chunk cannot be included.
- **State-root binding.** Final validation binds the replayed `ChunkExtra` and outgoing-receipts root to the witness chunk header (`chunk_validation.rs:752`); the intermediate `post_state_root` checks (`:622`, `:738`) are explicitly described in-code as debug aids, not the authoritative check.
- **Receipts/transactions are self-proving.** `applied_receipts_hash` must match the receipts extracted from proofs (`:361`); `transactions` must merklize to the last new chunk's `tx_root` (`:375`); each source-receipt proof's shard ids and Merkle path are verified (`validate_receipt_proof`, `:529`). Mismatch → `Error::InvalidChunkStateWitness`.
- **Implicit-transition count must match** between recomputed range and witness (`:662`), preventing a producer from hiding or inventing missing-chunk transitions.
- **Signature provenance.** Witness parts, contract accesses/requests/responses, and endorsements are all signed; validation rejects on bad signature or wrong signer (`validate.rs`, `chunk_endorsement.rs`). A contract-code responder must be a chunk producer for the shard (`validate.rs:371`).
- **Relevance gating drops, not errors.** `validate_chunk_relevant` (`validate.rs:260`) returns `TooLate` (height ≤ final head), `TooEarly` (> head + `MAX_HEIGHTS_AHEAD = 5`), or `UnknownEpochId`; these are dropped silently, while `NotAChunkValidator` / `InvalidShardId` / bad-signature are hard errors (potential malicious behavior, `// TODO: ban sending peer`).
- **Orphan witnesses** outside `2..6` of head or over size are not stored (`chunk_validation_actor.rs:239`/`:253`); stored ones are purged below the last final height (`:304`).
- **Size caps** reject oversized parts (`encoded_length`/`part_size` checks, `validate.rs:83`/`:107`) and decode failures surface as `InvalidPartialChunkStateWitness` ("failed to reed solomon decode witness parts", `partial_witness_tracker.rs:468`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives/src/stateless_validation/state_witness.rs:96` | `ChunkStateWitness` | Versioned witness enum (V2 live). |
| `core/primitives/src/stateless_validation/state_witness.rs:104` | `ChunkStateWitnessV2` | Witness fields: main + implicit transitions, source receipt proofs, transactions. |
| `core/primitives/src/stateless_validation/state_witness.rs:282` | `ChunkStateTransition` | `base_state` proof + `post_state_root`. |
| `core/primitives/src/stateless_validation/partial_witness.rs:254` | `VersionedPartialEncodedStateWitness` | V1-only at v86; V2 inert. |
| `core/primitives/src/stateless_validation/chunk_endorsement.rs:17` | `ChunkEndorsement` | Endorsement V2 (inner+sig in header, metadata for routing). |
| `core/primitives/src/stateless_validation/validator_assignment.rs:60` | `compute_endorsement_state` | > 2/3 stake aggregation. |
| `core/primitives/src/stateless_validation/contract_distribution.rs:254` | `ContractCodeResponse::encode` | Signed V2 iff `SignedContractCodeResponse`. |
| `chain/chain/src/stateless_validation/state_witness.rs:45` | `create_state_witness` | Producer assembles the witness. |
| `chain/chain/src/stateless_validation/state_witness.rs:100` | `collect_state_transition_data` | Walks back, collects main + implicit transitions. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:310` | `pre_validate_chunk_state_witness` | Cheap chain-relative checks + receipt/tx hashing. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:144` | `get_state_witness_block_range` | Classifies blocks by new-chunks-seen (0/1/2). |
| `chain/chain/src/stateless_validation/chunk_validation.rs:449` | `validate_source_receipt_proofs` | Extracts/validates receipts to apply. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:565` | `validate_chunk_state_witness_impl` | Re-executes main + implicit transitions, binds to header. |
| `chain/chain/src/stateless_validation/chunk_endorsement.rs:19` | `validate_chunk_endorsements_in_block` | Block-level endorsement + bitmap validation. |
| `chain/client/src/stateless_validation/state_witness_producer.rs:15` | `send_chunk_state_witness_to_chunk_validators` | Production entry; self-endorse shortcut. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:205` | `handle_distribute_state_witness_request` | 3-stream priority distribution. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:1016` | `generate_state_witness_parts` | Reed-Solomon split, one part per validator. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_tracker.rs:273` | `CacheEntry::try_finalize` | Reconstruct witness once parts+contracts ready. |
| `chain/client/src/stateless_validation/chunk_validation_actor.rs:359` | `start_validating_chunk` | Pre-validate → chunk-extra shortcut → spawn validate → endorse. |
| `chain/client/src/stateless_validation/chunk_validator/mod.rs:22` | `send_chunk_endorsement_to_block_producers` | Build + gossip endorsement to next 5 BPs. |
| `chain/client/src/stateless_validation/chunk_endorsement.rs:82` | `collect_chunk_endorsements` | Aggregate cached endorsements for inclusion. |
| `chain/client/src/stateless_validation/validate.rs:260` | `validate_chunk_relevant` | Relevance gating (TooLate/TooEarly/UnknownEpochId). |
| `chain/client/src/stateless_validation/partial_witness/encoding.rs:6` | `WITNESS_RATIO_DATA_PARTS` | 0.6 data-parts ratio for erasure coding. |
| `core/primitives-core/src/version.rs:567` | `SignedContractCodeResponse` | Activates at 85. |
| `core/primitives-core/src/version.rs:579` | `EarlyKickout` | Nightly @ 152; gates witness V2. |

## Open questions

- The `ChunkStateWitnessV2.chunk_header` field is marked `// TODO(stateless_validation): Deprecate this field in the next version of the state witness` (`state_witness.rs:112`) — it is currently load-bearing for endorsement (validator must know the chunk hash it signs), so removal is not yet realized at v86.
- The resharding-boundary caveat on `collect_source_receipt_proofs` (`state_witness.rs:260`, "generates invalid proofs on resharding boundaries") is documented as a known TODO; exactly how/whether the post-verification filtering it describes is fully wired was not traced end-to-end here.
