# Stateless validation

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/primitives/src/stateless_validation/`, `chain/chain/src/stateless_validation/`, `chain/client/src/stateless_validation/`

## Role

Stateless validation (NEP-509) is how the protocol verifies a chunk's state transition without every validator holding the full shard state. After a chunk producer applies a chunk, it packages the *recorded* trie reads (a Merkle proof) together with the transactions and source receipts into a **`ChunkStateWitness`**. The witness is compressed, Reed-Solomon erasure-coded, and distributed part-by-part to the chunk's assigned **chunk validators**. Each validator reconstructs the witness, re-applies the chunk against the partial trie (no full state needed), checks the replayed `ChunkExtra` / outgoing-receipts root against the chunk header, and emits a signed **`ChunkEndorsement`**. A chunk may be included in a block only once endorsements covering > 2/3 of the chunk-validator stake are collected. This component sits downstream of [sharding-chunks](sharding-chunks.md) (which produces the chunk) and [runtime-execution](runtime-execution.md) (which is replayed during validation against a partial trie from [state-storage](state-storage.md)); chunk-validator assignment comes from [epoch-validators-staking](epoch-validators-staking.md); witnesses/endorsements travel over [networking-p2p](networking-p2p.md); endorsements gate chunk inclusion in [chain-block-processing](chain-block-processing.md).

## Key data structures

- **`ChunkStateWitness`** — `core/primitives/src/stateless_validation/state_witness.rs:96` — versioned enum, only live variant `V2(Box<ChunkStateWitnessV2>) = 1` (V1 removed). `ChunkStateWitnessV2` (`:104`) fields: `epoch_id` (the epoch of the block *after* the chunk's prev block); `chunk_header` (the `ShardChunkHeader` being attested, kept so the validator knows what it endorses); `main_state_transition: ChunkStateTransition` (the new-chunk transition of the *last new chunk* of this shard); `source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>` (a non-strict superset of the receipts to apply, with proofs against outgoing-receipt roots); `applied_receipts_hash` (redundant hash of the ordered `Vec<Receipt>` to apply, for diagnosis); `transactions` (the transactions the last new chunk converted to receipts); `implicit_transitions: Vec<ChunkStateTransition>` (one per missing chunk between the last new chunk and this chunk, forward chronological); `new_transactions` (transactions in *this* chunk).
- **`ChunkStateTransition`** — `core/primitives/src/stateless_validation/state_witness.rs:282` — `block_hash`, `base_state: PartialState` (recorded trie nodes = the proof), `post_state_root` (redundant; the real check is re-deriving it). See [state-storage](state-storage.md) for `PartialState`/proof mechanics.
- **`EncodedChunkStateWitness`** — `core/primitives/src/stateless_validation/state_witness.rs:37` — zstd-compressed borsh of the witness. Raw cap `MAX_UNCOMPRESSED_STATE_WITNESS_SIZE = 64 MiB` (`:19`, 512 MiB under `test_features`); compression level 1 (`:21`), 4 workers (`:22`).
- **`PartialEncodedStateWitness`** / **`VersionedPartialEncodedStateWitness`** — `core/primitives/src/stateless_validation/partial_witness.rs:26` / `:254` — one Reed-Solomon part: `epoch_id`, `shard_id`, `height_created`, `part_ord`, `part: Box<[u8]>`, `encoded_length`, a `signature_differentiator`, and the chunk-producer signature over the inner bytes. `VersionedPartialEncodedStateWitness` is `V1 = 0` / `V2 = 1`; at v86 only V1 is emitted/accepted — `new` ignores its `protocol_version` argument and always returns V1 (`:269`), and handlers drop V2 (V2 wiring is inert, plumbed for a future `EarlyKickout` rollout; V2 adds `prev_block_hash`, `:196`). Compressed cap `MAX_COMPRESSED_STATE_WITNESS_SIZE = 48 MiB` (`:18`).
- **`ChunkEndorsement`** — `core/primitives/src/stateless_validation/chunk_endorsement.rs:17` — versioned enum, live variant `V2(ChunkEndorsementV2) = 1`. `ChunkEndorsementV2` (`:100`) has `inner: ChunkEndorsementInnerV1` (`chunk_hash` + `"ChunkEndorsement"` differentiator, `:130`), `signature` over `inner`, `metadata: ChunkEndorsementMetadata` (`account_id`, `shard_id`, `epoch_id`, `height_created`, `:121`), and `metadata_signature` over the metadata. The `inner`+`signature` are what land in the block header; the metadata pair is used only for validation routing (`ChunkEndorsement::new`, `:22`).
- **`ChunkValidatorAssignments`** — `core/primitives/src/stateless_validation/validator_assignment.rs:33` — ordered `Vec<(AccountId, Balance)>` of validators + their stake for one chunk, plus a `HashSet` for membership. `compute_endorsement_state` (`:60`) walks the ordered list, sums `total_stake` and `endorsed_stake` for accounts that supplied a signature, and sets `is_endorsed` via `has_enough_stake` (`:18`) = `endorsed_stake >= required_stake` where `required_stake = total_stake*2/3 + 1` (`:22`). If not endorsed the signature vector is cleared (`:81`).
- **`ChunkProductionKey`** — `core/primitives/src/stateless_validation/mod.rs:19` — `(shard_id, epoch_id, height_created)`; the routing key for every witness/endorsement/contract message.
- **`StoredChunkStateTransitionData`** — `core/primitives/src/stateless_validation/stored_chunk_state_transition_data.rs:12` — persisted per chunk (incl. missing chunks) in `DBCol::StateTransitionData`; live variant `V1(StoredChunkStateTransitionDataV1) = 0` (`:25`): `base_state` (the recorded proof), `receipts_hash`, `contract_accesses: Vec<CodeHash>`, `contract_deploys: Vec<CodeBytes>`. This is the producer-side source from which a witness is later assembled.
- **Contract-distribution types** — `core/primitives/src/stateless_validation/contract_distribution.rs` — `ContractUpdates` (`:462`: `contract_accesses: HashSet<CodeHash>`, `contract_deploys: Vec<ContractCode>`), `ChunkContractAccesses` (`:30`, versioned; signed code-hash list), `ContractCodeRequest` (`:136`, versioned), `ContractCodeResponse` (`:248`, `V1` unsigned / `V2` signed), `ChunkContractDeploys` (`:479`) + `PartialEncodedContractDeploys` (`:502`, erasure-coded deploy distribution). `MAX_CONTRACTS_PER_REQUEST = 1282` (`:20`).

## Behavior

### 1. Witness production (chunk producer, after applying its chunk)

`Client::send_chunk_state_witness_to_chunk_validators` (`chain/client/src/stateless_validation/state_witness_producer.rs:15`) drives production for a newly produced chunk:

1. Early-return if `ProtocolFeature::Spice` is enabled (`:23`) — SPICE has its own path (not enabled at v86).
2. Require a local validator signer (`:42`, else `Error::NotAValidator`), then build the witness via `ChainStore::create_state_witness` (`chain/chain/src/stateless_validation/state_witness.rs:45`).
3. If `save_latest_witnesses` is set, persist it for debugging (`:53`).
4. **Self-endorse shortcut**: if the producer is itself in the chunk-validator set for `(epoch, shard, height)`, it endorses immediately (bypassing validation, since it just applied the chunk) by calling `send_chunk_endorsement_to_block_producers` and feeding the returned self-endorsement to its own endorsement tracker (`:57`–`:72`).
5. Send a `DistributeStateWitnessRequest` to the `PartialWitnessActor` (`:74`).

`create_state_witness` (`state_witness.rs:45`) assembles the witness:
- `collect_state_transition_data` (`:100`) walks backwards from `chunk_header.prev_block_hash()` to the block at `prev_chunk_header.height_included()` (exclusive), reading each block's stored `StateTransitionData` as an *implicit transition* (one per missing chunk, plus an extra one when a resharding shard-id change happens between blocks, `:132`). Transitions are collected newest-first then `reverse()`d to forward order (`:165`). The **main transition** is the last new chunk's transition (`:168`), read via `get_state_transition` (`:194`) from `DBCol::StateTransitionData` keyed by `(block_hash, shard_id)`, with `post_state_root` taken from that block's `ChunkExtra` (`:231`). Genesis prev-chunk uses `get_genesis_state_transition` with empty base state (`:238`).
- `collect_source_receipt_proofs` (`:264`) gathers all incoming receipts for the *previous* chunk and their proofs (so the witness self-proves which receipts to apply), converting from `[block_hash -> Vec<ReceiptProof>]` to `[chunk_hash -> ReceiptProof]` (`:316`); a duplicate chunk proof is an error (`:330`). Genesis prev-chunk yields an empty map (`:270`).
- The witness carries `prev_chunk.to_transactions()` as `transactions` and the current `chunk.to_transactions()` as `new_transactions` (`:89`, `:91`).

### 2. Encoding and distribution (`PartialWitnessActor`)

`handle_distribute_state_witness_request` (`chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:205`) sends three streams in priority order (`:225`–`:277`):
1. **Contract accesses first** (only if contracts were accessed): `send_contract_accesses_to_chunk_validators` (`:727`) sends a signed `ChunkContractAccesses` (code-hashes only, not code) to chunk validators *except* chunk producers tracking the same shard (`:743`), so validators can request missing code while witness parts are still in flight.
2. **Witness parts** (compression/encoding moved to `witness_creation_spawner` to avoid blocking the actor, `:260`): `compress_and_distribute_witness` (`:280`) compresses (`compress_witness`, `:1060`), then `send_state_witness_parts` (`:344`) → `generate_state_witness_parts` (`:1016`) erasure-codes the compressed bytes into **one part per chunk validator** using a Reed-Solomon encoder sized to `chunk_validators.len()` with `WITNESS_RATIO_DATA_PARTS = 0.6` (`partial_witness/encoding.rs:6`). Part `i` is signed and addressed to chunk validator `i` (the part owner); sent as `NetworkRequests::PartialEncodedStateWitness` (`:394`). The witness is recorded in `ChunkStateWitnessTracker` for round-trip-time metrics (`:387`).
3. **Contract deploys** (lower priority): `send_chunk_contract_deploys_parts` (`:762`) compresses newly-deployed code and erasure-codes it to non-producer validators, who will need it in later turns.

### 3. Part forwarding and reconstruction (chunk validator)

- A part owner receives its part via `handle_partial_encoded_state_witness` (`:400`). V2 parts are dropped immediately (`:415`). It validates the part (`validate_partial_encoded_state_witness`, `validate.rs:64`) on `partial_witness_spawner`, then **forwards** it to all other chunk validators except itself and the producer (`NetworkRequests::PartialEncodedStateWitnessForward`, `:464`) and stores its own copy (`:472`).
- Forwarded parts arrive at `handle_partial_encoded_state_witness_forward` (`:499`); V2 dropped (`:514`), validated and stored (`:537`–`:540`).
- `PartialEncodedStateWitnessTracker` (`partial_witness/partial_witness_tracker.rs:345`) accumulates parts per `ChunkProductionKey` in a per-shard LRU `CacheEntry` (`:72`). `process_witness_part` (`:144`) feeds a `ReedSolomonPartsTracker`; when enough data parts arrive it decodes (`InsertPartResult::Decoded`, `:210`). `try_finalize` (`:273`) waits for **both** the witness parts (decoded) and the accessed-contract state to be ready; `AccessedContractsState::Unknown` counts as ready (`:284`) — the chunk may have no contracts, or the accesses message was lost (best-effort). On finalize, the decoded witness's main-transition `base_state` values are **extended** with the separately-fetched contract codes (`:494`–`:495`), and the full `ChunkStateWitness` is sent to the `ChunkValidationActor` as a `ChunkStateWitnessMessage` (`:509`).

### 4. Validation (`ChunkValidationActor`)

`process_chunk_state_witness_message` (`chunk_validation_actor.rs:513`):
1. Reject if this node is not a validator (`:520`, `Error::Other`); send a `ChunkStateWitnessAck` back to the chunk producer for RTT metrics (`:530`).
2. If the witness's prev block is not yet known, park it as an **orphan** in `OrphanStateWitnessPool` — only if `head_distance` is within `ALLOWED_ORPHAN_WITNESS_DISTANCE_FROM_HEAD = 2..6` (`:45`, `:239`) and under `max_orphan_witness_size` (`:253`); reprocessed when the block arrives via `process_ready_orphan_witnesses` (`:275`).
3. Otherwise `process_chunk_state_witness` (`:323`) → `start_validating_chunk` (`:359`).

`start_validating_chunk` (`:359`):
1. Confirm the witness `epoch_id` equals `get_epoch_id_from_prev_block` (`:385`).
2. `pre_validate_chunk_state_witness` (`chain/chain/src/stateless_validation/chunk_validation.rs:310`) — synchronous, cheap checks (below), producing a `PreValidationOutput` (struct `:82`, constructed `:443`).
3. **Chunk-extra shortcut**: if the validator already has the prev block's `ChunkExtra` on disk (e.g. it produced/tracked the shard), it validates the header directly via `validate_chunk_with_chunk_extra_and_roots` and endorses without replaying (`:427`–`:438`).
4. Otherwise spawn the heavy `validate_chunk_state_witness` on `validation_spawner` (`:473`); on `Ok`, call `send_chunk_endorsement_to_block_producers` (`:488`).

`pre_validate_chunk_state_witness` (`chunk_validation.rs:310`):
- Check the chunk header version is allowed at the epoch's protocol version (`:320`) and that every `new_transaction` is valid for that protocol version (`:323`, else `Error::InvalidChunkStateWitness`).
- `get_state_witness_block_range` (`:144`) walks backwards from the witness's prev block, classifying each block by `num_new_chunks_seen` for the shard: 0 → contributes an `ApplyOldChunk` implicit transition; 1 → contributes a source-receipt block; 2 → stop. Resharding boundaries inject a `Resharding` implicit transition (`get_resharding_transition`, `:258`, keyed to the split's `left_child_shard`/`right_child_shard`, `:290`).
- `validate_source_receipt_proofs` (`:449`) extracts the receipts to apply from the proofs, validating each proof's `from_shard_id`/`to_shard_id` and Merkle path against the source chunk's outgoing-receipts root (`validate_receipt_proof`, `:529`), filtering for the target shard (`:501`) and shuffling into application order (`:508`). It also rejects extraneous proofs (`:519`). The hash of the ordered receipts must equal `applied_receipts_hash()` (`:361`), and the witness's `transactions` must merklize to the last new chunk's `tx_root` (`:375`).
- Builds `MainTransition::Genesis` (`:408`) or `MainTransition::NewChunk` (`:422`) with `StorageDataSource::Recorded(PartialStorage { nodes: base_state })` — i.e. the runtime will read from the recorded proof, not a full trie (`:432`).

`validate_chunk_state_witness_impl` (`chunk_validation.rs:565`) — the consensus-critical re-execution:
1. **Main transition**: replay the last new chunk via `apply_new_chunk` against the recorded partial storage with `MaybePinnedMemtrieRoot::no_memtries()` (`:605`), producing a `ChunkExtra` and outgoing receipts. (Genesis skips replay, `:602`; a per-shard LRU cache of size 20 can short-circuit repeats, `:594`/`:99`.)
2. Check the resulting `state_root` equals `main_state_transition.post_state_root` (`:622`) — an early debug check; the binding check is the chunk header at the end.
3. **Implicit transitions**: the count must equal `state_witness.implicit_transitions().len()` (`:662`). For each, replay an old chunk (`apply_old_chunk`, `:690`) or apply a resharding split (`retain_split_shard`, `:730`, with child congestion info recomputed from the parent trie, `:720`) against that transition's recorded `base_state`, updating `state_root` + `congestion_info` (`:736`), and checking each against the transition's `post_state_root` (`:738`, debug-only).
4. **Final binding checks** (in parallel via `rayon::join`, `:752`): merklize the outgoing-receipt hashes and call `validate_chunk_with_chunk_extra_and_receipts_root` to bind `chunk_extra` (state root, gas, congestion) and the outgoing-receipts root to the **witness chunk header** (`:755`); and check `new_transactions` merklize to the header `tx_root` (`:764`, else `Error::InvalidTxRoot`) and the encoded-merkle-root via `validate_chunk_with_encoded_merkle_root` (`:767`).

On any failure `Error::InvalidChunkStateWitness` is returned; if `save_witness_if_invalid` is set the witness is cloned and persisted for debugging (`validate_chunk_state_witness`, `:782`).

### 5. Endorsement creation, gossip, aggregation

- `send_chunk_endorsement_to_block_producers` (`chain/client/src/stateless_validation/chunk_validator/mod.rs:22`) builds a `ChunkEndorsement::new` (`:62`) and sends it to the next `NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT = 5` unique block producers (`:16`, `:47`) — covering the case where the height-`h` block producer is skipped. If the signer is itself one of those producers, the endorsement is returned for immediate local processing (`:63`–`:72`).
- A block producer validates and stores received endorsements in `ChunkEndorsementTracker::process_chunk_endorsement` (`chain/client/src/stateless_validation/chunk_endorsement.rs:43`): dedup per validator (`:50`), `validate_chunk_endorsement` (relevance + both signatures, `validate.rs:148`), then cache by `ChunkProductionKey` in a 100-entry LRU (`:19`, `:60`).
- When producing a block, `collect_chunk_endorsements` (`:82`) filters cached endorsements to those whose `chunk_hash` matches the header (`:109`), then `compute_endorsement_state` decides inclusion: a chunk is includable iff `is_endorsed` (> 2/3 stake). The ordered signature vector (or empty) is what goes into the block body.
- `validate_chunk_endorsements_in_block` (`chain/chain/src/stateless_validation/chunk_endorsement.rs:19`) re-validates at block processing: chunk count == endorsement-vector count (`:24`); for each new chunk, the signature list length equals the ordered chunk-validator count (`:74`), each present signature verifies via `ChunkEndorsement::validate_signature` over the `inner` bytes (`:93`), > 2/3 stake is endorsed (`:113`), and the header's endorsement **bitmap** is consistent (length `:121`, bit-per-signature `:130`, trailing zeros `:141`). Old chunks (`height_included != block height`) must carry empty endorsements (`:50`). `validate_chunk_endorsements_in_header` (`:156`) checks the bitmap shape against the chunk mask.

### 6. Contract-code distribution (avoid shipping code in every witness)

Contract code is excluded from the witness and distributed out-of-band:
- Producer sends `ChunkContractAccesses` (hashes) to validators (step 2.1).
- Validator, in `handle_chunk_contract_accesses` (`partial_witness_actor.rs:675`), filters out hashes already in its compiled-contract cache (`:692`), stores the missing set in the tracker (`:704`), and sends a signed `ContractCodeRequest` to a *random* chunk producer for the shard (`:706`–`:719`).
- Producer answers in `handle_contract_code_request` (`:862`): dedups per `(key, requester)` (`:875`), re-derives the `MainTransitionKey` (`derive_main_transition_key`, `:787`) and confirms it matches the request (`:895`), confirms each requested hash was actually accessed (`valid_accesses`, `:917`/`:930`), reads the raw code from the trie (`:939`), and replies with `ContractCodeResponse::encode` — **V2 (signed) iff `SignedContractCodeResponse` is enabled, else V1 (unsigned)** (`contract_distribution.rs:254`, gate at `:260`).
- Validator stores the received code (`handle_contract_code_response`, `:964`), which the tracker merges into the witness base state at finalize (step 3). `validate_contract_code_response` requires and verifies the responder signature only when `SignedContractCodeResponse` is enabled (`validate.rs:217`, `:371`).
- Newly-deployed contracts ride the separate `PartialEncodedContractDeploys` erasure-coded path (`handle_partial_encoded_contract_deploys`, `:571`; deployed code is precompiled once decoded, `:648`) so future-turn validators have them ahead of time.

## Interactions

| Consumes | Produces |
|---|---|
| Produced chunk + applied `StateTransitionData` from [sharding-chunks](sharding-chunks.md) / [runtime-execution](runtime-execution.md) | `ChunkStateWitness` (compressed, erasure-coded parts) |
| Chunk-validator assignment & validator keys from [epoch-validators-staking](epoch-validators-staking.md) (`get_chunk_validator_assignments`, `get_chunk_producer_info`) | `ChunkEndorsement`s sent to upcoming block producers |
| Recorded `PartialState` proof from [state-storage](state-storage.md); runtime re-execution from [runtime-execution](runtime-execution.md) | Endorsement aggregate gating chunk inclusion in [chain-block-processing](chain-block-processing.md) |
| Witness/endorsement/contract messages over [networking-p2p](networking-p2p.md) | Contract-code request/response & deploy parts |

Re-execution during validation calls the runtime (`apply_new_chunk`/`apply_old_chunk`) exactly as block processing does, but with `StorageDataSource::Recorded` (a partial trie) instead of full state — see [state-storage](state-storage.md) for proof verification and [runtime-execution](runtime-execution.md) for the state-transition itself. This spec does not re-document those.

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` (this 2.13.0 tree). The whole NEP-509 family (`_DeprecatedStatelessValidation` @ 69, `_DeprecatedChunkEndorsementV2` / `_DeprecatedChunkEndorsementsInBlockHeader` @ 72, `_DeprecatedExcludeContractCodeFromStateWitness` @ 73, `_DeprecatedRelaxedChunkValidation` @ 74, `_DeprecatedVersionedStateWitness` @ 78) is **deprecated and always-on** at v86 (`MIN_SUPPORTED_PROTOCOL_VERSION = 83`, `version.rs:600`) — these behaviors are now unconditional in the code paths above. Support for pre-relaxed-validation and balance-checking code has been removed entirely (feature docstrings at `version.rs:286`, `:294`).

Still-gated features touching this component at v86:

| Feature | Enum decl | Activates | Effect on this component |
|---|---|---|---|
| `SignedContractCodeResponse` | `version.rs:447` | 85 | `ContractCodeResponse::encode` emits the signed `V2` variant; `validate_contract_code_response` requires & verifies the responder signature (`validate.rs:217`, `:371`). Below 85: unsigned `V1`. Enabled at v86. |
| `ExecutionMetadataV4` | `version.rs:433` | 85 | Execution-metadata shape produced during re-execution (consumed via [runtime-execution](runtime-execution.md)); witness validation replays under whatever metadata version the epoch's runtime config selects. Enabled at v86. |
| `ExcludeExistingCodeFromWitnessForCodeLen` | `version.rs:300` | 83 | Excludes existing contract code in deploy-contract / delete-account actions from the witness, checking code size via trie nodes instead. Enabled at v86. |
| `EnforcePerReceiptStorageProofLimit` | `version.rs:449` | 86 | The PV-86 feature on this release. Enforces the per-receipt storage-proof size limit during application/re-execution (limit itself lives in the runtime config, read via [runtime-execution](runtime-execution.md)). Newly enabled at v86. |
| `EarlyKickout` | `version.rs:401` | 152 (nightly) | Future rollout switch for `VersionedPartialEncodedStateWitness` V1→V2 (V2 adds `prev_block_hash` for hash-based chunk-producer lookup). **Inert at v86**: `new` always emits V1 and handlers drop V2 (`partial_witness.rs:269`, `partial_witness_actor.rs:415`/`:514`). |
| `Spice` | `version.rs:355` | 180 (nightly) | When enabled, the non-SPICE witness path is skipped entirely (`state_witness_producer.rs:23`); SPICE uses the `SpiceChunk*` contract-distribution types (`contract_distribution.rs:608`+). Not enabled at v86. |

> FEATURE NOTE: this 2.13.0 tree has **no `FixContractLoadingError` feature** (an earlier baseline did); the highest stable feature is `EnforcePerReceiptStorageProofLimit` @ 86. The `MAINNET` upgrade schedule votes PV 86 on `2026-07-20` (`core/primitives/src/version.rs:63`).

The unconditional 64/48 MiB witness size caps (`MAX_UNCOMPRESSED_STATE_WITNESS_SIZE`, `MAX_COMPRESSED_STATE_WITNESS_SIZE`) are compile-time constants, not protocol-feature-gated; per-receipt / combined-transaction storage-proof soft limits live in the runtime config (`core/parameters`) and are read during re-execution via [runtime-execution](runtime-execution.md).

## Invariants & failure modes

- **> 2/3 stake to endorse.** `has_enough_stake` requires `endorsed_stake >= total_stake*2/3 + 1` (`validator_assignment.rs:18`/`:22`); below threshold `compute_endorsement_state` clears the signature vector (`:81`) so the chunk cannot be included. Re-checked at block processing (`chunk_endorsement.rs:113`).
- **State-root binding.** Final validation binds the replayed `ChunkExtra` and outgoing-receipts root to the witness chunk header (`chunk_validation.rs:755`); the intermediate `post_state_root` checks (`:622`, `:738`) are explicitly described in-code as debug aids, not the authoritative check.
- **Receipts/transactions are self-proving.** `applied_receipts_hash` must match the receipts extracted from proofs (`:361`); `transactions` must merklize to the last new chunk's `tx_root` (`:375`); each source-receipt proof's shard ids and Merkle path are verified (`validate_receipt_proof`, `:529`); extraneous proofs are rejected (`:519`). Mismatch → `Error::InvalidChunkStateWitness`.
- **Implicit-transition count must match** between recomputed range and witness (`:662`), preventing a producer from hiding or inventing missing-chunk transitions.
- **Signature provenance.** Witness parts, contract accesses/requests/responses, and endorsements are all signed; validation rejects on bad signature or wrong signer (`validate.rs`, `chunk_endorsement.rs`). A contract-code responder must be a chunk producer for the shard (`validate.rs:371`/`:383`).
- **Relevance gating drops, not errors.** `validate_chunk_relevant` (`validate.rs:260`) returns `TooLate` (height ≤ final head, `:289`), `TooEarly` (> head + `MAX_HEIGHTS_AHEAD = 5`, `:22`/`:300`), or `UnknownEpochId` (`:318`); these are dropped silently, while `NotAChunkValidator` (`:246`) / `InvalidShardId` (`:275`) / bad-signature are hard errors (potential malicious behavior, `// TODO: ban sending peer`, `partial_witness_actor.rs:484`).
- **Orphan witnesses** outside `2..6` of head or over `max_orphan_witness_size` are not stored (`chunk_validation_actor.rs:239`/`:253`).
- **Size caps** reject oversized parts (`encoded_length` `validate.rs:83`, `part_size` `:107`) as `InvalidPartialChunkStateWitness`; decode failures surface as the same error ("failed to reed solomon decode witness parts, maybe malicious or corrupt data", `partial_witness_tracker.rs:466`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives/src/stateless_validation/state_witness.rs:96` | `ChunkStateWitness` | Versioned witness enum (V2 live). |
| `core/primitives/src/stateless_validation/state_witness.rs:104` | `ChunkStateWitnessV2` | Witness fields: main + implicit transitions, source receipt proofs, transactions. |
| `core/primitives/src/stateless_validation/state_witness.rs:282` | `ChunkStateTransition` | `base_state` proof + `post_state_root`. |
| `core/primitives/src/stateless_validation/partial_witness.rs:254` | `VersionedPartialEncodedStateWitness` | V1-only at v86; V2 inert. |
| `core/primitives/src/stateless_validation/chunk_endorsement.rs:22` | `ChunkEndorsement::new` | Builds V2 endorsement (inner+sig in header, metadata for routing). |
| `core/primitives/src/stateless_validation/validator_assignment.rs:60` | `compute_endorsement_state` | > 2/3 stake aggregation. |
| `core/primitives/src/stateless_validation/contract_distribution.rs:254` | `ContractCodeResponse::encode` | Signed V2 iff `SignedContractCodeResponse`. |
| `chain/chain/src/stateless_validation/state_witness.rs:45` | `create_state_witness` | Producer assembles the witness. |
| `chain/chain/src/stateless_validation/state_witness.rs:100` | `collect_state_transition_data` | Walks back, collects main + implicit transitions. |
| `chain/chain/src/stateless_validation/state_witness.rs:264` | `collect_source_receipt_proofs` | Gathers incoming-receipt proofs for the prev chunk. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:310` | `pre_validate_chunk_state_witness` | Cheap chain-relative checks + receipt/tx hashing. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:144` | `get_state_witness_block_range` | Classifies blocks by new-chunks-seen (0/1/2). |
| `chain/chain/src/stateless_validation/chunk_validation.rs:449` | `validate_source_receipt_proofs` | Extracts/validates receipts to apply. |
| `chain/chain/src/stateless_validation/chunk_validation.rs:565` | `validate_chunk_state_witness_impl` | Re-executes main + implicit transitions, binds to header. |
| `chain/chain/src/stateless_validation/chunk_endorsement.rs:19` | `validate_chunk_endorsements_in_block` | Block-level endorsement + bitmap validation. |
| `chain/client/src/stateless_validation/state_witness_producer.rs:15` | `send_chunk_state_witness_to_chunk_validators` | Production entry; self-endorse shortcut. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:205` | `handle_distribute_state_witness_request` | 3-stream priority distribution. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:1016` | `generate_state_witness_parts` | Reed-Solomon split, one part per validator. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs:862` | `handle_contract_code_request` | Serves requested contract code from the trie. |
| `chain/client/src/stateless_validation/partial_witness/partial_witness_tracker.rs:273` | `CacheEntry::try_finalize` | Reconstruct witness once parts+contracts ready. |
| `chain/client/src/stateless_validation/chunk_validation_actor.rs:359` | `start_validating_chunk` | Pre-validate → chunk-extra shortcut → spawn validate → endorse. |
| `chain/client/src/stateless_validation/chunk_validator/mod.rs:22` | `send_chunk_endorsement_to_block_producers` | Build + gossip endorsement to next 5 BPs. |
| `chain/client/src/stateless_validation/chunk_endorsement.rs:82` | `collect_chunk_endorsements` | Aggregate cached endorsements for inclusion. |
| `chain/client/src/stateless_validation/validate.rs:260` | `validate_chunk_relevant` | Relevance gating (TooLate/TooEarly/UnknownEpochId). |
| `chain/client/src/stateless_validation/validate.rs:64` | `validate_partial_encoded_state_witness` | Part size/ord/signature checks; drops V2. |
| `chain/client/src/stateless_validation/partial_witness/encoding.rs:6` | `WITNESS_RATIO_DATA_PARTS` | 0.6 data-parts ratio for erasure coding. |
| `core/primitives-core/src/version.rs:447` | `SignedContractCodeResponse` | Activates at 85. |
| `core/primitives-core/src/version.rs:449` | `EnforcePerReceiptStorageProofLimit` | PV-86 feature on this release. |
| `core/primitives-core/src/version.rs:401` | `EarlyKickout` | Nightly @ 152; gates witness V2. |

## Open questions

- The `ChunkStateWitnessV2.chunk_header` field is marked `// TODO(stateless_validation): Deprecate this field in the next version of the state witness` (`state_witness.rs:112`) — it is currently load-bearing for endorsement (the validator must know the chunk hash it signs), so removal is not yet realized at v86.
- The resharding-boundary caveat on `collect_source_receipt_proofs` (`state_witness.rs:260`, "generates invalid proofs on resharding boundaries") is a documented TODO; the post-verification filtering it describes (via `filter_incoming_receipts_for_shard` at validation time, `chunk_validation.rs:501`) was not traced end-to-end across a live split here.
