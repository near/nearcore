# Data structures & serialization

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/primitives/src/block.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block_body.rs`, `core/primitives/src/sharding.rs`, `core/primitives/src/transaction.rs`, `core/primitives/src/receipt.rs`, `core/primitives/src/merkle.rs`, `core/primitives-core/src/account.rs`, `core/primitives-core/src/hash.rs`, `core/primitives/src/state.rs`

## Role

This component defines the shared wire/storage/consensus data structures of the protocol — blocks, chunks, transactions, receipts, accounts, hashes — and how they are serialized. It sits underneath essentially every other component: consensus and sync move `Block`/`BlockHeader`/`ShardChunkHeader`; the transaction pool and runtime move `SignedTransaction` and `Receipt`; state storage persists `Account`, `Receipt`, and trie values. The defining discipline here is **versioning by enum**: nearly every top-level type is an enum of `V0/V1/…` structs so the borsh wire format can evolve while old bytes remain parseable. This spec documents the *shape*, *versioning*, and *hashing* of these types; the algorithms that produce/consume them live in sibling specs (linked below) — notably [consensus](consensus.md), [chunk-production](chunk-production.md), [runtime-execution](runtime-execution.md), [state-storage](state-storage.md), and [accounts-keys](accounts-keys.md).

## Key data structures

### Hashing primitives

- **`CryptoHash`** — `core/primitives-core/src/hash.rs:26` — a `[u8; 32]` SHA-256 digest, the universal ID/commitment type. `hash_bytes` (`:36`) digests raw bytes; `hash_borsh` (`:46`) digests the borsh serialization of any `BorshSerialize` value; `hash_borsh_iter` (`:59`) prefixes a `u32` little-endian length then digests each item (equivalent to hashing a `Vec`). Serde renders it as base58 (`:137`), borsh as the raw 32 bytes.
- **`MerkleHash` / `StateRoot`** — `core/primitives-core/src/types.rs:9` and `core/primitives/src/types.rs:24` — both are type aliases for `CryptoHash`.
- **`merklize` / `combine_hash`** — `core/primitives/src/merkle.rs:47`, `:42` — `combine_hash` = `hash_borsh((h1, h2))`; `merklize` builds a binary Merkle tree over borsh-hashed leaves and returns `(root, per-leaf MerklePath)`. An empty array merklizes to `CryptoHash::default()` (`:48`). `MerklePathItem` (`:18`) carries a sibling `hash` + `Direction::{Left,Right}` (`:37`).

### Block

- **`Block`** — `core/primitives/src/block.rs:90` — versioned enum with explicit borsh discriminants (`#[borsh(use_discriminant = true)]`, `:88`): `BlockV1=0`, `BlockV2=1`, `BlockV3=2`, `BlockV4=3`. On this release all newly produced blocks are `BlockV4` (`:98` `new_block`, which panics if handed a `BlockBodyV1`).
- **`BlockV4`** — `:80` — `{ header: BlockHeader, body: BlockBody }`. `BlockV3` (`:73`) held a bare `BlockBodyV1`; V1/V2 (`:48`, `:60`) inlined `chunks`, deprecated `challenges`, and the VRF value/proof directly (no separate body). The `Chunks<'_>` view (`:703`) uniformly exposes chunk headers across all four layouts.
- **`Tip`** — `:857` — `{ height, last_block_hash, prev_block_hash, epoch_id, next_epoch_id }`; the persisted fork-head handle.

### Block header

- **`BlockHeader`** — `core/primitives/src/block_header.rs:770` — versioned enum `BlockHeaderV1..V7`. Each `BlockHeaderVN` (`:507`, …, `:677`) is `{ prev_hash, inner_lite: BlockHeaderInnerLite, inner_rest: BlockHeaderInnerRest*, signature, #[borsh(skip)] hash }`. The `hash` field is `#[borsh(skip)]` and recomputed on deserialize via `#[borsh(init=init)]` (`:506`, `:524`).
- **`BlockHeaderInnerLite`** — `:30` — the light-client-facing half: `{ height, epoch_id, next_epoch_id, prev_state_root, prev_outcome_root, timestamp, next_bp_hash, block_merkle_root }`.
- **`BlockHeaderInnerRest*`** — `:52` (V1) … `:366` (V7) — the remainder that gets hashed but is not shipped to light clients. Version deltas, in order: V1→V2 drops `chunks_included` (`:94`); V2→V3 adds `block_ordinal`/`prev_height`/`epoch_sync_data_hash` and switches to the new `ValidatorStake` (`:137`); V3→V4 adds `block_body_hash` (`:192`); V4→V5 adds the `chunk_endorsements: ChunkEndorsementsBitmap` (`:249`); V5→V6 adds `shard_split: Option<(ShardId, AccountId)>` and removes the deprecated `challenges_root`/`challenges_result` (`:308`); V6→V7 adds spice fields `prev_last_certified_block_epoch_id` + `spice_chunk_endorsement_stats` (`:366`).
- **`Approval` / `ApprovalInner`** — `:442`, `:433` — a block approval is `{ inner: Endorsement(prev_hash) | Skip(prev_height), target_height, signature, account_id }`. `approvals: Vec<Option<Box<Signature>>>` is embedded directly in `inner_rest`.

### Chunk

- **`ShardChunkHeader`** — `core/primitives/src/sharding.rs:391` — versioned enum `V1=0/V2=1/V3=2`. V3 (`:238`) wraps a versioned `ShardChunkHeaderInner`; V1/V2 (`:141`, `:155`) wrap the fixed `ShardChunkHeaderInnerV1`. Each carries `height_included`, a producer `signature`, and a `#[borsh(skip)] hash: ChunkHash` recomputed on init.
- **`ShardChunkHeaderInner`** — `core/primitives/src/sharding/shard_chunk_header_inner.rs:18` — versioned enum `V1..V6` (discriminants 0..5). Fields accrete across versions: V3 adds `congestion_info` (`:214`), V4 adds `bandwidth_requests` (`:227`), V5 adds `proposed_split: Option<TrieSplit>` for dynamic resharding (`sharding.rs:319`), V6 (`ShardChunkHeaderInnerV6SpiceTxOnly`) is a tx-only spice variant. The common fields include `prev_block_hash`, `prev_state_root`, `prev_outcome_root`, `encoded_merkle_root`, `encoded_length`, `height_created`, `shard_id`, `prev_gas_used`, `gas_limit`, `prev_balance_burnt`, `prev_outgoing_receipts_root`, `tx_root`, `prev_validator_proposals`.
- **`ChunkHash`** — `sharding.rs:39` — newtype over `CryptoHash`.
- **`ShardChunk`** (full/decoded chunk) — `:1025` — enum `V1=0/V2=1`; `ShardChunkV2` (`:1015`) = `{ chunk_hash, header: ShardChunkHeader, transactions: Vec<SignedTransaction>, prev_outgoing_receipts: Vec<Receipt> }`.
- **`EncodedShardChunk`** — `:1246` — enum `V1/V2`; the Reed-Solomon-erasure-encoded form actually gossiped as `PartialEncodedChunk` (`:780`), whose parts each carry a `merkle_proof` against `encoded_merkle_root`. `TransactionReceipt(Vec<SignedTransaction>, Vec<Receipt>)` (`:1229`) is the payload that gets encoded.

### Transaction

- **`Transaction`** — `core/primitives/src/transaction.rs:148` — enum `V0(TransactionV0) | V1(TransactionV1)` with a **hand-written** borsh impl (not a derived discriminant). `TransactionV0` (`:33`) = `{ signer_id, public_key, nonce: Nonce, receiver_id, block_hash, actions }`. `TransactionV1` (`:119`) replaces `nonce` with `TransactionNonce` (`:63`, either `Nonce` or `GasKeyNonce{nonce, nonce_index}`) and adds `nonce_mode: NonceMode` (`:110`, `Monotonic`/`Strict`).
- **`SignedTransaction`** — `:407` — `{ transaction: Transaction, signature, #[borsh(skip)] hash, #[borsh(skip)] size }`; `hash`/`size` recomputed via `#[borsh(init=init)]` from the borsh bytes of the inner `Transaction` (`:424`). Serde ser/de is base64 of the borsh bytes (`:497`, `:510`).
- **`ValidatedTransaction`** — `:277` — newtype wrapper proving signature + config checks passed (`:281`).
- **`ExecutionOutcome`** — `:625` — `{ logs, receipt_ids, gas_burnt, #[borsh(skip)] compute_usage, tokens_burnt, executor_id, status: ExecutionStatus, metadata: ExecutionMetadata }`. `ExecutionStatus` (`:543`) is a discriminant enum `Unknown=0/Failure=1/SuccessValue=2/SuccessReceiptId=3`. `ExecutionMetadata` (`:665`) is `V1=0/V2=1/V3=2/V4=3`; V4 (`:684`) adds a per-action `Vec<AccountContract>`. `ExecutionOutcomeWithId` (`:715`) pairs the outcome with its `id` (tx hash or receipt id).

### Receipt

- **`Receipt`** — `core/primitives/src/receipt.rs:74` — enum with only `V0(ReceiptV0)` present, using a **hand-written** borsh impl (`:221`, `:229`) that serializes `ReceiptV0` transparently; a future V1 is reserved via the second-byte discrimination trick documented in-source. `ReceiptV0` (`:57`) = `{ predecessor_id, receiver_id, receipt_id, receipt: ReceiptEnum }`.
- **`ReceiptEnum`** — `:567` — discriminant enum: `Action=0`, `Data=1`, `PromiseYield=2`, `PromiseResume=3`, `GlobalContractDistribution=4`, `ActionV2=5`, `PromiseYieldV2=6`. `ActionReceipt` (`:591`) carries `{signer_id, signer_public_key, gas_price, output_data_receivers, input_data_ids, actions}`; `ActionReceiptV2` (`:623`) additionally carries `refund_to: Option<AccountId>`. `DataReceipt` (`:828`) = `{data_id, data: Option<Vec<u8>>}`. `VersionedActionReceipt`/`VersionedReceiptEnum` (`:647`, `:747`) are `Cow`-based read wrappers that unify V1/V2 action receipts.
- **`GlobalContractDistributionReceipt`** — `:876` — enum `V1=0/V2=1`; V2 adds a `nonce: u64` for distribution idempotency (`:1036`).
- **`StateStoredReceipt`** — `:97` — enum `V0/V1` used when a receipt is persisted in state (delayed/buffered/yield queues). Custom borsh (`:235`): two leading `STATE_STORED_RECEIPT_TAG` (`0xFF`, `:134`) bytes then a version byte then the variant. `StateStoredReceiptMetadata` (`:124`) = `{congestion_gas, congestion_size}`. `ReceiptOrStateStoredReceipt` (`:150`) is a migration wrapper whose custom deserializer (`:312`) sniffs the first two bytes to decide between a plain `Receipt` and a `StateStoredReceipt`.

### Account & access keys (shape only; semantics → [accounts-keys](accounts-keys.md))

- **`Account`** — `core/primitives-core/src/account.rs:39` — enum `V1(AccountV1) | V2(AccountV2)` with **hand-written** borsh (`:437`). `AccountV1` (`:55`) = `{amount, locked, code_hash, storage_usage}` serialized *unversioned* (legacy layout, no tag). `AccountV2` is written behind a `SERIALIZATION_SENTINEL` u128 followed by a `BorshVersionedAccount` tag (`:405`, `:437`); on deserialize the first u128 is read and compared to the sentinel to disambiguate (`:410`). `AccountV2` replaces `code_hash` with an `AccountContract`.
- **`AccountContract`** — `:89` — `None | Local(CryptoHash) | Global(CryptoHash) | GlobalByAccount(AccountId)`.
- **`AccessKey`** — `:467` — `{nonce: Nonce, permission: AccessKeyPermission}`. `AccessKeyPermission` (`:575`) = `FunctionCall | FullAccess | GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission) | GasKeyFullAccess(GasKeyInfo)`.

### State/trie values

- **`ValueRef`** — `core/primitives/src/state.rs:46` — `{length: u32, hash: CryptoHash}`, the 36-byte value reference (`:62`). `FlatStateValue` (`:107`) = `Ref=0 | Inlined=1`. `PartialState::TrieValues` (`:14`) is the set of trie nodes/values used for proofs and state parts. Trie internals → [state-storage](state-storage.md).

## Behavior

### Serialization format selection

1. **Borsh** is used everywhere a byte layout must be *canonical* — hashing, consensus, storage, and the erasure-coded chunk payload. Borsh guarantees no two byte strings deserialize to the same value, which is why all hashed/committed types derive `BorshSerialize`/`BorshDeserialize` (see `docs/architecture/how/serialization.md`). Every such type also derives `ProtocolSchema` (`near_schema_checker_lib`) so a CI check catches accidental wire-format changes.
2. **Protobuf** is used only for the peer-to-peer transport envelope (`chain/network/src/network_protocol/network.proto`); the payloads it carries (blocks, chunks, txs) are borsh-encoded blobs inside proto fields. Network types are documented in [networking](networking.md).
3. **JSON** (serde) is the RPC/view representation. `CryptoHash` serializes as base58 (`hash.rs:137`); `SignedTransaction` serializes as base64 of its borsh bytes (`transaction.rs:497`); `DataReceipt.data` and global-contract `code` use base64 (`serde_as = Base64`). JSON is never hashed.

The distinction matters for hashing: only the borsh bytes are stable and canonical, so all commitments (block hash, chunk hash, tx hash, receipt/outcome roots) are defined over borsh, never JSON.

### How IDs and hashes are derived

1. **Transaction hash** — `transaction.rs:141` `Transaction::get_hash_and_size` = `hash(borsh(transaction))`. `SignedTransaction.hash` caches this (`:424`). The signature is *not* part of the hash; the signature signs the hash bytes (`:293`).
2. **Block body hash** — `block_body.rs:212` `BlockBody::compute_hash` = `hash_borsh(self)` for V2/V3 (hashes the whole versioned enum including the discriminant); `BlockBodyV1` (`:24`) hashed the bare struct.
3. **Block header hash** — `block_header.rs:787` `compute_hash(prev_hash, inner_lite, inner_rest)` = `combine_hash(compute_inner_hash(inner_lite, inner_rest), prev_hash)`, where `compute_inner_hash` (`:781`) = `combine_hash(hash(inner_lite_bytes), hash(inner_rest_bytes))`. The two-part split lets light clients verify `inner_lite` without downloading `inner_rest`. `block_body_hash` lives inside `inner_rest` (from V4 on), so the body is committed transitively through the header hash.
4. **Block hash** — the block's hash *is* its header hash (`block.rs:588`).
5. **Chunk hash** — `sharding.rs:291` `ShardChunkHeaderV3::compute_hash(inner)` = `ChunkHash(combine_hash(hash(borsh(inner)), inner.encoded_merkle_root()))`. `ShardChunkHeaderV2` does the same (`:191`); `ShardChunkHeaderV1` (`:733`) is just `hash(borsh(inner))` with no combine.
6. **Receipt / data / action ids** — `core/primitives/src/utils.rs:328` `create_hash_index(base, block_height, salt)` = `hash(base || block_height.to_le_bytes() || salt.to_le_bytes())`. Receipt id from a tx uses salt 0 (`:270`); from a parent receipt uses the receipt index (`:278`); action hashes count *down* from `u64::MAX` to avoid colliding with receipt-id salts (`:287`). Block *height* (not hash) is the fork-uniqueness salt because optimistic blocks lack a hash at id-generation time (`:326`).
7. **Merkle roots in the header** — computed from the chunk headers via the `Chunks` view (`block.rs:807`–`830`): `prev_state_root` = `merklize(prev_state_roots)`, plus receipts/headers/tx/outcome roots, each a `merklize` over the corresponding per-chunk field.
8. **Outcome hashing for proofs** — `transaction.rs:746` `ExecutionOutcomeWithId::to_hashes` produces `[id, hash_borsh(PartialExecutionOutcome), per-log hashes]`; `PartialExecutionOutcome` (`:573`) excludes logs/metadata so light clients hash a stable subset.

### Versioned deserialization dispatch

Some types use bespoke borsh dispatch instead of a leading discriminant, relying on the fact that an `AccountId`'s borsh length prefix has a zero second byte:

1. **`Transaction`** (`transaction.rs:236`) — reads two bytes: `u2 == 0` ⇒ V0 (both bytes pushed back); `u1 == 1 && u2 != 0` ⇒ V1 (consume the `1` tag). Any other `u1` errors with "invalid transaction version tag".
2. **`Receipt`** (`receipt.rs:229`) — currently always V0; the same second-byte reservation is documented for a future V1.
3. **`ReceiptOrStateStoredReceipt`** (`receipt.rs:312`) — sniffs for the two `0xFF` sentinel bytes to distinguish a state-stored receipt from a plain receipt.
4. **`Account`** (`account.rs:410`) — reads the leading u128; sentinel ⇒ versioned `BorshVersionedAccount`, otherwise legacy `AccountV1`.

For enum types that *do* use `#[borsh(use_discriminant = true)]` (Block, BlockBody, ShardChunkHeader, ShardChunkHeaderInner, ReceiptEnum, ExecutionStatus, …), the leading byte is the explicit variant tag pinned in the source, so those tags are part of the wire contract.

## Interactions

- **Consumes:** `Action`/`AccountId`/`Signature`/`PublicKey` primitives, `ValidatorStake`, `CongestionInfo`, `BandwidthRequests`, `ChunkEndorsementsBitmap`, `TrieSplit` — defined elsewhere and embedded in these structs.
- **Produced/consumed by:** [consensus](consensus.md) & [chain](chain.md) (Block/BlockHeader/Approval), [chunk-production](chunk-production.md) & [chunk-distribution](chunk-distribution.md) (ShardChunk/EncodedShardChunk/PartialEncodedChunk), [transactions](transactions.md) & [runtime-execution](runtime-execution.md) (SignedTransaction/Receipt/ExecutionOutcome), [state-storage](state-storage.md) (Account/StateStoredReceipt/ValueRef/FlatStateValue), [networking](networking.md) (proto envelope), [light-client](light-client.md) (`BlockHeaderInnerLite`, `PartialExecutionOutcome`).
- **Semantics deliberately not covered here:** account balance/storage rules and access-key permission logic → [accounts-keys](accounts-keys.md); trie structure → [state-storage](state-storage.md).

## Protocol-version-gated behavior

Activation versions verified against `core/primitives-core/src/version.rs` in this tree (stable head = PV 86, `MIN_SUPPORTED_PROTOCOL_VERSION = 83`, `version.rs:600`). Only features that change a data structure's shape/serialization are listed.

| Feature | Version | Effect on structures |
|---|---|---|
| `GasKeys` | 85 (`version.rs:562`) | Gates acceptance of `TransactionV1` (`transaction.rs:307`); enables `TransactionNonce::GasKeyNonce` and `AccessKeyPermission::GasKey*`. |
| `StrictNonce` | 85 (`version.rs:566`) | Gates `NonceMode::Strict` on `TransactionV1` (`transaction.rs:312`). |
| `PostQuantumSignatures` | 85 (`version.rs:567`) | Rejects ML-DSA-65 key/signature material pre-feature (`transaction.rs:326`); switches the size charged against limits from body-only `get_size` to full `wire_size` (`transaction.rs:458`). |
| `DelegateV2` | 85 (`version.rs:575`) | Adds the `Action::DelegateV2` variant (Action enum → [transactions](transactions.md)). |
| `ExecutionMetadataV4` | 85 (`version.rs:571`) | Producers emit `ExecutionMetadata::V4` with per-action `Vec<AccountContract>` (`transaction.rs:678`); new borsh discriminant `3`. |
| `GlobalContractDistributionNonce` | 83 (`version.rs:553`) | `GlobalContractDistributionReceipt::new` builds V2 (with `nonce`) instead of V1 (`receipt.rs:890`). |
| `DynamicResharding` | 85 (`version.rs:564`) | Header build path selects `BlockHeaderInnerRestV6` (`block_header.rs:1027`) and `ShardChunkHeaderInnerV5` with `proposed_split` (`sharding.rs:318`). |
| `Spice` | 180 / nightly-only (`version.rs:586`) | When enabled, headers become `BlockHeaderV7`/`BlockHeaderInnerRestV7` (`block_header.rs:989`), block bodies `SpiceBlockBodyV3`, and chunk inner `ShardChunkHeaderInnerV6SpiceTxOnly`. Not active at PV 86. |
| `EnforcePerReceiptStorageProofLimit` | 86 (`version.rs:576`) | The PV-86 gate on this release; affects witness/proof-size enforcement, not the shapes here. No struct-shape change. |

Notes for this release: the block-header/receipt versioning that predates PV 83 (V1–V5 headers, e.g. `BlockHeaderV5` from `ChunkEndorsementsInBlockHeader` at PV 72, and `StateStoredReceipt` at PV 72) is baked in below `MIN_SUPPORTED_PROTOCOL_VERSION` and no longer gated at runtime. There is **no `FixContractLoadingError`** in this tree; the related feature is `FixContractLoadingCost` and it is nightly-only (PV 129, `version.rs:579`). The MAINNET upgrade schedule votes PV 86 on 2026-07-20 (`core/primitives/src/version.rs:60`).

## Invariants & failure modes

- **Borsh canonicality:** every hashed/committed type round-trips through borsh with a unique byte representation; a schema drift is caught by the `protocol-schema-check` CI (`docs/architecture/how/serialization.md`). Adding/reordering enum variants without a new versioned type can silently corrupt stored/consensus data.
- **`#[borsh(skip)]` cached fields must be recomputed:** `BlockHeaderVN.hash`, `SignedTransaction.{hash,size}`, and `ShardChunkHeaderV*.hash` are skipped on the wire and rebuilt by the `#[borsh(init=…)]` hook on deserialize (`block_header.rs:524`, `transaction.rs:424`, `sharding.rs:187`). `ExecutionOutcome.compute_usage` is skipped and is `None` after a DB read (`transaction.rs:637`).
- **Version-tag validity:** `Transaction::deserialize_reader` errors with "invalid transaction version tag" for an unknown first byte (`transaction.rs:268`); `StateStoredReceipt::deserialize_reader` errors on a wrong sentinel or unknown version byte (`receipt.rs:265`, `:283`).
- **Block self-consistency:** `Block::check_validity` (`block.rs:602`) recomputes `prev_state_root`, receipts/headers/tx/outcome roots, and the `chunk_mask` from the embedded chunk headers and returns `BlockValidityError::{InvalidStateRoot, InvalidReceiptRoot, InvalidChunkHeaderRoot, InvalidTransactionRoot, InvalidChunkMask}` (`:39`) on mismatch.
- **New blocks are BlockV4:** `Block::new_block` panics ("attempted to include BlockBodyV1 in new protocol version") if constructed with a `BlockBodyV1` (`block.rs:103`).
- **Header build requires the endorsement bitmap:** `new_impl` panics if `chunk_endorsements` is `None` for V5+ headers (`block_header.rs:983`), and (under Spice) if the spice fields are absent (`:990`).
- **`merklize` of an empty slice** yields `CryptoHash::default()` (`merkle.rs:48`), so a block with no chunks has zero-valued roots.

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/hash.rs:26` | `CryptoHash` | 32-byte SHA-256 hash; base58 serde, raw borsh |
| `core/primitives-core/src/hash.rs:46` | `CryptoHash::hash_borsh` | hash of borsh bytes of any value |
| `core/primitives/src/merkle.rs:47` | `merklize` | binary Merkle tree over borsh-hashed leaves |
| `core/primitives/src/merkle.rs:42` | `combine_hash` | `hash_borsh((h1,h2))` internal-node combiner |
| `core/primitives/src/block.rs:90` | `Block` | versioned block enum, discriminants 0–3 |
| `core/primitives/src/block.rs:588` | `Block::hash` | block hash = header hash |
| `core/primitives/src/block.rs:602` | `Block::check_validity` | recompute+verify header roots vs chunks |
| `core/primitives/src/block_body.rs:128` | `BlockBody` | V1/V2/V3(spice) body enum |
| `core/primitives/src/block_body.rs:212` | `BlockBody::compute_hash` | body hash for header commitment |
| `core/primitives/src/block_header.rs:770` | `BlockHeader` | V1–V7 header enum |
| `core/primitives/src/block_header.rs:787` | `BlockHeader::compute_hash` | header/block hash formula |
| `core/primitives/src/block_header.rs:939` | `BlockHeader::new_impl` | selects V5/V6/V7 inner_rest by feature |
| `core/primitives/src/sharding.rs:391` | `ShardChunkHeader` | V1/V2/V3 chunk header enum |
| `core/primitives/src/sharding.rs:291` | `ShardChunkHeaderV3::compute_hash` | chunk hash = combine(inner hash, enc merkle root) |
| `core/primitives/src/sharding/shard_chunk_header_inner.rs:18` | `ShardChunkHeaderInner` | V1–V6 inner header enum |
| `core/primitives/src/sharding.rs:1025` | `ShardChunk` | full decoded chunk V1/V2 |
| `core/primitives/src/sharding.rs:780` | `PartialEncodedChunk` | gossiped erasure-coded chunk parts |
| `core/primitives/src/transaction.rs:148` | `Transaction` | V0/V1 enum, hand-written borsh |
| `core/primitives/src/transaction.rs:236` | `Transaction::deserialize_reader` | 2-byte version dispatch |
| `core/primitives/src/transaction.rs:407` | `SignedTransaction` | tx+signature+cached hash/size |
| `core/primitives/src/transaction.rs:625` | `ExecutionOutcome` | outcome fields incl. versioned metadata |
| `core/primitives/src/transaction.rs:665` | `ExecutionMetadata` | V1–V4 metadata enum |
| `core/primitives/src/transaction.rs:746` | `ExecutionOutcomeWithId::to_hashes` | proof hashes (partial outcome + logs) |
| `core/primitives/src/receipt.rs:74` | `Receipt` | V0-only enum, hand-written borsh |
| `core/primitives/src/receipt.rs:567` | `ReceiptEnum` | Action/Data/Yield/Resume/Global/V2 variants |
| `core/primitives/src/receipt.rs:97` | `StateStoredReceipt` | V0/V1 state-persisted receipt, tagged borsh |
| `core/primitives/src/receipt.rs:150` | `ReceiptOrStateStoredReceipt` | migration wrapper, sniffing deserializer |
| `core/primitives/src/utils.rs:328` | `create_hash_index` | receipt/data/action id derivation |
| `core/primitives-core/src/account.rs:39` | `Account` | V1/V2 enum, sentinel-tagged borsh |
| `core/primitives-core/src/account.rs:467` | `AccessKey` | nonce + permission |
| `core/primitives/src/state.rs:46` | `ValueRef` | 36-byte trie value reference |
| `core/primitives-core/src/version.rs:576` | `ProtocolFeature::EnforcePerReceiptStorageProofLimit` | PV-86 feature on this release |

## Open questions

None.
