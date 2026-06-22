# Data structures & serialization

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `core/primitives/src/block.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block_body.rs`, `core/primitives/src/sharding.rs`, `core/primitives/src/transaction.rs`, `core/primitives/src/receipt.rs`, `core/primitives-core/src/account.rs`, `core/primitives/src/state.rs`, `core/primitives-core/src/hash.rs`, `core/primitives/src/merkle.rs`

## Role

This component defines the shared wire/storage data structures of the protocol and how they are serialized and hashed. These types are produced and consumed by nearly every other component: blocks and headers ([chain-block-processing](chain-block-processing.md), [consensus-finality](consensus-finality.md)), chunks ([sharding-chunks](sharding-chunks.md)), transactions and receipts ([runtime-execution](runtime-execution.md), [cross-shard-congestion](cross-shard-congestion.md)), accounts and access keys ([accounts-keys](accounts-keys.md)), and state values ([state-storage](state-storage.md)). This spec describes their *shape, versioning, and hashing*, not the algorithms that consume them — those are in the linked sibling specs. The central serialization rule: **Borsh** is the canonical, consensus-relevant encoding (everything hashed/stored uses it deterministically), **Proto** wraps borsh bytes for the network layer ([networking-p2p](networking-p2p.md)), and **JSON/base58/base64** is the RPC view layer only. Versioning is implemented almost everywhere as an explicit Rust enum (`V1`/`V2`/…), with a few types using hand-rolled tag-byte discrimination for backward compatibility.

## Key data structures

### Hashing primitives

- **`CryptoHash`** — `core/primitives-core/src/hash.rs:26` — a `pub [u8; 32]` newtype, the universal 256-bit ID. `hash_bytes` (`:36`) is raw SHA-256; `hash_borsh` (`:46`) SHA-256s the borsh serialization of a value; `hash_borsh_iter` (`:59`) prefixes a `u32` LE length then each element. Serde renders it as base58 (`:137`). `StateRoot` and `MerkleHash` are type aliases for `CryptoHash` (`core/primitives/src/types.rs:24`, `core/primitives-core/src/types.rs:9`).
- **`combine_hash`** — `core/primitives/src/merkle.rs:42` — `hash_borsh((hash1, hash2))`; the binary node combiner used for all Merkle trees and for the block/chunk hash construction.
- **`merklize`** — `core/primitives/src/merkle.rs:47` — builds a Merkle root + per-leaf `MerklePath` over borsh-hashed leaves; empty input → `CryptoHash::default()`. `verify_path` (`:113`) re-folds a path against a root.

### Block

- **`Block`** — `core/primitives/src/block.rs:90` — `#[borsh(use_discriminant=true)] #[repr(u8)]` enum `BlockV1(0) | BlockV2(1) | BlockV3(2) | BlockV4(3)`. V1/V2 inline `chunks` + deprecated `challenges` + VRF; **V3** split content into `BlockBodyV1`; **V4** (current) is `{ header: BlockHeader, body: BlockBody }` (`:80`). `new_block` (`:98`) panics if asked to wrap a `BlockBodyV1` and always emits `BlockV4` for `BlockBody::V2`/`V3`.
- **`BlockBody`** — `core/primitives/src/block_body.rs:128` — `#[repr(u8)]` enum `V1(BlockBodyV1)=0 | V2(BlockBodyV2)=1 | V3(SpiceBlockBodyV3)=2`. Carries `chunks: Vec<ShardChunkHeader>`, `vrf_value`/`vrf_proof`, and (V2) `chunk_endorsements: Vec<ChunkEndorsementSignatures>` indexed `[shard][validator]` of `Option<Box<Signature>>` (`:47`,`:50`). V3 is spice-only and replaces endorsements with `SpiceCoreStatements` (`:56`). `compute_hash` (`:212`): V1 hashes the inner struct directly (legacy), V2/V3 `hash_borsh(self)` (i.e. include the enum discriminant).
- **`BlockHeader`** — `core/primitives/src/block_header.rs:770` — non-discriminant enum `BlockHeaderV1..V7`. Each `BlockHeaderV{N}` (e.g. `:677`) is `{ prev_hash, inner_lite: BlockHeaderInnerLite, inner_rest: BlockHeaderInnerRest{V…}, signature, #[borsh(skip)] hash }`. The `hash` is recomputed on deserialization via `#[borsh(init=init)]` (`:744`).
- **`BlockHeaderInnerLite`** — `:30` — the light-client-relevant fields: `height`, `epoch_id`, `next_epoch_id`, `prev_state_root`, `prev_outcome_root`, `timestamp`, `next_bp_hash`, `block_merkle_root`.
- **`BlockHeaderInnerRestV7`** — `:366` (the version `new_impl` builds when `Spice` is on) / **`…RestV6`** — `:308` (built when `DynamicResharding` is on — the version current at PV86) / **`…RestV5`** — `:249` (pre-resharding). RestV6 adds `shard_split: Option<(ShardId, AccountId)>` and removes the deprecated `challenges_root`/`challenges_result`; RestV5 added `chunk_endorsements: ChunkEndorsementsBitmap`; RestV4 added `block_body_hash`. All carry `prev_chunk_outgoing_receipts_root`, `chunk_headers_root`, `chunk_tx_root`, `random_value`, `prev_validator_proposals`, `chunk_mask`, `next_gas_price`, `total_supply`, `last_final_block`, `last_ds_final_block`, `block_ordinal`, `prev_height`, `epoch_sync_data_hash`, `approvals: Vec<Option<Box<Signature>>>`, `latest_protocol_version`.
- **`Approval` / `ApprovalInner`** — `:442` / `:433` — a block producer's `Endorsement(parent_hash)` or `Skip(parent_height)` plus `target_height`, `signature`, `account_id`. The signed payload is `borsh(inner) ++ target_height.to_le_bytes()` (`get_data_for_sig`, `:492`).

### Chunk

- **`ShardChunkHeader`** — `core/primitives/src/sharding.rs:391` — `#[repr(u8)]` enum `V1=0 | V2=1 | V3=2`. V1/V2 wrap `ShardChunkHeaderInnerV1`; **V3** (current) wraps the versioned `ShardChunkHeaderInner`. Each header carries `{ inner, height_included, signature, #[borsh(skip)] hash: ChunkHash }`.
- **`ShardChunkHeaderInner`** — `core/primitives/src/sharding/shard_chunk_header_inner.rs:18` — `#[repr(u8)]` enum `V1..V6`. V4 is the baseline accepted at PV86; **V5** (built when `DynamicResharding` is on) adds `proposed_split: Option<TrieSplit>`; V6 is spice tx-only. Common fields: `prev_block_hash`, `prev_state_root`, `prev_outcome_root`, `encoded_merkle_root`, `encoded_length`, `height_created`, `shard_id`, `prev_gas_used`, `gas_limit`, `prev_balance_burnt`, `prev_outgoing_receipts_root`, `tx_root`, `prev_validator_proposals`, `congestion_info`, `bandwidth_requests`. `validate_version` (`sharding.rs:606`) gates which inner version is legal for a protocol version.
- **`ChunkHash`** — `sharding.rs:39` — newtype over `CryptoHash`; the chunk's content-commitment ID (see Behavior).
- **`ShardChunk`** — `sharding.rs:1025` — `V1=0 | V2=1` enum; the *decoded* chunk `{ chunk_hash, header, transactions: Vec<SignedTransaction>, prev_outgoing_receipts: Vec<Receipt> }`.
- **`EncodedShardChunk`** — `sharding.rs:1246` — `V1=0 | V2=1`; header + `EncodedShardChunkBody { parts: Vec<Option<Box<[u8]>>> }` (`:1201`). The body is the Reed–Solomon erasure encoding of `TransactionReceipt(Vec<SignedTransaction>, Vec<Receipt>)` (`:1229`).
- **`PartialEncodedChunk`** — `sharding.rs:780` — `V1=0 | V2=1`; the over-the-wire fragment: header + `parts: Vec<PartialEncodedChunkPart>` (each `{ part_ord, part: Box<[u8]>, merkle_proof }`, `:990`) + `prev_outgoing_receipts: Vec<ReceiptProof>`.
- **`ReceiptProof`** — `sharding.rs:963` — `(Vec<Receipt>, ShardProof)`; verified against a receipts root by hashing `ReceiptList(to_shard_id, &receipts)` and `verify_path` (`:981`). It has a total `Ord` by `(from_shard_id, to_shard_id)` to keep messages deterministic (`:973`).

### Transaction

- **`SignedTransaction`** — `core/primitives/src/transaction.rs:407` — `{ transaction: Transaction, signature, #[borsh(skip)] hash, #[borsh(skip)] size }`; `init` (`:424`) caches `hash`/`size` = the borsh-hash and byte length of the inner `Transaction`. Serde encodes the *whole* signed tx as base64-of-borsh (`:497`).
- **`Transaction`** — `:148` — enum `V0(TransactionV0) | V1(TransactionV1)` with a **hand-rolled** Borsh impl (`:219`/`:232`): V0 serializes with no tag (legacy); V1 is prefixed with tag byte `1`. Deserialization peeks two bytes — second byte `0` ⇒ V0 (the AccountId length's high byte is always 0), else first byte `1` ⇒ V1 (`:236`–`:269`).
- **`TransactionV0`** — `:33` — `{ signer_id, public_key, nonce: Nonce(u64), receiver_id, block_hash, actions: Vec<Action> }`.
- **`TransactionV1`** — `:119` — adds `nonce: TransactionNonce` (`:63`, either `Nonce` or `GasKeyNonce { nonce, nonce_index }`) and `nonce_mode: NonceMode` (`Monotonic`/`Strict`, `:110`). V1 requires `GasKeys`; `Strict` requires `StrictNonce` (gated in `check_valid_for_config`, `:307`).
- **`ValidatedTransaction`** — `:277` — newtype proving size limits + signature were checked (`new`/`check_valid_for_config`, `:281`/`:302`).
- **`ExecutionOutcome` / `ExecutionOutcomeWithId`** — `:625` / `:715` — per-tx-or-receipt result: `logs`, `receipt_ids`, `gas_burnt`, `tokens_burnt`, `executor_id`, `status: ExecutionStatus`, `metadata: ExecutionMetadata`; `id` is the tx hash (for a tx) or the receipt id (for a receipt). `ExecutionStatus` (`:543`, `#[repr(u8)]`) is `Unknown(0)|Failure(1)|SuccessValue(2)|SuccessReceiptId(3)`.
- **`ExecutionMetadata`** — `:665` — `#[repr(u8)]` `V1(0)|V2(1)|V3(2)|V4(3)`. V4 (`ExecutionMetadataV4`, `:684`) adds a per-action `Vec<AccountContract>` recording the contract attached to the receiver before each action ran; gated by `ExecutionMetadataV4`.

### Receipt

- **`Receipt`** — `core/primitives/src/receipt.rs:73` — enum with only `V0(ReceiptV0)`; **hand-rolled** Borsh (`:220`/`:228`) serializes/deserializes V0 directly (no tag) for forward room. `ReceiptV0` (`:56`): `{ predecessor_id, receiver_id, receipt_id, receipt: ReceiptEnum }`.
- **`ReceiptEnum`** — `:566` — `#[repr(u8)]` `Action(0)|Data(1)|PromiseYield(2)|PromiseResume(3)|GlobalContractDistribution(4)|ActionV2(5)|PromiseYieldV2(6)`. `Action`/`PromiseYield` hold `ActionReceipt`; `ActionV2`/`PromiseYieldV2` hold `ActionReceiptV2` (`:622`) which adds `refund_to: Option<AccountId>`. `Data`/`PromiseResume` hold `DataReceipt` (`:827`, `{ data_id, data: Option<Vec<u8>> }`).
- **`ActionReceipt`** — `:590` — `{ signer_id, signer_public_key, gas_price, output_data_receivers: Vec<DataReceiver>, input_data_ids: Vec<CryptoHash>, actions: Vec<Action> }`.
- **`VersionedReceiptEnum` / `VersionedActionReceipt`** — `:746` / `:646` — `Cow`-based read wrappers giving uniform field access over V1/V2 action receipts without cloning.
- **`StateStoredReceipt`** — `:96` — enum `V0|V1` wrapping `{ receipt: Cow<Receipt>, metadata: StateStoredReceiptMetadata }`; metadata (`:123`) is `{ congestion_gas, congestion_size }`. **Hand-rolled** Borsh (`:234`/`:258`): two `STATE_STORED_RECEIPT_TAG` (`0xFF`) bytes + a version byte. New receipts are written as V1 (`new_owned`, `:180`); V1 vs V0 signals whether outgoing-buffer metadata must be updated (`should_update_outgoing_metadatas`, `:212`).
- **`ReceiptOrStateStoredReceipt`** — `:149` — migration vehicle: its `deserialize` (`:311`) peeks the first two bytes — `0xFF,0xFF` ⇒ `StateStoredReceipt`, else a bare `Receipt`. Lets state hold either representation transparently.

### Account & access keys (shape only — semantics in [accounts-keys](accounts-keys.md))

- **`Account`** — `core/primitives-core/src/account.rs:39` — enum `V1(AccountV1)|V2(AccountV2)`. `AccountV1` (`:55`): `{ amount, locked, code_hash, storage_usage }`. `AccountV2` (`:146`) replaces `code_hash` with `contract: AccountContract` (`:89`, `None|Local(hash)|Global(hash)|GlobalByAccount(id)`). Borsh is **hand-rolled** (`:410`/`:437`): V1 serializes unversioned (legacy compat); V2 is prefixed with a `SERIALIZATION_SENTINEL = Balance::MAX` (`:164`) where the `amount` field would be, then a `BorshVersionedAccount` enum. Serde uses a flat `SerdeAccount` shim (`:307`) with a `version` field for JSON. `Account::new` (`:166`) keeps V1 when the contract is `None`/`Local` and only upgrades to V2 for global contracts.
- **`AccessKey`** — `:467` — `{ nonce: Nonce, permission: AccessKeyPermission }`. `AccessKeyPermission` (`:575`): `FunctionCall | FullAccess | GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission) | GasKeyFullAccess(GasKeyInfo)`; `GasKeyInfo` (`:546`) is `{ balance, num_nonces }`. (No explicit version enum; the gas-key variants are the `GasKeys` extension.)

### State values

- **`ValueRef`** — `core/primitives/src/state.rs:46` — `{ length: u32, hash: CryptoHash }`, the 36-byte reference to a trie value.
- **`FlatStateValue`** — `:107` — `#[repr(u8)]` `Ref(ValueRef)=0 | Inlined(Vec<u8>)=1`; inlines small values (`should_inline`, `:149`).
- **`PartialState`** — `:14` — `#[repr(u8)]` `TrieValues(Vec<TrieValue>)=0`; the recorded trie nodes for proofs. See [state-storage](state-storage.md).

## Behavior

### Serialization format split (why it matters for hashing)

1. **Borsh is canonical.** Every consensus-relevant ID is a hash of a *borsh* serialization, and Borsh is deterministic (fixed field order, LE integers, length-prefixed `Vec`/`String`, fixed enum tag bytes). This is why hashing must use the exact byte layout — any field reorder or version-tag change changes the hash. `CryptoHash::hash_borsh` (`core/primitives-core/src/hash.rs:46`) is the single funnel.
2. **Versioned enums carry a tag byte.** For enums marked `#[borsh(use_discriminant=true)] #[repr(u8)]` (e.g. `Block` `block.rs:88`, `ReceiptEnum` `receipt.rs:564`, `ShardChunkHeaderInner`), the discriminant is the leading byte of the encoding and is part of any hash of the whole enum. This is why `BlockBody::compute_hash` (`block_body.rs:212`) is *version-sensitive*: V1 hashes the inner struct (no tag, legacy), V2/V3 hash the enum (tag included).
3. **Hand-rolled Borsh for backward compatibility.** `Transaction` (`transaction.rs:219`), `Receipt` (`receipt.rs:220`), `Account` (`account.rs:410`), `StateStoredReceipt`/`ReceiptOrStateStoredReceipt` (`receipt.rs:234`,`:311`) do *not* use a derived leading tag. They reuse a structural property — the second byte of a borsh `AccountId` length is always 0 — or a reserved sentinel (`0xFF,0xFF` / `Balance::MAX`) to distinguish versions while keeping the legacy V0/V1 bytes decodable. New variants must preserve this.
4. **Proto wraps borsh on the network.** `chain/network/src/network_protocol/network.proto` messages (`Block`, `BlockHeader`, `SignedTransaction`, `PartialEncodedChunk`, …) are each just `bytes borsh = 1` — the protobuf layer never re-encodes fields, it transports the borsh blob ([networking-p2p](networking-p2p.md)).
5. **JSON/base58/base64 is the view layer.** `CryptoHash` serde → base58 (`hash.rs:137`); `SignedTransaction` serde → base64-of-borsh (`transaction.rs:497`); `Account` serde → the flat `SerdeAccount`. These are for RPC and never feed hashing.

### Block hash construction

`BlockHeader::compute_hash` (`block_header.rs:787`) is `combine_hash(compute_inner_hash(inner_lite, inner_rest), prev_hash)`, where `compute_inner_hash` (`:781`) = `combine_hash(hash(borsh(inner_lite)), hash(borsh(inner_rest)))`. Splitting `inner_lite` from `inner_rest` lets light clients verify the lite half without the full rest. The block producer signs `hash.as_ref()` directly (`compute_hash_and_sign`, `:1096`). The header version emitted by `new_impl` (`:939`) is chosen by feature gate: `Spice` ⇒ V7, else `DynamicResharding` ⇒ V6, else V5. `block_body_hash` in `inner_rest` (V4+) binds the header to its body via `BlockBody::compute_hash`.

### Chunk hash construction

`ChunkHash` commits to the chunk's content through the header. For V3 headers, `ShardChunkHeaderV3::compute_hash` (`sharding.rs:291`) = `ChunkHash(combine_hash(hash(borsh(inner)), inner.encoded_merkle_root()))`. The `encoded_merkle_root` it folds in is the Merkle root over the Reed–Solomon-encoded body parts (`EncodedShardChunkBody::get_merkle_hash_and_paths`, `sharding.rs:1218`), so the header transitively commits to the transactions+receipts content while staying small. V1 headers hash the inner alone (`:733`); V2 fold in the merkle root like V3 but over the V1 inner (`:191`). The producer signs `hash.as_ref()` (`from_inner`, `:381`).

### ID/hash derivation summary

- **Block hash** — `combine_hash(inner_hash, prev_hash)` as above.
- **Chunk hash** — `combine_hash(hash(borsh(inner)), encoded_merkle_root)` as above.
- **Transaction hash** — `Transaction::get_hash_and_size` (`transaction.rs:141`) = `hash(borsh(transaction))`; this is *also* the signed payload and the tx's `ExecutionOutcome` id.
- **Receipt id** — not a content hash; assigned deterministically at creation. From a tx: `create_receipt_id_from_transaction` (`utils.rs:270`) = `hash(tx_hash ++ block_height_le ++ 0)`. From a parent receipt: `create_receipt_id_from_receipt_id` (`:278`) salts with the child index. The salt/height construction (`create_hash_index`, `:328`) guarantees uniqueness across forks and across children. See [runtime-execution](runtime-execution.md).
- **Outcome→root leaves** — `ExecutionOutcomeWithId::to_hashes` (`transaction.rs:746`) = `[id, hash_borsh(PartialExecutionOutcome), hash(each log)]`; these leaves are merklized into the block's `prev_outcome_root`.
- **Receipt/tx/header roots** — `merklize` (`merkle.rs:47`) over the respective borsh-hashed item lists produces `chunk_tx_root`, `prev_chunk_outgoing_receipts_root`, `chunk_headers_root` carried in `inner_rest`.

### How versioning enables compatibility

- **Forward room without breaking old decoders:** the hand-rolled discriminators (tx, receipt, account, state-stored receipt) let a new version be added while every existing serialized blob still decodes to its original variant. A newer binary recognizes the new tag; an older one fails cleanly (e.g. `invalid transaction version tag`, `transaction.rs:268`).
- **Derived `#[repr(u8)]` enums** add variants by appending discriminants; old variants keep their byte value (e.g. `ReceiptEnum::ActionV2 = 5` appended after the original 0–4).
- **`#[deprecated]` fields are kept, not removed** (e.g. `challenges`/`challenges_root`/`challenges_result` in older header/body versions) so old bytes still parse; new header versions (V6+) drop them by being a new struct, leaving the wire format of old blocks intact.

## Interactions

- **Produces / is consumed by:** essentially every component. `Block`/`BlockHeader` flow to [chain-block-processing](chain-block-processing.md) and [consensus-finality](consensus-finality.md); chunk types to [sharding-chunks](sharding-chunks.md) and [stateless-validation](stateless-validation.md); `SignedTransaction`/`Receipt`/`ExecutionOutcome` to [runtime-execution](runtime-execution.md); `Account`/`AccessKey` semantics to [accounts-keys](accounts-keys.md); `CongestionInfo`/`BandwidthRequests` in chunk headers to [cross-shard-congestion](cross-shard-congestion.md); state value types to [state-storage](state-storage.md).
- **Network transport:** all of these cross the wire as borsh bytes inside proto wrappers ([networking-p2p](networking-p2p.md)).
- **Version gating source:** the feature flags that select struct versions all come from [protocol-versioning](protocol-versioning.md) (`core/primitives-core/src/version.rs`).

## Protocol-version-gated behavior

Activation versions read from `core/primitives-core/src/version.rs:453`–`584` (`ProtocolFeature::protocol_version`); `enabled` is `version >= activation` (`:587`). At **PV86** the features active for this component are:

| Feature | Activates at | Effect on data structures |
|---|---|---|
| `GasKeys` | 85 (`version.rs:557`) | Enables `Transaction::V1`/`TransactionV1` with `TransactionNonce::GasKeyNonce` and the `GasKey*` `AccessKeyPermission` variants (`transaction.rs:118`, `account.rs:582`). Pre-85, a V1 tx is rejected `InvalidTransactionVersion` (`transaction.rs:307`). |
| `StrictNonce` | 85 (`version.rs:561`) | Enables `NonceMode::Strict` on `TransactionV1`; rejected pre-85 (`transaction.rs:312`). |
| `PostQuantumSignatures` | 85 (`version.rs:562`) | Admits ML-DSA-65 keys/signatures in txs and actions; also switches the size charged against limits from `get_size` (body only) to `wire_size` (body+signature) (`transaction.rs:458`,`:326`). |
| `DynamicResharding` | 85 (`version.rs:559`) | Selects `BlockHeaderInnerRestV6` (adds `shard_split`, drops challenges) and `ShardChunkHeaderInnerV5` (adds `proposed_split`) instead of RestV5/InnerV4 (`block_header.rs:1027`, `sharding.rs:318`). |
| `ExecutionMetadataV4` | 85 (`version.rs:566`) | Allows `ExecutionMetadata::V4` carrying per-action `AccountContract` (`transaction.rs:678`). |

Long-active (deprecated flag, baseline at PV86, well below `MIN_SUPPORTED_PROTOCOL_VERSION = 83`): `ChunkValidation`/`StatelessValidation` (block→V4, body→V2 with endorsements, header→V5 with `chunk_endorsements` bitmap), `StateStoredReceipt` (V0/V1 state-stored receipts), `GlobalContracts` + `DeterministicAccountIds` (`Account::V2`/`AccountContract`). The `Spice` feature (180, `version.rs:582`) — `BlockV4` with `BlockBody::V3`/`SpiceBlockBodyV3`, `BlockHeaderV7`/RestV7, `ShardChunkHeaderInnerV6` — is **not active at PV86** and only compiled under the spice build (`PROTOCOL_VERSION` resolves to `STABLE_PROTOCOL_VERSION = 86` in the default build, `version.rs:636`,`:624`).

## Invariants & failure modes

- **Borsh determinism is consensus-critical.** Because IDs are borsh-hashes, any non-deterministic or reordered serialization would fork the chain. Enforced structurally by Borsh; the `#[borsh(skip)]` cached `hash`/`size` fields are recomputed via `init` on every deserialize (`transaction.rs:424`, `block_header.rs:524`) so they cannot be forged on the wire.
- **Version-tag discrimination must stay collision-free.** The tx/receipt/account decoders rely on "second byte of an AccountId length is 0" and reserved sentinels; a future version that breaks this would silently mis-decode. Asserted by roundtrip tests (`transaction.rs:919`, `receipt.rs:1219`, `account.rs:697`).
- **`Account` can't hold both local and global contract code:** serde deserialize rejects a code_hash together with a global field, and rejects two global-contract fields (`account.rs:328`–`:342`).
- **Invalid version bytes error, don't panic:** unknown tx tag → `invalid transaction version tag` (`transaction.rs:268`); bad state-stored tag/version → `InvalidData` (`receipt.rs:270`,`:288`).
- **Chunk header version must match protocol version:** `ShardChunkHeader::validate_version` (`sharding.rs:606`) returns `BadHeaderForProtocolVersionError` (`:694`) if an inner version is illegal for the epoch's protocol version (e.g. V5 requires `DynamicResharding`, V6 requires `Spice`).
- **Spice-only block construction guards:** `Block::new_block` panics if handed a `BlockBodyV1` (`block.rs:103`); `BlockHeader::new_impl` panics if the chunk-endorsement bitmap is missing for V5+ or if spice fields are missing when `Spice` is on (`block_header.rs:983`,`:990`).
- **Header→body binding:** `block_body_hash` in `inner_rest` (V4+) must equal `BlockBody::compute_hash`; verified during block validation ([chain-block-processing](chain-block-processing.md)).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/hash.rs:46` | `CryptoHash::hash_borsh` | The single borsh-then-SHA256 hashing funnel. |
| `core/primitives/src/merkle.rs:42` | `combine_hash` | Binary Merkle/parent-hash combiner `hash_borsh((a,b))`. |
| `core/primitives/src/merkle.rs:47` | `merklize` | Builds Merkle root + paths over borsh-hashed leaves. |
| `core/primitives/src/block.rs:90` | `Block` | Versioned block enum V1–V4; V4 is `{header, body}`. |
| `core/primitives/src/block.rs:98` | `Block::new_block` | Emits BlockV4; panics on BlockBodyV1. |
| `core/primitives/src/block_body.rs:128` | `BlockBody` | V1/V2/V3 body; V2 holds chunk endorsements. |
| `core/primitives/src/block_body.rs:212` | `BlockBody::compute_hash` | Version-sensitive body hash (V1 inner vs V2/V3 enum). |
| `core/primitives/src/block_header.rs:770` | `BlockHeader` | V1–V7 header enum, hash cached via `init`. |
| `core/primitives/src/block_header.rs:787` | `BlockHeader::compute_hash` | `combine_hash(inner_hash, prev_hash)`. |
| `core/primitives/src/block_header.rs:939` | `BlockHeader::new_impl` | Feature-gated selection of RestV5/V6/V7. |
| `core/primitives/src/sharding.rs:391` | `ShardChunkHeader` | V1/V2/V3 chunk header enum. |
| `core/primitives/src/sharding.rs:291` | `ShardChunkHeaderV3::compute_hash` | `ChunkHash = combine_hash(hash(inner), encoded_merkle_root)`. |
| `core/primitives/src/sharding.rs:606` | `ShardChunkHeader::validate_version` | Gates inner version vs protocol version. |
| `core/primitives/src/sharding.rs:1246` | `EncodedShardChunk` | Header + Reed–Solomon-encoded part body. |
| `core/primitives/src/sharding/shard_chunk_header_inner.rs:18` | `ShardChunkHeaderInner` | V1–V6 inner; V5 adds `proposed_split`. |
| `core/primitives/src/transaction.rs:148` | `Transaction` | V0/V1 enum with hand-rolled tag-byte Borsh. |
| `core/primitives/src/transaction.rs:236` | `BorshDeserialize for Transaction` | Two-byte peek discriminates V0 vs V1. |
| `core/primitives/src/transaction.rs:141` | `Transaction::get_hash_and_size` | Tx hash = `hash(borsh(tx))`. |
| `core/primitives/src/transaction.rs:625` | `ExecutionOutcome` | Per tx/receipt result; status + metadata. |
| `core/primitives/src/transaction.rs:746` | `ExecutionOutcomeWithId::to_hashes` | Outcome→Merkle leaves. |
| `core/primitives/src/receipt.rs:73` | `Receipt` | V0-only enum, hand-rolled Borsh. |
| `core/primitives/src/receipt.rs:566` | `ReceiptEnum` | Action/Data/PromiseYield/…/ActionV2 variants. |
| `core/primitives/src/receipt.rs:96` | `StateStoredReceipt` | V0/V1 with `0xFF,0xFF` tag + congestion metadata. |
| `core/primitives/src/receipt.rs:311` | `BorshDeserialize for ReceiptOrStateStoredReceipt` | Migration peek: tag vs bare receipt. |
| `core/primitives-core/src/account.rs:39` | `Account` | V1/V2 enum, sentinel-based Borsh. |
| `core/primitives-core/src/account.rs:410` | `BorshDeserialize for Account` | `Balance::MAX` sentinel selects versioned form. |
| `core/primitives-core/src/account.rs:467` | `AccessKey` | nonce + permission (incl. gas-key variants). |
| `core/primitives/src/state.rs:46` | `ValueRef` | 36-byte trie value reference. |
| `core/primitives/src/utils.rs:328` | `create_hash_index` | Receipt-id derivation `hash(base ++ height ++ salt)`. |
| `chain/network/src/network_protocol/network.proto:99` | `message Block` (`bytes borsh`) | Proto wraps borsh blob, no re-encode. |
| `core/primitives-core/src/version.rs:496` | `ProtocolFeature::protocol_version` | Authoritative feature→version mapping. |

## Open questions

- The plan asks about `*V2`/`*V3` *struct-version* introductions per `ProtocolFeature`, but `version.rs` collapses many features into shared activation versions (e.g. the whole `=> 85` block) and most struct-version bumps map to *deprecated* features whose original names no longer appear (only `_Deprecated…` placeholders remain). The exact feature that first introduced, e.g., `ReceiptEnum::ActionV2`/`PromiseYieldV2` or `StateStoredReceiptV1` is not recoverable from `version.rs` alone at this commit; those features have been folded into deprecated entries. Resolved-enough for PV86 (all are active), but the historical mapping would need git archaeology beyond the pinned commit.
