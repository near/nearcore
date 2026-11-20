# Block and Block Header

The data structures for block and block headers are

```rust
pub enum Block {
    BlockV1(BlockV1) = 0,
    BlockV2(BlockV2) = 1,
    BlockV3(BlockV3) = 2,
    /// New blocks use it from protocol version 69.
    BlockV4(BlockV4) = 3,
}

pub struct BlockV4 {
    pub header: BlockHeader,
    pub body: BlockBody,
}
```

```rust
pub enum BlockBody {
    V1(BlockBodyV1) = 0,
    /// New blocks use it from protocol version 69.
    V2(BlockBodyV2) = 1,
    /// Used only with the Spice protocol feature, which no stable protocol version enables.
    V3(SpiceBlockBodyV3) = 2,
}

pub struct BlockBodyV2 {
    /// Headers of chunks in the block
    pub chunks: Vec<ShardChunkHeader>,
    /// Deprecated. Always empty.
    pub challenges: Vec<Challenge>,

    /// Data to confirm the correctness of randomness beacon output
    pub vrf_value: Value,
    pub vrf_proof: Proof,

    /// Chunk endorsements. For each shard, there is one entry for each of the shard's
    /// ordered chunk validators. An entry is `None` if that validator did not endorse the chunk.
    /// If the block has no new chunk for a shard, the list for that shard is empty.
    pub chunk_endorsements: Vec<ChunkEndorsementSignatures>,
}

pub type ChunkEndorsementSignatures = Vec<Option<Box<Signature>>>;
```

Blocks produced from protocol version 69 (nearcore 2.0.0) are `BlockV4` with a `BlockBodyV2` body.
Older blocks keep the version they were produced with.
Borsh writes the version of `Block` and `BlockBody` as a one-byte tag before the content.

The header field `block_body_hash` is `sha256(borsh(body))`.
For `BlockBodyV2` and later versions, `body` is the `BlockBody` enum, so the hash input starts with the version tag.

```rust
pub enum BlockHeader {
    BlockHeaderV1(BlockHeaderV1),
    BlockHeaderV2(BlockHeaderV2),
    BlockHeaderV3(BlockHeaderV3),
    BlockHeaderV4(BlockHeaderV4),
    /// New blocks used it from protocol version 72 to 84.
    BlockHeaderV5(BlockHeaderV5),
    /// New blocks use it from protocol version 85.
    BlockHeaderV6(BlockHeaderV6),
    /// Used only with the Spice protocol feature in nearcore 2.13.0. nearcore 2.14.0 does not produce it.
    BlockHeaderV7(BlockHeaderV7),
    /// Used only with the Spice protocol feature, which no stable protocol version enables.
    BlockHeaderV8(BlockHeaderV8),
}

pub struct BlockHeaderV6 {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRestV6,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    #[borsh(skip)]
    pub hash: CryptoHash,
}
```

Borsh writes the version of `BlockHeader` as a one-byte tag, which is the index of the variant.
The `hash` field is not serialized.

where `BlockHeaderInnerLite` and `BlockHeaderInnerRestV6` are

```rust
pub struct BlockHeaderInnerLite {
    /// Height of this block.
    pub height: BlockHeight,
    /// Epoch start hash of this block's epoch.
    /// Used for retrieving validator information
    pub epoch_id: EpochId,
    pub next_epoch_id: EpochId,
    /// Root hash of the state at the previous block.
    pub prev_state_root: MerkleHash,
    /// Root of the outcomes of transactions and receipts from the previous chunks.
    pub prev_outcome_root: MerkleHash,
    /// Timestamp at which the block was built (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub timestamp: u64,
    /// Hash of the next epoch block producers set
    pub next_bp_hash: CryptoHash,
    /// Merkle root of block hashes up to the current block.
    pub block_merkle_root: CryptoHash,
}
```

```rust
pub struct BlockHeaderInnerRestV6 {
    /// Hash of the block body
    pub block_body_hash: CryptoHash,
    /// Root hash of the previous chunks' outgoing receipts in the given block.
    pub prev_chunk_outgoing_receipts_root: MerkleHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: MerkleHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: MerkleHash,
    /// The output of the randomness beacon
    pub random_value: CryptoHash,
    /// Validator proposals from the previous chunks.
    pub prev_validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Gas price for chunks in the next block.
    pub next_gas_price: Balance,
    /// Total supply of tokens in the system
    pub total_supply: Balance,

    /// Last block that has full BFT finality
    pub last_final_block: CryptoHash,
    /// Last block that has doomslug finality
    pub last_ds_final_block: CryptoHash,

    /// The ordinal of the Block on the Canonical Chain
    pub block_ordinal: NumBlocks,

    pub prev_height: BlockHeight,

    pub epoch_sync_data_hash: Option<CryptoHash>,

    /// All the approvals included in this block
    pub approvals: Vec<Option<Box<Signature>>>,

    /// Latest protocol version that this block producer has.
    pub latest_protocol_version: ProtocolVersion,

    /// For each shard, a bitmap with one bit per chunk validator.
    /// A bit is set if the block body has the endorsement of that validator.
    /// If the block has no new chunk for a shard, the bitmap for that shard is empty.
    pub chunk_endorsements: ChunkEndorsementsBitmap,

    /// Shard ID and boundary account for the upcoming resharding.
    /// This field may be set only for the last block of an epoch.
    /// Split proposed at the end of epoch N will be executed in epoch N+2.
    pub shard_split: Option<(ShardId, AccountId)>,
}
```

From protocol version 85 (nearcore 2.13.0), new blocks use `BlockHeaderV6`.
From protocol version 72 to 84, new blocks used `BlockHeaderV5` with `BlockHeaderInnerRestV5`.
`BlockHeaderInnerRestV5` has no `shard_split` field.
It also has two deprecated fields for challenges.
`challenges_root` is a `MerkleHash` and comes after `chunk_tx_root`.
`challenges_result` is a `Vec<SlashedValidator>` and comes after `total_supply`.
In a valid block, `challenges_root` is 32 zero bytes and `challenges_result` is an empty list.

Here `CryptoHash` is a 32-byte hash and `EpochId` is a 32-byte identifier.
`MerkleHash` is another name for `CryptoHash`.
`BlockHeight` and `NumBlocks` are `u64`, and `ProtocolVersion` is `u32`.
`Balance` is an amount of yoctoNEAR and is encoded as a `u128`.
`ShardId` is encoded as a `u64`.
`Value` and `Proof` are the 32-byte output and the 64-byte proof of the verifiable random function (VRF).
`ValidatorStake` is a versioned enum, so the encoding of each proposal starts with a one-byte version tag.

`ChunkEndorsementsBitmap` is encoded as a `Vec<Vec<u8>>` with one byte list per shard.
Bit `i` of a shard is bit `i % 8` of byte `i / 8`, where bit 0 is the lowest bit of a byte.
Bit `i` belongs to the `i`-th ordered chunk validator of the shard.
For a shard with `n` chunk validators, the list has `ceil(n / 8)` bytes, and the unused bits are zero.

The block RPC returns some of these fields under older names.
It returns `prev_chunk_outgoing_receipts_root` as `chunk_receipts_root`, `prev_outcome_root` as `outcome_root`, `prev_validator_proposals` as `validator_proposals`, and `next_gas_price` as `gas_price`.

## Block Hash

The hash of a block is computed by

```rust
sha256(concat(
    sha256(concat(
        sha256(borsh(inner_lite)),
        sha256(borsh(inner_rest))
    )),
    prev_hash
))
```

`inner_lite` and `inner_rest` are structs, not versioned enums, so their borsh encoding does not start with a version tag.

# Chunk and Chunk Header

The data structures for chunk and chunk header are

```rust
pub enum ShardChunkHeader {
    V1(ShardChunkHeaderV1) = 0,
    V2(ShardChunkHeaderV2) = 1,
    /// New chunks use it from protocol version 49.
    V3(ShardChunkHeaderV3) = 2,
}

pub struct ShardChunkHeaderV3 {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    #[borsh(skip)]
    pub hash: ChunkHash,
}
```

```rust
pub enum ShardChunk {
    V1(ShardChunkV1) = 0,
    /// New chunks use it from protocol version 41.
    V2(ShardChunkV2) = 1,
}

pub struct ShardChunkV2 {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    /// Outgoing receipts produced by the previous chunk of this shard.
    pub prev_outgoing_receipts: Vec<Receipt>,
}
```

where `ShardChunkHeaderInner` is

```rust
pub enum ShardChunkHeaderInner {
    V1(ShardChunkHeaderInnerV1) = 0,
    V2(ShardChunkHeaderInnerV2) = 1,
    /// Adds `congestion_info`. New chunks used it from protocol version 68 to 73.
    V3(ShardChunkHeaderInnerV3) = 2,
    /// Adds `bandwidth_requests`. New chunks used it from protocol version 74 to 84.
    V4(ShardChunkHeaderInnerV4) = 3,
    /// Adds `proposed_split`. New chunks use it from protocol version 85.
    V5(ShardChunkHeaderInnerV5) = 4,
    /// Used only with the Spice protocol feature, which no stable protocol version enables.
    V6(ShardChunkHeaderInnerV6SpiceTxOnly) = 5,
}

pub struct ShardChunkHeaderInnerV5 {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: StateRoot,
    /// Root of the outcomes of the transactions and receipts of the previous chunk.
    pub prev_outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    /// Shard ID.
    pub shard_id: ShardId,
    /// Gas used in the previous chunk.
    pub prev_gas_used: Gas,
    /// Gas limit of the chunk. It must equal the gas limit of the previous chunk of the shard,
    /// so it stays at the genesis `gas_limit`.
    pub gas_limit: Gas,
    /// Total balance burnt in the previous chunk.
    pub prev_balance_burnt: Balance,
    /// Previous chunk's outgoing receipts merkle root.
    pub prev_outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals from the previous chunk.
    pub prev_validator_proposals: Vec<ValidatorStake>,
    /// Congestion info about this shard after the previous chunk was applied.
    pub congestion_info: CongestionInfo,
    /// Requests for bandwidth to send receipts to other shards.
    pub bandwidth_requests: BandwidthRequests,
    /// Proposed split of this shard (dynamic resharding).
    /// `None` if the shard is not above the resharding threshold.
    pub proposed_split: Option<TrieSplit>,
}
```

From protocol version 85 (nearcore 2.13.0), new chunks use `ShardChunkHeaderInnerV5`.
From protocol version 74 to 84, new chunks used `ShardChunkHeaderInnerV4`, which has no `proposed_split` field.

Here `StateRoot` is another name for `CryptoHash`, and `ChunkHash` is a 32-byte hash.
`Gas` is encoded as a `u64`.
`CongestionInfo` and `BandwidthRequests` are versioned enums like `ValidatorStake`, so their encoding also starts with a one-byte version tag.
`TrieSplit` holds `boundary_account: AccountId`, `left_memory: u64` and `right_memory: u64`.

The chunk RPC returns `prev_outcome_root`, `prev_gas_used`, `prev_balance_burnt`, `prev_outgoing_receipts_root` and `prev_validator_proposals` without the `prev_` prefix.

## Chunk Hash

Chunk hash is computed by

```rust
sha256(
    concat(
        sha256(borsh(inner)),
        encoded_merkle_root
    )
)
```

For `ShardChunkHeaderV3`, `inner` is the versioned `ShardChunkHeaderInner` enum, so `borsh(inner)` starts with the one-byte version tag.
