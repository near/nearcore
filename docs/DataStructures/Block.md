# Block and Block Header

The data structures for block and block headers are

```rust
pub struct Block {
    /// Block Header
    pub header: BlockHeader,
    /// Headers of chunk in the block
    pub chunks: Vec<ShardChunkHeader>,
    /// Challenges, but they are not used today
    pub challenges: Challenges,

    /// Data to confirm the correctness of randomness beacon output
    pub vrf_value: [u8; 32],
    pub vrf_proof: [u8; 64],
}
```

```rust
pub struct BlockHeader {
    pub prev_hash: CryptoHash,

    /// Inner part of the block header that gets hashed, split into two parts, one that is sent
    ///    to light clients, and the rest
    pub inner_lite: BlockHeaderInnerLite,
    pub inner_rest: BlockHeaderInnerRest,

    /// Signature of the block producer.
    pub signature: Signature,

    /// Cached value of hash for this block.
    pub hash: CryptoHash,
}
```

where `BlockHeaderInnerLite` and `BlockHeaderInnerRest` are

```rust
pub struct BlockHeaderInnerLite {
    /// Height of this block.
    pub height: u64,
    /// Epoch start hash of this block's epoch.
    /// Used for retrieving validator information
    pub epoch_id: EpochId,
    pub next_epoch_id: EpochId,
    /// Root hash of the state at the previous block.
    pub prev_state_root: CryptoHash,
    /// Root of the outcomes of transactions and receipts.
    pub outcome_root: CryptoHash,
    /// Timestamp at which the block was built (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub timestamp: u64,
    /// Hash of the next epoch block producers set
    pub next_bp_hash: CryptoHash,
    /// Merkle root of block hashes up to the current block.
    pub block_merkle_root: CryptoHash,
}
```

```rust
pub struct BlockHeaderInnerRest {
    /// Root hash of the chunk receipts in the given block.
    pub chunk_receipts_root: CryptoHash,
    /// Root hash of the chunk headers in the given block.
    pub chunk_headers_root: CryptoHash,
    /// Root hash of the chunk transactions in the given block.
    pub chunk_tx_root: CryptoHash,
    /// Root hash of the challenges in the given block.
    pub challenges_root: CryptoHash,
    /// The output of the randomness beacon
    pub random_value: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Mask for new chunks included in the block
    pub chunk_mask: Vec<bool>,
    /// Gas price. Same for all chunks
    pub gas_price: u128,
    /// Total supply of tokens in the system
    pub total_supply: u128,
    /// List of challenges result from previous block.
    pub challenges_result: ChallengesResult,

    /// Last block that has full BFT finality
    pub last_final_block: CryptoHash,
    /// Last block that has doomslug finality
    pub last_ds_final_block: CryptoHash,

    /// The ordinal of the Block on the Canonical Chain
    pub block_ordinal: u64,
    
    /// All the approvals included in this block
    pub approvals: Vec<Option<Signature>>,

    /// Latest protocol version that this block producer has.
    pub latest_protocol_version: u32,
}
```

Here `CryptoHash` is a 32-byte hash and `EpochId` is a 32-byte identifier.

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

# Chunk and Chunk Header

The data structures for chunk and chunk header are

```rust
pub struct ShardChunkHeader {
    pub inner: ShardChunkHeaderInner,

    pub height_included: BlockHeight,

    /// Signature of the chunk producer.
    pub signature: Signature,

    pub hash: ChunkHash,
}
```

```rust
pub struct ShardChunk {
    pub chunk_hash: ChunkHash,
    pub header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
}
```

where `ShardChunkHeaderInner` is

```rust
pub struct ShardChunkHeaderInner {
    /// Previous block hash.
    pub prev_block_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    /// Root of the outcomes from execution transactions and results.
    pub outcome_root: CryptoHash,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: u64,
    /// Shard index.
    pub shard_id: u64,
    /// Gas used in this chunk.
    pub gas_used: u64,
    /// Gas limit voted by validators.
    pub gas_limit: u64,
    /// Total balance burnt in previous chunk
    pub balance_burnt: u128,
    /// Outgoing receipts merkle root.
    pub outgoing_receipts_root: CryptoHash,
    /// Tx merkle root.
    pub tx_root: CryptoHash,
    /// Validator proposals.
    pub validator_proposals: Vec<ValidatorStake>,
}
```

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
