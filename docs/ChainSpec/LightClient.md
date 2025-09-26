# Light Client

The state of the light client is defined by:

1. `BlockHeaderInnerLiteView` for the current head (which contains `height`, `epoch_id`, `next_epoch_id`, `prev_state_root`, `outcome_root`, `timestamp`, the hash of the block producers set for the next epoch `next_bp_hash`, and the merkle root of all the block hashes `block_merkle_root`);
2. The set of block producers for the current and next epochs.

The `epoch_id` refers to the epoch to which the block that is the current known head belongs, and `next_epoch_id` is the epoch that will follow.

Light clients operate by periodically fetching instances of `LightClientBlockView` via particular RPC end-point described [below](#rpc-end-points).

Light client doesn't need to receive `LightClientBlockView` for all the blocks. Having the `LightClientBlockView` for block `B` is sufficient to be able to verify any statement about state or outcomes in any block in the ancestry of `B` (including `B` itself). In particular, having the `LightClientBlockView` for the head is sufficient to locally verify any statement about state or outcomes in any block on the canonical chain.

However, to verify the validity of a particular `LightClientBlockView`, the light client must have verified a `LightClientBlockView` for at least one block in the preceding epoch, thus to sync to the head the light client will have to fetch and verify a `LightClientBlockView` per epoch passed.

## Validating Light Client Block Views

```rust
pub enum ApprovalInner {
    Endorsement(CryptoHash),
    Skip(BlockHeight)
}

pub struct ValidatorStakeView {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub stake: Balance,
}

pub struct BlockHeaderInnerLiteView {
    pub height: BlockHeight,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub timestamp: u64,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
}

pub struct LightClientBlockLiteView {
    pub prev_block_hash: CryptoHash,
    pub inner_rest_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
}


pub struct LightClientBlockView {
    pub prev_block_hash: CryptoHash,
    pub next_block_inner_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
    pub inner_rest_hash: CryptoHash,
    pub next_bps: Option<Vec<ValidatorStakeView>>,
    pub approvals_after_next: Vec<Option<Signature>>,
}
```

Recall that the hash of the block is

```rust
sha256(concat(
    sha256(concat(
        sha256(borsh(inner_lite)),
        sha256(borsh(inner_rest))
    )),
    prev_hash
))
```

The fields `prev_block_hash`, `next_block_inner_hash` and `inner_rest_hash` are used to reconstruct the hashes of the current and next block, and the approvals that will be signed, in the following way (where `block_view` is an instance of `LightClientBlockView`):

```python
def reconstruct_light_client_block_view_fields(block_view):
    current_block_hash = sha256(concat(
        sha256(concat(
            sha256(borsh(block_view.inner_lite)),
            block_view.inner_rest_hash,
        )),
        block_view.prev_block_hash
    ))

    next_block_hash = sha256(concat(
        block_view.next_block_inner_hash,
        current_block_hash
    ))

    approval_message = concat(
        borsh(ApprovalInner::Endorsement(next_block_hash)),
        little_endian(block_view.inner_lite.height + 2)
    )

    return (current_block_hash, next_block_hash, approval_message)
```

The light client updates its head with the information from `LightClientBlockView` iff:

1. The height of the block is higher than the height of the current head;
2. The epoch of the block is equal to the `epoch_id` or `next_epoch_id` known for the current head;
3. If the epoch of the block is equal to the `next_epoch_id` of the head, then `next_bps` is not `None`;
4. `approvals_after_next` contain valid signatures on `approval_message` from the block producers of the corresponding epoch (see next section);
5. The signatures present in `approvals_after_next` correspond to more than 2/3 of the total stake (see next section).
6. If `next_bps` is not none, `sha256(borsh(next_bps))` corresponds to the `next_bp_hash` in `inner_lite`.

```python
def validate_and_update_head(block_view):
    global head
    global epoch_block_producers_map

    current_block_hash, next_block_hash, approval_message = reconstruct_light_client_block_view_fields(block_view)

    # (1)
    if block_view.inner_lite.height <= head.inner_lite.height:
        return False

    # (2)
    if block_view.inner_lite.epoch_id not in [head.inner_lite.epoch_id, head.inner_lite.next_epoch_id]:
        return False

    # (3)
    if block_view.inner_lite.epoch_id == head.inner_lite.next_epoch_id and block_view.next_bps is None:
        return False

    # (4) and (5)
    total_stake = 0
    approved_stake = 0

    epoch_block_producers = epoch_block_producers_map[block_view.inner_lite.epoch_id]
    for maybe_signature, block_producer in zip(block_view.approvals_after_next, epoch_block_producers):
        total_stake += block_producer.stake

        if maybe_signature is None:
            continue

        approved_stake += block_producer.stake
        if not verify_signature(
            public_key: block_producer.public_key,
            signature: maybe_signature,
            message: approval_message
        ):
            return False

    threshold = total_stake * 2 // 3
    if approved_stake <= threshold:
        return False

    # (6)
    if block_view.next_bps is not None:
        if sha256(borsh(block_view.next_bps)) != block_view.inner_lite.next_bp_hash:
            return False

        epoch_block_producers_map[block_view.inner_lite.next_epoch_id] = block_view.next_bps

    head = block_view
```

## Signature verification

To simplify the protocol we require that the next block and the block after next are both in the same epoch as the block that `LightClientBlockView` corresponds to. It is guaranteed that each epoch has at least one final block for which the next two blocks that build on top of it are in the same epoch.

By construction by the time the `LightClientBlockView` is being validated, the block producers set for its epoch is known. Specifically, when the first light client block view of the previous epoch was processed, due to (3) above the `next_bps` was not `None`, and due to (6) above it was corresponding to the `next_bp_hash` in the block header.

The sum of all the stakes of `next_bps` in the previous epoch is `total_stake` referred to in (5) above.

The signatures in the `LightClientBlockView::approvals_after_next` are signatures on `approval_message`. The  `i`-th signature in `approvals_after_next`, if present, must validate against the `i`-th public key in `next_bps` from the previous epoch. `approvals_after_next` can contain fewer elements than `next_bps` in the previous epoch.

`approvals_after_next` can also contain more signatures than the length of `next_bps` in the previous epoch. This is due to the fact that, as per [consensus specification](./Consensus.md), the last blocks in each epoch contain signatures from both the block producers of the current epoch, and the next epoch. The trailing signatures can be safely ignored by the light client implementation.

## Proof Verification


### Transaction Outcome Proofs

To verify that a transaction or receipt happens on chain, a light client can request a proof through rpc by providing `id`, which is of type

```rust
pub enum TransactionOrReceiptId {
    Transaction { hash: CryptoHash, sender: AccountId },
    Receipt { id: CryptoHash, receiver: AccountId },
}
```

and the block hash of light client head. The rpc will return the following struct

```rust
pub struct RpcLightClientExecutionProofResponse {
    /// Proof of execution outcome
    pub outcome_proof: ExecutionOutcomeWithIdView,
    /// Proof of shard execution outcome root
    pub outcome_root_proof: MerklePath,
    /// A light weight representation of block that contains the outcome root
    pub block_header_lite: LightClientBlockLiteView,
    /// Proof of the existence of the block in the block merkle tree,
    /// which consists of blocks up to the light client head
    pub block_proof: MerklePath,
}
```

which includes everything that a light client needs to prove the execution outcome of the given transaction or receipt.
Here `ExecutionOutcomeWithIdView` is

```rust
pub struct ExecutionOutcomeWithIdView {
    /// Proof of the execution outcome
    pub proof: MerklePath,
    /// Block hash of the block that contains the outcome root
    pub block_hash: CryptoHash,
    /// Id of the execution (transaction or receipt)
    pub id: CryptoHash,
    /// The actual outcome
    pub outcome: ExecutionOutcomeView,
}
```

The proof verification can be broken down into two steps, execution outcome root verification and block merkle root
verification.

#### Execution Outcome Root Verification

If the outcome root of the transaction or receipt is included in block `H`, then `outcome_proof` includes the block hash
of `H`, as well as the merkle proof of the execution outcome in its given shard. The outcome root in `H` can be
reconstructed by

```python
shard_outcome_root = compute_root(sha256(borsh(execution_outcome)), outcome_proof.proof)
block_outcome_root = compute_root(sha256(borsh(shard_outcome_root)), outcome_root_proof)
```

This outcome root must match the outcome root in `block_header_lite.inner_lite`.

#### Block Merkle Root Verification

Recall that block hash can be computed from `LightClientBlockLiteView` by

```rust
sha256(concat(
    sha256(concat(
        sha256(borsh(inner_lite)),
        sha256(borsh(inner_rest))
    )),
    prev_hash
))
```

The expected block merkle root can be computed by

```python
block_hash = compute_block_hash(block_header_lite)
block_merkle_root = compute_root(block_hash, block_proof)
```

which must match the block merkle root in the light client block of the light client head.

## RPC end-points

### Light Client Block

There's a single end-point that full nodes exposed that light clients can use to fetch new `LightClientBlockView`s:

```
http post http://127.0.0.1:3030/ jsonrpc=2.0 method=next_light_client_block params:="[<last known hash>]" id="dontcare"
```

The RPC returns the `LightClientBlock` for the block as far into the future from the last known hash as possible for the light client to still accept it. Specifically, it either returns the last final block of the next epoch, or the last final known block. If there's no newer final block than the one the light client knows about, the RPC returns an empty result.

A standalone light client would bootstrap by requesting next blocks until it receives an empty result, and then periodically request the next light client block.

A smart contract-based light client that enables a bridge to NEAR on a different blockchain naturally cannot request blocks itself. Instead external oracles query the next light client block from one of the full nodes, and submit it to the light client smart contract. The smart contract-based light client performs the same checks described above, so the oracle doesn't need to be trusted.

### Light Client Proof

The following rpc end-point returns `RpcLightClientExecutionProofResponse` that a light client needs for verifying execution outcomes.

For transaction execution outcome, the rpc is

```
http post http://127.0.0.1:3030/ jsonrpc=2.0 method=EXPERIMENTAL_light_client_proof params:="{"type": "transaction", "transaction_hash": <transaction_hash>, "sender_id": <sender_id>, "light_client_head": <light_client_head>}" id="dontcare"
```

For receipt execution outcome, the rpc is

```
http post http://127.0.0.1:3030/ jsonrpc=2.0 method=EXPERIMENTAL_light_client_proof params:="{"type": "receipt", "receipt_id": <receipt_id>, "receiver_id": <receiver_id>, "light_client_head": <light_client_head>}" id="dontcare"
```
