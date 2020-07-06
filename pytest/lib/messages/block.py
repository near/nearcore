from messages.crypto import PublicKey, Signature, MerklePath, ShardProof
from messages.tx import Receipt, SignedTransaction


class Block:
    pass


class BlockV1:
    pass


class BlockHeader:
    pass


class BlockHeaderV1:
    pass


class BlockHeaderInnerLite:
    pass


class BlockHeaderInnerRest:
    pass


class ShardChunk:
    pass


class ShardChunkHeader:
    pass


class ShardChunkHeaderInner:
    pass


class PartialEncodedChunkPart:
    pass


class ReceiptProof:
    pass


class PartialEncodedChunk:
    pass


class PartialEncodedChunkRequestMsg:
    pass


class PartialEncodedChunkResponseMsg:
    pass


class ValidatorStake:
    pass


class Approval:
    pass


class ApprovalInner:
    pass


block_schema = [
    [
        Block, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['BlockV1', BlockV1]
            ]
        }
    ],
    [
        BlockV1, {
            'kind': 'struct',
            'fields': [
                ['header', BlockHeader],
                ['chunks', [ShardChunkHeader]],
                ['challenges', [()]], # TODO

                ['vrf_value', [32]],
                ['vrf_proof', [64]],
            ]
        }
    ],
    [
        BlockHeader, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['BlockHeaderV1', BlockHeaderV1]
            ]
        }
    ],
    [
        BlockHeaderV1, {
            'kind': 'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRest],
                ['signature', Signature],
            ]
        }
    ],
    [
        BlockHeaderInnerLite, {
            'kind': 'struct',
            'fields': [
                ['height', 'u64'],
                ['epoch_id', [32]],
                ['next_epoch_id', [32]],
                ['prev_state_root', [32]],
                ['outcome_root', [32]],
                ['timestamp', 'u64'],
                ['next_bp_hash', [32]],
                ['block_merkle_root', [32]],
            ]
        }
    ],
    [
        BlockHeaderInnerRest, {
            'kind': 'struct',
            'fields': [
                ['chunk_receipts_root', [32]],
                ['chunk_headers_root', [32]],
                ['chunk_tx_root', [32]],
                ['chunks_included', 'u64'],
                ['challenges_root', [32]],
                ['random_value', [32]],
                ['validator_proposals', [ValidatorStake]],
                ['chunk_mask', ['u8']],
                ['gas_price', 'u128'],
                ['total_supply', 'u128'],
                ['challenges_result', [()]], # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['approvals', [{'kind': 'option', 'type': Signature}]],
                ['latest_protocol_verstion', 'u32'],
            ]
        }
    ],
    [
        ShardChunkHeader, {
            'kind': 'struct',
            'fields': [
                ['inner', ShardChunkHeaderInner],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderInner, {
            'kind': 'struct',
            'fields': [
                ['prev_block_hash', [32]],
                ['prev_state_root', [32]],
                ['outcome_root', [32]],
                ['encoded_merkle_root', [32]],
                ['encoded_length', 'u64'],
                ['height_created', 'u64'],
                ['shard_id', 'u64'],
                ['gas_used', 'u64'],
                ['gas_limit', 'u64'],
                ['balance_burnt', 'u128'],
                ['outgoing_receipt_root', [32]],
                ['tx_root', [32]],
                ['validator_proposals', [ValidatorStake]],
            ]
        }
    ],
    [
        ShardChunk, {
            'kind': 'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['header', ShardChunkHeader],
                ['transactions', [SignedTransaction]],
                ['receipts', [Receipt]],
            ]
        }
    ],
    [
        PartialEncodedChunkPart, {
            'kind': 'struct',
            'fields': [
                ['part_ord', 'u64'],
                ['part', ['u8']],
                ['merkle_proof', MerklePath],
            ]
        }
    ],
    [
        ReceiptProof, {
            'kind': 'struct',
            'fields': [
                ['f1', [Receipt]],
                ['f2', ShardProof],
            ]
        }
    ],
    [
        PartialEncodedChunk, {
            'kind': 'struct',
            'fields': [
                ['header', ShardChunkHeader],
                ['parts', [PartialEncodedChunkPart]],
                ['receipts', [ReceiptProof]]
            ]
        }
    ],
    [
        PartialEncodedChunkRequestMsg, {
            'kind': 'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['part_ords', ['u64']],
                ['tracking_shards', ['u64']]
            ]
        }
    ],
    [
        PartialEncodedChunkResponseMsg, {
            'kind': 'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['parts', [PartialEncodedChunkPart]],
                ['receipts', [ReceiptProof]]
            ]
        }
    ],
    [
        ValidatorStake, {
            'kind': 'struct',
            'fields': [
                ['account_id', 'string'],
                ['public_key', PublicKey],
                ['stake', 'u128'],
            ]
        }
    ],
    [
        Approval, {
            'kind': 'struct',
            'fields': [
                ['inner', ApprovalInner],
                ['target_height', 'u64'],
                ['signature', Signature],
                ['account_id', 'string'],
            ]
        }
    ],
    [
        ApprovalInner, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['Endorsement', [32]],
                ['Skip', 'u64'],
            ]
        }
    ],
]

