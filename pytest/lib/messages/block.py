from messages.crypto import PublicKey, Signature, MerklePath, ShardProof
from messages.tx import Receipt, SignedTransaction


class Block:
    pass


class BlockV1:
    pass


class BlockV2:
    pass


class BlockHeader:
    def inner_lite(self):
        if self.enum == 'BlockHeaderV3':
            return self.BlockHeaderV3.inner_lite
        elif self.enum == 'BlockHeaderV2':
            return self.BlockHeaderV2.inner_lite
        elif self.enum == 'BlockHeaderV1':
            return self.BlockHeaderV1.inner_lite
        assert False, "inner_lite is called on BlockHeader, but the enum variant `%s` is unknown" % self.enum


class BlockHeaderV1:
    pass


class BlockHeaderV2:
    pass


class BlockHeaderV3:
    pass

class BlockHeaderInnerLite:
    pass


class BlockHeaderInnerRest:
    pass


class BlockHeaderInnerRestV2:
    pass


class BlockHeaderInnerRestV3:
    pass

class ShardChunk:
    pass


class ShardChunkV1:
    pass


class ShardChunkV2:
    pass


class ShardChunkHeader:
    pass


class ShardChunkHeaderV1:
    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer
        inner_serialized = BinarySerializer(dict(block_schema + crypto_schema)).serialize(inner)
        return hashlib.sha256(inner_serialized).digest()

class ShardChunkHeaderV2:
    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer
        inner_serialized = BinarySerializer(dict(block_schema + crypto_schema)).serialize(inner)
        inner_hash = hashlib.sha256(inner_serialized).digest()

        return hashlib.sha256(inner_hash + inner.encoded_merkle_root).digest()

class ShardChunkHeaderV3:
    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer
        inner_serialized = BinarySerializer(dict(block_schema + crypto_schema)).serialize(inner)
        inner_hash = hashlib.sha256(inner_serialized).digest()

        return hashlib.sha256(inner_hash + inner.encoded_merkle_root).digest()

class ShardChunkHeaderInner:
    pass

class ShardChunkHeaderInnerV1:
    pass

class ShardChunkHeaderInnerV2:
    pass

class PartialEncodedChunkPart:
    pass


class ReceiptProof:
    pass


class PartialEncodedChunk:
    def inner_header(self):
        version = self.enum
        if version == 'V1':
            return self.V1.header.inner
        elif version == 'V2':
            header = self.V2.header
            header_version = header.enum
            if header_version == 'V1':
                return header.V1.inner
            elif header_version == 'V2':
                return header.V2.inner
    
    def header_version(self):
        version = self.enum
        if version == 'V1':
            return version
        elif version == 'V2':
            return self.V2.header.enum


class PartialEncodedChunkV1:
    pass


class PartialEncodedChunkV2:
    pass


class PartialEncodedChunkRequestMsg:
    pass


class PartialEncodedChunkResponseMsg:
    pass


class PartialEncodedChunkForwardMsg:
    pass

class ValidatorStake:
    pass

class ValidatorStakeV1:
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
                ['BlockV1', BlockV1],
                ['BlockV2', BlockV2],
            ]
        }
    ],
    [
        BlockV1, {
            'kind': 'struct',
            'fields': [
                ['header', BlockHeader],
                ['chunks', [ShardChunkHeaderV1]],
                ['challenges', [()]], # TODO

                ['vrf_value', [32]],
                ['vrf_proof', [64]],
            ]
        }
    ],
    [
        BlockV2, {
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
                ['BlockHeaderV1', BlockHeaderV1],
                ['BlockHeaderV2', BlockHeaderV2],
                ['BlockHeaderV3', BlockHeaderV3]
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
        BlockHeaderV2, {
            'kind' : 'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRestV2],
                ['signature', Signature],
            ]
        }
    ],
    [
        BlockHeaderV3, {
            'kind' : 'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRestV3],
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
                ['validator_proposals', [ValidatorStakeV1]],
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
        BlockHeaderInnerRestV2, {
            'kind': 'struct',
            'fields': [
                ['chunk_receipts_root', [32]],
                ['chunk_headers_root', [32]],
                ['chunk_tx_root', [32]],
                ['challenges_root', [32]],
                ['random_value', [32]],
                ['validator_proposals', [ValidatorStakeV1]],
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
        BlockHeaderInnerRestV3, {
            'kind': 'struct',
            'fields': [
                ['chunk_receipts_root', [32]],
                ['chunk_headers_root', [32]],
                ['chunk_tx_root', [32]],
                ['challenges_root', [32]],
                ['random_value', [32]],
                ['validator_proposals', [ValidatorStake]],
                ['chunk_mask', ['u8']],
                ['gas_price', 'u128'],
                ['total_supply', 'u128'],
                ['challenges_result', [()]], # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['block_ordinal', 'u64'],
                ['prev_height', 'u64'],
                ['epoch_sync_data_hash', {'kind': 'option', 'type': [32]}],
                ['approvals', [{'kind': 'option', 'type': Signature}]],
                ['latest_protocol_verstion', 'u32'],
            ]
        }
    ],
    [
        ShardChunkHeader, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', ShardChunkHeaderV1],
                ['V2', ShardChunkHeaderV2],
                ['V3', ShardChunkHeaderV3]
            ]
        }
    ],
    [
        ShardChunkHeaderV1, {
            'kind': 'struct',
            'fields': [
                ['inner', ShardChunkHeaderInnerV1],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderV2, {
            'kind': 'struct',
            'fields': [
                ['inner', ShardChunkHeaderInnerV1],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderV3, {
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
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', ShardChunkHeaderInnerV1],
                ['V2', ShardChunkHeaderInnerV2]
            ]
        }
    ],
    [
        ShardChunkHeaderInnerV1, {
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
                ['validator_proposals', [ValidatorStakeV1]],
            ]
        }
    ],
    [
        ShardChunkHeaderInnerV2, {
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
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', ShardChunkV1],
                ['V2', ShardChunkV2]
            ]
        }
    ],
    [
        ShardChunkV1, {
            'kind': 'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['header', ShardChunkHeaderV1],
                ['transactions', [SignedTransaction]],
                ['receipts', [Receipt]],
            ]
        }
    ],
    [
        ShardChunkV2, {
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
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', PartialEncodedChunkV1],
                ['V2', PartialEncodedChunkV2]
            ]
        }
    ],
    [
        PartialEncodedChunkV1, {
            'kind': 'struct',
            'fields': [
                ['header', ShardChunkHeaderV1],
                ['parts', [PartialEncodedChunkPart]],
                ['receipts', [ReceiptProof]]
            ]
        }
    ],
    [
        PartialEncodedChunkV2, {
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
        PartialEncodedChunkForwardMsg, {
            'kind': 'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['inner_header_hash', [32]],
                ['merkle_root', [32]],
                ['signature', Signature],
                ['prev_block_hash', [32]],
                ['height_created', 'u64'],
                ['shard_id', 'u64'],
                ['parts', [PartialEncodedChunkPart]]
            ]
        }
    ],
    [
        ValidatorStake, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', ValidatorStakeV1]
            ]
        }
    ],
    [
        ValidatorStakeV1, {
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

