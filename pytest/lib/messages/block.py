from messages.crypto import PublicKey, Signature, MerklePath, ShardProof
from messages.tx import Receipt, SignedTransaction


class Block:

    def header(self):
        if self.enum == 'BlockV1':
            return self.BlockV1.header
        elif self.enum == 'BlockV2':
            return self.BlockV2.header
        elif self.enum == 'BlockV3':
            return self.BlockV3.header
        elif self.enum == 'BlockV4':
            return self.BlockV4.header
        assert False, "header is called on Block, but the enum variant `%s` is unknown" % self.enum

    def chunks(self):
        if self.enum == 'BlockV1':
            return self.BlockV1.chunks
        elif self.enum == 'BlockV2':
            return self.BlockV2.chunks
        elif self.enum == 'BlockV3':
            return self.BlockV3.body.chunks
        elif self.enum == 'BlockV4':
            return self.BlockV3.body.chunks
        assert False, "chunks is called on Block, but the enum variant `%s` is unknown" % self.enum


class BlockV1:
    pass


class BlockV2:
    pass


class BlockV3:
    pass


class BlockV4:
    pass


class BlockBody:
    pass


class BlockBodyV1:
    pass


class BlockBodyV2:
    pass


class BlockHeader:

    def inner_lite(self):
        if self.enum == 'BlockHeaderV5':
            return self.BlockHeaderV5.inner_lite
        elif self.enum == 'BlockHeaderV4':
            return self.BlockHeaderV4.inner_lite
        elif self.enum == 'BlockHeaderV3':
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


class BlockHeaderV4:
    pass


class BlockHeaderV5:
    pass


class BlockHeaderInnerLite:
    pass


class BlockHeaderInnerRest:
    pass


class BlockHeaderInnerRestV2:
    pass


class BlockHeaderInnerRestV3:
    pass


class BlockHeaderInnerRestV4:
    pass


class BlockHeaderInnerRestV5:
    pass


class ChunkEndorsementsBitmap:
    pass


class ShardChunk:
    pass


class ShardChunkV1:
    pass


class ShardChunkV2:
    pass


class ShardChunkHeader:

    @property
    def signature(self):
        if self.enum == 'V1':
            return self.V1.signature
        elif self.enum == 'V2':
            return self.V2.signature
        elif self.enum == 'V3':
            return self.V3.signature
        assert False, "signature is called on ShardChunkHeader, but the enum variant `%s` is unknown" % self.enum


class ShardChunkHeaderV1:

    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer
        inner_serialized = BinarySerializer(
            dict(block_schema + crypto_schema)).serialize(inner)
        return hashlib.sha256(inner_serialized).digest()


class ShardChunkHeaderV2:

    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer
        inner_serialized = BinarySerializer(
            dict(block_schema + crypto_schema)).serialize(inner)
        inner_hash = hashlib.sha256(inner_serialized).digest()

        return hashlib.sha256(inner_hash + inner.encoded_merkle_root).digest()


class ShardChunkHeaderV3:

    @staticmethod
    def chunk_hash(inner):
        import hashlib
        from messages.crypto import crypto_schema
        from serializer import BinarySerializer

        # We combine the hash of this inner object (of type ShardChunkHeaderInner)
        # and the encoded merkle root obtained from the versioned-inner object
        # inside the variants of this inner object.
        encoded_merkle_root = None
        if inner.enum == 'V1':
            encoded_merkle_root = inner.V1.encoded_merkle_root
        elif inner.enum == 'V2':
            encoded_merkle_root = inner.V2.encoded_merkle_root
        elif inner.enum == 'V3':
            encoded_merkle_root = inner.V3.encoded_merkle_root
        assert encoded_merkle_root is not None, f"Unknown ShardChunkHeaderV3 enum variant: {inner.enum}"

        inner_serialized = BinarySerializer(
            dict(block_schema + crypto_schema)).serialize(inner)
        inner_hash = hashlib.sha256(inner_serialized).digest()

        return hashlib.sha256(inner_hash + encoded_merkle_root).digest()


class ShardChunkHeaderInner:
    pass


class ShardChunkHeaderInnerV1:
    pass


class ShardChunkHeaderInnerV2:
    pass


class ShardChunkHeaderInnerV3:
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
            elif header_version == 'V3':
                v3_inner_version = header.V3.inner.enum
                if v3_inner_version == 'V1':
                    return header.V3.inner.V1
                elif v3_inner_version == 'V2':
                    return header.V3.inner.V2
                elif v3_inner_version == 'V3':
                    return header.V3.inner.V3
            assert False, "unknown header version"

    def chunk_hash(self):
        version = self.enum
        if version == 'V1':
            return ShardChunkHeaderV1.chunk_hash(self.V1.header.inner)
        elif version == 'V2':
            header = self.V2.header
            header_version = header.enum
            if header_version == 'V1':
                return ShardChunkHeaderV1.chunk_hash(header.V1.inner)
            elif header_version == 'V2':
                return ShardChunkHeaderV2.chunk_hash(header.V2.inner)
            elif header_version == 'V3':
                return ShardChunkHeaderV3.chunk_hash(header.V3.inner)
            assert False, "unknown header version"


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


class ValidatorStakeV2:
    pass


class Approval:
    pass


class ApprovalInner:
    pass


class CongestionInfo:
    pass


class CongestionInfoV1:
    pass


class ChunkEndorsement:
    pass


class ChunkEndorsementV1:
    pass


class ChunkEndorsementV2:
    pass


class ChunkEndorsementInner:
    pass


class ChunkEndorsementMetadata:
    pass


class ChunkStateWitnessAck:
    pass


class PartialEncodedStateWitness:
    pass


class PartialEncodedStateWitnessInner:
    pass


class SignatureDifferentiator:
    pass


block_schema = [
    [
        Block, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['BlockV1', BlockV1],
                ['BlockV2', BlockV2],
                ['BlockV3', BlockV3],
                ['BlockV4', BlockV4],
            ]
        }
    ],
    [
        BlockV1,
        {
            'kind':
                'struct',
            'fields': [
                ['header', BlockHeader],
                ['chunks', [ShardChunkHeaderV1]],
                ['challenges', [()]],  # TODO
                ['vrf_value', [32]],
                ['vrf_proof', [64]],
            ]
        }
    ],
    [
        BlockV2,
        {
            'kind':
                'struct',
            'fields': [
                ['header', BlockHeader],
                ['chunks', [ShardChunkHeader]],
                ['challenges', [()]],  # TODO
                ['vrf_value', [32]],
                ['vrf_proof', [64]],
            ]
        }
    ],
    [
        BlockV3, {
            'kind': 'struct',
            'fields': [
                ['header', BlockHeader],
                ['body', BlockBodyV1],
            ]
        }
    ],
    [
        BlockV4, {
            'kind': 'struct',
            'fields': [
                ['header', BlockHeader],
                ['body', BlockBody],
            ]
        }
    ],
    [
        BlockBody, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['V1', BlockBodyV1],
                ['V2', BlockBodyV2],
            ]
        }
    ],
    [
        BlockBodyV1,
        {
            'kind':
                'struct',
            'fields': [
                ['chunks', [ShardChunkHeader]],
                ['challenges', [()]],  # TODO
                ['vrf_value', [32]],
                ['vrf_proof', [64]],
            ]
        }
    ],
    [
        BlockBodyV2,
        {
            'kind':
                'struct',
            'fields': [
                ['chunks', [ShardChunkHeader]],
                ['challenges', [()]],  # TODO
                ['vrf_value', [32]],
                ['vrf_proof', [64]],
                [
                    'chunk_endorsements',
                    [[{
                        'kind': 'option',
                        'type': Signature
                    }]]
                ],
            ]
        }
    ],
    [
        BlockHeader, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['BlockHeaderV1', BlockHeaderV1],
                       ['BlockHeaderV2', BlockHeaderV2],
                       ['BlockHeaderV3', BlockHeaderV3],
                       ['BlockHeaderV4', BlockHeaderV4],
                       ['BlockHeaderV5', BlockHeaderV5]]
        }
    ],
    [
        BlockHeaderV1, {
            'kind':
                'struct',
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
            'kind':
                'struct',
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
            'kind':
                'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRestV3],
                ['signature', Signature],
            ]
        }
    ],
    [
        BlockHeaderV4, {
            'kind':
                'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRestV4],
                ['signature', Signature],
            ]
        }
    ],
    [
        BlockHeaderV5, {
            'kind':
                'struct',
            'fields': [
                ['prev_hash', [32]],
                ['inner_lite', BlockHeaderInnerLite],
                ['inner_rest', BlockHeaderInnerRestV5],
                ['signature', Signature],
            ]
        }
    ],
    [
        BlockHeaderInnerLite, {
            'kind':
                'struct',
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
        BlockHeaderInnerRest,
        {
            'kind':
                'struct',
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
                ['challenges_result', [()]],  # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['approvals', [{
                    'kind': 'option',
                    'type': Signature
                }]],
                ['latest_protocol_version', 'u32'],
            ]
        }
    ],
    [
        BlockHeaderInnerRestV2,
        {
            'kind':
                'struct',
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
                ['challenges_result', [()]],  # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['approvals', [{
                    'kind': 'option',
                    'type': Signature
                }]],
                ['latest_protocol_version', 'u32'],
            ]
        }
    ],
    [
        BlockHeaderInnerRestV3,
        {
            'kind':
                'struct',
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
                ['challenges_result', [()]],  # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['block_ordinal', 'u64'],
                ['prev_height', 'u64'],
                ['epoch_sync_data_hash', {
                    'kind': 'option',
                    'type': [32]
                }],
                ['approvals', [{
                    'kind': 'option',
                    'type': Signature
                }]],
                ['latest_protocol_version', 'u32'],
            ]
        }
    ],
    [
        BlockHeaderInnerRestV4,
        {
            'kind':
                'struct',
            'fields': [
                ['block_body_hash', [32]],
                ['chunk_receipts_root', [32]],
                ['chunk_headers_root', [32]],
                ['chunk_tx_root', [32]],
                ['challenges_root', [32]],
                ['random_value', [32]],
                ['validator_proposals', [ValidatorStake]],
                ['chunk_mask', ['u8']],
                ['gas_price', 'u128'],
                ['total_supply', 'u128'],
                ['challenges_result', [()]],  # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['block_ordinal', 'u64'],
                ['prev_height', 'u64'],
                ['epoch_sync_data_hash', {
                    'kind': 'option',
                    'type': [32]
                }],
                ['approvals', [{
                    'kind': 'option',
                    'type': Signature
                }]],
                ['latest_protocol_version', 'u32'],
            ]
        }
    ],
    [
        BlockHeaderInnerRestV5,
        {
            'kind':
                'struct',
            'fields': [
                ['block_body_hash', [32]],
                ['chunk_receipts_root', [32]],
                ['chunk_headers_root', [32]],
                ['chunk_tx_root', [32]],
                ['challenges_root', [32]],
                ['random_value', [32]],
                ['validator_proposals', [ValidatorStake]],
                ['chunk_mask', ['u8']],
                ['gas_price', 'u128'],
                ['total_supply', 'u128'],
                ['challenges_result', [()]],  # TODO
                ['last_final_block', [32]],
                ['last_ds_final_block', [32]],
                ['block_ordinal', 'u64'],
                ['prev_height', 'u64'],
                ['epoch_sync_data_hash', {
                    'kind': 'option',
                    'type': [32]
                }],
                ['approvals', [{
                    'kind': 'option',
                    'type': Signature
                }]],
                ['latest_protocol_version', 'u32'],
                ['chunk_endorsements', ChunkEndorsementsBitmap],
            ]
        }
    ],
    [
        ChunkEndorsementsBitmap, {
            'kind': 'struct',
            'fields': [['inner', [['u8']]],],
        }
    ],
    [
        ShardChunkHeader, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['V1', ShardChunkHeaderV1], ['V2', ShardChunkHeaderV2],
                       ['V3', ShardChunkHeaderV3]]
        }
    ],
    [
        ShardChunkHeaderV1, {
            'kind':
                'struct',
            'fields': [
                ['inner', ShardChunkHeaderInnerV1],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderV2, {
            'kind':
                'struct',
            'fields': [
                ['inner', ShardChunkHeaderInnerV1],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderV3, {
            'kind':
                'struct',
            'fields': [
                ['inner', ShardChunkHeaderInner],
                ['height_included', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ShardChunkHeaderInner, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['V1', ShardChunkHeaderInnerV1],
                       ['V2', ShardChunkHeaderInnerV2],
                       ['V3', ShardChunkHeaderInnerV3]]
        }
    ],
    [
        ShardChunkHeaderInnerV1, {
            'kind':
                'struct',
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
            'kind':
                'struct',
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
        ShardChunkHeaderInnerV3, {
            'kind':
                'struct',
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
                ['congestion_info', CongestionInfo],
            ]
        }
    ],
    [
        ShardChunk, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V1', ShardChunkV1], ['V2', ShardChunkV2]]
        }
    ],
    [
        ShardChunkV1, {
            'kind':
                'struct',
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
            'kind':
                'struct',
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
            'kind':
                'struct',
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
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['V1', PartialEncodedChunkV1],
                       ['V2', PartialEncodedChunkV2]]
        }
    ],
    [
        PartialEncodedChunkV1, {
            'kind':
                'struct',
            'fields': [['header', ShardChunkHeaderV1],
                       ['parts', [PartialEncodedChunkPart]],
                       ['receipts', [ReceiptProof]]]
        }
    ],
    [
        PartialEncodedChunkV2, {
            'kind':
                'struct',
            'fields': [['header', ShardChunkHeader],
                       ['parts', [PartialEncodedChunkPart]],
                       ['receipts', [ReceiptProof]]]
        }
    ],
    [
        PartialEncodedChunkRequestMsg, {
            'kind':
                'struct',
            'fields': [['chunk_hash', [32]], ['part_ords', ['u64']],
                       ['tracking_shards', ['u64']]]
        }
    ],
    [
        PartialEncodedChunkResponseMsg, {
            'kind':
                'struct',
            'fields': [['chunk_hash', [32]],
                       ['parts', [PartialEncodedChunkPart]],
                       ['receipts', [ReceiptProof]]]
        }
    ],
    [
        PartialEncodedChunkForwardMsg, {
            'kind':
                'struct',
            'fields': [['chunk_hash', [32]], ['inner_header_hash', [32]],
                       ['merkle_root', [32]], ['signature', Signature],
                       ['prev_block_hash', [32]], ['height_created', 'u64'],
                       ['shard_id', 'u64'],
                       ['parts', [PartialEncodedChunkPart]]]
        }
    ],
    [
        ValidatorStake, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V1', ValidatorStakeV1], ['V2', ValidatorStakeV2]]
        }
    ],
    [
        ValidatorStakeV1, {
            'kind':
                'struct',
            'fields': [
                ['account_id', 'string'],
                ['public_key', PublicKey],
                ['stake', 'u128'],
            ]
        }
    ],
    [
        Approval, {
            'kind':
                'struct',
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
    [
        CongestionInfo, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V1', CongestionInfoV1]]
        }
    ],
    [
        CongestionInfoV1, {
            'kind':
                'struct',
            'fields': [
                ['delayed_receipts_gas', 'u128'],
                ['buffered_receipts_gas', 'u128'],
                ['receipt_bytes', 'u64'],
                ['allowed_shard', 'u16'],
            ]
        }
    ],
    [
        ChunkEndorsement, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V1', ChunkEndorsementV1], ['V2', ChunkEndorsementV2]]
        }
    ],
    [
        ChunkEndorsementV1, {
            'kind':
                'struct',
            'fields': [
                ['inner', ChunkEndorsementInner],
                ['account_id', 'string'],
                ['signature', Signature],
            ]
        }
    ],
    [
        ChunkEndorsementV2, {
            'kind':
                'struct',
            'fields': [
                ['inner', ChunkEndorsementInner],
                ['signature', Signature],
                ['metadata', ChunkEndorsementMetadata],
                ['metadata_signature', Signature],
            ]
        }
    ],
    [
        ChunkEndorsementInner, {
            'kind':
                'struct',
            'fields': [
                ['chunk_hash', [32]],
                ['signature_differentiator', SignatureDifferentiator],
            ]
        }
    ],
    [
        ChunkEndorsementMetadata, {
            'kind':
                'struct',
            'fields': [
                ['account_id', 'string'],
                ['shard_id', 'u64'],
                ['epoch_id', [32]],
                ['height_created', 'u64'],
            ]
        }
    ],
    [
        ChunkStateWitnessAck, {
            'kind': 'struct',
            'fields': [['chunk_hash', [32]],]
        }
    ],
    [
        PartialEncodedStateWitness, {
            'kind':
                'struct',
            'fields': [
                ['inner', PartialEncodedStateWitnessInner],
                ['signature', Signature],
            ]
        }
    ],
    [
        PartialEncodedStateWitnessInner, {
            'kind':
                'struct',
            'fields': [
                ['epoch_id', [32]],
                ['shard_id', 'u64'],
                ['height_created', 'u64'],
                ['part_ord', 'u64'],
                ['part', ['u8']],
                ['encoded_length', 'u64'],
                ['signature_differentiator', SignatureDifferentiator],
            ]
        }
    ],
    [SignatureDifferentiator, {
        'kind': 'struct',
        'fields': [['0', 'string']]
    }]
]
