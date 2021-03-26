from serializer import BinarySerializer
import hashlib, base58
import nacl.signing
from utils import combine_hash

ED_PREFIX = "ed25519:"


class BlockHeaderInnerLite:
    pass


inner_lite_schema = dict([
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
])


def compute_block_hash(inner_lite_view, inner_rest_hash, prev_hash):
    inner_rest_hash = base58.b58decode(inner_rest_hash)
    prev_hash = base58.b58decode(prev_hash)

    inner_lite = BlockHeaderInnerLite()
    inner_lite.height = inner_lite_view['height']
    inner_lite.epoch_id = base58.b58decode(inner_lite_view['epoch_id'])
    inner_lite.next_epoch_id = base58.b58decode(
        inner_lite_view['next_epoch_id'])
    inner_lite.prev_state_root = base58.b58decode(
        inner_lite_view['prev_state_root'])
    inner_lite.outcome_root = base58.b58decode(inner_lite_view['outcome_root'])
    inner_lite.timestamp = int(inner_lite_view['timestamp_nanosec'])
    inner_lite.next_bp_hash = base58.b58decode(inner_lite_view['next_bp_hash'])
    inner_lite.block_merkle_root = base58.b58decode(
        inner_lite_view['block_merkle_root'])

    msg = BinarySerializer(inner_lite_schema).serialize(inner_lite)
    inner_lite_hash = hashlib.sha256(msg).digest()
    inner_hash = combine_hash(inner_lite_hash, inner_rest_hash)
    final_hash = combine_hash(inner_hash, prev_hash)

    return base58.b58encode(final_hash)


# follows the spec from NEP 25 (https://github.com/nearprotocol/NEPs/pull/25)
def validate_light_client_block(last_known_block,
                                new_block,
                                block_producers_map,
                                panic=False):
    new_block_hash = compute_block_hash(new_block['inner_lite'],
                                        new_block['inner_rest_hash'],
                                        new_block['prev_block_hash'])
    next_block_hash_decoded = combine_hash(
        base58.b58decode(new_block['next_block_inner_hash']),
        base58.b58decode(new_block_hash))

    if new_block['inner_lite']['epoch_id'] not in [
            last_known_block['inner_lite']['epoch_id'],
            last_known_block['inner_lite']['next_epoch_id']
    ]:
        if panic:
            assert False
        return False

    block_producers = block_producers_map[new_block['inner_lite']['epoch_id']]
    if len(new_block['approvals_after_next']) != len(block_producers):
        if panic:
            assert False
        return False

    total_stake = 0
    approved_stake = 0

    for approval, stake in zip(new_block['approvals_after_next'],
                               block_producers):
        total_stake += int(stake['stake'])

        if approval is None:
            continue

        approved_stake += int(stake['stake'])

        public_key = stake['public_key']

        signature = base58.b58decode(approval[len(ED_PREFIX):])
        verify_key = nacl.signing.VerifyKey(
            base58.b58decode(public_key[len(ED_PREFIX):]))

        approval_message = bytearray()
        approval_message.append(0)
        approval_message += next_block_hash_decoded
        approval_message.append(new_block['inner_lite']['height'] + 2)
        for i in range(7):
            approval_message.append(0)
        approval_message = bytes(approval_message)
        verify_key.verify(approval_message, signature)

    threshold = total_stake * 2 // 3
    if approved_stake <= threshold:
        if panic:
            assert False
        return False

    if new_block['inner_lite']['epoch_id'] == last_known_block['inner_lite'][
            'next_epoch_id']:
        if new_block['next_bps'] is None:
            if panic:
                assert False
            return False

        serialized_next_bp = bytearray()
        serialized_next_bp.append(len(new_block['next_bps']))
        for i in range(3):
            serialized_next_bp.append(0)
        for bp in new_block['next_bps']:
            if 'validator_stake_struct_version' in bp:
                # version of ValidatorStake enum
                version = int(bp['validator_stake_struct_version'][1:]) - 1
                serialized_next_bp.append(version)
            serialized_next_bp.append(5)
            for i in range(3):
                serialized_next_bp.append(0)
            serialized_next_bp += bp['account_id'].encode('utf-8')
            serialized_next_bp.append(0)  # public key type
            serialized_next_bp += base58.b58decode(
                bp['public_key'][len(ED_PREFIX):])
            stake = int(bp['stake'])
            for i in range(16):
                serialized_next_bp.append(stake & 255)
                stake >>= 8

        serialized_next_bp = bytes(serialized_next_bp)

        computed_hash = base58.b58encode(
            hashlib.sha256(serialized_next_bp).digest())
        if computed_hash != new_block['inner_lite']['next_bp_hash'].encode(
                'utf-8'):
            if panic:
                assert False
            return False

        block_producers_map[new_block['inner_lite']
                            ['next_epoch_id']] = new_block['next_bps']
