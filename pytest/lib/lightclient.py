from serializer import BinarySerializer
import hashlib, base58


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


def combine_hash(hash1, hash2):
    return hashlib.sha256(hash1 + hash2).digest()


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
    inner_lite.timestamp = inner_lite_view['timestamp']
    inner_lite.next_bp_hash = base58.b58decode(inner_lite_view['next_bp_hash'])
    inner_lite.block_merkle_root = base58.b58decode(inner_lite_view['block_merkle_root'])

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

    if new_block['inner_lite']['epoch_id'] not in [
            last_known_block['inner_lite']['epoch_id'],
            last_known_block['inner_lite']['next_epoch_id']
    ]:
        if panic: assert False
        return False

    block_producers = block_producers_map[new_block['inner_lite']['epoch_id']]
    if len(new_block['approvals_after_next']) != len(block_producers):
        if panic: assert False
        return False

    total_stake = 0
    approved_stake = 0

    for approval, stake in zip(new_block['approvals_after_next'],
                               block_producers):
        if approval is not None:
            approved_stake += int(stake['stake'])

        total_stake += int(stake['stake'])

    threshold = total_stake * 2 // 3
    if approved_stake <= threshold:
        if panic: assert False
        return False

    if new_block['inner_lite']['epoch_id'] == last_known_block['inner_lite'][
            'next_epoch_id']:
        if new_block['next_bps'] is None:
            if panic: assert False
            return False

        # TODO: MOO check hash

        block_producers_map[new_block['inner_lite']
                            ['next_epoch_id']] = new_block['next_bps']
