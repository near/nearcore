from serializer import BinarySerializer
import hashlib, base58

class BlockHeaderInnerLite:
    pass

inner_lite_schema = dict([[BlockHeaderInnerLite, {'kind': 'struct', 'fields': [
        ['block_index', 'u64'],
        ['epoch_id', [32]],
        ['next_epoch_id', [32]],
        ['prev_state_root', [32]],
        ['outcome_root', [32]],
        ['timestamp', 'u64'],
        ['next_bp_hash', [32]],
]} ],
])

def combine_hash(hash1, hash2):
    return hashlib.sha256(hash1 + hash2).digest()

def compute_block_hash(inner_lite_view, inner_rest_hash, prev_hash):
    inner_rest_hash = base58.b58decode(inner_rest_hash)
    prev_hash = base58.b58decode(prev_hash)

    inner_lite = BlockHeaderInnerLite()
    inner_lite.block_index = inner_lite_view['block_index']
    inner_lite.epoch_id = base58.b58decode(inner_lite_view['epoch_id'])
    inner_lite.next_epoch_id = base58.b58decode(inner_lite_view['next_epoch_id'])
    inner_lite.prev_state_root = base58.b58decode(inner_lite_view['prev_state_root'])
    inner_lite.outcome_root = base58.b58decode(inner_lite_view['outcome_root'])
    inner_lite.timestamp = inner_lite_view['timestamp']
    inner_lite.next_bp_hash = base58.b58decode(inner_lite_view['next_bp_hash'])

    msg = BinarySerializer(inner_lite_schema).serialize(inner_lite)
    inner_lite_hash = hashlib.sha256(msg).digest()
    inner_hash = combine_hash(inner_lite_hash, inner_rest_hash)
    final_hash = combine_hash(inner_hash, prev_hash)

    return base58.b58encode(final_hash)

# follows the spec from NEP 25 (https://github.com/nearprotocol/NEPs/pull/25)
def validate_light_client_block(last_known_block, new_block, block_producers_map, panic=False):
    new_block_hash = compute_block_hash(new_block['inner_lite'], new_block['inner_rest_hash'], new_block['prev_hash'])

    if new_block['inner_lite']['epoch_id'] not in [last_known_block['inner_lite']['epoch_id'], last_known_block['inner_lite']['next_epoch_id']]:
        if panic: assert False
        return False

    block_producers = block_producers_map[new_block['inner_lite']['epoch_id']]
    if len(new_block['qv_approvals']) != len(block_producers) or len(new_block['qc_approvals']) != len(block_producers):
        if panic: assert False
        return False

    if len(new_block['future_inner_hashes']) > 50:
        if panic: assert False
        return False
        
    qv_blocks = set()
    qc_blocks = set()

    prev_hash = base58.b58decode(new_block_hash)
    passed_qv = False
    qv_hash = base58.b58decode(new_block['qv_hash'])
    for future_inner_hash in new_block['future_inner_hashes']:
        cur_hash = combine_hash(base58.b58decode(future_inner_hash), prev_hash)
        if cur_hash == qv_hash:
            passed_qv = True
        if passed_qv:
            qc_blocks.add(cur_hash)
        else:
            qv_blocks.add(cur_hash)
        prev_hash = cur_hash

    if not passed_qv:
        if panic: assert False
        return False

    qv_blocks = [base58.b58encode(x).decode('ascii') for x in qv_blocks]
    qc_blocks = [base58.b58encode(x).decode('ascii') for x in qc_blocks]

    total_stake = 0
    qv_stake = 0
    qc_stake = 0

    for qv_approval, qc_approval, stake in zip(new_block['qv_approvals'], new_block['qc_approvals'], block_producers):
        if qv_approval is not None:
            qv_stake += int(stake['amount'])
            if qv_approval['parent_hash'] not in qv_blocks and qv_approval['parent_hash'] != new_block_hash.decode('ascii'):
                if panic: assert False
                return False
            if qv_approval['reference_hash'] in qv_blocks:
                if panic: assert False
                return False
            #if not validate_signature(qv_approval.signature, hash(qv_approval), stake.public_key):
            #    if panic: assert False
            #    return False

        if qc_approval is not None:
            qc_stake += int(stake['amount'])
            if qc_approval['parent_hash'] not in qc_blocks:
                if panic: assert False
                return False
            if qc_approval['reference_hash'] in qc_blocks or qc_approval['reference_hash'] in qv_blocks:
                if panic: assert False
                return False
            #if not validate_signature(qc_approval.signature, hash(qc_approval), stake.public_key):
            #    return false

        total_stake += int(stake['amount'])

    threshold = total_stake * 2 // 3
    if qv_stake <= threshold:
        if panic: assert False
        return False
    if qc_stake <= threshold:
        if panic: assert False
        return False

    if new_block['inner_lite']['epoch_id'] == last_known_block['inner_lite']['next_epoch_id']:
        if new_block['next_bps'] is None:
            if panic: assert False
            return False
            
        # TODO: MOO check hash

        block_producers_map[new_block['inner_lite']['next_epoch_id']] = new_block['next_bps']

