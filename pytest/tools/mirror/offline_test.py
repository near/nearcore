#!/usr/bin/env python3
# Tests `neard mirror` using the fork-network workflow.
# Builds two images (imgA=forked state, imgB=source chain), starts 4 target
# validators from imgA, runs mirror to replay imgB's txs, then validates.
# Saves images to disk so subsequent runs skip the slow build phases.
# cspell:ignore bhash

import argparse
import base58
import json
import os
import pathlib
import shutil
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import spin_up_node, init_cluster, load_config
from configured_logger import logger
import key
import transaction
import utils

import mirror_utils

MIRROR_DIR = pathlib.Path.home() / '.near' / 'test-mirror'
SAVED_IMG_A = MIRROR_DIR / 'saved-imgA'
SAVED_IMG_B = MIRROR_DIR / 'saved-imgB'
METADATA_FILE = MIRROR_DIR / 'metadata.json'
TARGET_VALIDATORS = mirror_utils.TARGET_VALIDATORS


def build_images(config):
    """Phases 1-4: build imgA (forked state) and imgB (source chain with traffic)."""
    neard = os.path.join(config['near_root'],
                         config.get('binary_name', 'neard'))

    # Clean up any stale state from previous runs
    if MIRROR_DIR.exists():
        shutil.rmtree(MIRROR_DIR)

    # Phase 1: start 1 source node (single validator, 4 shards, archive)
    logger.info('Phase 1: starting source node')
    near_root, node_dirs = init_cluster(
        num_nodes=1,
        num_observers=0,
        num_shards=4,
        config=config,
        genesis_config_changes=[["epoch_length", 100]],
        client_config_changes={
            0: {
                "tracked_shards_config": "AllShards",
                "archive": True,
            }
        },
    )
    source_node = spin_up_node(config,
                               near_root,
                               node_dirs[0],
                               ordinal=0,
                               single_node=True)

    # Phase 2: deploy contract, create implicit account, wait for height > 12
    logger.info('Phase 2: issuing initial transactions')
    nonce = 2
    implicit = mirror_utils.ImplicitAccount()
    initial_txs_sent = False

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        bhash = base58.b58decode(block_hash.encode('utf8'))
        if not initial_txs_sent:
            implicit.transfer_to(source_node, source_node.signer_key, 10**24,
                                 bhash, nonce)
            nonce += 1
            mirror_utils.deploy_addkey_contract(source_node,
                                                source_node.signer_key,
                                                mirror_utils.CONTRACT_PATH,
                                                nonce, bhash)
            nonce += 1
            # Gas key 1: added pre-fork, baked into forked state (imgA)
            gas_key_1 = key.Key.from_random('test0')
            num_nonces_1 = 3
            mirror_utils.send_add_gas_key(source_node, source_node.signer_key,
                                           gas_key_1, num_nonces_1, nonce,
                                           bhash)
            nonce += 1
            mirror_utils.fund_gas_key(source_node, source_node.signer_key,
                                       gas_key_1.decoded_pk(), 10**24, nonce,
                                       bhash)
            nonce += 1
            initial_txs_sent = True
        implicit.send_if_inited(source_node, [('test0', height)], bhash)
        if height > 12:
            break

    # Phase 3: stop source, copy its DB, run fork-network on the copy -> imgA
    logger.info('Phase 3: creating imgA via fork-network')
    source_node.kill()
    source_home = pathlib.Path(source_node.node_dir)
    fork_base = MIRROR_DIR / 'fork-base'
    os.makedirs(MIRROR_DIR, exist_ok=True)

    mirror_utils.copy_near_home(source_home, fork_base)
    shutil.copyfile(source_home / 'validator_key.json',
                    fork_base / 'validator_key.json')
    validator_keys = mirror_utils.fork_network(neard, fork_base,
                                               TARGET_VALIDATORS)

    # Phase 4: restart source, send ~100 blocks of traffic -> imgB
    logger.info('Phase 4: sending traffic to create imgB')
    source_node.start()
    time.sleep(5)

    tip = source_node.get_latest_block()
    bhash = base58.b58decode(tip.hash.encode('utf8'))
    start_source_height = tip.height
    expectations = []

    # Gas key 2: added post-fork, mirror must replay AddKey + TransferToGasKey
    gas_key_2 = key.Key.from_random('test0')
    num_nonces_2 = 2
    mirror_utils.send_add_gas_key(source_node, source_node.signer_key,
                                   gas_key_2, num_nonces_2, nonce, bhash)
    nonce += 1
    mirror_utils.fund_gas_key(source_node, source_node.signer_key,
                               gas_key_2.decoded_pk(), 10**24, nonce, bhash)
    nonce += 1
    mirror_utils.withdraw_from_gas_key(source_node, source_node.signer_key,
                                       gas_key_2.decoded_pk(), 10**22, nonce,
                                       bhash)
    nonce += 1

    subaccount_key = mirror_utils.AddedKey(
        mirror_utils.create_subaccount(source_node,
                                       'foo',
                                       source_node.signer_key,
                                       nonce,
                                       bhash,
                                       extra_key=True))
    nonce += 1
    expectations.append({'type': 'account_exists', 'account_id': 'foo.test0'})

    k = key.Key.from_random('test0')
    new_key = mirror_utils.AddedKey(k)
    mirror_utils.send_add_access_key(source_node, source_node.signer_key, k,
                                     nonce, bhash)
    nonce += 1
    # Direct AddKey action: mirror maps the public key
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(k.pk)
    })

    contract_key = key.Key.from_random('test0')
    contract_extra_key = key.Key.from_random('test0')
    mirror_utils.call_addkey(source_node,
                             source_node.signer_key,
                             contract_key,
                             nonce,
                             bhash,
                             extra_actions=[
                                 transaction.create_full_access_key_action(
                                     contract_extra_key.decoded_pk())
                             ])
    nonce += 1
    # contract_key is added by contract execution (unmapped args)
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0',
        'public_key': contract_key.pk
    })
    # contract_extra_key is a direct AddKey action: mirror maps it
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(contract_extra_key.pk)
    })
    contract_key = mirror_utils.AddedKey(contract_key)
    contract_extra_key = mirror_utils.AddedKey(contract_extra_key)

    sub_key = mirror_utils.AddedKey(key.Key.from_random('test0.test0'))
    mirror_utils.call_create_account(source_node, source_node.signer_key,
                                     sub_key.key.account_id, sub_key.key.pk,
                                     nonce, bhash)
    nonce += 1
    expectations.append({'type': 'account_exists', 'account_id': 'test0.test0'})
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0.test0',
        'public_key': sub_key.key.pk
    })

    # Send 1 yocto (fails to create), then enough to actually create
    implicit2 = mirror_utils.ImplicitAccount()
    implicit2.transfer_to(source_node, source_node.signer_key, 1, bhash, nonce)
    nonce += 1
    time.sleep(2)
    implicit2.transfer_to(source_node, source_node.signer_key, 10**24, bhash,
                          nonce)
    nonce += 1
    # Implicit account ID is derived from public key, which gets mapped
    expectations.append({
        'type': 'account_exists',
        'account_id': mirror_utils.map_account_no_secret(implicit2.account_id())
    })

    contract_deployed = False
    staked = False
    added_keys = [
        new_key, subaccount_key, contract_key, contract_extra_key, sub_key
    ]

    # Gas key V1 transfer targets
    gk1_recipient = mirror_utils.ImplicitAccount()
    gk2_recipient = mirror_utils.ImplicitAccount()
    gas_key_transfers_sent = False

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        bhash = base58.b58decode(block_hash.encode('utf8'))

        tx = transaction.sign_payment_tx(source_node.signer_key,
                                         source_node.signer_key.account_id, 300,
                                         nonce, bhash)
        source_node.send_tx(tx)
        nonce += 1

        implicit.send_if_inited(source_node, [('test0', height)], bhash)
        implicit2.send_if_inited(source_node, [('test0', height)], bhash)
        mirror_utils.added_keys_send_transfers(
            [source_node], added_keys,
            [implicit.account_id(),
             implicit2.account_id(), 'test0'], height, bhash)

        if subaccount_key.inited():
            if not contract_deployed:
                subaccount_key.nonce += 1
                mirror_utils.deploy_addkey_contract(source_node,
                                                    subaccount_key.key,
                                                    mirror_utils.CONTRACT_PATH,
                                                    subaccount_key.nonce, bhash)
                contract_deployed = True
                expectations.append({
                    'type': 'contract_deployed',
                    'account_id': subaccount_key.account_id()
                })
            elif not staked and mirror_utils.contract_deployed(
                    source_node, subaccount_key.account_id()):
                subaccount_key.nonce += 1
                mirror_utils.call_stake(source_node, subaccount_key.key, 10**28,
                                        subaccount_key.key.pk,
                                        subaccount_key.nonce, bhash)
                staked = True

        # Send V1 gas key transfers once gas keys are likely funded
        if not gas_key_transfers_sent and height - start_source_height > 10:
            # Gas key nonces start at block_height * multiplier; use nonce 1
            # which is always valid for a freshly created gas key
            for gk, ni_max, recipient in [
                (gas_key_1, num_nonces_1, gk1_recipient),
                (gas_key_2, num_nonces_2, gk2_recipient),
            ]:
                for ni in range(ni_max):
                    mirror_utils.send_gas_key_transfer(
                        source_node, gk, recipient.account_id(), 10**24,
                        height * 1000000 + 1, ni, bhash)
            gas_key_transfers_sent = True

        if height - start_source_height >= 100:
            break

    # Gas key expectations
    # Gas key 1 (pre-fork): key baked into forked state, V1 txs replayed
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(gas_key_1.pk)
    })
    expectations.append({
        'type': 'gas_key_nonces',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(gas_key_1.pk),
        'num_nonces': num_nonces_1
    })
    # Gas key 2 (post-fork): mirror must replay AddKey + TransferToGasKey + WithdrawFromGasKey
    expectations.append({
        'type': 'has_key',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(gas_key_2.pk)
    })
    expectations.append({
        'type': 'gas_key_nonces',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(gas_key_2.pk),
        'num_nonces': num_nonces_2
    })
    expectations.append({
        'type': 'gas_key_balance_below',
        'account_id': 'test0',
        'public_key': mirror_utils.map_key_no_secret(gas_key_2.pk),
        'upper_bound': 10**24 - 10**22,
    })
    # V1 tx success: implicit account creation proves gas key transfers landed
    expectations.append({
        'type':
            'account_exists',
        'account_id':
            mirror_utils.map_account_no_secret(gk1_recipient.account_id())
    })
    expectations.append({
        'type':
            'account_exists',
        'account_id':
            mirror_utils.map_account_no_secret(gk2_recipient.account_id())
    })

    end_source_height = source_node.get_latest_block().height
    source_node.kill()
    logger.info('Phase 4: source stopped, imgB ready')

    # Save images for reuse on subsequent runs
    logger.info('Saving images for reuse')
    mirror_utils.copy_near_home(fork_base, SAVED_IMG_A)
    mirror_utils.copy_near_home(source_home, SAVED_IMG_B)
    shutil.copyfile(source_home / 'validator_key.json',
                    SAVED_IMG_B / 'validator_key.json')
    with open(METADATA_FILE, 'w') as f:
        json.dump(
            {
                'validator_keys': [k.to_json() for k in validator_keys],
                'end_source_height': end_source_height,
                'expectations': expectations,
                'gas_keys': {
                    'gas_key_1_pk': gas_key_1.pk,
                    'num_nonces_1': num_nonces_1,
                    'gas_key_2_pk': gas_key_2.pk,
                    'num_nonces_2': num_nonces_2,
                },
            },
            f,
            indent=2)


def run_mirror(config, validator_keys, end_source_height, expectations=None):
    """Phases 5-6: start target network, run mirror, validate."""
    near_root = config['near_root']

    logger.info('Phase 5: restoring images and starting target network')
    for name in ['stdout', 'stderr', 'config.json']:
        p = MIRROR_DIR / name
        if p.exists():
            p.unlink()

    # Restore working copies from saved images
    source_dir = MIRROR_DIR / 'source'
    mirror_utils.copy_near_home(SAVED_IMG_B, source_dir)
    shutil.copyfile(SAVED_IMG_B / 'validator_key.json',
                    source_dir / 'validator_key.json')
    mirror_utils.copy_near_home(SAVED_IMG_A, MIRROR_DIR / 'target')

    # Prepare 4 target validator dirs: copy imgA + validator key + AllShards
    base_ordinal = 2
    target_node_dirs = []
    for i, name in enumerate(TARGET_VALIDATORS):
        d = MIRROR_DIR / f'test_target_{name}'
        target_node_dirs.append(str(d))
        mirror_utils.copy_near_home(SAVED_IMG_A, d)
        with open(d / 'validator_key.json', 'w') as f:
            json.dump(validator_keys[i].to_json(), f, indent=2)
        with open(d / 'config.json') as f:
            cfg = json.load(f)
        cfg['tracked_shards_config'] = 'AllShards'
        with open(d / 'config.json', 'w') as f:
            json.dump(cfg, f, indent=2)

    logger.info('Starting 4 target validators')
    target_nodes = []
    for i in range(len(TARGET_VALIDATORS)):
        node = spin_up_node(config,
                            near_root,
                            target_node_dirs[i],
                            base_ordinal + i,
                            boot_node=target_nodes or None)
        target_nodes.append(node)

    # Point mirror's target node at all validators
    target_home = MIRROR_DIR / 'target'
    with open(target_home / 'config.json') as f:
        target_cfg = json.load(f)
    target_cfg['network']['boot_nodes'] = ','.join(
        n.addr_with_pk() for n in target_nodes)
    with open(target_home / 'config.json', 'w') as f:
        json.dump(target_cfg, f, indent=2)

    # Start mirror (--no-secret: fork-network uses identity key mapping)
    logger.info('Starting mirror process')
    mirror = mirror_utils.MirrorProcess(near_root, str(source_dir),
                                        config.get('binary_name', 'neard'))
    time_limit = mirror_utils.allowed_run_time(target_node_dirs[0],
                                               mirror.start_time,
                                               end_source_height)

    while True:
        time.sleep(5)
        if not mirror.restart_once():
            break
        elapsed = time.time() - mirror.start_time
        if elapsed > time_limit:
            mirror.process.terminate()
            mirror.process.wait()
            assert False, f'mirror process timed out after {int(elapsed)}s'

    logger.info('Waiting for target chain to settle')
    time.sleep(15)

    # Phase 6: validate that mirror replayed a substantial fraction of txs.
    # Can't require exact match: blocks before the fork point are baked into
    # forked state and some mapped txs fail (nonce conflicts, etc.).
    logger.info('Phase 6: validating results')
    source_ordinal = base_ordinal + len(TARGET_VALIDATORS) + 1
    source_node = spin_up_node(config,
                               near_root,
                               str(source_dir),
                               source_ordinal,
                               single_node=True)
    time.sleep(5)

    with open(os.path.join(target_node_dirs[0], 'genesis.json')) as f:
        genesis_height = json.load(f)['genesis_height']
    total_source = mirror_utils.count_total_txs(source_node,
                                                min_height=genesis_height)
    total_target = mirror_utils.count_total_txs(target_nodes[0])
    logger.info(f'source txs: {total_source}, target txs: {total_target}')
    assert total_target >= total_source * 0.5, \
        f'target has too few txs: {total_target} vs source {total_source}'

    if expectations:
        mirror_utils.verify_expectations(target_nodes[0], expectations)

    logger.info('offline_test PASSED')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reuse-images',
                        action='store_true',
                        help='Reuse saved images from a previous run instead '
                        'of rebuilding them')
    args = parser.parse_args()

    config = load_config()

    have_images = (SAVED_IMG_A.exists() and SAVED_IMG_B.exists() and
                   METADATA_FILE.exists())
    if args.reuse_images and have_images:
        logger.info('Reusing saved images from previous run')
    else:
        if args.reuse_images:
            logger.info('No saved images found, building from scratch')
        build_images(config)

    with open(METADATA_FILE) as f:
        metadata = json.load(f)
    validator_keys = [key.Key.from_json(k) for k in metadata['validator_keys']]
    expectations = metadata.get('expectations', [])
    run_mirror(config, validator_keys, metadata['end_source_height'],
               expectations)


if __name__ == '__main__':
    main()
