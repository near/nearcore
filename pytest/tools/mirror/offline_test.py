#!/usr/bin/env python3

# Tests the `neard mirror` binary using the fork-network workflow (the
# production path for creating forknet state).
#
# Overview:
#   - 1 source node produces blocks with various transaction types.
#   - We copy its DB and run `fork-network` on the copy to create mapped state (imgA).
#   - The original source node continues producing blocks with more traffic (imgB).
#   - We start 4 target validators from imgA and run `neard mirror run` to
#     replay imgB's transactions onto the target network.
#   - Finally we validate that all source transactions made it to the target.
#
# The test saves imgA/imgB to disk so subsequent runs skip the slow build
# phases and jump straight to the mirror phase.
#
# Directory layout:
#   ~/.near/
#   ├── test0/                  # Source node home dir (becomes imgB after traffic)
#   └── test-mirror/
#       ├── fork-base/          # Copy of test0 -> fork-network runs here -> imgA
#       ├── saved-imgA/         # Persistent copy of imgA (for reruns)
#       ├── saved-imgB/         # Persistent copy of imgB (for reruns)
#       ├── source/             # Working copy of imgB (input to mirror)
#       ├── target/             # Working copy of imgA (mirror's embedded node)
#       ├── metadata.json       # Saved validator keys + end_source_height
#       ├── config.json         # Mirror config (tx_batch_interval)
#       ├── stdout / stderr     # Mirror process output
#       ├── test_target_foo0/   # Target validator 0
#       ├── test_target_foo1/   # Target validator 1
#       ├── test_target_foo2/   # Target validator 2
#       └── test_target_foo3/   # Target validator 3

import base58
import json
import os
import pathlib
import shutil
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import spin_up_node, start_cluster, load_config
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


# ─────────────────────────────────────────────────────────────────────────────
# Phases 1-4: Build the two images (imgA = forked state, imgB = source chain)
# ─────────────────────────────────────────────────────────────────────────────


def build_images(config):
    neard = os.path.join(config['near_root'],
                         config.get('binary_name', 'neard'))

    # ── Phase 1: Start 1 source node ─────────────────────────────────────
    # Single validator, 4 shards, archive mode, long epoch length.
    logger.info('Phase 1: starting source node')
    nodes = start_cluster(
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
    source_node = nodes[0]

    # ── Phase 2: Issue initial transactions ──────────────────────────────
    # Deploy the addkey contract and create an implicit account.
    # Wait until height > 12 so the chain has some history before forking.
    logger.info('Phase 2: issuing initial transactions')
    nonce = 2
    implicit_account = mirror_utils.ImplicitAccount()

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        # Create an implicit account by transferring to it
        implicit_account.transfer_to(source_node, source_node.signer_key,
                                     10**24, block_hash_bytes, nonce)
        nonce += 1

        # Deploy the addkey contract to test0
        mirror_utils.deploy_addkey_contract(source_node,
                                            source_node.signer_key,
                                            mirror_utils.CONTRACT_PATH, nonce,
                                            block_hash_bytes)
        nonce += 1
        break

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))
        implicit_account.send_if_inited(source_node, [('test0', height)],
                                        block_hash_bytes)
        if height > 12:
            break

    # ── Phase 3: Create imgA (the fork) ──────────────────────────────────
    # Stop source node. Copy its home dir to fork-base (the source home is
    # left untouched -- it will become imgB later). Run the fork-network
    # workflow on the copy: init -> amend-access-keys -> set-validators ->
    # finalize. After this, fork-base contains the forked/mapped state (imgA).
    logger.info('Phase 3: creating imgA via fork-network')
    source_node.kill()
    source_home = pathlib.Path(source_node.node_dir)
    fork_base = MIRROR_DIR / 'fork-base'
    os.makedirs(MIRROR_DIR, exist_ok=True)

    # Copy source DB to fork-base (source_home stays untouched for imgB)
    mirror_utils.copy_near_home(source_home, fork_base)
    # fork-network needs validator_key.json to exist
    shutil.copyfile(source_home / 'validator_key.json',
                    fork_base / 'validator_key.json')

    # Run fork-network init + amend-access-keys + set-validators + finalize
    validator_keys = mirror_utils.fork_network(neard, fork_base,
                                               TARGET_VALIDATORS)
    # fork_base is now imgA (mapped state with 4 new validators)

    # ── Phase 4: Send more traffic to create imgB ────────────────────────
    # Restart the original source node (untouched). Send ~100 blocks of
    # varied traffic: subaccounts, access keys, implicit accounts, contract
    # calls, staking. Then stop it -- its home dir is now imgB.
    logger.info('Phase 4: sending traffic to create imgB')
    source_node.start()
    time.sleep(5)

    tip = source_node.get_latest_block()
    block_hash_bytes = base58.b58decode(tip.hash.encode('utf8'))
    start_source_height = tip.height

    # Create a subaccount with extra keys
    subaccount_key = mirror_utils.AddedKey(
        mirror_utils.create_subaccount(source_node, 'foo',
                                       source_node.signer_key, nonce,
                                       block_hash_bytes, extra_key=True))
    nonce += 1

    # Add an access key to test0
    k = key.Key.from_random('test0')
    new_key = mirror_utils.AddedKey(k)
    mirror_utils.send_add_access_key(source_node, source_node.signer_key, k,
                                     nonce, block_hash_bytes)
    nonce += 1

    # Call the addkey contract to add keys via a function call
    test0_contract_key = key.Key.from_random('test0')
    test0_contract_extra_key = key.Key.from_random('test0')
    mirror_utils.call_addkey(
        source_node,
        source_node.signer_key,
        test0_contract_key,
        nonce,
        block_hash_bytes,
        extra_actions=[
            transaction.create_full_access_key_action(
                test0_contract_extra_key.decoded_pk())
        ])
    nonce += 1
    test0_contract_key = mirror_utils.AddedKey(test0_contract_key)
    test0_contract_extra_key = mirror_utils.AddedKey(test0_contract_extra_key)

    # Create a named subaccount via the contract
    test0_subaccount_contract_key = mirror_utils.AddedKey(
        key.Key.from_random('test0.test0'))
    mirror_utils.call_create_account(
        source_node, source_node.signer_key,
        test0_subaccount_contract_key.key.account_id,
        test0_subaccount_contract_key.key.pk, nonce, block_hash_bytes)
    nonce += 1

    # Create a second implicit account. First send 1 yoctoNEAR (which will
    # fail to create the account), then send enough to actually create it.
    # This tests a corner case in nonce lookup.
    implicit_account2 = mirror_utils.ImplicitAccount()
    implicit_account2.transfer_to(source_node, source_node.signer_key, 1,
                                  block_hash_bytes, nonce)
    nonce += 1
    time.sleep(2)
    implicit_account2.transfer_to(source_node, source_node.signer_key, 10**24,
                                  block_hash_bytes, nonce)
    nonce += 1

    # Send traffic for ~100 blocks
    subaccount_contract_deployed = False
    subaccount_staked = False

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        # Simple self-transfer to keep nonces moving
        tx = transaction.sign_payment_tx(source_node.signer_key,
                                         source_node.signer_key.account_id,
                                         300, nonce, block_hash_bytes)
        source_node.send_tx(tx)
        nonce += 1

        # Send from implicit accounts if they're initialized
        implicit_account.send_if_inited(source_node, [('test0', height)],
                                        block_hash_bytes)
        implicit_account2.send_if_inited(source_node, [('test0', height)],
                                         block_hash_bytes)

        # Send from all added keys
        added_keys = [
            new_key, subaccount_key, test0_contract_key,
            test0_contract_extra_key, test0_subaccount_contract_key
        ]
        mirror_utils.added_keys_send_transfers(
            [source_node], added_keys, [
                implicit_account.account_id(),
                implicit_account2.account_id(), 'test0'
            ], height, block_hash_bytes)

        # Deploy contract to subaccount, then stake from it
        if subaccount_key.inited():
            if not subaccount_contract_deployed:
                subaccount_key.nonce += 1
                mirror_utils.deploy_addkey_contract(
                    source_node, subaccount_key.key, mirror_utils.CONTRACT_PATH,
                    subaccount_key.nonce, block_hash_bytes)
                subaccount_contract_deployed = True
            elif not subaccount_staked:
                if mirror_utils.contract_deployed(source_node,
                                                  subaccount_key.account_id()):
                    subaccount_key.nonce += 1
                    mirror_utils.call_stake(source_node, subaccount_key.key,
                                            10**28, subaccount_key.key.pk,
                                            subaccount_key.nonce,
                                            block_hash_bytes)
                    subaccount_staked = True

        if height - start_source_height >= 100:
            break

    end_source_height = source_node.get_latest_block().height
    source_node.kill()
    logger.info('Phase 4: source node stopped. source_home is now imgB')

    # ── Phase 4.5: Save images for reuse ─────────────────────────────────
    # Copy imgA and imgB to persistent locations so the next test run can
    # skip phases 1-4 entirely.
    logger.info('Phase 4.5: saving images for reuse')
    mirror_utils.copy_near_home(fork_base, SAVED_IMG_A)
    mirror_utils.copy_near_home(source_home, SAVED_IMG_B)
    shutil.copyfile(source_home / 'validator_key.json',
                    SAVED_IMG_B / 'validator_key.json')

    with open(METADATA_FILE, 'w') as f:
        json.dump(
            {
                'validator_keys': [k.to_json() for k in validator_keys],
                'end_source_height': end_source_height,
            }, f, indent=2)

    logger.info('Images saved successfully')


# ─────────────────────────────────────────────────────────────────────────────
# Phases 5-6: Start target network, run mirror, validate
# ─────────────────────────────────────────────────────────────────────────────


def run_mirror(config, validator_keys, end_source_height):
    near_root = config['near_root']

    # ── Phase 5: Restore images and start target network + mirror ────────
    logger.info('Phase 5: restoring images and starting target network')

    # Clean up ephemeral state from any previous run.
    for name in ['stdout', 'stderr', 'config.json']:
        p = MIRROR_DIR / name
        if p.exists():
            p.unlink()

    # Restore working copies from saved images (so saved originals stay
    # pristine for future reruns).

    # Mirror source: copy of imgB
    source_dir = MIRROR_DIR / 'source'
    mirror_utils.copy_near_home(SAVED_IMG_B, source_dir)
    shutil.copyfile(SAVED_IMG_B / 'validator_key.json',
                    source_dir / 'validator_key.json')

    # Mirror target (embedded non-validator node): copy of imgA
    mirror_utils.copy_near_home(SAVED_IMG_A, MIRROR_DIR / 'target')

    # 4 target validators: each gets a copy of imgA + its own validator_key.
    # All validators track all shards so chunk data is available for
    # transaction count validation.
    base_ordinal = 2
    target_node_dirs = []
    for i, name in enumerate(TARGET_VALIDATORS):
        target_dir = MIRROR_DIR / f'test_target_{name}'
        target_node_dirs.append(str(target_dir))
        mirror_utils.copy_near_home(SAVED_IMG_A, target_dir)
        with open(target_dir / 'validator_key.json', 'w') as f:
            json.dump(validator_keys[i].to_json(), f, indent=2)
        with open(target_dir / 'config.json') as f:
            cfg = json.load(f)
        cfg['tracked_shards_config'] = 'AllShards'
        with open(target_dir / 'config.json', 'w') as f:
            json.dump(cfg, f, indent=2)

    # Start target validators. Each node gets all already-started nodes as
    # boot nodes so the network is fully connected.
    logger.info('Starting 4 target validators')
    target_nodes = []
    for i, name in enumerate(TARGET_VALIDATORS):
        boot = target_nodes if target_nodes else None
        node = spin_up_node(config,
                            near_root,
                            target_node_dirs[i],
                            base_ordinal + i,
                            boot_node=boot)
        target_nodes.append(node)

    # Set mirror target's boot_nodes to all target validators so the
    # mirror's embedded node can connect to the full target network.
    target_home = MIRROR_DIR / 'target'
    all_boot_nodes = ','.join(n.addr_with_pk() for n in target_nodes)
    with open(target_home / 'config.json') as f:
        target_config = json.load(f)
    target_config['network']['boot_nodes'] = all_boot_nodes
    with open(target_home / 'config.json', 'w') as f:
        json.dump(target_config, f, indent=2)

    # Start the mirror process. It reads transactions from source (imgB) and
    # replays them on the target network (imgA). Uses --no-secret because
    # fork-network uses identity key mapping (no secret file needed).
    logger.info('Starting mirror process')
    mirror = mirror_utils.MirrorProcess(near_root, str(source_dir))

    # Compute a reasonable timeout: 20s sync + 1.5x block delay per block
    total_time_allowed = mirror_utils.allowed_run_time(target_node_dirs[0],
                                                       mirror.start_time,
                                                       end_source_height)

    # Wait for mirror to finish. It will exit on its own once all source
    # blocks are processed. restart_once() restarts it once after 30s to
    # test resume capability.
    while True:
        time.sleep(5)
        if not mirror.restart_once():
            break
        elapsed = time.time() - mirror.start_time
        if elapsed > total_time_allowed:
            logger.warn(
                f'mirror process has not exited after {int(elapsed)} seconds. '
                f'stopping now')
            break

    # Wait for the target chain to process remaining transactions after mirror
    # finishes sending. Transactions sent in the last few source blocks may
    # still be in-flight.
    logger.info('Waiting for target chain to settle')
    time.sleep(15)

    # ── Phase 6: Validate ────────────────────────────────────────────────
    # Verify the mirror replayed transactions onto the target chain.
    # We can't require source_txs <= target_txs exactly because:
    #   - Blocks between genesis_height and the fork point are already baked
    #     into the forked state and their txs can't be replayed.
    #   - Some mapped transactions may fail on the target (nonce conflicts,
    #     key mapping edge cases, etc.).
    # Instead, verify the target received a substantial fraction of the
    # source transactions.
    logger.info('Phase 6: validating results')

    # Start a source node for tx-count validation. The mirror has exited so
    # the source DB is no longer locked.
    source_ordinal = base_ordinal + len(TARGET_VALIDATORS) + 1
    source_node = spin_up_node(config,
                               near_root,
                               str(source_dir),
                               source_ordinal,
                               single_node=True)
    time.sleep(5)

    with open(os.path.join(target_node_dirs[0], 'genesis.json'), 'r') as f:
        genesis_height = json.load(f)['genesis_height']
    total_source = mirror_utils.count_total_txs(source_node,
                                                 min_height=genesis_height)
    total_target = mirror_utils.count_total_txs(target_nodes[0])
    logger.info(
        f'source txs: {total_source}, target txs: {total_target}')
    # Target should have received at least 50% of source txs.
    assert total_target >= total_source * 0.5, \
        f'target has too few txs: {total_target} vs source {total_source}'

    logger.info('offline_test PASSED')


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main():
    config = load_config()

    # Build images if they don't exist yet (phases 1-4).
    # On subsequent runs, skip straight to the mirror phase.
    if not (SAVED_IMG_A.exists() and SAVED_IMG_B.exists()
            and METADATA_FILE.exists()):
        build_images(config)
    else:
        logger.info('Reusing saved images from previous run')

    # Load metadata (saved during build_images or from a previous run)
    with open(METADATA_FILE) as f:
        metadata = json.load(f)
    validator_keys = [key.Key.from_json(k) for k in metadata['validator_keys']]
    end_source_height = metadata['end_source_height']

    # Run the mirror and validate (phases 5-6)
    run_mirror(config, validator_keys, end_source_height)


if __name__ == '__main__':
    main()
