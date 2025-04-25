# Checks that creating a forked network with the neard fork-network commands works

# cspell:words subaccounts

import base58
import os
import json
import pathlib
import shutil
import subprocess
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import load_config, spin_up_node, start_cluster
from configured_logger import logger
import transaction
import key
import utils

import mirror_utils


# Create a bunch of subaccounts spanning 'a' to 'z'
def create_subaccounts(node, signer_key):
    height, block_hash = node.get_latest_block()
    block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))
    sub_keys = []
    for c in range(ord('a'), ord('z')):
        subaccount = chr(c) + 'aa'

        nonce = signer_key.get_nonce(node)
        assert nonce is not None
        sub_key = mirror_utils.create_subaccount(node, subaccount,
                                                 signer_key.key, nonce,
                                                 block_hash_bytes)
        sub_keys.append(mirror_utils.AddedKey(sub_key))
    return sub_keys


# Just to get some more test coverage, we add an extra key to each one
def add_extra_keys(node, implicit_accounts):
    keys = [key.Key.from_random(a.account_id()) for a in implicit_accounts]
    add_key_sent = [False] * len(implicit_accounts)
    key_found = [False] * len(implicit_accounts)

    for height, block_hash in utils.poll_blocks(node, timeout=200):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        for i, account in enumerate(implicit_accounts):
            if not add_key_sent[i]:
                nonce = account.get_nonce(node)
                if nonce is None:
                    continue
                mirror_utils.send_add_access_key(node, account.signer_key(),
                                                 keys[i], nonce,
                                                 block_hash_bytes)
                add_key_sent[i] = True
            elif not key_found[i]:
                nonce = node.get_nonce_for_pk(account.account_id(), keys[i].pk)
                if nonce is not None:
                    logger.info(
                        f'implicit account {account.account_id()} extra key inited'
                    )
                    key_found[i] = True

        if all(key_found):
            break


# Create some implicit accounts to test that part of the fork-network logic
def create_implicit_accounts(node, signer_key):
    height, block_hash = node.get_latest_block()
    block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

    implicit_accounts = []
    for i in range(30):
        a = mirror_utils.ImplicitAccount()
        nonce = signer_key.get_nonce(node)
        assert nonce is not None
        a.transfer_to(node, signer_key.key, 10**24, block_hash_bytes, nonce)

        implicit_accounts.append(a)

    add_extra_keys(node, implicit_accounts)
    return implicit_accounts


def send_txs(node, keys, num_blocks=15):
    # Here we are actually sending quite a lot so we check what happens with delayed receipts
    transfers = [(a.account_id(), 100) for a in keys]

    start_height = None
    for height, block_hash in utils.poll_blocks(node, timeout=200):
        if start_height is None:
            start_height = height

        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        print(f'send txs at {height}')
        for account in keys:
            account.send_if_inited(node, transfers, block_hash_bytes)

        if height - start_height > num_blocks:
            break


def run_cmd(cmd):
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(
            f'running `{" ".join([str(a) for a in cmd])}` returned {e.returncode}. output:\n{e.output.decode("utf-8")}'
        )


def copy_near_home(source_home_dir, target_home_dir):
    try:
        shutil.rmtree(target_home_dir)
    except FileNotFoundError:
        pass
    os.mkdir(target_home_dir)

    with open(source_home_dir / 'config.json') as source_f, \
        open(target_home_dir / 'config.json', 'w') as target_f:
        config = json.load(source_f)
        config['network']['boot_nodes'] = ''
        json.dump(config, target_f, indent=2)

    with open(target_home_dir / 'node_key.json', 'w') as f:
        node_key = key.Key.from_random('node')
        json.dump(node_key.to_json(), f, indent=2)

    shutil.copyfile(source_home_dir / 'genesis.json',
                    target_home_dir / 'genesis.json')
    shutil.copytree(source_home_dir / 'data', target_home_dir / 'data')


FORK_NET_EPOCH_LENGTH = 10


def fork_network(node_dir, neard_path, validator_key):
    logger.info(f'running fork-network commands on {node_dir}')
    run_cmd([
        neard_path,
        '--home',
        node_dir,
        'fork-network',
        'init',
    ])
    run_cmd([
        neard_path,
        '--home',
        node_dir,
        'fork-network',
        'amend-access-keys',
    ])

    os.mkdir(node_dir / 'setup')
    validators_file = node_dir / 'setup' / 'validators.json'
    validators = [{
        'account_id': validator_key.account_id,
        'public_key': validator_key.pk,
        'amount': str(10**33),
    }]
    with open(validators_file, 'w') as f:
        json.dump(validators, f, indent=2)

    run_cmd([
        neard_path,
        '--home',
        node_dir,
        'fork-network',
        'set-validators',
        '--validators',
        validators_file,
        '--chain-id-suffix',
        'local-forknet',
        '--epoch-length',
        str(FORK_NET_EPOCH_LENGTH),
    ])
    run_cmd([
        neard_path,
        '--home',
        node_dir,
        'fork-network',
        'finalize',
    ])


def run_forked_network(fork_dir, config, validator_key, num_original_nodes):
    home = pathlib.Path(fork_dir).parent
    validator_home = home / 'test_fork_validator'
    rpc_home = home / 'test_fork_rpc'

    copy_near_home(fork_dir, validator_home)
    with open(validator_home / 'validator_key.json', 'w') as f:
        json.dump(validator_key.to_json(), f, indent=2)

    copy_near_home(fork_dir, rpc_home)
    with open(rpc_home / 'validator_key.json', 'w') as f:
        # We don't really need it to have a validator key, but cluster.py expects one to be there
        rpc_key = key.Key.from_random('fork-rpc0')
        json.dump(rpc_key.to_json(), f, indent=2)

    # We start at a higher ordinal than the original nodes to start them with different ports
    node_idx = num_original_nodes + 1

    validator = spin_up_node(config, config['near_root'], str(validator_home),
                             node_idx)

    node_idx += 1
    rpc = spin_up_node(config,
                       config['near_root'],
                       str(rpc_home),
                       node_idx,
                       boot_node=validator)

    # TODO: start a mirror binary and check that traffic is sent
    # For now we just check that the nodes start and a couple epochs pass
    num_epochs = 0
    for height in utils.poll_epochs(rpc, epoch_length=FORK_NET_EPOCH_LENGTH):
        print(f'fork epoch {height}')
        num_epochs += 1
        if num_epochs > 2:
            break


def main():
    config = load_config()
    nodes = start_cluster(num_nodes=1,
                          num_observers=2,
                          num_shards=6,
                          config=config,
                          genesis_config_changes=[["epoch_length", 100]],
                          client_config_changes={
                              1: {
                                  "tracked_shards_config": "AllShards"
                              },
                              2: {
                                  "tracked_shards_config": "AllShards"
                              },
                          })

    # Put it in an AddedKey() just for nonce handling convenience
    signer_key = mirror_utils.AddedKey(nodes[0].signer_key)

    subaccounts = create_subaccounts(nodes[0], signer_key)

    implicit_accounts = create_implicit_accounts(nodes[0], signer_key)

    send_txs(nodes[0], implicit_accounts + subaccounts)

    # nodes[2] will be the node we use fork-network on, and nodes[1] will have extra transactions
    # past that point that will be used for mirroring transactions
    nodes[2].kill()
    send_txs(nodes[0], implicit_accounts)
    nodes[0].kill()
    nodes[1].kill()

    neard_path = pathlib.Path(config['near_root']) / pathlib.Path(
        config['binary_name'])

    fork_dir = pathlib.Path(nodes[2].node_dir)
    fork_validator_key = key.Key.from_random('fork-validator0')
    fork_network(fork_dir, neard_path, fork_validator_key)

    run_forked_network(fork_dir, config, fork_validator_key, len(nodes))


if __name__ == '__main__':
    main()
