#!/usr/bin/env python3

import sys, time, base58, random
import atexit
import base58
import json
import os
import pathlib
import shutil
import signal
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from mocknet import create_genesis_file
import transaction
import utils
import key

# This sets up an environment to test the tools/mirror process. It starts a localnet with a few validators
# and waits for some blocks to be produced. Then we fork the state and start a new chain from that, and
# start the mirror process that should mirror transactions from the source chain to the target chain.
# Halfway through we restart it to make sure that it still works properly when restarted

TIMEOUT = 240
NUM_VALIDATORS = 4
SHARDNET_VALIDATORS = ['foo0', 'foo1', 'foo2']


def mkdir_clean(dirname):
    try:
        os.mkdir(dirname)
    except FileExistsError:
        shutil.rmtree(dirname)
        os.mkdir(dirname)


def dot_near():
    return pathlib.Path.home() / '.near'


def ordinal_to_port(port, ordinal):
    return f'0.0.0.0:{port + 10 + ordinal}'


def init_shardnet_dir(neard, home, ordinal, validator_account=None):
    mkdir_clean(home)

    try:
        subprocess.check_output([neard, '--home', home, 'init'],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"neard init" command failed: output: {e.stdout}')
    shutil.copy(dot_near() / 'test0/config.json', home / 'config.json')
    shutil.copy(dot_near() / 'test0/forked/genesis.json', home / 'genesis.json')
    shutil.copy(dot_near() / 'test0/forked/records.json', home / 'records.json')

    with open(home / 'config.json', 'r') as f:
        config = json.load(f)
        config['genesis_records_file'] = 'records.json'
        config['network']['addr'] = ordinal_to_port(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_port(3030, ordinal)
    with open(home / 'config.json', 'w') as f:
        json.dump(config, f)

    if validator_account is None:
        os.remove(home / 'validator_key.json')
    else:
        # this key and the suffix -load-test.near are hardcoded in create_genesis_file()
        with open(home / 'validator_key.json', 'w') as f:
            f.write('{\n')
            f.write(
                f'"account_id": "{validator_account + "-load-test.near"}",\n')
            f.write(
                '"public_key": "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN",\n'
            )
            f.write(
                '"secret_key": "ed25519:3cCk8KUWBySGCxBcn1syMoY5u73wx5eaPLRbQcMi23LwBA3aLsqEbA33Ww1bsJaFrchmDciGe9otdn45SrDSkow2"\n'
            )
            f.write('}\n')


def init_shardnet_dirs(neard):
    ordinal = NUM_VALIDATORS + 1
    dirs = []

    for account_id in SHARDNET_VALIDATORS:
        home = dot_near() / f'shardnet_{account_id}'
        dirs.append(str(home))
        init_shardnet_dir(neard, home, ordinal, validator_account=account_id)
        ordinal += 1

    observer = dot_near() / 'mirror/target'
    init_shardnet_dir(neard, observer, ordinal, validator_account=None)
    shutil.copy(dot_near() / 'test0/output/mirror-secret.json',
                observer / 'mirror-secret.json')
    return dirs, observer


def create_shardnet(config, near_root):
    binary_name = config.get('binary_name', 'neard')
    neard = os.path.join(near_root, binary_name)
    try:
        subprocess.check_output([
            neard, "--home",
            dot_near() / 'test0', "view-state", "dump-state", "--stream"
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"dump-state" command failed: output: {e.stdout}')
    try:
        subprocess.check_output([
            os.path.join(near_root, 'mirror'),
            'prepare',
            '--records-file-in',
            dot_near() / 'test0/output/records.json',
            '--records-file-out',
            dot_near() / 'test0/output/mirror-records.json',
            '--secret-file-out',
            dot_near() / 'test0/output/mirror-secret.json',
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"mirror prepare" command failed: output: {e.stdout}')

    os.mkdir(dot_near() / 'test0/forked')
    genesis_filename_in = dot_near() / 'test0/output/genesis.json'
    genesis_filename_out = dot_near() / 'test0/forked/genesis.json'
    records_filename_in = dot_near() / 'test0/output/mirror-records.json'
    records_filename_out = dot_near() / 'test0/forked/records.json'
    create_genesis_file(SHARDNET_VALIDATORS,
                        genesis_filename_in=genesis_filename_in,
                        genesis_filename_out=genesis_filename_out,
                        records_filename_in=records_filename_in,
                        records_filename_out=records_filename_out,
                        rpc_node_names=[],
                        chain_id='shardnet',
                        append=True,
                        epoch_length=20,
                        node_pks=None,
                        increasing_stakes=0.0,
                        num_seats=len(SHARDNET_VALIDATORS))
    return init_shardnet_dirs(neard)


def init_mirror_dir(home, source_boot_node):
    mkdir_clean(dot_near() / 'mirror')
    os.rename(home, dot_near() / 'mirror/source')
    ordinal = NUM_VALIDATORS
    with open(dot_near() / 'mirror/source/config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = source_boot_node.addr_with_pk()
        config['network']['addr'] = ordinal_to_port(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_port(3030, ordinal)
    with open(dot_near() / 'mirror/source/config.json', 'w') as f:
        json.dump(config, f)


def mirror_cleanup(process):
    process.send_signal(signal.SIGINT)
    try:
        process.wait(5)
    except:
        logger.error('can\'t kill mirror process')


def start_mirror(near_root, target_home, boot_node):
    env = os.environ.copy()
    env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
        "RUST_LOG", "debug")
    with open(dot_near() / 'mirror/stdout', 'ab') as stdout, \
        open(dot_near() / 'mirror/stderr', 'ab') as stderr:
        process = subprocess.Popen([
            os.path.join(near_root, 'mirror'), 'run', "--source-home",
            dot_near() / 'mirror/source/', "--target-home", target_home,
            '--secret-file', target_home / 'mirror-secret.json'
        ],
                                   stdin=subprocess.DEVNULL,
                                   stdout=stdout,
                                   stderr=stderr,
                                   env=env)
    logger.info("Started mirror process")
    atexit.register(mirror_cleanup, process)
    with open(target_home / 'config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = boot_node.addr_with_pk()
    with open(target_home / 'config.json', 'w') as f:
        json.dump(config, f)
    return process


# we'll test out adding an access key and then sending txs signed with it
# since that hits some codepaths we want to test
def send_add_access_key(node, creator_key, nonce, block_hash):
    k = key.Key.from_random('test0')
    action = transaction.create_full_access_key_action(k.decoded_pk())
    tx = transaction.sign_and_serialize_transaction(
        'test0', nonce, [action], base58.b58decode(block_hash.encode('utf8')),
        'test0', creator_key.decoded_pk(), creator_key.decoded_sk())
    node.send_tx(tx)
    return k


def main():
    config_changes = {}
    for i in range(NUM_VALIDATORS + 1):
        config_changes[i] = {"tracked_shards": [0, 1, 2, 3], "archive": True}

    config = load_config()
    near_root, node_dirs = init_cluster(
        num_nodes=NUM_VALIDATORS,
        num_observers=1,
        num_shards=4,
        config=config,
        genesis_config_changes=[["min_gas_price", 0],
                                ["max_inflation_rate", [0, 1]],
                                ["epoch_length", 10],
                                ["block_producer_kickout_threshold", 70],
                                ["protocol_version", 29]],
        client_config_changes=config_changes)

    nodes = [spin_up_node(config, near_root, node_dirs[0], 0)]

    init_mirror_dir(node_dirs[NUM_VALIDATORS], nodes[0])

    for i in range(1, NUM_VALIDATORS):
        nodes.append(
            spin_up_node(config, near_root, node_dirs[i], i,
                         boot_node=nodes[0]))

    ctx = utils.TxContext([0, 0, 0, 0], nodes)

    for height, block_hash in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
        ctx.send_moar_txs(block_hash, 10, use_routing=False)
        if height > 12:
            break

    nodes[0].kill()
    shardnet_node_dirs, shardnet_observer_dir = create_shardnet(
        config, near_root)
    nodes[0].start(boot_node=nodes[1])

    ordinal = NUM_VALIDATORS + 1
    shardnet_nodes = [
        spin_up_node(config, near_root, shardnet_node_dirs[0], ordinal)
    ]
    for i in range(1, len(shardnet_node_dirs)):
        ordinal += 1
        shardnet_nodes.append(
            spin_up_node(config,
                         near_root,
                         shardnet_node_dirs[i],
                         ordinal,
                         boot_node=shardnet_nodes[0]))

    p = start_mirror(near_root, shardnet_observer_dir, shardnet_nodes[0])

    restarted = False
    new_key = None
    new_key_nonce = -1

    for height, block_hash in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
        ctx.send_moar_txs(block_hash, 10, use_routing=False)
        code = p.poll()
        if code is not None:
            if code != 0:
                logger.error('mirror process exited with an error!')
            break

        if new_key is not None:
            if new_key_nonce == -1:
                nonce = nodes[0].get_nonce_for_pk('test0', new_key.pk)
                if nonce is not None:
                    new_key_nonce = nonce
            else:
                tx = transaction.sign_payment_tx(
                    new_key, 'test1', 100, new_key_nonce,
                    base58.b58decode(block_hash.encode('utf8')))
                nodes[0].send_tx(tx)
                new_key_nonce += 1
        elif height >= 50:
            new_key = send_add_access_key(nodes[0], nodes[0].signer_key,
                                          ctx.next_nonce, block_hash)
            ctx.next_nonce += 1

        if not restarted and height >= 90:
            logger.info('stopping mirror process')
            p.terminate()
            p.wait()
            with open(dot_near() / 'mirror/stderr', 'ab') as stderr:
                stderr.write(
                    b'<><><><><><><><><><><><> restarting <><><><><><><><><><><><><><><><><><><><>\n'
                )
                stderr.write(
                    b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
                )
                stderr.write(
                    b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
                )
            p = start_mirror(near_root, shardnet_observer_dir,
                             shardnet_nodes[0])
            restarted = True

        if height >= 140:
            break


if __name__ == '__main__':
    main()
